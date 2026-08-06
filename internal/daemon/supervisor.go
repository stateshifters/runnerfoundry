package daemon

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
)

type generation interface {
	Prepare(ctx context.Context) error
	Activate(ctx context.Context) error
	Deactivate(ctx context.Context) error
	Close(ctx context.Context) error
	Finalizers(runnerPools []core.RunnerPool) []runtimeFinalizer
}

type generationBuilder func(context.Context, config.Config) (generation, error)

func affectedRunnerPools(active config.Config, replacement config.Config) ([]core.RunnerPool, error) {
	replacementProviders := make(map[core.RunnerPool]config.ProviderKind, len(replacement.ScaleSets))
	for _, scaleSet := range replacement.ScaleSets {
		kind, err := scaleSet.ProviderKind()
		if err != nil {
			return nil, fmt.Errorf("derive replacement provider kind for Runner Pool %q: %w", scaleSet.Name, err)
		}
		replacementProviders[core.RunnerPool(scaleSet.Name)] = kind
	}
	affected := make([]core.RunnerPool, 0)
	for _, scaleSet := range active.ScaleSets {
		runnerPool := core.RunnerPool(scaleSet.Name)
		replacementProvider, exists := replacementProviders[runnerPool]
		activeProvider, err := scaleSet.ProviderKind()
		if err != nil {
			return nil, fmt.Errorf("derive active provider kind for Runner Pool %q: %w", scaleSet.Name, err)
		}
		if !exists || replacementProvider != activeProvider {
			affected = append(affected, runnerPool)
		}
	}
	sort.Slice(affected, func(left int, right int) bool { return affected[left] < affected[right] })
	return affected, nil
}

func supervise(
	ctx context.Context,
	reloadRequests <-chan struct{},
	load func() (config.Config, error),
	build generationBuilder,
	notifier serviceNotifier,
	report func(error),
	policy ActiveRunnerPolicy,
) (resultErr error) {
	active, err := startInitialGeneration(ctx, load, build)
	if err != nil {
		return err
	}
	handoff := &generationHandoff{policy: policy, active: active}
	defer func() {
		resultErr = errors.Join(resultErr, finalizeSupervisor(
			context.WithoutCancel(ctx), handoff.active, policy, handoff.retainedFinalizers,
		))
	}()
	if err = notifier.Ready(ctx); err != nil {
		return fmt.Errorf("notify runtime ready: %w", err)
	}

	superviseReloads(ctx, reloadRequests, load, build, notifier, report, handoff)
	return nil
}

type managedGeneration struct {
	generation
	configured config.Config
}

type generationHandoff struct {
	policy             ActiveRunnerPolicy
	active             *managedGeneration
	retainedFinalizers []runtimeFinalizer
}

func startInitialGeneration(
	ctx context.Context,
	load func() (config.Config, error),
	build generationBuilder,
) (*managedGeneration, error) {
	active, err := prepareGeneration(ctx, load, build, "initial")
	if err != nil {
		return nil, err
	}
	if err = active.Activate(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("activate initial runtime: %w", err), active.Close(context.WithoutCancel(ctx)))
	}
	return active, nil
}

func prepareGeneration(
	ctx context.Context,
	load func() (config.Config, error),
	build generationBuilder,
	phase string,
) (*managedGeneration, error) {
	configured, err := load()
	if err != nil {
		return nil, fmt.Errorf("load %s configuration: %w", phase, err)
	}
	active, err := build(ctx, configured)
	if err != nil {
		return nil, fmt.Errorf("build %s runtime: %w", phase, err)
	}
	if err = active.Prepare(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("prepare %s runtime: %w", phase, err), active.Close(context.WithoutCancel(ctx)))
	}
	return &managedGeneration{generation: active, configured: configured}, nil
}

func superviseReloads(
	ctx context.Context,
	reloadRequests <-chan struct{},
	load func() (config.Config, error),
	build generationBuilder,
	notifier serviceNotifier,
	report func(error),
	handoff *generationHandoff,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-reloadRequests:
			if err := reloadGeneration(ctx, load, build, notifier, handoff); err != nil {
				report(err)
			}
		}
	}
}

func reloadGeneration(
	ctx context.Context,
	load func() (config.Config, error),
	build generationBuilder,
	notifier serviceNotifier,
	handoff *generationHandoff,
) (resultErr error) {
	if err := notifier.Reloading(ctx); err != nil {
		return fmt.Errorf("notify reload: %w", err)
	}
	restored := false
	defer func() {
		if resultErr != nil && !restored {
			resultErr = errors.Join(resultErr, notifier.Ready(context.WithoutCancel(ctx)))
		}
	}()
	replacement, err := prepareGeneration(ctx, load, build, "replacement")
	if err != nil {
		return err
	}
	replaceErr := replaceActiveGeneration(ctx, handoff, replacement)
	readyErr := notifier.Ready(ctx)
	restored = true
	if readyErr != nil {
		readyErr = fmt.Errorf("notify replacement ready: %w", readyErr)
	}
	return errors.Join(replaceErr, readyErr)
}

func replaceActiveGeneration(
	ctx context.Context,
	handoff *generationHandoff,
	replacement *managedGeneration,
) error {
	previous := handoff.active
	if err := activateReplacement(ctx, previous, replacement); err != nil {
		return err
	}
	handoff.active = replacement
	terminateErr := terminateReplacedGeneration(ctx, handoff, previous, replacement)
	return errors.Join(terminateErr, previous.Close(context.WithoutCancel(ctx)))
}

func activateReplacement(ctx context.Context, previous, replacement *managedGeneration) error {
	if err := previous.Deactivate(context.WithoutCancel(ctx)); err != nil {
		return errors.Join(fmt.Errorf("deactivate replaced runtime: %w", err), replacement.Close(context.WithoutCancel(ctx)))
	}
	if err := replacement.Activate(ctx); err != nil {
		return errors.Join(fmt.Errorf("activate replacement runtime: %w", err),
			replacement.Close(context.WithoutCancel(ctx)), reactivatePrevious(ctx, previous))
	}
	return nil
}

func reactivatePrevious(ctx context.Context, previous *managedGeneration) error {
	if err := previous.Activate(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("reactivate previous runtime: %w", err)
	}
	return nil
}

func terminateReplacedGeneration(
	ctx context.Context,
	handoff *generationHandoff,
	previous, replacement *managedGeneration,
) error {
	if handoff.policy != TerminateActiveRunners {
		return nil
	}
	affected, err := affectedRunnerPools(previous.configured, replacement.configured)
	if err != nil {
		return err
	}
	finalizers := append([]runtimeFinalizer(nil), handoff.retainedFinalizers...)
	finalizers = append(finalizers, previous.Finalizers(affected)...)
	handoff.retainedFinalizers, err = terminateFinalizers(context.WithoutCancel(ctx), finalizers)
	return err
}

func finalizeSupervisor(
	ctx context.Context,
	active *managedGeneration,
	policy ActiveRunnerPolicy,
	retainedFinalizers []runtimeFinalizer,
) error {
	stopErr := active.Close(ctx)
	if policy != TerminateActiveRunners {
		return stopErr
	}
	finalizers := append([]runtimeFinalizer(nil), retainedFinalizers...)
	finalizers = append(finalizers, active.Finalizers(configuredRunnerPools(active.configured))...)
	_, terminateErr := terminateFinalizers(ctx, finalizers)
	return errors.Join(stopErr, terminateErr)
}

func terminateFinalizers(ctx context.Context, finalizers []runtimeFinalizer) ([]runtimeFinalizer, error) {
	remainingFinalizers := make([]runtimeFinalizer, 0, len(finalizers))
	var resultErr error
	for _, finalizer := range finalizers {
		remainingProcessing, err := finalizer.Terminate(ctx)
		if err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("terminate Runner Pool %q: %w", finalizer.RunnerPool(), err))
		}
		if err != nil || remainingProcessing > 0 {
			remainingFinalizers = append(remainingFinalizers, finalizer)
		}
	}
	return remainingFinalizers, resultErr
}

func configuredRunnerPools(configured config.Config) []core.RunnerPool {
	runnerPools := make([]core.RunnerPool, 0, len(configured.ScaleSets))
	for _, scaleSet := range configured.ScaleSets {
		runnerPools = append(runnerPools, core.RunnerPool(scaleSet.Name))
	}
	return runnerPools
}
