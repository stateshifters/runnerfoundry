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
	Start(ctx context.Context) error
	Activate() error
	Stop(ctx context.Context) error
	Finalizers(runnerPools []core.RunnerPool) []runtimeFinalizer
}

type generationBuilder func(context.Context, config.Config) (generation, error)

func affectedRunnerPools(active config.Config, replacement config.Config) []core.RunnerPool {
	replacementProviders := make(map[core.RunnerPool]string, len(replacement.ScaleSets))
	for _, scaleSet := range replacement.ScaleSets {
		replacementProviders[core.RunnerPool(scaleSet.Name)] = runnerPoolProviderType(scaleSet)
	}
	affected := make([]core.RunnerPool, 0)
	for _, scaleSet := range active.ScaleSets {
		runnerPool := core.RunnerPool(scaleSet.Name)
		replacementProvider, exists := replacementProviders[runnerPool]
		if !exists || replacementProvider != runnerPoolProviderType(scaleSet) {
			affected = append(affected, runnerPool)
		}
	}
	sort.Slice(affected, func(left int, right int) bool { return affected[left] < affected[right] })
	return affected
}

func runnerPoolProviderType(scaleSet config.ScaleSetConfig) string {
	if scaleSet.Docker != nil {
		return "docker"
	}
	return "libvirt"
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
	if err = active.Activate(); err != nil {
		return nil, errors.Join(fmt.Errorf("activate initial runtime: %w", err), active.Stop(context.WithoutCancel(ctx)))
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
	if err = active.Start(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("start %s runtime: %w", phase, err), active.Stop(context.WithoutCancel(ctx)))
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
	if err := handoff.active.Stop(context.WithoutCancel(ctx)); err != nil {
		return errors.Join(fmt.Errorf("stop replaced runtime: %w", err), replacement.Stop(context.WithoutCancel(ctx)))
	}
	previous := handoff.active
	var terminateErr error
	if handoff.policy == TerminateActiveRunners {
		finalizers := append([]runtimeFinalizer(nil), handoff.retainedFinalizers...)
		finalizers = append(finalizers, previous.Finalizers(affectedRunnerPools(previous.configured, replacement.configured))...)
		handoff.retainedFinalizers, terminateErr = terminateFinalizers(context.WithoutCancel(ctx), finalizers)
	}
	handoff.active = replacement
	activateErr := replacement.Activate()
	if activateErr != nil {
		activateErr = fmt.Errorf("activate replacement runtime: %w", activateErr)
	}
	return errors.Join(terminateErr, activateErr)
}

func finalizeSupervisor(
	ctx context.Context,
	active *managedGeneration,
	policy ActiveRunnerPolicy,
	retainedFinalizers []runtimeFinalizer,
) error {
	stopErr := active.Stop(ctx)
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
