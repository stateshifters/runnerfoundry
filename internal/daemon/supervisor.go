package daemon

import (
	"context"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/config"
)

type generation interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type generationBuilder func(context.Context, config.Config) (generation, error)

func supervise(
	ctx context.Context,
	reloadRequests <-chan struct{},
	load func() (config.Config, error),
	build generationBuilder,
	notifier serviceNotifier,
	report func(error),
) (resultErr error) {
	active, err := startGeneration(ctx, load, build, "initial")
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, active.Shutdown(context.WithoutCancel(ctx)))
	}()
	if err = notifier.Ready(ctx); err != nil {
		return fmt.Errorf("notify runtime ready: %w", err)
	}

	superviseReloads(ctx, reloadRequests, load, build, notifier, report, &active)
	return nil
}

type managedGeneration struct{ generation }

func startGeneration(
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
	return &managedGeneration{generation: active}, nil
}

func superviseReloads(
	ctx context.Context,
	reloadRequests <-chan struct{},
	load func() (config.Config, error),
	build generationBuilder,
	notifier serviceNotifier,
	report func(error),
	active **managedGeneration,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-reloadRequests:
			if err := reloadGeneration(ctx, load, build, notifier, active); err != nil {
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
	active **managedGeneration,
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
	replacement, err := startGeneration(ctx, load, build, "replacement")
	if err != nil {
		return err
	}
	if err = replaceActiveGeneration(ctx, active, replacement); err != nil {
		return err
	}
	if err = notifier.Ready(ctx); err != nil {
		return fmt.Errorf("notify replacement ready: %w", err)
	}
	restored = true
	return nil
}

func replaceActiveGeneration(ctx context.Context, active **managedGeneration, replacement *managedGeneration) error {
	if err := (*active).Stop(context.WithoutCancel(ctx)); err != nil {
		return errors.Join(fmt.Errorf("stop replaced runtime: %w", err), replacement.Stop(context.WithoutCancel(ctx)))
	}
	*active = replacement
	return nil
}
