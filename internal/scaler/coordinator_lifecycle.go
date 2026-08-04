package scaler

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (coordinator *Coordinator) ConfigureRunnerLifecycle(
	reconciler *Reconciler,
	repository string,
	provider RunnerProvider,
) error {
	store, supported := coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return errors.New("demand store does not support runner lifecycle")
	}
	reconciler.ScaleSet.Repository = repository
	reconciler.ScaleSet.ProviderKind = provider
	reconciler.ScaleSet.LifecycleScaleSet = coordinator.scaleSet
	reconciler.ScaleSet.Lifecycle = store
	return nil
}

func (coordinator *Coordinator) HasRunnerLifecycle() bool {
	_, supported := coordinator.store.(RunnerLifecycleStore)
	return supported
}

func (coordinator *Coordinator) MarkRunnerBusy(
	ctx context.Context,
	repository string,
	identity RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	store, supported := coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return errors.New("demand store does not support runner lifecycle")
	}
	if err := store.MarkRunnerBusy(ctx, coordinator.scaleSet, repository, identity, githubRunnerID, startedAt); err != nil {
		return fmt.Errorf("mark runner busy: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) BindRunnerIdentity(
	ctx context.Context,
	repository string,
	identity RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	store, supported := coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return errors.New("demand store does not support runner lifecycle")
	}
	if err := store.BindRunnerIdentity(
		ctx, coordinator.scaleSet, repository, identity, githubRunnerID, startedAt,
	); err != nil {
		return fmt.Errorf("bind runner identity: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) CompleteRunnerLifecycleJob(
	ctx context.Context,
	completion JobCompletion,
	completedAt time.Time,
) error {
	store, supported := coordinator.store.(RunnerJobCompletionStore)
	if !supported {
		return errors.New("demand store does not support atomic runner job completion")
	}
	completion.ScaleSet = coordinator.scaleSet
	if err := store.CompleteRunnerLifecycleJob(ctx, completion, completedAt); err != nil {
		return fmt.Errorf("complete runner lifecycle job: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) RequestExpiredRunnerReaps(
	ctx context.Context,
	repository string,
	jobStartTimeout time.Duration,
	now time.Time,
) (int, error) {
	store, supported := coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return 0, errors.New("demand store does not support runner lifecycle")
	}
	changed, err := store.RequestExpiredRunnerReaps(ctx, coordinator.scaleSet, repository, now.Add(-jobStartTimeout), now)
	if err != nil {
		return 0, fmt.Errorf("request expired runner reaps: %w", err)
	}
	return changed, nil
}

func (coordinator *Coordinator) PendingRunnerReaps(ctx context.Context, repository string) ([]RunnerLifecycle, error) {
	store, supported := coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return nil, errors.New("demand store does not support runner lifecycle")
	}
	pending, err := store.PendingRunnerReaps(ctx, coordinator.scaleSet, repository)
	if err != nil {
		return nil, fmt.Errorf("load pending runner reaps: %w", err)
	}
	return pending, nil
}

func (coordinator *Coordinator) ConfirmRunnerReaped(ctx context.Context, runner RunnerLifecycle) error {
	store, supported := coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return errors.New("demand store does not support runner lifecycle")
	}
	if err := store.ConfirmRunnerReaped(ctx, runner); err != nil {
		return fmt.Errorf("confirm runner reaped: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) RegisterLibvirtRunner(ctx context.Context, runner LibvirtRunnerLifecycle) error {
	store, supported := coordinator.store.(LibvirtLifecycleStore)
	if !supported {
		return errors.New("demand store does not support libvirt runner lifecycle")
	}
	runner.ScaleSet = coordinator.scaleSet
	if err := store.RegisterLibvirtRunner(ctx, runner); err != nil {
		return fmt.Errorf("register libvirt runner: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) CompleteJob(ctx context.Context, completion JobCompletion) error {
	store, supported := coordinator.store.(LibvirtLifecycleStore)
	if !supported {
		return errors.New("demand store does not support libvirt runner lifecycle")
	}
	completion.ScaleSet = coordinator.scaleSet
	if err := store.CompleteJob(ctx, completion); err != nil {
		return fmt.Errorf("complete libvirt job: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) PendingLibvirtReaps(
	ctx context.Context,
	repository string,
) ([]LibvirtRunnerLifecycle, error) {
	store, supported := coordinator.store.(LibvirtLifecycleStore)
	if !supported {
		return nil, errors.New("demand store does not support libvirt runner lifecycle")
	}
	pending, err := store.PendingLibvirtReaps(ctx, coordinator.scaleSet, repository)
	if err != nil {
		return nil, fmt.Errorf("load pending libvirt reaps: %w", err)
	}
	return pending, nil
}

func (coordinator *Coordinator) ConfirmLibvirtReaped(ctx context.Context, runner LibvirtRunnerLifecycle) error {
	store, supported := coordinator.store.(LibvirtLifecycleStore)
	if !supported {
		return errors.New("demand store does not support libvirt runner lifecycle")
	}
	if err := store.ConfirmLibvirtReaped(ctx, runner); err != nil {
		return fmt.Errorf("confirm libvirt runner reaped: %w", err)
	}
	return nil
}
