package scaler

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ApplyMessageUpdate gives transient stores the same single-call semantics as
// durable backends. It is used by hermetic coordinator tests.
func (store *MemoryDemandStore) ApplyMessageUpdate(
	ctx context.Context,
	update MessageUpdate,
) (MessageUpdateResult, error) {
	candidate := store.cloneForMessageUpdate()
	err := applyMemoryMessageUpdate(ctx, candidate, update)
	if err != nil {
		return MessageUpdateResult{}, err
	}
	*store = *candidate
	return MessageUpdateResult{}, nil
}

func (store *MemoryDemandStore) cloneForMessageUpdate() *MemoryDemandStore {
	clone := &MemoryDemandStore{
		demand:          make(map[demandKey]QueuedDemand, len(store.demand)),
		libvirtRunners:  make(map[string]memoryLibvirtLifecycle, len(store.libvirtRunners)),
		runners:         make(map[string]RunnerLifecycle, len(store.runners)),
		nextObservation: store.nextObservation,
	}
	for key, demand := range store.demand {
		clone.demand[key] = demand
	}
	for key, runner := range store.libvirtRunners {
		clone.libvirtRunners[key] = runner
	}
	for key, runner := range store.runners {
		clone.runners[key] = runner
	}
	return clone
}

type memoryMessageUpdateState interface {
	Apply(ctx context.Context, scaleSet string, observed []JobDemand, removed []DemandRemoval) error
	MarkRunnerBusy(
		ctx context.Context,
		scaleSet string,
		repository string,
		identity RunnerIdentity,
		githubRunnerID int64,
		startedAt time.Time,
	) error
	BindRunnerIdentity(
		ctx context.Context,
		scaleSet string,
		repository string,
		identity RunnerIdentity,
		githubRunnerID int64,
		startedAt time.Time,
	) error
	CompleteRunnerLifecycleJob(ctx context.Context, completion JobCompletion, completedAt time.Time) error
	CompleteJob(ctx context.Context, completion JobCompletion) error
}

func applyMemoryMessageUpdate(
	ctx context.Context,
	store memoryMessageUpdateState,
	update MessageUpdate,
) error {
	removals := messageDemandRemovals(update)
	if err := store.Apply(ctx, update.ScaleSet, update.Available, removals); err != nil {
		return fmt.Errorf("apply memory message demand: %w", err)
	}
	if err := applyMemoryStartedRunners(ctx, store, update); err != nil {
		return err
	}
	return applyMemoryCompletedRunners(ctx, store, update)
}

func applyMemoryStartedRunners(ctx context.Context, store memoryMessageUpdateState, update MessageUpdate) error {
	for _, started := range update.Started {
		if err := applyMemoryStartedRunner(ctx, store, update, started); err != nil {
			return err
		}
	}
	return nil
}

func applyMemoryStartedRunner(
	ctx context.Context,
	store memoryMessageUpdateState,
	update MessageUpdate,
	started RunnerMessageIdentity,
) error {
	identity := started.RunnerIdentity
	var err error
	if identity.isPair() {
		err = store.BindRunnerIdentity(ctx, update.ScaleSet, update.Repository, identity,
			started.GitHubRunnerID, update.ObservedAt)
	} else {
		err = store.MarkRunnerBusy(ctx, update.ScaleSet, update.Repository, identity,
			started.GitHubRunnerID, update.ObservedAt)
	}
	return ignoreMemoryMessageNotFound(err, "mark message runner busy")
}

func applyMemoryCompletedRunners(ctx context.Context, store memoryMessageUpdateState, update MessageUpdate) error {
	for _, completed := range update.Completed {
		completion := JobCompletion{
			ScaleSet: update.ScaleSet, Repository: update.Repository,
			RunnerIdentity: completed.RunnerIdentity,
		}
		if err := ignoreMemoryMessageNotFound(
			store.CompleteRunnerLifecycleJob(ctx, completion, update.ObservedAt),
			"complete memory runner lifecycle",
		); err != nil {
			return err
		}
		if err := ignoreMemoryMessageNotFound(
			store.CompleteJob(ctx, completion), "complete memory compatibility lifecycle",
		); err != nil {
			return err
		}
	}
	return nil
}

func ignoreMemoryMessageNotFound(err error, operation string) error {
	if err == nil || errors.Is(err, ErrRunnerIdentityNotFound) {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}
