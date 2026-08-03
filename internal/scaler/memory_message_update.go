package scaler

import (
	"context"
	"database/sql"
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
	return applyMemoryMessageUpdate(ctx, store, update)
}

type memoryMessageUpdateState interface {
	Apply(ctx context.Context, scaleSet string, observed []JobDemand, removed []DemandRemoval) error
	MarkRunnerBusy(
		ctx context.Context,
		scaleSet string,
		repository string,
		requestID int64,
		runnerName string,
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
) (MessageUpdateResult, error) {
	removals := startedDemandRemovals(update)
	if err := store.Apply(ctx, update.ScaleSet, update.Available, removals); err != nil {
		return MessageUpdateResult{}, fmt.Errorf("apply memory message demand: %w", err)
	}
	for _, started := range update.Started {
		err := store.MarkRunnerBusy(ctx, update.ScaleSet, update.Repository, started.RequestID,
			started.RunnerName, started.GitHubRunnerID, update.ObservedAt)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return MessageUpdateResult{}, fmt.Errorf("mark message runner busy: %w", err)
		}
	}
	for _, completed := range update.Completed {
		completion := JobCompletion{
			ScaleSet: update.ScaleSet, Repository: update.Repository,
			RequestID: completed.RequestID, RunnerName: completed.RunnerName,
		}
		if err := store.CompleteRunnerLifecycleJob(ctx, completion, update.ObservedAt); err != nil {
			return MessageUpdateResult{}, fmt.Errorf("complete memory runner lifecycle: %w", err)
		}
		if err := store.CompleteJob(ctx, completion); err != nil {
			return MessageUpdateResult{}, fmt.Errorf("complete memory compatibility lifecycle: %w", err)
		}
	}
	return MessageUpdateResult{}, nil
}
