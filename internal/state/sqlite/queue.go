package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func (store *Store) Queue(ctx context.Context, request core.JobRequest) (_ core.EnqueueResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.EnqueueResult{}, fmt.Errorf("begin enqueue job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	result, err := queueInTransaction(ctx, transaction, request)
	if err != nil {
		return core.EnqueueResult{}, err
	}
	if err = transaction.Commit(ctx); err != nil {
		return core.EnqueueResult{}, fmt.Errorf("commit enqueue job: %w", err)
	}
	open = false
	return result, nil
}

func queueInTransaction(ctx context.Context, transaction Transaction, request core.JobRequest) (core.EnqueueResult, error) {
	result, err := transaction.Execute(ctx, insertQueuedJobSQL,
		request.Source.Adapter, request.Source.ID, string(request.RunnerPool), request.EnqueuedAt.UnixNano(),
		request.Source.Adapter, request.Source.ID, request.Source.Adapter, request.Source.ID)
	if err != nil {
		return core.EnqueueResult{}, fmt.Errorf("insert queued job: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return core.EnqueueResult{}, fmt.Errorf("read inserted job count: %w", err)
	}
	job, err := scanJob(transaction.QueryRow(ctx, queuedJobBySourceSQL, request.Source.Adapter, request.Source.ID))
	if errors.Is(err, sql.ErrNoRows) && rowsAffected == 0 {
		var rawOutcome string
		err = transaction.QueryRow(ctx, classifyQueueSourceSQL, request.Source.Adapter, request.Source.ID,
			request.Source.Adapter, request.Source.ID).Scan(&rawOutcome)
		if err != nil {
			return core.EnqueueResult{}, fmt.Errorf("classify existing source request: %w", err)
		}
		return core.EnqueueResult{Outcome: core.QueueOutcome(rawOutcome)}, nil
	}
	if err != nil {
		return core.EnqueueResult{}, fmt.Errorf("load enqueued job: %w", err)
	}
	outcome := core.QueueAlreadyQueued
	if rowsAffected == 1 {
		outcome = core.QueueQueued
	}
	return core.EnqueueResult{Job: job, Outcome: outcome}, nil
}
