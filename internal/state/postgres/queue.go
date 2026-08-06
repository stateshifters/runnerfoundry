package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

func (store *Store) Queue(ctx context.Context, request core.JobRequest) (_ core.EnqueueResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.EnqueueResult{}, fmt.Errorf("begin enqueue job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if _, lockErr := transaction.Execute(ctx, serializeSourceSQL, request.Source.Adapter, request.Source.ID); lockErr != nil {
		return core.EnqueueResult{}, fmt.Errorf("serialize source enqueue: %w", lockErr)
	}
	job, outcome, err := enqueueInTransaction(ctx, transaction, request)
	if err != nil {
		return core.EnqueueResult{}, err
	}
	if err := transaction.Commit(ctx); err != nil {
		return core.EnqueueResult{}, fmt.Errorf("commit enqueue job: %w", err)
	}
	open = false
	return core.EnqueueResult{Job: job, Outcome: outcome}, nil
}

func enqueueInTransaction(ctx context.Context, transaction Transaction, request core.JobRequest) (core.Job, core.QueueOutcome, error) {
	job, err := scanQueuedJob(transaction.QueryRow(ctx, insertQueuedJobSQL,
		request.Source.Adapter, request.Source.ID, string(request.RunnerPool), request.EnqueuedAt))
	if err == nil {
		return job, core.QueueQueued, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return core.Job{}, "", fmt.Errorf("insert queued job: %w", err)
	}
	job, err = scanQueuedJob(transaction.QueryRow(ctx, lookupQueuedJobBySourceSQL,
		request.Source.Adapter, request.Source.ID))
	if errors.Is(err, sql.ErrNoRows) {
		var rawOutcome string
		if classifyErr := transaction.QueryRow(ctx, classifyQueueSourceSQL,
			request.Source.Adapter, request.Source.ID).Scan(&rawOutcome); classifyErr != nil {
			return core.Job{}, "", fmt.Errorf("classify existing source request: %w", classifyErr)
		}
		return core.Job{}, core.QueueOutcome(rawOutcome), nil
	}
	if err != nil {
		return core.Job{}, "", fmt.Errorf("load existing queued job: %w", err)
	}
	return job, core.QueueAlreadyQueued, nil
}
