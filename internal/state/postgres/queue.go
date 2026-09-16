package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func (store *Store) Queue(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	if err := request.Validate(); err != nil {
		return core.EnqueueResult{}, fmt.Errorf("validate enqueue request: %w", err)
	}
	var result core.EnqueueResult
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		if lockErr := serializeEnqueue(ctx, transaction, request); lockErr != nil {
			return lockErr
		}
		job, outcome, operationErr := enqueueInTransaction(ctx, transaction, request)
		result = core.EnqueueResult{Job: job, Outcome: outcome}
		return operationErr
	})
	if err != nil {
		return core.EnqueueResult{}, fmt.Errorf("enqueue job: %w", err)
	}
	return result, nil
}

func serializeEnqueue(ctx context.Context, transaction Transaction, request core.JobRequest) error {
	if err := serializeDemandDomain(ctx, transaction, request.Source.Adapter, request.RunnerPool); err != nil {
		return fmt.Errorf("serialize enqueue demand domain: %w", err)
	}
	if request.Scope.IsValid() {
		if err := serializeDemandScope(ctx, transaction, request.Scope); err != nil {
			return fmt.Errorf("serialize demand-scope enqueue: %w", err)
		}
	}
	if err := serializeSource(ctx, transaction, request.Source); err != nil {
		return fmt.Errorf("serialize source enqueue: %w", err)
	}
	return nil
}

func enqueueInTransaction(ctx context.Context, transaction Transaction, request core.JobRequest) (core.Job, core.QueueOutcome, error) {
	job, err := readQueuedJob(ctx, transaction, insertQueuedJobSQL,
		request.Source.Adapter, request.Source.ID, string(request.RunnerPool), request.EnqueuedAt)
	if err == nil {
		return job, core.QueueQueued, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return core.Job{}, "", fmt.Errorf("insert queued job: %w", err)
	}
	job, err = readQueuedJob(ctx, transaction, lookupQueuedJobBySourceSQL,
		request.Source.Adapter, request.Source.ID)
	if errors.Is(err, sql.ErrNoRows) {
		var rawOutcome string
		if classifyErr := transaction.ReadRow(ctx, classifyQueueSourceSQL, func(row Scanner) error {
			return row.Scan(&rawOutcome)
		}, request.Source.Adapter, request.Source.ID); classifyErr != nil {
			return core.Job{}, "", fmt.Errorf("classify existing source request: %w", classifyErr)
		}
		return core.Job{}, core.QueueOutcome(rawOutcome), nil
	}
	if err != nil {
		return core.Job{}, "", fmt.Errorf("load existing queued job: %w", err)
	}
	return job, core.QueueAlreadyQueued, nil
}
