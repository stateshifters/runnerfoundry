package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func (store *Store) Queue(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	if err := request.Validate(); err != nil {
		return core.EnqueueResult{}, fmt.Errorf("validate queued demand: %w", err)
	}
	var result core.EnqueueResult
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		var err error
		result, err = enqueueInTransaction(ctx, transaction, request)
		return err
	})
	if err != nil {
		return core.EnqueueResult{}, fmt.Errorf("enqueue job: %w", err)
	}
	return result, nil
}

func enqueueInTransaction(ctx context.Context, transaction Transaction, request core.JobRequest) (core.EnqueueResult, error) {
	rowsAffected, err := transaction.Execute(ctx, insertQueuedJobSQL,
		request.Source.Adapter, request.Source.ID, string(request.RunnerPool), request.EnqueuedAt.UnixNano(),
		request.Source.Adapter, request.Source.ID, request.Source.Adapter, request.Source.ID)
	if err != nil {
		return core.EnqueueResult{}, fmt.Errorf("insert queued job: %w", err)
	}
	job, err := readJob(ctx, transaction, queuedJobBySourceSQL, request.Source.Adapter, request.Source.ID)
	if errors.Is(err, sql.ErrNoRows) && rowsAffected == 0 {
		var rawOutcome string
		err = transaction.ReadRow(ctx, classifyQueueSourceSQL, func(row Scanner) error { return row.Scan(&rawOutcome) }, request.Source.Adapter, request.Source.ID,
			request.Source.Adapter, request.Source.ID)
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
