package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

func retryUnstartedProcessing(ctx context.Context, transaction Transaction, jobID core.JobID) (core.RetryOutcome, error) {
	processing, outcome, err := eligibleProcessingRetry(ctx, transaction, jobID)
	if err != nil || outcome != "" {
		return outcome, err
	}
	if serializationErr := serializeProcessingRetry(ctx, transaction, processing.Job.Source); serializationErr != nil {
		return "", serializationErr
	}
	requeued, err := enqueueProcessingRetry(ctx, transaction, jobID)
	if err != nil {
		return "", err
	}
	if !requeued {
		return classifyProcessingRetryFailure(ctx, transaction, jobID)
	}
	if err = removeRetriedProcessingOwnership(ctx, transaction, jobID); err != nil {
		return "", err
	}
	return core.RetryRequeued, nil
}

func eligibleProcessingRetry(
	ctx context.Context,
	transaction Transaction,
	jobID core.JobID,
) (core.ProcessingJob, core.RetryOutcome, error) {
	processing, found, err := loadProcessingRetryCandidate(ctx, transaction, jobID)
	if err != nil || !found {
		return core.ProcessingJob{}, core.RetryMissing, err
	}
	if processing.SourceTerminal {
		return core.ProcessingJob{}, core.RetrySourceTerminal, nil
	}
	if processing.RunnerStarted {
		return core.ProcessingJob{}, core.RetryRunnerStarted, nil
	}
	return processing, "", nil
}

func removeRetriedProcessingOwnership(ctx context.Context, transaction Transaction, jobID core.JobID) error {
	removal, removalErr := transaction.Execute(ctx, deleteRetriedProcessingSQL, jobID)
	if removalErr != nil {
		return fmt.Errorf("remove retried processing ownership: %w", removalErr)
	}
	removed, removalErr := removal.RowsAffected()
	if removalErr != nil {
		return fmt.Errorf("read retried processing removal count: %w", removalErr)
	}
	if removed != 1 {
		return fmt.Errorf("remove retried processing ownership: expected one row, got %d", removed)
	}
	return nil
}

func classifyProcessingRetryFailure(
	ctx context.Context,
	transaction Transaction,
	jobID core.JobID,
) (core.RetryOutcome, error) {
	processing, found, err := loadProcessingRetryCandidate(ctx, transaction, jobID)
	if err != nil {
		return "", err
	}
	if !found {
		return core.RetryMissing, nil
	}
	if processing.SourceTerminal {
		return core.RetrySourceTerminal, nil
	}
	if processing.RunnerStarted {
		return core.RetryRunnerStarted, nil
	}
	return core.RetryClaimChanged, nil
}

func loadProcessingRetryCandidate(
	ctx context.Context,
	transaction Transaction,
	jobID core.JobID,
) (core.ProcessingJob, bool, error) {
	processing, err := scanProcessingJob(transaction.QueryRow(ctx, processingByIDSQL, jobID))
	if errors.Is(err, sql.ErrNoRows) {
		return core.ProcessingJob{}, false, nil
	}
	if err != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("load processing retry candidate: %w", err)
	}
	return processing, true, nil
}

func serializeProcessingRetry(ctx context.Context, transaction Transaction, source core.SourceReference) error {
	if _, err := transaction.Execute(ctx, serializeSourceSQL, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("serialize processing retry: %w", err)
	}
	return nil
}

func enqueueProcessingRetry(ctx context.Context, transaction Transaction, jobID core.JobID) (bool, error) {
	_, err := scanQueuedJob(transaction.QueryRow(ctx, retryProcessingSQL, jobID))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("return processing job to queue: %w", err)
	}
	return true, nil
}
