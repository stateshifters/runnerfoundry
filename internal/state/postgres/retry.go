package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

func retryUnstartedProcessing(
	ctx context.Context,
	transaction Transaction,
	expected core.ProcessingJob,
) (core.RetryOutcome, error) {
	processing, outcome, err := eligibleProcessingRetry(ctx, transaction, expected)
	if err != nil || outcome != "" {
		return outcome, err
	}
	if serializationErr := serializeProcessingRetry(ctx, transaction, processing.Job.Source); serializationErr != nil {
		return "", serializationErr
	}
	requeued, err := enqueueProcessingRetry(ctx, transaction, expected)
	if err != nil {
		return "", err
	}
	if !requeued {
		return classifyProcessingRetryFailure(ctx, transaction, expected)
	}
	if err = removeRetriedProcessingOwnership(ctx, transaction, expected); err != nil {
		return "", err
	}
	return core.RetryRequeued, nil
}

func eligibleProcessingRetry(
	ctx context.Context,
	transaction Transaction,
	expected core.ProcessingJob,
) (core.ProcessingJob, core.RetryOutcome, error) {
	processing, found, err := loadProcessingRetryCandidate(ctx, transaction, expected.Job.ID)
	if err != nil || !found {
		return core.ProcessingJob{}, core.RetryMissing, err
	}
	if outcome := state.ClassifyProcessingRetry(processing, expected); outcome != "" {
		return core.ProcessingJob{}, outcome, nil
	}
	return processing, "", nil
}

func removeRetriedProcessingOwnership(
	ctx context.Context,
	transaction Transaction,
	processing core.ProcessingJob,
) error {
	removed, removalErr := transaction.Execute(ctx, deleteRetriedProcessingSQL, processing.Job.ID,
		processing.ClaimedAt, processing.InventoryEndpointID, processing.RunnerName)
	if removalErr != nil {
		return fmt.Errorf("remove retried processing ownership: %w", removalErr)
	}
	if removed != 1 {
		return fmt.Errorf("remove retried processing ownership: expected one row, got %d", removed)
	}
	return nil
}

func classifyProcessingRetryFailure(
	ctx context.Context,
	transaction Transaction,
	expected core.ProcessingJob,
) (core.RetryOutcome, error) {
	processing, found, err := loadProcessingRetryCandidate(ctx, transaction, expected.Job.ID)
	if err != nil {
		return "", err
	}
	if !found {
		return core.RetryMissing, nil
	}
	if outcome := state.ClassifyProcessingRetry(processing, expected); outcome != "" {
		return outcome, nil
	}
	return core.RetryClaimChanged, nil
}

func loadProcessingRetryCandidate(
	ctx context.Context,
	transaction Transaction,
	jobID core.JobID,
) (core.ProcessingJob, bool, error) {
	processing, err := readClaimedProcessingJob(ctx, transaction, processingByClaimIDSQL, jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return core.ProcessingJob{}, false, nil
	}
	if err != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("load processing retry candidate: %w", err)
	}
	return processing, true, nil
}

func serializeProcessingRetry(ctx context.Context, transaction Transaction, source core.SourceReference) error {
	if _, err := transaction.Execute(ctx, serializeSourceSQL, advisoryLockIdentity(
		string(source.Adapter), string(source.ID))); err != nil {
		return fmt.Errorf("serialize processing retry: %w", err)
	}
	return nil
}

func enqueueProcessingRetry(
	ctx context.Context,
	transaction Transaction,
	processing core.ProcessingJob,
) (bool, error) {
	_, err := readQueuedJob(ctx, transaction, retryProcessingSQL, processing.Job.ID,
		processing.ClaimedAt, processing.InventoryEndpointID, processing.RunnerName)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("return processing job to queue: %w", err)
	}
	return true, nil
}
