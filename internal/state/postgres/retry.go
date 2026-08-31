package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
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
	if !samePostgresProcessingClaim(processing, expected) {
		return core.ProcessingJob{}, core.RetryClaimChanged, nil
	}
	if processing.SourceTerminal {
		return core.ProcessingJob{}, core.RetrySourceTerminal, nil
	}
	if processing.RunnerAccepted {
		return core.ProcessingJob{}, core.RetryRunnerAccepted, nil
	}
	if processing.ProvisioningFailure == core.ProvisioningFailureReported {
		return core.ProcessingJob{}, core.RetryFailureFinalizationPending, nil
	}
	if processing.ProvisioningFailure == core.ProvisioningFailureReporting {
		return core.ProcessingJob{}, core.RetryFailureReportingPending, nil
	}
	return processing, "", nil
}

func removeRetriedProcessingOwnership(
	ctx context.Context,
	transaction Transaction,
	processing core.ProcessingJob,
) error {
	removal, removalErr := transaction.Execute(ctx, deleteRetriedProcessingSQL, processing.Job.ID,
		processing.ClaimedAt, processing.InventoryEndpointID, processing.RunnerName)
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
	expected core.ProcessingJob,
) (core.RetryOutcome, error) {
	processing, found, err := loadProcessingRetryCandidate(ctx, transaction, expected.Job.ID)
	if err != nil {
		return "", err
	}
	if !found {
		return core.RetryMissing, nil
	}
	if !samePostgresProcessingClaim(processing, expected) {
		return core.RetryClaimChanged, nil
	}
	if processing.SourceTerminal {
		return core.RetrySourceTerminal, nil
	}
	if processing.RunnerAccepted {
		return core.RetryRunnerAccepted, nil
	}
	if processing.ProvisioningFailure == core.ProvisioningFailureReported {
		return core.RetryFailureFinalizationPending, nil
	}
	if processing.ProvisioningFailure == core.ProvisioningFailureReporting {
		return core.RetryFailureReportingPending, nil
	}
	return core.RetryClaimChanged, nil
}

func loadProcessingRetryCandidate(
	ctx context.Context,
	transaction Transaction,
	jobID core.JobID,
) (core.ProcessingJob, bool, error) {
	processing, err := scanClaimedProcessingJob(transaction.QueryRow(ctx, processingByClaimIDSQL, jobID))
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
	_, err := scanQueuedJob(transaction.QueryRow(ctx, retryProcessingSQL, processing.Job.ID,
		processing.ClaimedAt, processing.InventoryEndpointID, processing.RunnerName))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("return processing job to queue: %w", err)
	}
	return true, nil
}

func samePostgresProcessingClaim(actual, expected core.ProcessingJob) bool {
	return actual.Job.ID == expected.Job.ID && actual.ClaimedAt.Equal(expected.ClaimedAt) &&
		actual.InventoryEndpointID == expected.InventoryEndpointID && actual.RunnerName == expected.RunnerName
}
