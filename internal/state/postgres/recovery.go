package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

func recoverProcessingClaim(
	ctx context.Context,
	transaction Transaction,
	processing core.ProcessingJob,
) (core.RetryOutcome, error) {
	_, err := readQueuedJob(ctx, transaction, recoverProcessingSQL, processing.Job.ID,
		processing.ClaimedAt, processing.InventoryEndpointID, processing.Job.EnqueuedAt, processing.RunnerName)
	if errors.Is(err, sql.ErrNoRows) {
		return classifyRecoveryFailure(ctx, transaction, processing)
	}
	if err != nil {
		return "", fmt.Errorf("requeue expired processing: %w", err)
	}
	removed, err := transaction.Execute(ctx, deleteRecoveredProcessingSQL, processing.Job.ID,
		processing.ClaimedAt, processing.InventoryEndpointID, processing.RunnerName)
	if err != nil {
		return "", fmt.Errorf("remove recovered processing: %w", err)
	}
	if removed != 1 {
		return "", fmt.Errorf("remove recovered processing ownership: expected one row, got %d", removed)
	}
	return core.RetryRequeued, nil
}

func classifyRecoveryFailure(ctx context.Context, transaction Transaction, expected core.ProcessingJob) (core.RetryOutcome, error) {
	actual, err := readClaimedProcessingJob(ctx, transaction, processingByClaimIDSQL, expected.Job.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return core.RetryMissing, nil
	}
	if err != nil {
		return "", fmt.Errorf("classify processing recovery: %w", err)
	}
	if outcome := state.ClassifyProcessingRetry(actual, expected); outcome != "" {
		return outcome, nil
	}
	return core.RetryClaimChanged, nil
}
