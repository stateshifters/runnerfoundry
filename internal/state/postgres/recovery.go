package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

func recoverProcessingClaim(
	ctx context.Context,
	transaction Transaction,
	processing core.ProcessingJob,
) (core.RetryOutcome, error) {
	_, err := scanQueuedJob(transaction.QueryRow(ctx, recoverProcessingSQL, processing.Job.ID,
		processing.ClaimedAt, processing.InventoryEndpointID, processing.Job.EnqueuedAt))
	if errors.Is(err, sql.ErrNoRows) {
		return classifyRecoveryFailure(ctx, transaction, processing)
	}
	if err != nil {
		return "", fmt.Errorf("requeue expired processing: %w", err)
	}
	removal, err := transaction.Execute(ctx, deleteRecoveredProcessingSQL, processing.Job.ID,
		processing.ClaimedAt, processing.InventoryEndpointID)
	if err != nil {
		return "", fmt.Errorf("remove recovered processing: %w", err)
	}
	removed, err := removal.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("read recovered processing removal count: %w", err)
	}
	if removed != 1 {
		return "", fmt.Errorf("remove recovered processing ownership: expected one row, got %d", removed)
	}
	return core.RetryRequeued, nil
}

func classifyRecoveryFailure(ctx context.Context, transaction Transaction, expected core.ProcessingJob) (core.RetryOutcome, error) {
	actual, err := scanClaimedProcessingJob(transaction.QueryRow(ctx, processingByClaimIDSQL, expected.Job.ID))
	if errors.Is(err, sql.ErrNoRows) {
		return core.RetryMissing, nil
	}
	if err != nil {
		return "", fmt.Errorf("classify processing recovery: %w", err)
	}
	if actual.SourceTerminal {
		return core.RetrySourceTerminal, nil
	}
	if actual.RunnerStarted {
		return core.RetryRunnerStarted, nil
	}
	return core.RetryClaimChanged, nil
}
