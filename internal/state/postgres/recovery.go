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
) (bool, error) {
	_, err := scanQueuedJob(transaction.QueryRow(ctx, recoverProcessingSQL, processing.Job.ID,
		processing.ClaimedAt, processing.InventoryEndpointID, processing.Job.EnqueuedAt))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("requeue expired processing: %w", err)
	}
	if _, err = transaction.Execute(ctx, deleteRecoveredProcessingSQL, processing.Job.ID,
		processing.ClaimedAt, processing.InventoryEndpointID); err != nil {
		return false, fmt.Errorf("remove recovered processing: %w", err)
	}
	return true, nil
}
