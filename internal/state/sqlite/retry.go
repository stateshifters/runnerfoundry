package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

func (store *Store) RecoverUnstartedProcessing(
	ctx context.Context,
	processing core.ProcessingJob,
) (_ core.RetryResult, resultErr error) {
	return store.executeRetryTransition(ctx, "processing recovery", func(transaction Transaction) (core.RetryOutcome, error) {
		return recoverSQLiteProcessingClaim(ctx, transaction, processing)
	})
}

func (store *Store) executeRetryTransition(ctx context.Context, description string, transition func(Transaction) (core.RetryOutcome, error)) (core.RetryResult, error) {
	var outcome core.RetryOutcome
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		var err error
		outcome, err = transition(transaction)
		return err
	})
	if err != nil {
		return core.RetryResult{}, fmt.Errorf("%s: %w", description, err)
	}
	return core.RetryResult{Outcome: outcome}, nil
}

func recoverSQLiteProcessingClaim(
	ctx context.Context,
	transaction Transaction,
	processing core.ProcessingJob,
) (core.RetryOutcome, error) {
	recovered, err := transaction.Execute(ctx, recoverProcessingSQL, processing.Job.EnqueuedAt.UnixNano(),
		processing.Job.ID.String(), processing.ClaimedAt.UnixNano(), string(processing.InventoryEndpointID),
		processing.RunnerName)
	if err != nil {
		return "", fmt.Errorf("requeue expired processing: %w", err)
	}
	if recovered == 0 {
		return classifySQLiteRecoveryFailure(ctx, transaction, processing)
	}
	if err = removeRecoveredSQLiteOwnership(ctx, transaction, processing); err != nil {
		return "", err
	}
	return core.RetryRequeued, nil
}

func (store *Store) RetryUnstarted(
	ctx context.Context,
	processing core.ProcessingJob,
) (_ core.RetryResult, resultErr error) {
	return store.executeRetryTransition(ctx, "processing retry", func(transaction Transaction) (core.RetryOutcome, error) {
		return retryUnstartedProcessing(ctx, transaction, processing)
	})
}

func removeRecoveredSQLiteOwnership(ctx context.Context, transaction Transaction, processing core.ProcessingJob) error {
	removed, err := transaction.Execute(ctx, deleteRecoveredProcessingSQL, processing.Job.ID.String(),
		processing.ClaimedAt.UnixNano(), string(processing.InventoryEndpointID), processing.RunnerName)
	if err != nil {
		return fmt.Errorf("remove recovered processing: %w", err)
	}
	if removed != 1 {
		return fmt.Errorf("remove recovered processing ownership: expected one row, got %d", removed)
	}
	return nil
}

func retryUnstartedProcessing(
	ctx context.Context,
	transaction Transaction,
	expected core.ProcessingJob,
) (core.RetryOutcome, error) {
	outcome, err := classifyRetryCandidate(ctx, transaction, expected)
	if err != nil || outcome != "" {
		return outcome, err
	}
	return requeueProcessingOwnership(ctx, transaction, expected)
}

func classifyRetryCandidate(
	ctx context.Context,
	transaction Transaction,
	expected core.ProcessingJob,
) (core.RetryOutcome, error) {
	processing, err := readClaimedProcessingJob(ctx, transaction, processingByClaimIDSQL, expected.Job.ID.String())
	if errors.Is(err, sql.ErrNoRows) {
		return core.RetryMissing, nil
	}
	if err != nil {
		return "", fmt.Errorf("load processing retry candidate: %w", err)
	}
	if outcome := state.ClassifyProcessingRetry(processing, expected); outcome != "" {
		return outcome, nil
	}
	return "", nil
}

func requeueProcessingOwnership(
	ctx context.Context,
	transaction Transaction,
	processing core.ProcessingJob,
) (core.RetryOutcome, error) {
	requeued, err := enqueueSQLiteProcessingRetry(ctx, transaction, processing)
	if err != nil {
		return "", err
	}
	if !requeued {
		return classifyLostSQLiteRetry(ctx, transaction, processing)
	}
	if err := removeRetriedSQLiteOwnership(ctx, transaction, processing); err != nil {
		return "", err
	}
	return core.RetryRequeued, nil
}

func enqueueSQLiteProcessingRetry(
	ctx context.Context,
	transaction Transaction,
	processing core.ProcessingJob,
) (bool, error) {
	arguments := []any{processing.Job.ID.String(), processing.ClaimedAt.UnixNano(),
		string(processing.InventoryEndpointID), processing.RunnerName}
	rowsAffected, err := transaction.Execute(ctx, retryProcessingSQL, arguments...)
	if err != nil {
		return false, fmt.Errorf("return processing job to queue: %w", err)
	}
	return rowsAffected != 0, nil
}

func classifyLostSQLiteRetry(
	ctx context.Context,
	transaction Transaction,
	processing core.ProcessingJob,
) (core.RetryOutcome, error) {
	outcome, err := classifyRetryCandidate(ctx, transaction, processing)
	if err != nil || outcome != "" {
		return outcome, err
	}
	return core.RetryClaimChanged, nil
}

func removeRetriedSQLiteOwnership(
	ctx context.Context,
	transaction Transaction,
	processing core.ProcessingJob,
) error {
	arguments := []any{processing.Job.ID.String(), processing.ClaimedAt.UnixNano(),
		string(processing.InventoryEndpointID), processing.RunnerName}
	removed, removalErr := transaction.Execute(ctx, deleteRetriedProcessingSQL, arguments...)
	if removalErr != nil {
		return fmt.Errorf("remove retried processing ownership: %w", removalErr)
	}
	if removed != 1 {
		return fmt.Errorf("remove retried processing ownership: expected one row, got %d", removed)
	}
	return nil
}

func classifySQLiteRecoveryFailure(ctx context.Context, transaction Transaction, expected core.ProcessingJob) (core.RetryOutcome, error) {
	actual, err := readClaimedProcessingJob(ctx, transaction, processingByClaimIDSQL, expected.Job.ID.String())
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
