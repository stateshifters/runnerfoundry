package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

func (store *Store) ClaimOldest(
	ctx context.Context,
	runnerPool core.RunnerPool,
	claim core.ProcessingClaim,
) (_ core.ClaimResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.ClaimResult{}, fmt.Errorf("begin claim oldest job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	result, err := claimOldestInTransaction(ctx, transaction, runnerPool, claim)
	if err != nil {
		return core.ClaimResult{}, err
	}
	if err = transaction.Commit(ctx); err != nil {
		return core.ClaimResult{}, fmt.Errorf("commit job claim: %w", err)
	}
	open = false
	return result, nil
}

func claimOldestInTransaction(
	ctx context.Context,
	transaction Transaction,
	runnerPool core.RunnerPool,
	claim core.ProcessingClaim,
) (core.ClaimResult, error) {
	job, err := scanJob(transaction.QueryRow(ctx, oldestQueuedJobSQL, string(runnerPool)))
	if errors.Is(err, sql.ErrNoRows) {
		return core.ClaimResult{Outcome: core.ClaimEmpty}, nil
	}
	if err != nil {
		return core.ClaimResult{}, fmt.Errorf("select oldest queued job: %w", err)
	}
	runnerName := core.AttemptRunnerName(job.ID, claim.AttemptID)
	if _, err = transaction.Execute(ctx, insertClaimedProcessingJobSQL, job.ID.String(), job.Source.Adapter,
		job.Source.ID, string(job.RunnerPool), string(claim.MemberID), runnerName, claim.ClaimedAt.UnixNano(),
		string(claim.InventoryEndpointID)); err != nil {
		return core.ClaimResult{}, fmt.Errorf("insert processing claim: %w", err)
	}
	if _, err = transaction.Execute(ctx, deleteQueuedJobSQL, job.ID.String()); err != nil {
		return core.ClaimResult{}, fmt.Errorf("remove claimed job from queue: %w", err)
	}
	return core.ClaimResult{Processing: core.ProcessingJob{Job: job, MemberID: claim.MemberID,
		RunnerName: runnerName, ClaimedAt: claim.ClaimedAt,
		InventoryEndpointID: claim.InventoryEndpointID}, Outcome: core.ClaimClaimed}, nil
}
