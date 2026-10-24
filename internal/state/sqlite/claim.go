package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func (store *Store) ClaimOldest(ctx context.Context, runnerPool core.RunnerPool, claim core.ProcessingClaim) (core.ClaimResult, error) {
	if err := claim.Validate(); err != nil {
		return core.ClaimResult{}, fmt.Errorf("validate processing claim: %w", err)
	}
	var result core.ClaimResult
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		var err error
		result, err = claimOldestInTransaction(ctx, transaction, runnerPool, claim)
		return err
	})
	if err != nil {
		return core.ClaimResult{}, fmt.Errorf("claim oldest job: %w", err)
	}
	return result, nil
}

func claimOldestInTransaction(
	ctx context.Context,
	transaction Transaction,
	runnerPool core.RunnerPool,
	claim core.ProcessingClaim,
) (core.ClaimResult, error) {
	job, err := readJob(ctx, transaction, oldestQueuedJobSQL, string(runnerPool))
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
