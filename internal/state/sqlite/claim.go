package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

func claimOldestInTransaction(
	ctx context.Context,
	transaction Transaction,
	runnerPool core.RunnerPool,
	claim core.ProcessingClaim,
) (core.TakeResult, error) {
	job, err := scanJob(transaction.QueryRow(ctx, oldestQueuedJobSQL, string(runnerPool)))
	if errors.Is(err, sql.ErrNoRows) {
		return core.TakeResult{}, nil
	}
	if err != nil {
		return core.TakeResult{}, fmt.Errorf("select oldest queued job: %w", err)
	}
	if _, err = transaction.Execute(ctx, insertClaimedProcessingJobSQL, job.ID.String(), job.Source.Adapter,
		job.Source.ID, string(job.RunnerPool), string(claim.MemberID), claim.ClaimedAt.UnixNano(),
		string(claim.InventoryEndpointID)); err != nil {
		return core.TakeResult{}, fmt.Errorf("insert processing claim: %w", err)
	}
	if _, err = transaction.Execute(ctx, deleteQueuedJobSQL, job.ID.String()); err != nil {
		return core.TakeResult{}, fmt.Errorf("remove claimed job from queue: %w", err)
	}
	return core.TakeResult{Processing: core.ProcessingJob{Job: job, MemberID: claim.MemberID,
		ClaimedAt: claim.ClaimedAt, InventoryEndpointID: claim.InventoryEndpointID}, Taken: true}, nil
}
