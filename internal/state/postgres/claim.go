package postgres

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
	if _, err := transaction.Execute(ctx, serializeRunnerPoolClaimSQL, string(runnerPool)); err != nil {
		return core.TakeResult{}, fmt.Errorf("serialize Runner Pool claim: %w", err)
	}
	candidate, found, err := selectOldestQueuedJob(ctx, transaction, runnerPool)
	if err != nil || !found {
		return core.TakeResult{}, err
	}
	job, taken, err := takeQueuedJob(ctx, transaction, candidate)
	if err != nil || !taken {
		return core.TakeResult{}, err
	}
	if _, err = transaction.Execute(ctx, insertClaimedProcessingJobSQL, job.ID, job.Source.Adapter,
		job.Source.ID, string(job.RunnerPool), claim.MemberID, claim.ClaimedAt, claim.InventoryEndpointID); err != nil {
		return core.TakeResult{}, fmt.Errorf("insert processing claim: %w", err)
	}
	return core.TakeResult{Processing: core.ProcessingJob{Job: job, MemberID: claim.MemberID,
		ClaimedAt: claim.ClaimedAt, InventoryEndpointID: claim.InventoryEndpointID}, Taken: true}, nil
}

func takeOldestInTransaction(
	ctx context.Context,
	transaction Transaction,
	runnerPool core.RunnerPool,
	memberID core.MemberID,
) (core.ProcessingJob, bool, error) {
	if _, lockErr := transaction.Execute(ctx, serializeRunnerPoolClaimSQL, string(runnerPool)); lockErr != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("serialize Runner Pool pickup: %w", lockErr)
	}
	candidate, found, err := selectOldestQueuedJob(ctx, transaction, runnerPool)
	if err != nil || !found {
		return core.ProcessingJob{}, false, err
	}
	job, taken, err := takeQueuedJob(ctx, transaction, candidate)
	if err != nil || !taken {
		return core.ProcessingJob{}, false, err
	}
	if err := createProcessingOwnership(ctx, transaction, job, memberID); err != nil {
		return core.ProcessingJob{}, false, err
	}
	return core.ProcessingJob{Job: job, MemberID: memberID}, true, nil
}

func selectOldestQueuedJob(
	ctx context.Context,
	transaction Transaction,
	runnerPool core.RunnerPool,
) (core.Job, bool, error) {
	candidate, err := scanQueuedJob(transaction.QueryRow(ctx, selectOldestQueuedJobSQL, string(runnerPool)))
	if errors.Is(err, sql.ErrNoRows) {
		return core.Job{}, false, nil
	}
	if err != nil {
		return core.Job{}, false, fmt.Errorf("select oldest queued job: %w", err)
	}
	return candidate, true, nil
}

func takeQueuedJob(ctx context.Context, transaction Transaction, candidate core.Job) (core.Job, bool, error) {
	if _, err := transaction.Execute(ctx, serializeSourceSQL,
		candidate.Source.Adapter, candidate.Source.ID); err != nil {
		return core.Job{}, false, fmt.Errorf("serialize source pickup: %w", err)
	}
	job, err := scanQueuedJob(transaction.QueryRow(ctx, takeQueuedJobSQL, candidate.ID))
	if errors.Is(err, sql.ErrNoRows) {
		return core.Job{}, false, nil
	}
	if err != nil {
		return core.Job{}, false, fmt.Errorf("remove picked-up queued job: %w", err)
	}
	return job, true, nil
}

func createProcessingOwnership(
	ctx context.Context,
	transaction Transaction,
	job core.Job,
	memberID core.MemberID,
) error {
	if _, err := transaction.Execute(ctx, insertProcessingJobSQL, job.ID, job.Source.Adapter,
		job.Source.ID, string(job.RunnerPool), memberID); err != nil {
		return fmt.Errorf("insert processing job: %w", err)
	}
	return nil
}
