package postgres

import (
	"context"
	"fmt"
	"strconv"

	"github.com/stateshifters/runnerfoundry/internal/core"
	statecontract "github.com/stateshifters/runnerfoundry/internal/state"
)

const queuedJobColumns = `job_id, source_name, source_reference, runner_pool, enqueued_at`
const claimedProcessingJobColumns = `job_id, source_name, source_reference, runner_pool, member_id,
	runner_name, claimed_at, inventory_endpoint_id, runner_started,
	EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference) AS source_terminal,
	CASE WHEN EXISTS (SELECT 1 FROM public.runnerfoundry_provisioning_failures failures
		WHERE failures.job_id = runnerfoundry_processing_jobs.job_id AND failures.is_reported) THEN 'reported'
		WHEN provisioning_failure_reserved THEN 'reporting' ELSE '' END AS provisioning_failure_state`

const serializeRunnerPoolClaimSQL = `SELECT pg_advisory_xact_lock(1, hashtext($1))`
const serializeSourceSQL = `SELECT pg_advisory_xact_lock(2, hashtext($1))`
const serializeDemandScopeSQL = `SELECT pg_advisory_xact_lock(3, hashtext($1))`

func advisoryLockIdentity(firstComponent, secondComponent string) string {
	return strconv.Itoa(len(firstComponent)) + ":" + firstComponent + secondComponent
}

func SerializeDemandScope(ctx context.Context, transaction Transaction, scope core.DemandScope) error {
	_, err := transaction.Execute(ctx, serializeDemandScopeSQL,
		advisoryLockIdentity(string(scope.Adapter), string(scope.ID)))
	return err
}

func SerializeSource(ctx context.Context, transaction Transaction, source core.SourceReference) error {
	_, err := transaction.Execute(ctx, serializeSourceSQL,
		advisoryLockIdentity(string(source.Adapter), string(source.ID)))
	return err
}

const insertQueuedJobSQL = `INSERT INTO public.runnerfoundry_queued_jobs
	(source_name, source_reference, runner_pool, enqueued_at)
	SELECT $1, $2, $3, $4
	WHERE NOT EXISTS (
		SELECT 1 FROM public.runnerfoundry_processing_jobs
		WHERE source_name = $1 AND source_reference = $2
	) AND NOT EXISTS (
		SELECT 1 FROM public.runnerfoundry_source_tombstones
		WHERE source_name = $1 AND source_reference = $2
	)
	ON CONFLICT(source_name, source_reference) DO NOTHING
	RETURNING ` + queuedJobColumns
const lookupQueuedJobBySourceSQL = `SELECT ` + queuedJobColumns + `
	FROM public.runnerfoundry_queued_jobs WHERE source_name = $1 AND source_reference = $2`
const classifyQueueSourceSQL = `SELECT CASE
	WHEN EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones WHERE source_name = $1 AND source_reference = $2)
		THEN 'source_terminal'
	WHEN EXISTS (SELECT 1 FROM public.runnerfoundry_processing_jobs WHERE source_name = $1 AND source_reference = $2)
		THEN 'processing'
	ELSE 'already_queued' END`
const selectOldestQueuedJobSQL = `SELECT ` + queuedJobColumns + `
	FROM public.runnerfoundry_queued_jobs
	WHERE runner_pool = $1
	ORDER BY enqueued_at LIMIT 1`
const takeQueuedJobSQL = `DELETE FROM public.runnerfoundry_queued_jobs WHERE job_id = $1
	RETURNING ` + queuedJobColumns
const insertClaimedProcessingJobSQL = `INSERT INTO public.runnerfoundry_processing_jobs
	(job_id, source_name, source_reference, runner_pool, member_id, runner_name, claimed_at, inventory_endpoint_id)
	VALUES($1, $2, $3, $4, $5, $6, $7, $8)`
const deleteQueuedJobBySourceSQL = `DELETE FROM public.runnerfoundry_queued_jobs
	WHERE source_name = $1 AND source_reference = $2`
const processingByMemberSQL = `SELECT ` + claimedProcessingJobColumns + `
	FROM public.runnerfoundry_processing_jobs WHERE member_id = $1 AND runner_pool = $2`

type Store struct{ executor Executor }

var _ statecontract.JobQueueStore = (*Store)(nil)
var _ statecontract.ProcessingJobStore = (*Store)(nil)

func NewStore(executor Executor) *Store { return &Store{executor: executor} }

func (store *Store) ClaimOldest(
	ctx context.Context,
	runnerPool core.RunnerPool,
	claim core.ProcessingClaim,
) (_ core.ClaimResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.ClaimResult{}, fmt.Errorf("begin claim job: %w", err)
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

func (store *Store) ProcessingByMember(
	ctx context.Context,
	memberID core.MemberID,
	runnerPool core.RunnerPool,
) (_ []core.ProcessingJob, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin processing jobs lookup: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	rows, err := transaction.Query(ctx, processingByMemberSQL, memberID, string(runnerPool))
	if err != nil {
		return nil, fmt.Errorf("query processing jobs: %w", err)
	}
	jobs, err := scanClaimedProcessingJobs(rows)
	if err != nil {
		return nil, err
	}
	if err := transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit processing jobs lookup: %w", err)
	}
	open = false
	return jobs, nil
}

func (store *Store) ProcessingByPool(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (_ []core.ProcessingJob, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin pool processing lookup: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	rows, err := transaction.Query(ctx, processingByPoolSQL, string(runnerPool))
	if err != nil {
		return nil, fmt.Errorf("query pool processing: %w", err)
	}
	jobs, err := scanClaimedProcessingJobs(rows)
	if err != nil {
		return nil, err
	}
	if err = transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit pool processing lookup: %w", err)
	}
	open = false
	return jobs, nil
}

func (store *Store) RecoverUnstartedProcessing(
	ctx context.Context,
	processing core.ProcessingJob,
) (_ core.RetryResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.RetryResult{}, fmt.Errorf("begin processing recovery: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if _, err = transaction.Execute(ctx, serializeSourceSQL, advisoryLockIdentity(
		string(processing.Job.Source.Adapter), string(processing.Job.Source.ID))); err != nil {
		return core.RetryResult{}, fmt.Errorf("serialize processing recovery: %w", err)
	}
	outcome, err := recoverProcessingClaim(ctx, transaction, processing)
	if err != nil {
		return core.RetryResult{}, err
	}
	if err = transaction.Commit(ctx); err != nil {
		return core.RetryResult{}, fmt.Errorf("commit processing recovery: %w", err)
	}
	open = false
	return core.RetryResult{Outcome: outcome}, nil
}

func (store *Store) RetryUnstarted(
	ctx context.Context,
	processing core.ProcessingJob,
) (_ core.RetryResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.RetryResult{}, fmt.Errorf("begin retry processing job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	outcome, err := retryUnstartedProcessing(ctx, transaction, processing)
	if err != nil {
		return core.RetryResult{}, err
	}
	if err := transaction.Commit(ctx); err != nil {
		return core.RetryResult{}, fmt.Errorf("commit processing retry: %w", err)
	}
	open = false
	return core.RetryResult{Outcome: outcome}, nil
}

func (store *Store) RemoveTerminal(ctx context.Context, jobID core.JobID) (_ core.TerminalRemovalResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.TerminalRemovalResult{}, fmt.Errorf("begin remove terminal processing job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	result, err := transaction.Execute(ctx, deleteTerminalProcessingSQL, jobID)
	if err != nil {
		return core.TerminalRemovalResult{}, fmt.Errorf("remove terminal processing job: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return core.TerminalRemovalResult{}, fmt.Errorf("read remove terminal processing job count: %w", err)
	}
	outcome := core.TerminalRemoved
	if rows == 0 {
		outcome, err = classifyTerminalRemoval(ctx, transaction, jobID)
		if err != nil {
			return core.TerminalRemovalResult{}, err
		}
	}
	if err = transaction.Commit(ctx); err != nil {
		return core.TerminalRemovalResult{}, fmt.Errorf("commit remove terminal processing job: %w", err)
	}
	open = false
	return core.TerminalRemovalResult{Outcome: outcome}, nil
}

func classifyTerminalRemoval(ctx context.Context, transaction Transaction, jobID core.JobID) (core.TerminalRemovalOutcome, error) {
	var exists bool
	if err := transaction.QueryRow(ctx, processingExistsSQL, jobID).Scan(&exists); err != nil {
		return "", fmt.Errorf("classify terminal processing job: %w", err)
	}
	if exists {
		return core.TerminalNotTerminal, nil
	}
	return core.TerminalMissing, nil
}
