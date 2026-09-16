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
const serializeDemandDomainSQL = `SELECT pg_advisory_xact_lock(4, hashtext($1))`

func advisoryLockIdentity(firstComponent, secondComponent string) string {
	return strconv.Itoa(len(firstComponent)) + ":" + firstComponent + secondComponent
}

func serializeDemandScope(ctx context.Context, transaction Transaction, scope core.DemandScope) error {
	if _, err := transaction.Execute(ctx, serializeDemandScopeSQL,
		advisoryLockIdentity(string(scope.Adapter), string(scope.ID))); err != nil {
		return fmt.Errorf("serialize demand scope: %w", err)
	}
	return nil
}

func serializeDemandDomain(
	ctx context.Context,
	transaction Transaction,
	adapter core.SourceAdapter,
	runnerPool core.RunnerPool,
) error {
	if _, err := transaction.Execute(ctx, serializeDemandDomainSQL,
		advisoryLockIdentity(string(adapter), string(runnerPool))); err != nil {
		return fmt.Errorf("serialize demand domain: %w", err)
	}
	return nil
}

func serializeSource(ctx context.Context, transaction Transaction, source core.SourceReference) error {
	if _, err := transaction.Execute(ctx, serializeSourceSQL,
		advisoryLockIdentity(string(source.Adapter), string(source.ID))); err != nil {
		return fmt.Errorf("serialize source: %w", err)
	}
	return nil
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
	VALUES($1, $2, $3, $4, $5, $6, $7, $8)
	RETURNING claimed_at`
const deleteQueuedJobBySourceSQL = `DELETE FROM public.runnerfoundry_queued_jobs
	WHERE source_name = $1 AND source_reference = $2`
const processingByMemberSQL = `SELECT ` + claimedProcessingJobColumns + `
	FROM public.runnerfoundry_processing_jobs WHERE member_id = $1 AND runner_pool = $2`

type Store struct{ executor executor }

var _ statecontract.JobQueueStore = (*Store)(nil)
var _ statecontract.ProcessingJobStore = (*Store)(nil)
var _ statecontract.ProcessingTransitions = (*Store)(nil)
var _ statecontract.ProvisioningFailureTransitions = (*Store)(nil)

func newStore(executor executor) *Store { return &Store{executor: executor} }

func (store *Store) ClaimOldest(
	ctx context.Context,
	runnerPool core.RunnerPool,
	claim core.ProcessingClaim,
) (core.ClaimResult, error) {
	if err := claim.Validate(); err != nil {
		return core.ClaimResult{}, fmt.Errorf("validate processing claim: %w", err)
	}
	var result core.ClaimResult
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		var operationErr error
		result, operationErr = claimOldestInTransaction(ctx, transaction, runnerPool, claim)
		return operationErr
	})
	if err != nil {
		return core.ClaimResult{}, fmt.Errorf("claim job: %w", err)
	}
	return result, nil
}

func (store *Store) ProcessingByMember(
	ctx context.Context,
	memberID core.MemberID,
	runnerPool core.RunnerPool,
) ([]core.ProcessingJob, error) {
	return store.processingJobs(ctx, processingByMemberSQL, memberID, string(runnerPool))
}

func (store *Store) ProcessingByPool(
	ctx context.Context,
	runnerPool core.RunnerPool,
) ([]core.ProcessingJob, error) {
	return store.processingJobs(ctx, processingByPoolSQL, string(runnerPool))
}

func (store *Store) processingJobs(ctx context.Context, statement string, arguments ...any) ([]core.ProcessingJob, error) {
	var jobs []core.ProcessingJob
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		var operationErr error
		jobs, operationErr = readClaimedProcessingJobs(ctx, transaction, statement, arguments...)
		return operationErr
	})
	if err != nil {
		return nil, fmt.Errorf("load processing jobs: %w", err)
	}
	return jobs, nil
}

func (store *Store) RecoverUnstartedProcessing(
	ctx context.Context,
	processing core.ProcessingJob,
) (core.RetryResult, error) {
	return store.executeRetryTransition(ctx, "processing recovery", func(transaction Transaction) (core.RetryOutcome, error) {
		if _, err := transaction.Execute(ctx, serializeSourceSQL, advisoryLockIdentity(
			string(processing.Job.Source.Adapter), string(processing.Job.Source.ID))); err != nil {
			return "", fmt.Errorf("serialize processing recovery: %w", err)
		}
		return recoverProcessingClaim(ctx, transaction, processing)
	})
}

func (store *Store) RetryUnstarted(
	ctx context.Context,
	processing core.ProcessingJob,
) (core.RetryResult, error) {
	return store.executeRetryTransition(ctx, "processing retry", func(transaction Transaction) (core.RetryOutcome, error) {
		return retryUnstartedProcessing(ctx, transaction, processing)
	})
}

func (store *Store) executeRetryTransition(
	ctx context.Context,
	description string,
	transition func(Transaction) (core.RetryOutcome, error),
) (core.RetryResult, error) {
	var outcome core.RetryOutcome
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		var operationErr error
		outcome, operationErr = transition(transaction)
		return operationErr
	})
	if err != nil {
		return core.RetryResult{}, fmt.Errorf("%s: %w", description, err)
	}
	return core.RetryResult{Outcome: outcome}, nil
}

func (store *Store) RemoveTerminal(ctx context.Context, jobID core.JobID) (core.TerminalRemovalResult, error) {
	outcome := core.TerminalRemoved
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		rows, operationErr := transaction.Execute(ctx, deleteTerminalProcessingSQL, jobID)
		if operationErr != nil {
			return fmt.Errorf("remove terminal processing job: %w", operationErr)
		}
		if rows == 0 {
			outcome, operationErr = classifyTerminalRemoval(ctx, transaction, jobID)
		}
		return operationErr
	})
	if err != nil {
		return core.TerminalRemovalResult{}, fmt.Errorf("remove terminal processing: %w", err)
	}
	return core.TerminalRemovalResult{Outcome: outcome}, nil
}

func classifyTerminalRemoval(ctx context.Context, transaction Transaction, jobID core.JobID) (core.TerminalRemovalOutcome, error) {
	var exists bool
	if err := transaction.ReadRow(ctx, processingExistsSQL, func(row Scanner) error {
		return row.Scan(&exists)
	}, jobID); err != nil {
		return "", fmt.Errorf("classify terminal processing job: %w", err)
	}
	if exists {
		return core.TerminalNotTerminal, nil
	}
	return core.TerminalMissing, nil
}
