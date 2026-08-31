package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
	statecontract "github.com/stateshifters/runnerfoundry/internal/state"
)

const queuedJobColumns = `job_id, source_name, source_reference, runner_pool, enqueued_at`
const processingJobColumns = `job_id, source_name, source_reference, runner_pool, member_id,
	runner_name, runner_started, EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference) AS source_terminal,
	CASE WHEN EXISTS (SELECT 1 FROM runnerfoundry_provisioning_failures failures
		WHERE failures.job_id = runnerfoundry_processing_jobs.job_id AND failures.is_reported = 1) THEN 'reported'
		WHEN provisioning_failure_reserved = 1 THEN 'reporting' ELSE '' END AS provisioning_failure_state`
const claimedProcessingJobColumns = `job_id, source_name, source_reference, runner_pool, member_id,
	runner_name, claimed_at, inventory_endpoint_id, runner_started,
	EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference) AS source_terminal,
	CASE WHEN EXISTS (SELECT 1 FROM runnerfoundry_provisioning_failures failures
		WHERE failures.job_id = runnerfoundry_processing_jobs.job_id AND failures.is_reported = 1) THEN 'reported'
		WHEN provisioning_failure_reserved = 1 THEN 'reporting' ELSE '' END AS provisioning_failure_state`

const insertQueuedJobSQL = `INSERT INTO runnerfoundry_queued_jobs
	(job_id, source_name, source_reference, runner_pool, enqueued_at)
	SELECT lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
		substr(lower(hex(randomblob(2))), 2) || '-' || substr('89ab', (random() & 3) + 1, 1) ||
		substr(lower(hex(randomblob(2))), 2) || '-' || lower(hex(randomblob(6))), ?, ?, ?, ?
	WHERE NOT EXISTS (SELECT 1 FROM runnerfoundry_processing_jobs
		WHERE source_name = ? AND source_reference = ?)
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones
		WHERE source_name = ? AND source_reference = ?)
	ON CONFLICT(source_name, source_reference) DO NOTHING`

const queuedJobBySourceSQL = `SELECT ` + queuedJobColumns + ` FROM runnerfoundry_queued_jobs
	WHERE source_name = ? AND source_reference = ?`
const classifyQueueSourceSQL = `SELECT CASE
	WHEN EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones WHERE source_name = ? AND source_reference = ?)
		THEN 'source_terminal'
	WHEN EXISTS (SELECT 1 FROM runnerfoundry_processing_jobs WHERE source_name = ? AND source_reference = ?)
		THEN 'processing'
	ELSE 'already_queued' END`
const oldestQueuedJobSQL = `SELECT ` + queuedJobColumns + ` FROM runnerfoundry_queued_jobs
	WHERE runner_pool = ? ORDER BY enqueued_at LIMIT 1`
const insertClaimedProcessingJobSQL = `INSERT INTO runnerfoundry_processing_jobs
	(job_id, source_name, source_reference, runner_pool, member_id, runner_name, claimed_at, inventory_endpoint_id)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
const insertTombstoneSQL = `INSERT INTO runnerfoundry_source_tombstones(source_name, source_reference)
	VALUES (?, ?) ON CONFLICT(source_name, source_reference) DO NOTHING`
const deleteQueuedJobSQL = `DELETE FROM runnerfoundry_queued_jobs WHERE job_id = ?`
const cancelQueuedJobSQL = `DELETE FROM runnerfoundry_queued_jobs
	WHERE source_name = ? AND source_reference = ?`
const markProcessingAcceptedSQL = `UPDATE runnerfoundry_processing_jobs SET runner_started = 1
	WHERE source_name = ? AND source_reference = ? AND runner_started = 0 AND provisioning_failure_reserved = 0
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones
		WHERE source_name = ? AND source_reference = ?)`
const markProcessingAttemptAcceptedSQL = `UPDATE runnerfoundry_processing_jobs SET runner_started = 1
	WHERE source_name = ? AND source_reference = ? AND runner_name = ? AND runner_started = 0
	AND provisioning_failure_reserved = 0
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones
		WHERE source_name = ? AND source_reference = ?)`
const sourceAcceptedProcessingSQL = `SELECT EXISTS (SELECT 1 FROM runnerfoundry_processing_jobs
	WHERE source_name = ? AND source_reference = ?
	AND (runner_started = 1 OR provisioning_failure_reserved = 1))`
const attemptAcceptedProcessingSQL = `SELECT EXISTS (SELECT 1 FROM runnerfoundry_processing_jobs
	WHERE source_name = ? AND source_reference = ? AND runner_name = ?
	AND (runner_started = 1 OR provisioning_failure_reserved = 1))`
const tombstoneDelayedAcceptanceSQL = `INSERT INTO runnerfoundry_source_tombstones(source_name, source_reference)
	SELECT ?, ? WHERE EXISTS (SELECT 1 FROM runnerfoundry_queued_jobs
		WHERE source_name = ? AND source_reference = ?)
	ON CONFLICT(source_name, source_reference) DO NOTHING`
const processingByMemberSQL = `SELECT ` + claimedProcessingJobColumns + `
	FROM runnerfoundry_processing_jobs WHERE member_id = ? AND runner_pool = ?`
const processingByPoolSQL = `SELECT ` + claimedProcessingJobColumns + `
	FROM runnerfoundry_processing_jobs WHERE runner_pool = ?`

type Store struct {
	executor Executor
	close    func() error
}

var _ statecontract.JobQueueStore = (*Store)(nil)
var _ statecontract.ProcessingJobStore = (*Store)(nil)

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
	rows, err := transaction.Query(ctx, processingByMemberSQL, string(memberID), string(runnerPool))
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
	return store.executeRetryTransition(ctx, "processing recovery", func(transaction Transaction) (core.RetryOutcome, error) {
		return recoverSQLiteProcessingClaim(ctx, transaction, processing)
	})
}

func (store *Store) executeRetryTransition(
	ctx context.Context,
	description string,
	transition func(Transaction) (core.RetryOutcome, error),
) (_ core.RetryResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.RetryResult{}, fmt.Errorf("begin %s: %w", description, err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	outcome, err := transition(transaction)
	if err != nil {
		return core.RetryResult{}, err
	}
	if err = transaction.Commit(ctx); err != nil {
		return core.RetryResult{}, fmt.Errorf("commit %s: %w", description, err)
	}
	open = false
	return core.RetryResult{Outcome: outcome}, nil
}

func recoverSQLiteProcessingClaim(
	ctx context.Context,
	transaction Transaction,
	processing core.ProcessingJob,
) (core.RetryOutcome, error) {
	result, err := transaction.Execute(ctx, recoverProcessingSQL, processing.Job.EnqueuedAt.UnixNano(),
		processing.Job.ID.String(), processing.ClaimedAt.UnixNano(), string(processing.InventoryEndpointID),
		processing.RunnerName)
	if err != nil {
		return "", fmt.Errorf("requeue expired processing: %w", err)
	}
	recovered, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("read processing recovery count: %w", err)
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
	removal, err := transaction.Execute(ctx, deleteRecoveredProcessingSQL, processing.Job.ID.String(),
		processing.ClaimedAt.UnixNano(), string(processing.InventoryEndpointID), processing.RunnerName)
	if err != nil {
		return fmt.Errorf("remove recovered processing: %w", err)
	}
	removed, err := removal.RowsAffected()
	if err != nil {
		return fmt.Errorf("read recovered processing removal count: %w", err)
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
	processing, err := scanClaimedProcessingJob(
		transaction.QueryRow(ctx, processingByClaimIDSQL, expected.Job.ID.String()),
	)
	if errors.Is(err, sql.ErrNoRows) {
		return core.RetryMissing, nil
	}
	if err != nil {
		return "", fmt.Errorf("load processing retry candidate: %w", err)
	}
	if !sameSQLiteProcessingClaim(processing, expected) {
		return core.RetryClaimChanged, nil
	}
	if processing.SourceTerminal {
		return core.RetrySourceTerminal, nil
	}
	if processing.RunnerAccepted {
		return core.RetryRunnerAccepted, nil
	}
	if processing.ProvisioningFailure == core.ProvisioningFailureReported {
		return core.RetryFailureFinalizationPending, nil
	}
	if processing.ProvisioningFailure == core.ProvisioningFailureReporting {
		return core.RetryFailureReportingPending, nil
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
	result, err := transaction.Execute(ctx, retryProcessingSQL, arguments...)
	if err != nil {
		return false, fmt.Errorf("return processing job to queue: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read processing retry count: %w", err)
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
	removal, removalErr := transaction.Execute(ctx, deleteRetriedProcessingSQL, arguments...)
	if removalErr != nil {
		return fmt.Errorf("remove retried processing ownership: %w", removalErr)
	}
	removed, removalErr := removal.RowsAffected()
	if removalErr != nil {
		return fmt.Errorf("read retried processing removal count: %w", removalErr)
	}
	if removed != 1 {
		return fmt.Errorf("remove retried processing ownership: expected one row, got %d", removed)
	}
	return nil
}

func sameSQLiteProcessingClaim(actual, expected core.ProcessingJob) bool {
	return actual.Job.ID == expected.Job.ID && actual.ClaimedAt.Equal(expected.ClaimedAt) &&
		actual.InventoryEndpointID == expected.InventoryEndpointID && actual.RunnerName == expected.RunnerName
}

func (store *Store) RemoveTerminal(ctx context.Context, jobID core.JobID) (_ core.TerminalRemovalResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.TerminalRemovalResult{}, fmt.Errorf("begin remove terminal processing job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	result, err := transaction.Execute(ctx, deleteTerminalProcessingSQL, jobID.String())
	if err != nil {
		return core.TerminalRemovalResult{}, fmt.Errorf("remove terminal processing job: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return core.TerminalRemovalResult{}, fmt.Errorf("read remove terminal processing job count: %w", err)
	}
	outcome := core.TerminalRemoved
	if rows == 0 {
		outcome, err = classifyTerminalRemoval(ctx, transaction, jobID.String())
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

func classifyTerminalRemoval(ctx context.Context, transaction Transaction, jobID string) (core.TerminalRemovalOutcome, error) {
	var exists bool
	if err := transaction.QueryRow(ctx, processingExistsSQL, jobID).Scan(&exists); err != nil {
		return "", fmt.Errorf("classify terminal processing job: %w", err)
	}
	if exists {
		return core.TerminalNotTerminal, nil
	}
	return core.TerminalMissing, nil
}

func classifySQLiteRecoveryFailure(ctx context.Context, transaction Transaction, expected core.ProcessingJob) (core.RetryOutcome, error) {
	actual, err := scanClaimedProcessingJob(transaction.QueryRow(ctx, processingByClaimIDSQL, expected.Job.ID.String()))
	if errors.Is(err, sql.ErrNoRows) {
		return core.RetryMissing, nil
	}
	if err != nil {
		return "", fmt.Errorf("classify processing recovery: %w", err)
	}
	if !sameSQLiteProcessingClaim(actual, expected) {
		return core.RetryClaimChanged, nil
	}
	if actual.SourceTerminal {
		return core.RetrySourceTerminal, nil
	}
	if actual.RunnerAccepted {
		return core.RetryRunnerAccepted, nil
	}
	if actual.ProvisioningFailure == core.ProvisioningFailureReported {
		return core.RetryFailureFinalizationPending, nil
	}
	if actual.ProvisioningFailure == core.ProvisioningFailureReporting {
		return core.RetryFailureReportingPending, nil
	}
	return core.RetryClaimChanged, nil
}
