package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

const queuedJobColumns = `job_id, source_name, source_reference, runner_pool, enqueued_at`
const processingJobColumns = `job_id, source_name, source_reference, runner_pool, member_id,
	runner_started, EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference) AS source_terminal`
const claimedProcessingJobColumns = `job_id, source_name, source_reference, runner_pool, member_id,
	claimed_at, inventory_endpoint_id, runner_started,
	EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference) AS source_terminal`

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
const oldestQueuedJobSQL = `SELECT ` + queuedJobColumns + ` FROM runnerfoundry_queued_jobs
	WHERE runner_pool = ? ORDER BY enqueued_at LIMIT 1`
const insertProcessingJobSQL = `INSERT INTO runnerfoundry_processing_jobs
	(job_id, source_name, source_reference, runner_pool, member_id) VALUES (?, ?, ?, ?, ?)`
const insertClaimedProcessingJobSQL = `INSERT INTO runnerfoundry_processing_jobs
	(job_id, source_name, source_reference, runner_pool, member_id, claimed_at, inventory_endpoint_id)
	VALUES (?, ?, ?, ?, ?, ?, ?)`
const insertTombstoneSQL = `INSERT INTO runnerfoundry_source_tombstones(source_name, source_reference)
	VALUES (?, ?) ON CONFLICT(source_name, source_reference) DO NOTHING`
const deleteQueuedJobSQL = `DELETE FROM runnerfoundry_queued_jobs WHERE job_id = ?`
const cancelQueuedJobSQL = `DELETE FROM runnerfoundry_queued_jobs
	WHERE source_name = ? AND source_reference = ?`
const markProcessingStartedSQL = `UPDATE runnerfoundry_processing_jobs SET runner_started = 1
	WHERE source_name = ? AND source_reference = ?
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones
		WHERE source_name = ? AND source_reference = ?)`
const tombstoneDelayedStartSQL = `INSERT INTO runnerfoundry_source_tombstones(source_name, source_reference)
	SELECT ?, ? WHERE EXISTS (SELECT 1 FROM runnerfoundry_queued_jobs
		WHERE source_name = ? AND source_reference = ?)
	ON CONFLICT(source_name, source_reference) DO NOTHING`
const processingByMemberSQL = `SELECT ` + processingJobColumns + `
	FROM runnerfoundry_processing_jobs WHERE member_id = ? AND runner_pool = ?`
const processingByPoolSQL = `SELECT ` + claimedProcessingJobColumns + `
	FROM runnerfoundry_processing_jobs WHERE runner_pool = ?`
const recoverProcessingSQL = `INSERT INTO runnerfoundry_queued_jobs
	(job_id, source_name, source_reference, runner_pool, enqueued_at)
	SELECT job_id, source_name, source_reference, runner_pool, ?
	FROM runnerfoundry_processing_jobs processing
	WHERE processing.job_id = ? AND processing.claimed_at = ?
	AND processing.inventory_endpoint_id = ? AND processing.runner_started = 0
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)
	ON CONFLICT(source_name, source_reference) DO NOTHING`
const deleteRecoveredProcessingSQL = `DELETE FROM runnerfoundry_processing_jobs
	WHERE job_id = ? AND claimed_at = ? AND inventory_endpoint_id = ? AND runner_started = 0
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference)`
const retryProcessingSQL = `INSERT INTO runnerfoundry_queued_jobs
	(job_id, source_name, source_reference, runner_pool, enqueued_at)
	SELECT job_id, source_name, source_reference, runner_pool,
		CAST((julianday('now') - 2440587.5) * 86400000000000 AS INTEGER)
	FROM runnerfoundry_processing_jobs processing
	WHERE processing.job_id = ? AND processing.runner_started = 0
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)`
const deleteRetriedProcessingSQL = `DELETE FROM runnerfoundry_processing_jobs
	WHERE job_id = ? AND runner_started = 0`
const deleteTerminalProcessingSQL = `DELETE FROM runnerfoundry_processing_jobs
	WHERE job_id = ? AND EXISTS (
		SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference)`

type Store struct {
	executor Executor
	close    func() error
}

var _ core.JobQueueStore = (*Store)(nil)
var _ core.ProcessingJobStore = (*Store)(nil)

func NewStore(executor Executor) *Store { return &Store{executor: executor} }

func (store *Store) Close() error {
	if store.close == nil {
		return nil
	}
	return store.close()
}

func (store *Store) Enqueue(ctx context.Context, request core.JobRequest) (_ core.Job, _ bool, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.Job{}, false, fmt.Errorf("begin enqueue job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	result, err := transaction.Execute(ctx, insertQueuedJobSQL,
		request.Source.Adapter, request.Source.ID, string(request.RunnerPool), request.EnqueuedAt.UnixNano(),
		request.Source.Adapter, request.Source.ID, request.Source.Adapter, request.Source.ID)
	if err != nil {
		return core.Job{}, false, fmt.Errorf("insert queued job: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return core.Job{}, false, fmt.Errorf("read inserted job count: %w", err)
	}
	job, err := scanJob(transaction.QueryRow(ctx, queuedJobBySourceSQL, request.Source.Adapter, request.Source.ID))
	if errors.Is(err, sql.ErrNoRows) && rowsAffected == 0 {
		job, err = core.Job{}, nil
	}
	if err != nil {
		return core.Job{}, false, fmt.Errorf("load enqueued job: %w", err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return core.Job{}, false, fmt.Errorf("commit enqueue job: %w", err)
	}
	open = false
	return job, rowsAffected == 1, nil
}

func (store *Store) Queue(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	job, enqueued, err := store.Enqueue(ctx, request)
	return core.EnqueueResult{Job: job, Enqueued: enqueued}, err
}

func (store *Store) TakeOldest(
	ctx context.Context,
	runnerPool core.RunnerPool,
	memberID core.MemberID,
) (_ core.ProcessingJob, _ bool, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("begin take oldest job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	processingJob, found, err := moveOldestToProcessing(ctx, transaction, runnerPool, memberID)
	if err != nil {
		return core.ProcessingJob{}, false, err
	}
	if commitErr := transaction.Commit(ctx); commitErr != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("commit take oldest job: %w", commitErr)
	}
	open = false
	return processingJob, found, nil
}

func (store *Store) ClaimOldest(
	ctx context.Context,
	runnerPool core.RunnerPool,
	claim core.ProcessingClaim,
) (_ core.TakeResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.TakeResult{}, fmt.Errorf("begin claim oldest job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	result, err := claimOldestInTransaction(ctx, transaction, runnerPool, claim)
	if err != nil {
		return core.TakeResult{}, err
	}
	if err = transaction.Commit(ctx); err != nil {
		return core.TakeResult{}, fmt.Errorf("commit job claim: %w", err)
	}
	open = false
	return result, nil
}

func (store *Store) TerminalizeSource(ctx context.Context, source core.SourceReference) (resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin terminalize source: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if _, err := transaction.Execute(ctx, insertTombstoneSQL, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("tombstone terminal source: %w", err)
	}
	if _, err := transaction.Execute(ctx, cancelQueuedJobSQL, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("remove terminal queued job: %w", err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit source terminalization: %w", err)
	}
	open = false
	return nil
}

func (store *Store) MarkRunnerStarted(ctx context.Context, source core.SourceReference) (resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin mark runner started: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if _, err := transaction.Execute(ctx, markProcessingStartedSQL,
		source.Adapter, source.ID, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("mark processing runner started: %w", err)
	}
	if _, err := transaction.Execute(ctx, tombstoneDelayedStartSQL,
		source.Adapter, source.ID, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("tombstone delayed runner start: %w", err)
	}
	if _, err := transaction.Execute(ctx, cancelQueuedJobSQL, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("remove delayed-start queued job: %w", err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit runner start: %w", err)
	}
	open = false
	return nil
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
	rows, err := transaction.Query(ctx, processingByMemberSQL, string(memberID), string(runnerPool))
	if err != nil {
		return nil, fmt.Errorf("query processing jobs: %w", err)
	}
	jobs, err := scanProcessingJobs(rows)
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
) (_ core.TransitionResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.TransitionResult{}, fmt.Errorf("begin processing recovery: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	result, err := transaction.Execute(ctx, recoverProcessingSQL, processing.Job.EnqueuedAt.UnixNano(),
		processing.Job.ID.String(), processing.ClaimedAt.UnixNano(), string(processing.InventoryEndpointID))
	if err != nil {
		return core.TransitionResult{}, fmt.Errorf("requeue expired processing: %w", err)
	}
	recovered, err := result.RowsAffected()
	if err != nil {
		return core.TransitionResult{}, fmt.Errorf("read processing recovery count: %w", err)
	}
	if recovered == 1 {
		if _, err = transaction.Execute(ctx, deleteRecoveredProcessingSQL, processing.Job.ID.String(),
			processing.ClaimedAt.UnixNano(), string(processing.InventoryEndpointID)); err != nil {
			return core.TransitionResult{}, fmt.Errorf("remove recovered processing: %w", err)
		}
	}
	if err = transaction.Commit(ctx); err != nil {
		return core.TransitionResult{}, fmt.Errorf("commit processing recovery: %w", err)
	}
	open = false
	return core.TransitionResult{Applied: recovered == 1}, nil
}

func (store *Store) RetryUnstartedProcessing(ctx context.Context, jobID core.JobID) (_ bool, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin retry processing job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	retried, err := retryUnstartedProcessing(ctx, transaction, jobID)
	if err != nil {
		return false, err
	}
	if err := transaction.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit processing retry: %w", err)
	}
	open = false
	return retried, nil
}

func retryUnstartedProcessing(ctx context.Context, transaction Transaction, jobID core.JobID) (bool, error) {
	result, err := transaction.Execute(ctx, retryProcessingSQL, jobID.String())
	if err != nil {
		return false, fmt.Errorf("return processing job to queue: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read processing retry count: %w", err)
	}
	if rowsAffected == 0 {
		return false, nil
	}
	if _, removalErr := transaction.Execute(ctx, deleteRetriedProcessingSQL, jobID.String()); removalErr != nil {
		return false, fmt.Errorf("remove retried processing ownership: %w", removalErr)
	}
	return true, nil
}

func (store *Store) RemoveTerminalProcessing(ctx context.Context, jobID core.JobID) (bool, error) {
	return store.executeConditionalTransaction(ctx, "remove terminal processing job", deleteTerminalProcessingSQL, jobID.String())
}

func (store *Store) RetryUnstarted(ctx context.Context, jobID core.JobID) (core.TransitionResult, error) {
	applied, err := store.RetryUnstartedProcessing(ctx, jobID)
	return core.TransitionResult{Applied: applied}, err
}

func (store *Store) RemoveTerminal(ctx context.Context, jobID core.JobID) (core.TransitionResult, error) {
	applied, err := store.RemoveTerminalProcessing(ctx, jobID)
	return core.TransitionResult{Applied: applied}, err
}
