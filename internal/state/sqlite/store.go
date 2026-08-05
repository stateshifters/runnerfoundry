package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

const queuedJobColumns = `job_id, source_name, source_reference, runner_pool, enqueued_at`

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
const insertTombstoneSQL = `INSERT INTO runnerfoundry_source_tombstones(source_name, source_reference)
	VALUES (?, ?) ON CONFLICT(source_name, source_reference) DO NOTHING`
const deleteQueuedJobSQL = `DELETE FROM runnerfoundry_queued_jobs WHERE job_id = ?`
const cancelQueuedJobSQL = `DELETE FROM runnerfoundry_queued_jobs
	WHERE source_name = ? AND source_reference = ?`
const processingByMemberSQL = `SELECT job_id, source_name, source_reference, runner_pool, member_id
	FROM runnerfoundry_processing_jobs WHERE member_id = ? AND runner_pool = ?`
const deleteProcessingJobSQL = `DELETE FROM runnerfoundry_processing_jobs WHERE job_id = ?`

type Store struct {
	executor Executor
	close    func() error
}

var _ core.JobStore = (*Store)(nil)

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

func moveOldestToProcessing(
	ctx context.Context,
	transaction Transaction,
	runnerPool core.RunnerPool,
	memberID core.MemberID,
) (core.ProcessingJob, bool, error) {
	job, err := scanJob(transaction.QueryRow(ctx, oldestQueuedJobSQL, string(runnerPool)))
	if errors.Is(err, sql.ErrNoRows) {
		return core.ProcessingJob{}, false, nil
	}
	if err != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("select oldest queued job: %w", err)
	}
	if _, err := transaction.Execute(ctx, insertProcessingJobSQL, job.ID.String(), job.Source.Adapter,
		job.Source.ID, string(job.RunnerPool), memberID.String()); err != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("insert processing job: %w", err)
	}
	if _, err := transaction.Execute(ctx, insertTombstoneSQL, job.Source.Adapter, job.Source.ID); err != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("tombstone processing job: %w", err)
	}
	if _, err := transaction.Execute(ctx, deleteQueuedJobSQL, job.ID.String()); err != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("remove job from queue: %w", err)
	}
	return core.ProcessingJob{Job: job, MemberID: memberID}, true, nil
}

func (store *Store) CancelQueued(ctx context.Context, source core.SourceReference) (resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin cancel queued job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if _, err := transaction.Execute(ctx, insertTombstoneSQL, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("tombstone canceled job: %w", err)
	}
	if _, err := transaction.Execute(ctx, cancelQueuedJobSQL, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("remove canceled queued job: %w", err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit cancel queued job: %w", err)
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
	rows, err := transaction.Query(ctx, processingByMemberSQL, memberID.String(), string(runnerPool))
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

func (store *Store) RemoveProcessing(ctx context.Context, jobID core.JobID) error {
	return store.executeTransaction(ctx, "remove processing job", deleteProcessingJobSQL, jobID.String())
}
