package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

const queuedJobColumns = `job_id, source_name, source_reference, runner_pool, enqueued_at`
const processingJobColumns = `job_id, source_name, source_reference, runner_pool, member_id,
	runner_started, EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference) AS source_terminal`

const serializeRunnerPoolClaimSQL = `SELECT pg_advisory_xact_lock(1, hashtext($1))`
const serializeSourceSQL = `SELECT pg_advisory_xact_lock(2, hashtext($1 || chr(0) || $2))`

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
const selectOldestQueuedJobSQL = `SELECT ` + queuedJobColumns + `
	FROM public.runnerfoundry_queued_jobs
	WHERE runner_pool = $1
	ORDER BY enqueued_at LIMIT 1`
const takeQueuedJobSQL = `DELETE FROM public.runnerfoundry_queued_jobs WHERE job_id = $1
	RETURNING ` + queuedJobColumns
const insertProcessingJobSQL = `INSERT INTO public.runnerfoundry_processing_jobs
	(job_id, source_name, source_reference, runner_pool, member_id) VALUES($1, $2, $3, $4, $5)`
const insertTombstoneSQL = `INSERT INTO public.runnerfoundry_source_tombstones(source_name, source_reference)
	VALUES($1, $2) ON CONFLICT(source_name, source_reference) DO NOTHING`
const deleteQueuedJobBySourceSQL = `DELETE FROM public.runnerfoundry_queued_jobs
	WHERE source_name = $1 AND source_reference = $2`
const markProcessingStartedSQL = `UPDATE public.runnerfoundry_processing_jobs SET runner_started = true
	WHERE source_name = $1 AND source_reference = $2
	AND NOT EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones
		WHERE source_name = $1 AND source_reference = $2)`
const tombstoneDelayedStartSQL = `INSERT INTO public.runnerfoundry_source_tombstones(source_name, source_reference)
	SELECT $1, $2 WHERE EXISTS (SELECT 1 FROM public.runnerfoundry_queued_jobs
		WHERE source_name = $1 AND source_reference = $2)
	ON CONFLICT(source_name, source_reference) DO NOTHING`
const processingByMemberSQL = `SELECT ` + processingJobColumns + `
	FROM public.runnerfoundry_processing_jobs WHERE member_id = $1 AND runner_pool = $2`
const processingByIDSQL = `SELECT ` + processingJobColumns + `
	FROM public.runnerfoundry_processing_jobs WHERE job_id = $1`
const retryProcessingSQL = `INSERT INTO public.runnerfoundry_queued_jobs
	(job_id, source_name, source_reference, runner_pool)
	SELECT job_id, source_name, source_reference, runner_pool
	FROM public.runnerfoundry_processing_jobs processing
	WHERE processing.job_id = $1 AND NOT processing.runner_started
	AND NOT EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)
	RETURNING ` + queuedJobColumns
const deleteRetriedProcessingSQL = `DELETE FROM public.runnerfoundry_processing_jobs
	WHERE job_id = $1 AND NOT runner_started`
const deleteTerminalProcessingSQL = `DELETE FROM public.runnerfoundry_processing_jobs
	WHERE job_id = $1 AND EXISTS (
		SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = runnerfoundry_processing_jobs.source_name
		AND tombstones.source_reference = runnerfoundry_processing_jobs.source_reference)`

type Store struct{ executor Executor }

var _ core.JobQueueStore = (*Store)(nil)
var _ core.ProcessingJobStore = (*Store)(nil)

func NewStore(executor Executor) *Store { return &Store{executor: executor} }

func (store *Store) Enqueue(ctx context.Context, request core.JobRequest) (_ core.Job, _ bool, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.Job{}, false, fmt.Errorf("begin enqueue job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if _, lockErr := transaction.Execute(ctx, serializeSourceSQL, request.Source.Adapter, request.Source.ID); lockErr != nil {
		return core.Job{}, false, fmt.Errorf("serialize source enqueue: %w", lockErr)
	}
	job, created, err := enqueueInTransaction(ctx, transaction, request)
	if err != nil {
		return core.Job{}, false, err
	}
	if err := transaction.Commit(ctx); err != nil {
		return core.Job{}, false, fmt.Errorf("commit enqueue job: %w", err)
	}
	open = false
	return job, created, nil
}

func enqueueInTransaction(ctx context.Context, transaction Transaction, request core.JobRequest) (core.Job, bool, error) {
	job, err := scanQueuedJob(transaction.QueryRow(ctx, insertQueuedJobSQL,
		request.Source.Adapter, request.Source.ID, string(request.RunnerPool), request.EnqueuedAt))
	if err == nil {
		return job, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return core.Job{}, false, fmt.Errorf("insert queued job: %w", err)
	}
	job, err = scanQueuedJob(transaction.QueryRow(ctx, lookupQueuedJobBySourceSQL,
		request.Source.Adapter, request.Source.ID))
	if errors.Is(err, sql.ErrNoRows) {
		return core.Job{}, false, nil
	}
	if err != nil {
		return core.Job{}, false, fmt.Errorf("load existing queued job: %w", err)
	}
	return job, false, nil
}

func (store *Store) TakeOldest(
	ctx context.Context,
	runnerPool core.RunnerPool,
	memberID core.MemberID,
) (_ core.ProcessingJob, _ bool, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("begin take job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	processing, taken, err := takeOldestInTransaction(ctx, transaction, runnerPool, memberID)
	if err != nil {
		return core.ProcessingJob{}, false, err
	}
	if commitErr := transaction.Commit(ctx); commitErr != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("commit job pickup: %w", commitErr)
	}
	open = false
	return processing, taken, nil
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

func createProcessingOwnership(ctx context.Context, transaction Transaction, job core.Job, memberID core.MemberID) error {
	if _, err := transaction.Execute(ctx, insertProcessingJobSQL, job.ID, job.Source.Adapter,
		job.Source.ID, string(job.RunnerPool), memberID); err != nil {
		return fmt.Errorf("insert processing job: %w", err)
	}
	return nil
}

func (store *Store) TerminalizeSource(ctx context.Context, source core.SourceReference) (resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin terminalize source: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if _, err := transaction.Execute(ctx, serializeSourceSQL, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("serialize source terminalization: %w", err)
	}
	if _, err := transaction.Execute(ctx, insertTombstoneSQL, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("tombstone terminal source: %w", err)
	}
	if _, err := transaction.Execute(ctx, deleteQueuedJobBySourceSQL, source.Adapter, source.ID); err != nil {
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
	if _, err := transaction.Execute(ctx, serializeSourceSQL, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("serialize runner start: %w", err)
	}
	if _, err := transaction.Execute(ctx, markProcessingStartedSQL, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("mark processing runner started: %w", err)
	}
	if _, err := transaction.Execute(ctx, tombstoneDelayedStartSQL, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("tombstone delayed runner start: %w", err)
	}
	if _, err := transaction.Execute(ctx, deleteQueuedJobBySourceSQL, source.Adapter, source.ID); err != nil {
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
	rows, err := transaction.Query(ctx, processingByMemberSQL, memberID, string(runnerPool))
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
	processing, found, err := loadProcessingRetryCandidate(ctx, transaction, jobID)
	if err != nil || !found {
		return false, err
	}
	if serializationErr := serializeProcessingRetry(ctx, transaction, processing.Job.Source); serializationErr != nil {
		return false, serializationErr
	}
	requeued, err := enqueueProcessingRetry(ctx, transaction, jobID)
	if err != nil || !requeued {
		return false, err
	}
	if _, removalErr := transaction.Execute(ctx, deleteRetriedProcessingSQL, jobID); removalErr != nil {
		return false, fmt.Errorf("remove retried processing ownership: %w", removalErr)
	}
	return true, nil
}

func loadProcessingRetryCandidate(
	ctx context.Context,
	transaction Transaction,
	jobID core.JobID,
) (core.ProcessingJob, bool, error) {
	processing, err := scanProcessingJob(transaction.QueryRow(ctx, processingByIDSQL, jobID))
	if errors.Is(err, sql.ErrNoRows) {
		return core.ProcessingJob{}, false, nil
	}
	if err != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("load processing retry candidate: %w", err)
	}
	return processing, true, nil
}

func serializeProcessingRetry(ctx context.Context, transaction Transaction, source core.SourceReference) error {
	if _, err := transaction.Execute(ctx, serializeSourceSQL, source.Adapter, source.ID); err != nil {
		return fmt.Errorf("serialize processing retry: %w", err)
	}
	return nil
}

func enqueueProcessingRetry(ctx context.Context, transaction Transaction, jobID core.JobID) (bool, error) {
	_, err := scanQueuedJob(transaction.QueryRow(ctx, retryProcessingSQL, jobID))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("return processing job to queue: %w", err)
	}
	return true, nil
}

func (store *Store) RemoveTerminalProcessing(ctx context.Context, jobID core.JobID) (bool, error) {
	return store.executeConditionalTransaction(ctx, "remove terminal processing job", deleteTerminalProcessingSQL, jobID)
}
