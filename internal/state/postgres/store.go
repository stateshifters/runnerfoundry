package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

const jobColumns = `job_id, source_name, source_reference, runner_pool, state, enqueued_at,
	member_id, claimed_at, started_at, acquisition_started_at, acquired_at, result, result_origin, result_reason`

const insertJobSQL = `INSERT INTO public.runnerfoundry_jobs
	(source_name, source_reference, runner_pool, state, enqueued_at)
	SELECT $1, $2, $3, 'queued', $4
	WHERE NOT EXISTS (SELECT 1 FROM public.runnerfoundry_source_receipts
		WHERE source_name = $5 AND source_reference = $6)
	ON CONFLICT(source_name, source_reference) DO NOTHING`

const lookupJobBySourceSQL = `SELECT ` + jobColumns + ` FROM public.runnerfoundry_jobs
	WHERE source_name = $1 AND source_reference = $2`
const lookupJobSQL = `SELECT ` + jobColumns + ` FROM public.runnerfoundry_jobs WHERE job_id = $1`
const attemptsByMemberSQL = `SELECT ` + jobColumns + ` FROM public.runnerfoundry_jobs
	WHERE member_id = $1 AND runner_pool = $2 AND state = 'attempting' ORDER BY claimed_at`
const pendingResultsSQL = `SELECT ` + jobColumns + ` FROM public.runnerfoundry_jobs
	WHERE state = 'result_pending' ORDER BY enqueued_at LIMIT $1`
const pendingAcquisitionsSQL = `SELECT ` + jobColumns + ` FROM public.runnerfoundry_jobs
	WHERE state = 'attempting' AND acquisition_started_at IS NOT NULL AND acquired_at IS NULL
	ORDER BY claimed_at LIMIT $1`
const beginAcquisitionSQL = `UPDATE public.runnerfoundry_jobs
	SET acquisition_started_at = COALESCE(acquisition_started_at, $1)
	WHERE job_id = $2 AND state = 'attempting' RETURNING ` + jobColumns
const confirmAcquisitionSQL = `UPDATE public.runnerfoundry_jobs SET acquired_at = COALESCE(acquired_at, $1)
	WHERE job_id = $2 AND state = 'attempting' AND acquisition_started_at IS NOT NULL RETURNING ` + jobColumns

const serializeRunnerPoolClaimSQL = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`
const serializeSourceSQL = `SELECT pg_advisory_xact_lock(hashtextextended($1 || chr(0) || $2, 0))`

const claimOldestSQL = `UPDATE public.runnerfoundry_jobs
	SET state = 'attempting', member_id = $1, claimed_at = $2
	WHERE job_id = (
		SELECT job_id FROM public.runnerfoundry_jobs
		WHERE runner_pool = $3 AND state = 'queued'
		ORDER BY enqueued_at LIMIT 1
		FOR UPDATE
	) AND state = 'queued'
	RETURNING ` + jobColumns

const markStartedBySourceSQL = `UPDATE public.runnerfoundry_jobs
	SET started_at = CASE WHEN state = 'attempting' THEN $1 ELSE started_at END
	WHERE source_name = $2 AND source_reference = $3 AND state IN ('attempting', 'result_pending')
	RETURNING ` + jobColumns
const setResultPendingSQL = `UPDATE public.runnerfoundry_jobs
	SET state = 'result_pending', result = $1, result_origin = $2, result_reason = $3
	WHERE job_id = $4 AND (state IN ('queued', 'attempting')
		OR (state = 'result_pending' AND result = $5 AND result_origin = $6 AND result_reason = $7))
	RETURNING ` + jobColumns
const setResultPendingBySourceSQL = `UPDATE public.runnerfoundry_jobs
	SET state = 'result_pending', result = $1, result_origin = $2, result_reason = $3
	WHERE source_name = $4 AND source_reference = $5 AND (state IN ('queued', 'attempting')
		OR (state = 'result_pending' AND result = $6 AND result_origin = $7 AND result_reason = $8))
	RETURNING ` + jobColumns
const setMemberAttemptsResultPendingSQL = `UPDATE public.runnerfoundry_jobs
	SET state = 'result_pending', result = $1, result_origin = $2, result_reason = $3
	WHERE member_id = $4 AND state = 'attempting' RETURNING ` + jobColumns
const setUnstartedAttemptsResultPendingSQL = `UPDATE public.runnerfoundry_jobs
	SET state = 'result_pending', result = $1, result_origin = $2, result_reason = $3
	WHERE state = 'attempting' AND runner_pool = $4 AND started_at IS NULL AND claimed_at <= $5 RETURNING ` + jobColumns
const insertReceiptSQL = `INSERT INTO public.runnerfoundry_source_receipts
	(source_name, source_reference, result, result_origin, delivery)
	VALUES ($1, $2, $3, $4, $5)`
const deleteJobSQL = `DELETE FROM public.runnerfoundry_jobs WHERE job_id = $1 AND state = 'result_pending'`
const lookupReceiptSQL = `SELECT source_name, source_reference, result, result_origin, delivery
	FROM public.runnerfoundry_source_receipts WHERE source_name = $1 AND source_reference = $2`

type Store struct{ executor Executor }

var _ core.JobStore = (*Store)(nil)

func NewStore(executor Executor) *Store { return &Store{executor: executor} }

func (store *Store) Enqueue(ctx context.Context, request core.JobRequest) (core.JobRecord, bool, error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.JobRecord{}, false, fmt.Errorf("begin enqueue job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open)
	if _, lockErr := transaction.Execute(ctx, serializeSourceSQL, request.Source.Adapter, request.Source.ID); lockErr != nil {
		return core.JobRecord{}, false, fmt.Errorf("serialize source enqueue: %w", lockErr)
	}
	result, err := transaction.Execute(ctx, insertJobSQL, request.Source.Adapter, request.Source.ID,
		string(request.RunnerPool), request.EnqueuedAt, request.Source.Adapter, request.Source.ID)
	if err != nil {
		return core.JobRecord{}, false, fmt.Errorf("insert queued job: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return core.JobRecord{}, false, fmt.Errorf("read inserted job count: %w", err)
	}
	record, finalized, err := loadEnqueuedJob(ctx, transaction, request.Source, rowsAffected)
	if err != nil {
		return core.JobRecord{}, false, err
	}
	open = false
	if err := transaction.Commit(ctx); err != nil {
		return core.JobRecord{}, false, fmt.Errorf("commit enqueue job: %w", err)
	}
	return record, !finalized && rowsAffected == 1, nil
}

func loadEnqueuedJob(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
	rowsAffected int64,
) (core.JobRecord, bool, error) {
	record, err := scanJobRow(transaction.QueryRow(ctx, lookupJobBySourceSQL, source.Adapter, source.ID))
	if err == nil {
		return record, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) || rowsAffected != 0 {
		return core.JobRecord{}, false, fmt.Errorf("load enqueued job: %w", err)
	}
	if _, receiptErr := scanReceiptRow(transaction.QueryRow(ctx, lookupReceiptSQL, source.Adapter, source.ID)); receiptErr != nil {
		return core.JobRecord{}, false, fmt.Errorf("confirm finalized source request: %w", receiptErr)
	}
	return core.JobRecord{}, true, nil
}

func (store *Store) ClaimOldest(
	ctx context.Context,
	runnerPool core.RunnerPool,
	memberID core.MemberID,
	claimedAt time.Time,
) (core.JobRecord, bool, error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.JobRecord{}, false, fmt.Errorf("begin claim job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open)
	record, claimed, err := claimOldestInTransaction(ctx, transaction, runnerPool, memberID, claimedAt)
	if err != nil {
		return core.JobRecord{}, false, err
	}
	open = false
	if err := transaction.Commit(ctx); err != nil {
		return core.JobRecord{}, false, fmt.Errorf("commit job claim: %w", err)
	}
	return record, claimed, nil
}

func claimOldestInTransaction(
	ctx context.Context,
	transaction Transaction,
	runnerPool core.RunnerPool,
	memberID core.MemberID,
	claimedAt time.Time,
) (core.JobRecord, bool, error) {
	if _, err := transaction.Execute(ctx, serializeRunnerPoolClaimSQL, string(runnerPool)); err != nil {
		return core.JobRecord{}, false, fmt.Errorf("serialize runner pool claim: %w", err)
	}
	record, err := scanJobRow(transaction.QueryRow(ctx, claimOldestSQL,
		memberID, claimedAt, string(runnerPool)))
	if errors.Is(err, sql.ErrNoRows) {
		return core.JobRecord{}, false, nil
	}
	if err != nil {
		return core.JobRecord{}, false, fmt.Errorf("claim oldest queued job: %w", err)
	}
	return record, true, nil
}

func (store *Store) MarkStartedBySource(
	ctx context.Context,
	source core.SourceReference,
	startedAt time.Time,
) (core.JobRecord, error) {
	return scanChangedJob(store.executor.QueryRow(ctx, markStartedBySourceSQL, startedAt, source.Adapter, source.ID),
		"mark job started")
}

func (store *Store) BeginAcquisition(ctx context.Context, jobID core.JobID, at time.Time) (core.JobRecord, error) {
	return scanChangedJob(store.executor.QueryRow(ctx, beginAcquisitionSQL, at, jobID), "begin job acquisition")
}

func (store *Store) ConfirmAcquisition(ctx context.Context, jobID core.JobID, at time.Time) (core.JobRecord, error) {
	return scanChangedJob(store.executor.QueryRow(ctx, confirmAcquisitionSQL, at, jobID), "confirm job acquisition")
}

func (store *Store) SetResultPending(ctx context.Context, jobID core.JobID, result core.Result) (core.JobRecord, error) {
	return store.changeResultPending(ctx, setResultPendingSQL, lookupJobSQL, result,
		[]any{jobID}, []any{jobID})
}

func (store *Store) SetResultPendingBySource(
	ctx context.Context,
	source core.SourceReference,
	result core.Result,
) (core.JobRecord, error) {
	return store.changeResultPending(ctx, setResultPendingBySourceSQL, lookupJobBySourceSQL, result,
		[]any{source.Adapter, source.ID}, []any{source.Adapter, source.ID})
}

func (store *Store) changeResultPending(
	ctx context.Context,
	statement, lookupStatement string,
	result core.Result,
	arguments, lookupArguments []any,
) (core.JobRecord, error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.JobRecord{}, fmt.Errorf("begin set job result pending: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open)
	record, err := resolveResultPendingRow(ctx, transaction, statement, lookupStatement, result, arguments, lookupArguments)
	if err != nil {
		return core.JobRecord{}, err
	}
	open = false
	if err := transaction.Commit(ctx); err != nil {
		return core.JobRecord{}, fmt.Errorf("commit job result pending: %w", err)
	}
	return record, nil
}

func resolveResultPendingRow(
	ctx context.Context,
	transaction Transaction,
	statement, lookupStatement string,
	result core.Result,
	arguments, lookupArguments []any,
) (core.JobRecord, error) {
	resultArguments := make([]any, 0, 3+len(arguments))
	resultArguments = append(resultArguments, string(result.Outcome), string(result.Origin), result.Reason)
	queryArguments := append(resultArguments, arguments...)
	queryArguments = append(queryArguments, resultArguments...)
	record, err := scanJobRow(transaction.QueryRow(ctx, statement, queryArguments...))
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return core.JobRecord{}, fmt.Errorf("set job result pending: %w", err)
		}
		existing, lookupErr := scanJobRow(transaction.QueryRow(ctx, lookupStatement, lookupArguments...))
		if lookupErr == nil && existing.Result != nil && *existing.Result != result {
			return core.JobRecord{}, core.ErrConflictingResult
		}
		return core.JobRecord{}, fmt.Errorf("set job result pending: %w", err)
	}
	return record, nil
}

func (store *Store) SetMemberAttemptsResultPending(
	ctx context.Context,
	memberID core.MemberID,
	result core.Result,
) ([]core.JobRecord, error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin member job cleanup: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open)
	rows, err := transaction.Query(ctx, setMemberAttemptsResultPendingSQL,
		string(result.Outcome), string(result.Origin), result.Reason, memberID)
	if err != nil {
		return nil, fmt.Errorf("set member jobs result pending: %w", err)
	}
	records, err := scanJobRows(rows)
	if err != nil {
		return nil, err
	}
	open = false
	if err := transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit member job cleanup: %w", err)
	}
	return records, nil
}

func (store *Store) SetUnstartedAttemptsResultPending(
	ctx context.Context,
	runnerPool core.RunnerPool,
	before time.Time,
	result core.Result,
) ([]core.JobRecord, error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin unstarted job cleanup: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open)
	rows, err := transaction.Query(ctx, setUnstartedAttemptsResultPendingSQL,
		string(result.Outcome), string(result.Origin), result.Reason, string(runnerPool), before)
	if err != nil {
		return nil, fmt.Errorf("set unstarted jobs result pending: %w", err)
	}
	records, err := scanJobRows(rows)
	if err != nil {
		return nil, err
	}
	open = false
	if err := transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit unstarted job cleanup: %w", err)
	}
	return records, nil
}

func (store *Store) Finalize(
	ctx context.Context,
	jobID core.JobID,
	delivery core.ResultDelivery,
) (core.Receipt, error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.Receipt{}, fmt.Errorf("begin finalize job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open)
	receipt, err := finalizeJobInTransaction(ctx, transaction, jobID, delivery)
	if err != nil {
		return core.Receipt{}, err
	}
	open = false
	if err := transaction.Commit(ctx); err != nil {
		return core.Receipt{}, fmt.Errorf("commit finalize job: %w", err)
	}
	return receipt, nil
}

func finalizeJobInTransaction(
	ctx context.Context,
	transaction Transaction,
	jobID core.JobID,
	delivery core.ResultDelivery,
) (core.Receipt, error) {
	record, err := scanJobRow(transaction.QueryRow(ctx, lookupJobSQL, jobID))
	if err != nil {
		return core.Receipt{}, fmt.Errorf("load job for finalization: %w", err)
	}
	if record.State != core.JobStateResultPending || record.Result == nil {
		return core.Receipt{}, errors.New("finalize job: result is not pending")
	}
	if _, err := transaction.Execute(ctx, serializeSourceSQL, record.Job.Source.Adapter, record.Job.Source.ID); err != nil {
		return core.Receipt{}, fmt.Errorf("serialize source finalization: %w", err)
	}
	if _, err := transaction.Execute(ctx, insertReceiptSQL, record.Job.Source.Adapter, record.Job.Source.ID,
		string(record.Result.Outcome), string(record.Result.Origin), string(delivery)); err != nil {
		return core.Receipt{}, fmt.Errorf("insert source receipt: %w", err)
	}
	if _, err := transaction.Execute(ctx, deleteJobSQL, jobID); err != nil {
		return core.Receipt{}, fmt.Errorf("delete finalized job: %w", err)
	}
	return core.Receipt{
		Source: record.Job.Source, Outcome: record.Result.Outcome,
		Origin: record.Result.Origin, Delivery: delivery,
	}, nil
}
