package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
	statecontract "github.com/stateshifters/runnerfoundry/internal/state"
)

const activeJobsSQL = `SELECT job_id, source_name, source_reference, runner_pool, 'queued' AS job_phase,
		enqueued_at AS state_changed_at
	FROM public.runnerfoundry_queued_jobs
	WHERE source_name = $1 AND runner_pool = $2
	UNION ALL
	SELECT job_id, source_name, source_reference, runner_pool, 'processing' AS job_phase,
		claimed_at AS state_changed_at
	FROM public.runnerfoundry_processing_jobs
	WHERE source_name = $1 AND runner_pool = $2`

const removeQueuedJobSQL = `DELETE FROM public.runnerfoundry_queued_jobs
	WHERE job_id = $1 AND source_name = $2 AND source_reference = $3
		AND runner_pool = $4 AND enqueued_at = $5`

type demandTransaction struct {
	transaction Transaction
	key         statecontract.DemandTransactionKey
}

var _ statecontract.DemandTransactionStore = (*Store)(nil)
var _ statecontract.DemandTransaction = demandTransaction{}

func (store *Store) WithDemandTransaction(
	ctx context.Context,
	key statecontract.DemandTransactionKey,
	reconcile func(statecontract.DemandTransaction) error,
) (resultErr error) {
	if !key.Scope.IsValid() || key.RunnerPool == "" {
		return errors.New("invalid demand transaction key")
	}
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		if err := serializeDemandDomain(ctx, transaction, key.Scope.Adapter, key.RunnerPool); err != nil {
			return fmt.Errorf("serialize demand reconciliation domain: %w", err)
		}
		if err := serializeDemandScope(ctx, transaction, key.Scope); err != nil {
			return fmt.Errorf("serialize demand reconciliation: %w", err)
		}
		return reconcile(demandTransaction{transaction: transaction, key: key})
	})
	if err != nil {
		return fmt.Errorf("reconcile demand: %w", err)
	}
	return nil
}

func (demand demandTransaction) ActiveJobs(
	ctx context.Context,
) ([]statecontract.ActiveJob, error) {
	var jobs []statecontract.ActiveJob
	err := demand.transaction.VisitRows(ctx, activeJobsSQL, func(row Scanner) error {
		job, err := scanActiveJob(row)
		if err != nil {
			return err
		}
		jobs = append(jobs, job)
		return nil
	}, string(demand.key.Scope.Adapter), string(demand.key.RunnerPool))
	if err != nil {
		return nil, fmt.Errorf("query active jobs: %w", err)
	}
	return jobs, nil
}

func scanActiveJob(row Scanner) (statecontract.ActiveJob, error) {
	var job statecontract.ActiveJob
	var sourceAdapter, sourceIdentifier, runnerPool, phase string
	if err := row.Scan(&job.ID, &sourceAdapter, &sourceIdentifier, &runnerPool,
		&phase, &job.StateChangedAt); err != nil {
		return statecontract.ActiveJob{}, fmt.Errorf("scan active job: %w", err)
	}
	job.Source = core.SourceReference{Adapter: core.SourceAdapter(sourceAdapter), ID: core.SourceIdentifier(sourceIdentifier)}
	job.RunnerPool = core.RunnerPool(runnerPool)
	job.Phase = statecontract.ActiveJobPhase(phase)
	return job, nil
}

func (demand demandTransaction) Queue(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	if err := request.Validate(); err != nil {
		return core.EnqueueResult{}, fmt.Errorf("validate demand enqueue request: %w", err)
	}
	if request.Scope != demand.key.Scope || request.Source.Adapter != demand.key.Scope.Adapter ||
		request.RunnerPool != demand.key.RunnerPool {
		return core.EnqueueResult{}, fmt.Errorf("%w: queued request does not match transaction key",
			statecontract.ErrDemandTransactionMismatch)
	}
	if err := serializeSource(ctx, demand.transaction, request.Source); err != nil {
		return core.EnqueueResult{}, fmt.Errorf("serialize demand enqueue: %w", err)
	}
	job, outcome, err := enqueueInTransaction(ctx, demand.transaction, request)
	if err != nil {
		return core.EnqueueResult{}, fmt.Errorf("enqueue demand job: %w", err)
	}
	return core.EnqueueResult{Job: job, Outcome: outcome}, nil
}

func (demand demandTransaction) RemoveQueued(
	ctx context.Context,
	job statecontract.ActiveJob,
) (statecontract.QueuedRemovalOutcome, error) {
	if job.Source.Adapter != demand.key.Scope.Adapter || job.RunnerPool != demand.key.RunnerPool {
		return "", fmt.Errorf("%w: queued job does not match transaction key",
			statecontract.ErrDemandTransactionMismatch)
	}
	if job.Phase != statecontract.ActiveJobQueued {
		return statecontract.QueuedJobMissing, nil
	}
	if err := serializeSource(ctx, demand.transaction, job.Source); err != nil {
		return "", fmt.Errorf("serialize queued demand removal: %w", err)
	}
	rowsAffected, err := demand.transaction.Execute(ctx, removeQueuedJobSQL, job.ID, job.Source.Adapter,
		job.Source.ID, string(job.RunnerPool), job.StateChangedAt)
	if err != nil {
		return "", fmt.Errorf("remove queued demand job: %w", err)
	}
	if rowsAffected == 0 {
		return statecontract.QueuedJobMissing, nil
	}
	return statecontract.QueuedJobRemoved, nil
}
