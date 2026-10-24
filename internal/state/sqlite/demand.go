package sqlite

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
	"time"
)

type demandTransaction struct {
	transaction Transaction
	key         state.DemandTransactionKey
}

func (store *Store) WithDemandTransaction(ctx context.Context, key state.DemandTransactionKey, reconcile func(state.DemandTransaction) error) error {
	if !key.Scope.IsValid() || key.RunnerPool == "" {
		return errors.New("invalid demand scope")
	}
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		return reconcile(demandTransaction{transaction: transaction, key: key})
	})
	if err != nil {
		return fmt.Errorf("reconcile demand: %w", err)
	}
	return nil
}
func (demand demandTransaction) Queue(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	if err := request.Validate(); err != nil {
		return core.EnqueueResult{}, fmt.Errorf("validate transactional demand: %w", err)
	}
	if request.Scope != demand.key.Scope || request.Source.Adapter != demand.key.Scope.Adapter || request.RunnerPool != demand.key.RunnerPool {
		return core.EnqueueResult{}, state.ErrDemandTransactionMismatch
	}
	return enqueueInTransaction(ctx, demand.transaction, request)
}
func (demand demandTransaction) RemoveQueued(ctx context.Context, job state.ActiveJob) (state.QueuedRemovalOutcome, error) {
	if job.Source.Adapter != demand.key.Scope.Adapter || job.RunnerPool != demand.key.RunnerPool {
		return "", state.ErrDemandTransactionMismatch
	}
	if job.Phase != state.ActiveJobQueued {
		return state.QueuedJobMissing, nil
	}
	removed, err := demand.transaction.Execute(ctx, `DELETE FROM runnerfoundry_queued_jobs
 WHERE job_id = ? AND source_name = ? AND source_reference = ? AND runner_pool = ? AND enqueued_at = ?`,
		job.ID.String(), job.Source.Adapter, job.Source.ID, string(job.RunnerPool), job.StateChangedAt.UnixNano())
	if err != nil {
		return "", fmt.Errorf("remove queued demand: %w", err)
	}
	if removed == 0 {
		return state.QueuedJobMissing, nil
	}
	return state.QueuedJobRemoved, nil
}
func (demand demandTransaction) ActiveJobs(ctx context.Context) ([]state.ActiveJob, error) {
	var jobs []state.ActiveJob
	err := demand.transaction.VisitRows(ctx, `SELECT job_id, source_name, source_reference, runner_pool, enqueued_at, 'queued'
 FROM runnerfoundry_queued_jobs WHERE source_name = ? AND runner_pool = ?
 UNION ALL
 SELECT job_id, processing.source_name, processing.source_reference, runner_pool, claimed_at, 'processing'
 FROM runnerfoundry_processing_jobs processing WHERE source_name = ? AND runner_pool = ?
 AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
	WHERE tombstones.source_name = processing.source_name
	AND tombstones.source_reference = processing.source_reference)`,
		func(row Scanner) error {
			job, err := scanActiveJob(row)
			if err != nil {
				return err
			}
			jobs = append(jobs, job)
			return nil
		}, string(demand.key.Scope.Adapter), string(demand.key.RunnerPool), string(demand.key.Scope.Adapter), string(demand.key.RunnerPool))
	if err != nil {
		return nil, fmt.Errorf("read active demand: %w", err)
	}
	return jobs, nil
}
func scanActiveJob(row Scanner) (state.ActiveJob, error) {
	var jobID, adapter, reference, runnerPool, phase string
	var changedAt int64
	if err := row.Scan(&jobID, &adapter, &reference, &runnerPool, &changedAt, &phase); err != nil {
		return state.ActiveJob{}, fmt.Errorf("scan active demand: %w", err)
	}
	identity, err := uuid.Parse(jobID)
	if err != nil {
		return state.ActiveJob{}, fmt.Errorf("parse active demand identity: %w", err)
	}
	return state.ActiveJob{ID: identity, Source: core.SourceReference{Adapter: core.SourceAdapter(adapter), ID: core.SourceIdentifier(reference)},
		RunnerPool: core.RunnerPool(runnerPool), Phase: state.ActiveJobPhase(phase), StateChangedAt: time.Unix(0, changedAt).UTC()}, nil
}
