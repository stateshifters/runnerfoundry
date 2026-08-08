package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

const selectGitHubActiveDemandSQL = `SELECT job_id, source_name, source_reference, 'queued' AS job_state,
		enqueued_at AS state_changed_at
	FROM public.runnerfoundry_queued_jobs
	WHERE runner_pool = $1 AND source_name = $2
	UNION ALL
	SELECT job_id, source_name, source_reference, 'processing' AS job_state,
		claimed_at AS state_changed_at
	FROM public.runnerfoundry_processing_jobs
	WHERE runner_pool = $1 AND source_name = $2`

type activeDemandJob struct {
	jobID          core.JobID
	source         core.SourceReference
	state          string
	stateChangedAt time.Time
	aggregate      bool
}

const deleteAggregateQueuedJobSQL = `DELETE FROM public.runnerfoundry_queued_jobs
	WHERE job_id = $1 AND source_name = $2 AND source_reference = $3`

var _ githubsource.AssignedDemandRepository = (*Store)(nil)

// ReconcileAssignedDemand atomically adjusts one GitHub repository and Runner
// Pool scope using ordinary durable jobs. Exact and processing demand count
// toward the desired total but cannot be removed; contraction deletes only the
// newest aggregate queued jobs. The opaque demand-scope lock serializes source
// observations without coupling other repositories in the same Runner Pool.
// Counts are returned only after commit; any protocol or commit uncertainty
// returns an error and no result.
func (store *Store) ReconcileAssignedDemand(
	ctx context.Context,
	demand githubsource.AssignedDemand,
) (_ githubsource.AssignedDemandResult, resultErr error) {
	request, err := githubsource.AggregateDemandRequest(demand, 1)
	if err != nil {
		return githubsource.AssignedDemandResult{}, fmt.Errorf("identify assigned demand scope: %w", err)
	}
	return store.reconcileValidAssignedDemand(ctx, demand, request.Scope)
}

func (store *Store) reconcileValidAssignedDemand(
	ctx context.Context,
	demand githubsource.AssignedDemand,
	scope core.DemandScope,
) (_ githubsource.AssignedDemandResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return githubsource.AssignedDemandResult{}, fmt.Errorf("begin assigned demand reconciliation: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if _, err = transaction.Execute(ctx, serializeDemandScopeSQL, scope.Adapter, scope.ID); err != nil {
		return githubsource.AssignedDemandResult{}, fmt.Errorf("serialize assigned demand reconciliation: %w", err)
	}
	result, err := reconcileAssignedDemandInTransaction(ctx, transaction, demand)
	if err != nil {
		return githubsource.AssignedDemandResult{}, err
	}
	if err = transaction.Commit(ctx); err != nil {
		return githubsource.AssignedDemandResult{}, fmt.Errorf("commit assigned demand reconciliation: %w", err)
	}
	open = false
	return result, nil
}

func reconcileAssignedDemandInTransaction(
	ctx context.Context,
	transaction Transaction,
	demand githubsource.AssignedDemand,
) (githubsource.AssignedDemandResult, error) {
	jobs, err := loadMatchingDemand(ctx, transaction, demand)
	if err != nil {
		return githubsource.AssignedDemandResult{}, err
	}
	result := countAssignedDemand(jobs)
	if err = addAssignedDemandDeficit(ctx, transaction, demand, &result); err != nil {
		return githubsource.AssignedDemandResult{}, err
	}
	if err = removeAggregateSurplus(ctx, transaction, jobs, demand.DesiredJobs, &result); err != nil {
		return githubsource.AssignedDemandResult{}, err
	}
	return result, nil
}

func addAssignedDemandDeficit(
	ctx context.Context,
	transaction Transaction,
	demand githubsource.AssignedDemand,
	result *githubsource.AssignedDemandResult,
) error {
	for ordinal := 1; result.ActiveJobs < demand.DesiredJobs && ordinal <= demand.DesiredJobs; ordinal++ {
		added, addErr := addAggregateDemandJob(ctx, transaction, demand, ordinal)
		if addErr != nil {
			return addErr
		}
		if added {
			result.ActiveJobs++
			result.QueuedJobs++
			result.JobsAdded++
		}
	}
	return nil
}

func loadMatchingDemand(
	ctx context.Context,
	transaction Transaction,
	demand githubsource.AssignedDemand,
) (_ []activeDemandJob, resultErr error) {
	rows, err := transaction.Query(ctx, selectGitHubActiveDemandSQL, string(demand.RunnerPool), githubsource.AdapterName)
	if err != nil {
		return nil, fmt.Errorf("query active GitHub demand: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	var jobs []activeDemandJob
	for rows.Next() {
		var job activeDemandJob
		var sourceAdapter, sourceIdentifier string
		if err := rows.Scan(&job.jobID, &sourceAdapter, &sourceIdentifier, &job.state, &job.stateChangedAt); err != nil {
			return nil, fmt.Errorf("scan active GitHub demand: %w", err)
		}
		job.source = core.SourceReference{Adapter: core.SourceAdapter(sourceAdapter), ID: core.SourceIdentifier(sourceIdentifier)}
		matches, aggregate := githubsource.ClassifyDemandIdentity(job.source, demand.Repository, demand.RunnerPool)
		if matches {
			job.aggregate = aggregate
			jobs = append(jobs, job)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active GitHub demand: %w", err)
	}
	return jobs, nil
}

func removeAggregateSurplus(
	ctx context.Context,
	transaction Transaction,
	jobs []activeDemandJob,
	desiredJobs int,
	result *githubsource.AssignedDemandResult,
) error {
	for _, candidate := range newestAggregateQueuedJobs(jobs) {
		if result.ActiveJobs <= desiredJobs {
			break
		}
		removed, err := removeAggregateDemandJob(ctx, transaction, candidate)
		if err != nil {
			return err
		}
		if removed {
			result.ActiveJobs--
			result.QueuedJobs--
			result.JobsRemoved++
		}
	}
	return nil
}

func newestAggregateQueuedJobs(jobs []activeDemandJob) []activeDemandJob {
	candidates := make([]activeDemandJob, 0, len(jobs))
	for _, job := range jobs {
		if job.state == "queued" && job.aggregate {
			candidates = append(candidates, job)
		}
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].stateChangedAt.Equal(candidates[right].stateChangedAt) {
			return candidates[left].jobID.String() > candidates[right].jobID.String()
		}
		return candidates[left].stateChangedAt.After(candidates[right].stateChangedAt)
	})
	return candidates
}

func removeAggregateDemandJob(ctx context.Context, transaction Transaction, candidate activeDemandJob) (bool, error) {
	if _, err := transaction.Execute(ctx, serializeSourceSQL, candidate.source.Adapter, candidate.source.ID); err != nil {
		return false, fmt.Errorf("serialize aggregate demand removal: %w", err)
	}
	removed, err := transaction.Execute(ctx, deleteAggregateQueuedJobSQL,
		candidate.jobID, candidate.source.Adapter, candidate.source.ID)
	if err != nil {
		return false, fmt.Errorf("remove surplus aggregate demand job: %w", err)
	}
	rowsAffected, err := removed.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read surplus aggregate demand removal count: %w", err)
	}
	return rowsAffected > 0, nil
}

func countAssignedDemand(jobs []activeDemandJob) githubsource.AssignedDemandResult {
	result := githubsource.AssignedDemandResult{ActiveJobs: len(jobs)}
	for _, job := range jobs {
		if job.state == "queued" {
			result.QueuedJobs++
		} else {
			result.ProcessingJobs++
		}
	}
	return result
}

func addAggregateDemandJob(
	ctx context.Context,
	transaction Transaction,
	demand githubsource.AssignedDemand,
	ordinal int,
) (bool, error) {
	request, err := githubsource.AggregateDemandRequest(demand, ordinal)
	if err != nil {
		return false, fmt.Errorf("identify aggregate demand job: %w", err)
	}
	if _, err = transaction.Execute(ctx, serializeSourceSQL, request.Source.Adapter, request.Source.ID); err != nil {
		return false, fmt.Errorf("serialize aggregate demand enqueue: %w", err)
	}
	_, outcome, err := enqueueInTransaction(ctx, transaction, request)
	if err != nil {
		return false, err
	}
	return outcome == core.QueueQueued, nil
}
