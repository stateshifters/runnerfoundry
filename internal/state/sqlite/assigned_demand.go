package sqlite

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

const assignedDemandJobsSQL = `SELECT job_id, source_name, source_reference, runner_pool, enqueued_at, 0
	FROM runnerfoundry_queued_jobs WHERE source_name = ? AND runner_pool = ?
	UNION ALL
	SELECT job_id, source_name, source_reference, runner_pool, claimed_at, 1
	FROM runnerfoundry_processing_jobs WHERE source_name = ? AND runner_pool = ?`
const removeAssignedDemandJobSQL = `DELETE FROM runnerfoundry_queued_jobs WHERE job_id = ?`

type assignedDemandJob struct {
	jobID      string
	source     core.SourceReference
	timestamp  int64
	processing bool
	aggregate  bool
}

var _ githubsource.AssignedDemandRepository = (*Store)(nil)

// ReconcileAssignedDemand atomically adjusts aggregate GitHub recovery demand
// using ordinary durable jobs. Matching exact requests and processing jobs
// count toward the desired total but are never removed; only the newest queued
// aggregate jobs may be contracted. A returned result describes committed
// state. Any protocol or commit error returns no potentially uncertain counts.
func (store *Store) ReconcileAssignedDemand(
	ctx context.Context,
	demand githubsource.AssignedDemand,
) (_ githubsource.AssignedDemandResult, resultErr error) {
	if err := demand.Validate(); err != nil {
		return githubsource.AssignedDemandResult{}, fmt.Errorf("validate assigned demand: %w", err)
	}
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return githubsource.AssignedDemandResult{}, fmt.Errorf("begin assigned demand reconciliation: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)

	result, err := reconcileAssignedDemandTransaction(ctx, transaction, demand)
	if err != nil {
		return githubsource.AssignedDemandResult{}, err
	}
	if err := transaction.Commit(ctx); err != nil {
		return githubsource.AssignedDemandResult{}, fmt.Errorf("commit assigned demand reconciliation: %w", err)
	}
	open = false
	return result, nil
}

func reconcileAssignedDemandTransaction(
	ctx context.Context,
	transaction Transaction,
	demand githubsource.AssignedDemand,
) (githubsource.AssignedDemandResult, error) {
	jobs, err := loadAssignedDemandJobs(ctx, transaction, demand)
	if err != nil {
		return githubsource.AssignedDemandResult{}, err
	}
	result := summarizeAssignedDemandJobs(jobs)
	if result.ActiveJobs > demand.DesiredJobs {
		removed, removeErr := removeSurplusAssignedDemand(ctx, transaction, jobs, result.ActiveJobs-demand.DesiredJobs)
		if removeErr != nil {
			return githubsource.AssignedDemandResult{}, removeErr
		}
		result.ActiveJobs -= removed
		result.QueuedJobs -= removed
		result.JobsRemoved = removed
	}
	if err := addMissingAssignedDemand(ctx, transaction, demand, &result); err != nil {
		return githubsource.AssignedDemandResult{}, err
	}
	return result, nil
}

func addMissingAssignedDemand(
	ctx context.Context,
	transaction Transaction,
	demand githubsource.AssignedDemand,
	result *githubsource.AssignedDemandResult,
) error {
	for ordinal := 1; result.ActiveJobs < demand.DesiredJobs && ordinal <= demand.DesiredJobs; ordinal++ {
		queued, queueErr := queueAggregateAssignedDemand(ctx, transaction, demand, ordinal)
		if queueErr != nil {
			return queueErr
		}
		if queued.Outcome == core.QueueQueued {
			result.ActiveJobs++
			result.QueuedJobs++
			result.JobsAdded++
		}
	}
	return nil
}

func queueAggregateAssignedDemand(
	ctx context.Context,
	transaction Transaction,
	demand githubsource.AssignedDemand,
	ordinal int,
) (core.EnqueueResult, error) {
	request, err := githubsource.AggregateDemandRequest(demand, ordinal)
	if err != nil {
		return core.EnqueueResult{}, fmt.Errorf("identify aggregate assigned demand: %w", err)
	}
	return queueInTransaction(ctx, transaction, request)
}

func removeSurplusAssignedDemand(
	ctx context.Context,
	transaction Transaction,
	jobs []assignedDemandJob,
	surplus int,
) (int, error) {
	candidates := aggregateRemovalCandidates(jobs)
	surplus = min(surplus, len(candidates))
	for index := 0; index < surplus; index++ {
		if err := removeAssignedDemandJob(ctx, transaction, candidates[index].jobID); err != nil {
			return 0, err
		}
	}
	return surplus, nil
}

func aggregateRemovalCandidates(jobs []assignedDemandJob) []assignedDemandJob {
	candidates := make([]assignedDemandJob, 0, len(jobs))
	for _, job := range jobs {
		if job.aggregate && !job.processing {
			candidates = append(candidates, job)
		}
	}
	sort.Slice(candidates, func(left, right int) bool {
		return assignedDemandJobIsNewer(candidates[left], candidates[right])
	})
	return candidates
}

func assignedDemandJobIsNewer(left, right assignedDemandJob) bool {
	if left.timestamp == right.timestamp {
		return left.jobID > right.jobID
	}
	return left.timestamp > right.timestamp
}

func removeAssignedDemandJob(ctx context.Context, transaction Transaction, jobID string) error {
	result, err := transaction.Execute(ctx, removeAssignedDemandJobSQL, jobID)
	if err != nil {
		return fmt.Errorf("remove surplus assigned demand: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read removed assigned demand count: %w", err)
	}
	if removed != 1 {
		return fmt.Errorf("remove surplus assigned demand: expected one row, got %d", removed)
	}
	return nil
}

func loadAssignedDemandJobs(
	ctx context.Context,
	transaction Transaction,
	demand githubsource.AssignedDemand,
) ([]assignedDemandJob, error) {
	rows, err := transaction.Query(ctx, assignedDemandJobsSQL,
		githubsource.AdapterName, string(demand.RunnerPool), githubsource.AdapterName, string(demand.RunnerPool))
	if err != nil {
		return nil, fmt.Errorf("query assigned demand jobs: %w", err)
	}
	return scanAssignedDemandJobs(rows, demand)
}

func scanAssignedDemandJobs(
	rows Rows,
	demand githubsource.AssignedDemand,
) (jobs []assignedDemandJob, resultErr error) {
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		var job assignedDemandJob
		var sourceAdapter, sourceIdentifier, runnerPool string
		if err := rows.Scan(&job.jobID, &sourceAdapter, &sourceIdentifier, &runnerPool,
			&job.timestamp, &job.processing); err != nil {
			return nil, fmt.Errorf("scan assigned demand job: %w", err)
		}
		job.source = core.SourceReference{
			Adapter: core.SourceAdapter(sourceAdapter), ID: core.SourceIdentifier(sourceIdentifier),
		}
		matches, aggregate := githubsource.ClassifyDemandIdentity(job.source, demand.Repository, demand.RunnerPool)
		if matches && core.RunnerPool(runnerPool) == demand.RunnerPool {
			job.aggregate = aggregate
			jobs = append(jobs, job)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate assigned demand jobs: %w", err)
	}
	return jobs, nil
}

func summarizeAssignedDemandJobs(jobs []assignedDemandJob) githubsource.AssignedDemandResult {
	result := githubsource.AssignedDemandResult{ActiveJobs: len(jobs)}
	for _, job := range jobs {
		if job.processing {
			result.ProcessingJobs++
		} else {
			result.QueuedJobs++
		}
	}
	return result
}
