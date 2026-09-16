package persistence

import (
	"context"
	"fmt"
	"sort"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

type AssignedDemandStore struct {
	transactions state.DemandTransactionStore
}

var _ githubsource.AssignedDemandRepository = (*AssignedDemandStore)(nil)

func NewAssignedDemandStore(transactions state.DemandTransactionStore) *AssignedDemandStore {
	return &AssignedDemandStore{transactions: transactions}
}

func (store *AssignedDemandStore) ReconcileAssignedDemand(
	ctx context.Context,
	demand githubsource.AssignedDemand,
) (githubsource.AssignedDemandResult, error) {
	requests, err := assignedDemandRequests(demand)
	if err != nil {
		return githubsource.AssignedDemandResult{}, err
	}
	var result githubsource.AssignedDemandResult
	err = store.transactions.WithDemandTransaction(ctx, state.DemandTransactionKey{Scope: requests[0].Scope, RunnerPool: demand.RunnerPool}, func(transaction state.DemandTransaction) error {
		transactionResult, reconcileErr := reconcileAssignedDemand(ctx, transaction, demand, requests)
		if reconcileErr != nil {
			return reconcileErr
		}
		result = transactionResult
		return nil
	})
	if err != nil {
		return githubsource.AssignedDemandResult{}, fmt.Errorf("reconcile assigned demand: %w", err)
	}
	return result, nil
}

func assignedDemandRequests(demand githubsource.AssignedDemand) ([]core.JobRequest, error) {
	requestCount := max(demand.DesiredJobs, 1)
	requests := make([]core.JobRequest, requestCount)
	for ordinal := 1; ordinal <= requestCount; ordinal++ {
		request, err := githubsource.AggregateDemandRequest(demand, ordinal)
		if err != nil {
			return nil, fmt.Errorf("identify assigned demand scope: %w", err)
		}
		requests[ordinal-1] = request
	}
	return requests, nil
}

func reconcileAssignedDemand(
	ctx context.Context,
	transaction state.DemandTransaction,
	demand githubsource.AssignedDemand,
	requests []core.JobRequest,
) (githubsource.AssignedDemandResult, error) {
	jobs, err := transaction.ActiveJobs(ctx)
	if err != nil {
		return githubsource.AssignedDemandResult{}, fmt.Errorf("load active GitHub demand: %w", err)
	}
	matchingJobs := matchingAssignedDemandJobs(jobs, demand)
	result := summarizeAssignedDemand(matchingJobs)
	if err = removeAggregateSurplus(ctx, transaction, matchingJobs, demand, &result); err != nil {
		return githubsource.AssignedDemandResult{}, err
	}
	if err = addAggregateDeficit(ctx, transaction, requests, demand.DesiredJobs, &result); err != nil {
		return githubsource.AssignedDemandResult{}, err
	}
	return result, nil
}

func matchingAssignedDemandJobs(jobs []state.ActiveJob, demand githubsource.AssignedDemand) []state.ActiveJob {
	matching := make([]state.ActiveJob, 0, len(jobs))
	for _, job := range jobs {
		matches, _ := githubsource.ClassifyDemandIdentity(job.Source, demand.Repository, demand.RunnerPool)
		if matches && job.RunnerPool == demand.RunnerPool {
			matching = append(matching, job)
		}
	}
	return matching
}

func summarizeAssignedDemand(jobs []state.ActiveJob) githubsource.AssignedDemandResult {
	result := githubsource.AssignedDemandResult{ActiveJobs: len(jobs)}
	for _, job := range jobs {
		if job.Phase == state.ActiveJobQueued {
			result.QueuedJobs++
		} else {
			result.ProcessingJobs++
		}
	}
	return result
}

func removeAggregateSurplus(
	ctx context.Context,
	transaction state.DemandTransaction,
	jobs []state.ActiveJob,
	demand githubsource.AssignedDemand,
	result *githubsource.AssignedDemandResult,
) error {
	for _, candidate := range aggregateRemovalCandidates(jobs, demand) {
		if result.ActiveJobs <= demand.DesiredJobs {
			break
		}
		outcome, err := transaction.RemoveQueued(ctx, candidate)
		if err != nil {
			return fmt.Errorf("remove surplus aggregate demand: %w", err)
		}
		if outcome == state.QueuedJobRemoved {
			result.ActiveJobs--
			result.QueuedJobs--
			result.JobsRemoved++
		}
	}
	return nil
}

func aggregateRemovalCandidates(jobs []state.ActiveJob, demand githubsource.AssignedDemand) []state.ActiveJob {
	candidates := make([]state.ActiveJob, 0, len(jobs))
	for _, job := range jobs {
		_, aggregate := githubsource.ClassifyDemandIdentity(job.Source, demand.Repository, demand.RunnerPool)
		if job.Phase == state.ActiveJobQueued && aggregate {
			candidates = append(candidates, job)
		}
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].StateChangedAt.Equal(candidates[right].StateChangedAt) {
			return candidates[left].ID.String() > candidates[right].ID.String()
		}
		return candidates[left].StateChangedAt.After(candidates[right].StateChangedAt)
	})
	return candidates
}

func addAggregateDeficit(
	ctx context.Context,
	transaction state.DemandTransaction,
	requests []core.JobRequest,
	desiredJobs int,
	result *githubsource.AssignedDemandResult,
) error {
	for index := 0; result.ActiveJobs < desiredJobs && index < desiredJobs; index++ {
		queued, err := transaction.Queue(ctx, requests[index])
		if err != nil {
			return fmt.Errorf("enqueue aggregate demand job: %w", err)
		}
		if queued.Outcome == core.QueueQueued {
			result.ActiveJobs++
			result.QueuedJobs++
			result.JobsAdded++
		}
	}
	return nil
}
