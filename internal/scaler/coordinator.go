package scaler

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type JobDemand struct {
	RequestID  int64
	Repository string
	QueueTime  time.Time
	Acquired   bool
}

type RepositoryWorker interface {
	Running(ctx context.Context) (int, error)
	Exists(ctx context.Context, requestID int64) (bool, error)
	Acquire(ctx context.Context, requestID int64) error
	StartOne(ctx context.Context, requestID int64) error
	StartAssigned(ctx context.Context) error
}

type demandKey struct {
	Repository string
	RequestID  int64
}

type QueuedDemand struct {
	JobDemand
	observationOrder uint64
}

type DemandRemoval struct {
	Repository string
	RequestID  int64
}

type DemandStore interface {
	Apply(ctx context.Context, scaleSet string, observed []JobDemand, removed []DemandRemoval) error
	RemoveRepository(ctx context.Context, scaleSet string, repository string) error
	RemoveScaleSet(ctx context.Context, scaleSet string) error
	Ordered(ctx context.Context, scaleSet string) ([]QueuedDemand, error)
}

type Coordinator struct {
	maxRunners int
	scaleSet   string
	store      DemandStore
	workers    map[string]RepositoryWorker
	assigned   map[string]int
	mutex      sync.Mutex
}

type AssignedReconciliation struct {
	AssignedJobs   int
	RunningRunners int
	StartedRunners int
}

func NewCoordinator(maxRunners int) *Coordinator {
	return NewDurableCoordinator("", maxRunners, createMemoryDemandStore())
}

func NewDurableCoordinator(scaleSet string, maxRunners int, store DemandStore) *Coordinator {
	return &Coordinator{
		maxRunners: maxRunners,
		scaleSet:   scaleSet,
		store:      store,
		workers:    map[string]RepositoryWorker{},
		assigned:   map[string]int{},
	}
}

func (coordinator *Coordinator) AddRepository(repository string, worker RepositoryWorker) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.workers[repository] = worker
}

func (coordinator *Coordinator) RemoveRepository(ctx context.Context, repository string) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	delete(coordinator.workers, repository)
	delete(coordinator.assigned, repository)
	if err := coordinator.store.RemoveRepository(ctx, coordinator.scaleSet, repository); err != nil {
		return fmt.Errorf("remove repository demand: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) Observe(demand JobDemand) {
	_ = coordinator.Apply(context.Background(), []JobDemand{demand}, nil) //nolint:errcheck,gosec // Compatibility helper has no error return; durable callers use Apply.
}

func (coordinator *Coordinator) Apply(ctx context.Context, observed []JobDemand, removed []DemandRemoval) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if err := coordinator.store.Apply(ctx, coordinator.scaleSet, observed, removed); err != nil {
		return fmt.Errorf("apply demand changes: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) Remove(repository string, requestID int64) {
	_ = coordinator.Apply(context.Background(), nil, []DemandRemoval{{Repository: repository, RequestID: requestID}}) //nolint:errcheck,gosec // Compatibility helper has no error return; durable callers use Apply.
}

// ObserveAssigned replaces a repository's authoritative GitHub assigned-job count.
// It is safe for concurrent use and defers provisioning until the next scheduling pass.
func (coordinator *Coordinator) ObserveAssigned(repository string, assignedJobs int) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.assigned[repository] = assignedJobs
}

func (coordinator *Coordinator) Schedule(ctx context.Context) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	_, err := coordinator.reconcileCapacity(ctx)
	return err
}

func (coordinator *Coordinator) reconcileCapacity(ctx context.Context) (map[string]AssignedReconciliation, error) {
	runningCount, repositoryRunningCounts, err := coordinator.runningCounts(ctx)
	if err != nil {
		return nil, err
	}
	orderedDemand, err := coordinator.store.Ordered(ctx, coordinator.scaleSet)
	if err != nil {
		return nil, fmt.Errorf("load queued demand: %w", err)
	}
	remainingDemand, err := coordinator.removeRecoveredDemand(ctx, orderedDemand)
	if err != nil {
		return nil, err
	}
	runningCount, err = coordinator.admitRemainingDemand(ctx, remainingDemand, runningCount, repositoryRunningCounts)
	if err != nil {
		return nil, err
	}
	return coordinator.reconcileRetainedAssigned(ctx, runningCount, repositoryRunningCounts)
}

// ReconcileAssigned starts enough identity-free runners for one repository's
// assigned-job statistic without exceeding shared scale-set capacity.
func (coordinator *Coordinator) ReconcileAssigned(ctx context.Context, repository string, assignedJobs int) (AssignedReconciliation, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.assigned[repository] = assignedJobs
	reconciliations, err := coordinator.reconcileCapacity(ctx)
	if err != nil {
		return AssignedReconciliation{}, err
	}
	return reconciliations[repository], nil
}

func (coordinator *Coordinator) provisionAssignedForRepository(
	ctx context.Context,
	repository string,
	assignedJobs int,
	runningCount int,
	repositoryRunningCounts map[string]int,
) (AssignedReconciliation, error) {
	worker := coordinator.workers[repository]
	if worker == nil {
		return AssignedReconciliation{AssignedJobs: assignedJobs}, nil
	}
	repositoryRunningCount := repositoryRunningCounts[repository]
	missingAssignedRunners := assignedJobs - repositoryRunningCount
	availableCapacity := coordinator.maxRunners - runningCount
	result := AssignedReconciliation{AssignedJobs: assignedJobs, RunningRunners: repositoryRunningCount}
	for missingAssignedRunners > 0 && availableCapacity > 0 {
		if err := worker.StartAssigned(ctx); err != nil {
			return AssignedReconciliation{}, fmt.Errorf("start assigned runner for repository %q: %w", repository, err)
		}
		missingAssignedRunners--
		availableCapacity--
		repositoryRunningCount++
		repositoryRunningCounts[repository] = repositoryRunningCount
		result.StartedRunners++
	}
	return result, nil
}

func (coordinator *Coordinator) reconcileRetainedAssigned(
	ctx context.Context,
	runningCount int,
	repositoryRunningCounts map[string]int,
) (map[string]AssignedReconciliation, error) {
	repositories := make([]string, 0, len(coordinator.assigned))
	for repository := range coordinator.assigned {
		repositories = append(repositories, repository)
	}
	sort.Strings(repositories)
	reconciliations := make(map[string]AssignedReconciliation, len(repositories))
	for _, repository := range repositories {
		result, err := coordinator.provisionAssignedForRepository(
			ctx, repository, coordinator.assigned[repository], runningCount, repositoryRunningCounts,
		)
		if err != nil {
			return nil, err
		}
		reconciliations[repository] = result
		runningCount += result.StartedRunners
	}
	return reconciliations, nil
}

func (coordinator *Coordinator) removeRecoveredDemand(ctx context.Context, orderedDemand []QueuedDemand) ([]QueuedDemand, error) {
	remainingDemand := orderedDemand[:0]
	for _, demand := range orderedDemand {
		worker := coordinator.workers[demand.Repository]
		if worker == nil {
			remainingDemand = append(remainingDemand, demand)
			continue
		}
		exists, existsErr := worker.Exists(ctx, demand.RequestID)
		if existsErr != nil {
			return nil, fmt.Errorf("recover runner for repository %q: %w", demand.Repository, existsErr)
		}
		if !exists {
			remainingDemand = append(remainingDemand, demand)
			continue
		}
	}
	return remainingDemand, nil
}

func (coordinator *Coordinator) admitRemainingDemand(
	ctx context.Context,
	remainingDemand []QueuedDemand,
	runningCount int,
	repositoryRunningCounts map[string]int,
) (int, error) {
	for _, demand := range remainingDemand {
		if runningCount >= coordinator.maxRunners {
			return runningCount, nil
		}
		admitted, err := coordinator.admitDemand(ctx, demand)
		if err != nil {
			return runningCount, err
		}
		if admitted {
			runningCount++
			repositoryRunningCounts[demand.Repository]++
		}
	}
	return runningCount, nil
}

func (coordinator *Coordinator) admitDemand(ctx context.Context, demand QueuedDemand) (bool, error) {
	worker := coordinator.workers[demand.Repository]
	if worker == nil {
		return false, nil
	}
	if !demand.Acquired {
		if err := worker.Acquire(ctx, demand.RequestID); err != nil {
			return false, fmt.Errorf("acquire job %d for repository %q: %w", demand.RequestID, demand.Repository, err)
		}
		demand.Acquired = true
		if err := coordinator.store.Apply(ctx, coordinator.scaleSet, []JobDemand{demand.JobDemand}, nil); err != nil {
			return false, fmt.Errorf("persist acquired job %d for repository %q: %w", demand.RequestID, demand.Repository, err)
		}
	}
	if err := worker.StartOne(ctx, demand.RequestID); err != nil {
		return false, fmt.Errorf("start runner for repository %q: %w", demand.Repository, err)
	}
	return true, nil
}

func (coordinator *Coordinator) runningCounts(ctx context.Context) (int, map[string]int, error) {
	runningCount := 0
	repositoryRunningCounts := make(map[string]int, len(coordinator.workers))
	for repository, worker := range coordinator.workers {
		workerRunningCount, err := worker.Running(ctx)
		if err != nil {
			return 0, nil, fmt.Errorf("count runners for repository %q: %w", repository, err)
		}
		runningCount += workerRunningCount
		repositoryRunningCounts[repository] = workerRunningCount
	}
	return runningCount, repositoryRunningCounts, nil
}

type memoryDemandStore struct {
	demand          map[demandKey]QueuedDemand
	nextObservation uint64
}

func createMemoryDemandStore() *memoryDemandStore {
	return &memoryDemandStore{demand: map[demandKey]QueuedDemand{}}
}

// NewMemoryDemandStore provides transient demand storage for tests and embedded callers.
func NewMemoryDemandStore() DemandStore { //nolint:ireturn // The exported test seam intentionally exposes only the production store contract.
	return createMemoryDemandStore()
}

func (store *memoryDemandStore) Apply(_ context.Context, _ string, observed []JobDemand, removed []DemandRemoval) error {
	for _, demand := range observed {
		key := demandKey{Repository: demand.Repository, RequestID: demand.RequestID}
		if existing, exists := store.demand[key]; exists {
			if demand.Acquired && !existing.Acquired {
				existing.Acquired = true
				store.demand[key] = existing
			}
			continue
		}
		store.nextObservation++
		store.demand[key] = QueuedDemand{JobDemand: demand, observationOrder: store.nextObservation}
	}
	for _, removal := range removed {
		delete(store.demand, demandKey(removal))
	}
	return nil
}

func (store *memoryDemandStore) RemoveRepository(_ context.Context, _ string, repository string) error {
	for key := range store.demand {
		if key.Repository == repository {
			delete(store.demand, key)
		}
	}
	return nil
}

func (store *memoryDemandStore) RemoveScaleSet(context.Context, string) error { return nil }

func (store *memoryDemandStore) Ordered(context.Context, string) ([]QueuedDemand, error) {
	ordered := make([]QueuedDemand, 0, len(store.demand))
	for _, demand := range store.demand {
		ordered = append(ordered, demand)
	}
	sort.Slice(ordered, func(leftIndex, rightIndex int) bool {
		left, right := ordered[leftIndex], ordered[rightIndex]
		if left.QueueTime.Equal(right.QueueTime) {
			return left.observationOrder < right.observationOrder
		}
		return left.QueueTime.Before(right.QueueTime)
	})
	return ordered, nil
}
