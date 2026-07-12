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
	Delete(ctx context.Context, scaleSet string, repository string, requestID int64) error
}

type Coordinator struct {
	maxRunners int
	scaleSet   string
	store      DemandStore
	workers    map[string]RepositoryWorker
	mutex      sync.Mutex
}

func NewCoordinator(maxRunners int) *Coordinator {
	return NewDurableCoordinator("", maxRunners, newMemoryDemandStore())
}

func NewDurableCoordinator(scaleSet string, maxRunners int, store DemandStore) *Coordinator {
	return &Coordinator{
		maxRunners: maxRunners,
		scaleSet:   scaleSet,
		store:      store,
		workers:    map[string]RepositoryWorker{},
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
	return coordinator.store.RemoveRepository(ctx, coordinator.scaleSet, repository)
}

func (coordinator *Coordinator) Observe(demand JobDemand) {
	_ = coordinator.Apply(context.Background(), []JobDemand{demand}, nil)
}

func (coordinator *Coordinator) Apply(ctx context.Context, observed []JobDemand, removed []DemandRemoval) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	return coordinator.store.Apply(ctx, coordinator.scaleSet, observed, removed)
}

func (coordinator *Coordinator) Remove(repository string, requestID int64) {
	_ = coordinator.Apply(context.Background(), nil, []DemandRemoval{{Repository: repository, RequestID: requestID}})
}

func (coordinator *Coordinator) Schedule(ctx context.Context) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	runningCount, err := coordinator.runningCount(ctx)
	if err != nil {
		return err
	}
	orderedDemand, err := coordinator.store.Ordered(ctx, coordinator.scaleSet)
	if err != nil {
		return fmt.Errorf("load queued demand: %w", err)
	}
	remainingDemand := orderedDemand[:0]
	for _, demand := range orderedDemand {
		worker := coordinator.workers[demand.Repository]
		if worker == nil {
			remainingDemand = append(remainingDemand, demand)
			continue
		}
		exists, existsErr := worker.Exists(ctx, demand.RequestID)
		if existsErr != nil {
			return fmt.Errorf("recover runner for repository %q: %w", demand.Repository, existsErr)
		}
		if !exists {
			remainingDemand = append(remainingDemand, demand)
			continue
		}
		if err := coordinator.store.Delete(ctx, coordinator.scaleSet, demand.Repository, demand.RequestID); err != nil {
			return fmt.Errorf("complete recovered demand: %w", err)
		}
	}
	for _, demand := range remainingDemand {
		if runningCount >= coordinator.maxRunners {
			return nil
		}
		admitted, err := coordinator.admitDemand(ctx, demand)
		if err != nil {
			return err
		}
		if admitted {
			runningCount++
		}
	}
	return nil
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
	if err := coordinator.store.Delete(ctx, coordinator.scaleSet, demand.Repository, demand.RequestID); err != nil {
		return false, fmt.Errorf("complete queued demand: %w", err)
	}
	return true, nil
}

func (coordinator *Coordinator) runningCount(ctx context.Context) (int, error) {
	runningCount := 0
	for repository, worker := range coordinator.workers {
		workerRunningCount, err := worker.Running(ctx)
		if err != nil {
			return 0, fmt.Errorf("count runners for repository %q: %w", repository, err)
		}
		runningCount += workerRunningCount
	}
	return runningCount, nil
}

type memoryDemandStore struct {
	demand          map[demandKey]QueuedDemand
	nextObservation uint64
}

func newMemoryDemandStore() *memoryDemandStore {
	return &memoryDemandStore{demand: map[demandKey]QueuedDemand{}}
}

// NewMemoryDemandStore provides transient demand storage for tests and embedded callers.
func NewMemoryDemandStore() DemandStore { return newMemoryDemandStore() }

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
		delete(store.demand, demandKey{Repository: removal.Repository, RequestID: removal.RequestID})
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

func (store *memoryDemandStore) Delete(_ context.Context, _ string, repository string, requestID int64) error {
	delete(store.demand, demandKey{Repository: repository, RequestID: requestID})
	return nil
}
