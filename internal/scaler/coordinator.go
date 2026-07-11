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
	Acquire(ctx context.Context, requestID int64) error
	StartOne(ctx context.Context, requestID int64) error
}

type demandKey struct {
	Repository string
	RequestID  int64
}

type queuedDemand struct {
	JobDemand
	observationOrder uint64
}

type Coordinator struct {
	maxRunners      int
	workers         map[string]RepositoryWorker
	demand          map[demandKey]queuedDemand
	nextObservation uint64
	mutex           sync.Mutex
}

func NewCoordinator(maxRunners int) *Coordinator {
	return &Coordinator{
		maxRunners: maxRunners,
		workers:    map[string]RepositoryWorker{},
		demand:     map[demandKey]queuedDemand{},
	}
}

func (coordinator *Coordinator) AddRepository(repository string, worker RepositoryWorker) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.workers[repository] = worker
}

func (coordinator *Coordinator) RemoveRepository(repository string) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	delete(coordinator.workers, repository)
	for key := range coordinator.demand {
		if key.Repository == repository {
			delete(coordinator.demand, key)
		}
	}
}

func (coordinator *Coordinator) Observe(demand JobDemand) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	key := demandKey{Repository: demand.Repository, RequestID: demand.RequestID}
	if existing, exists := coordinator.demand[key]; exists {
		if demand.Acquired && !existing.Acquired {
			existing.Acquired = true
			coordinator.demand[key] = existing
		}
		return
	}
	coordinator.nextObservation++
	coordinator.demand[key] = queuedDemand{JobDemand: demand, observationOrder: coordinator.nextObservation}
}

func (coordinator *Coordinator) Remove(repository string, requestID int64) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	delete(coordinator.demand, demandKey{Repository: repository, RequestID: requestID})
}

func (coordinator *Coordinator) Schedule(ctx context.Context) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	runningCount, err := coordinator.runningCount(ctx)
	if err != nil {
		return err
	}
	for _, demand := range coordinator.orderedDemand() {
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

func (coordinator *Coordinator) admitDemand(ctx context.Context, demand queuedDemand) (bool, error) {
	worker := coordinator.workers[demand.Repository]
	if worker == nil {
		return false, nil
	}
	if !demand.Acquired {
		if err := worker.Acquire(ctx, demand.RequestID); err != nil {
			return false, fmt.Errorf("acquire job %d for repository %q: %w", demand.RequestID, demand.Repository, err)
		}
	}
	if err := worker.StartOne(ctx, demand.RequestID); err != nil {
		return false, fmt.Errorf("start runner for repository %q: %w", demand.Repository, err)
	}
	delete(coordinator.demand, demandKey{Repository: demand.Repository, RequestID: demand.RequestID})
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

func (coordinator *Coordinator) orderedDemand() []queuedDemand {
	ordered := make([]queuedDemand, 0, len(coordinator.demand))
	for _, demand := range coordinator.demand {
		ordered = append(ordered, demand)
	}
	sort.Slice(ordered, func(leftIndex int, rightIndex int) bool {
		left := ordered[leftIndex]
		right := ordered[rightIndex]
		if left.QueueTime.Equal(right.QueueTime) {
			return left.observationOrder < right.observationOrder
		}
		return left.QueueTime.Before(right.QueueTime)
	})
	return ordered
}
