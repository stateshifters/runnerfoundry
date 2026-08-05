package scaler

import (
	"context"
	"sort"
)

type MemoryDemandStore struct {
	demand          map[demandKey]QueuedDemand
	runners         map[string]RunnerLifecycle
	nextObservation uint64
}

func createMemoryDemandStore() *MemoryDemandStore {
	return &MemoryDemandStore{
		demand: map[demandKey]QueuedDemand{}, runners: map[string]RunnerLifecycle{},
	}
}

// NewMemoryDemandStore provides transient demand storage for tests and embedded callers.
func NewMemoryDemandStore() *MemoryDemandStore {
	return createMemoryDemandStore()
}

func (store *MemoryDemandStore) Apply(_ context.Context, _ string, observed []JobDemand, removed []DemandRemoval) error {
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

func (store *MemoryDemandStore) RemoveRepository(_ context.Context, _ string, repository string) error {
	for key := range store.demand {
		if key.Repository == repository {
			delete(store.demand, key)
		}
	}
	return nil
}

func (store *MemoryDemandStore) RemoveScaleSet(context.Context, string) error { return nil }

func (store *MemoryDemandStore) Ordered(context.Context, string) ([]QueuedDemand, error) {
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
