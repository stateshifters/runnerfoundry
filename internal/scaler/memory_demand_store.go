package scaler

import (
	"context"
	"errors"
	"sort"
)

type MemoryDemandStore struct {
	demand          map[demandKey]QueuedDemand
	libvirtRunners  map[string]memoryLibvirtLifecycle
	runners         map[string]RunnerLifecycle
	nextObservation uint64
}

func createMemoryDemandStore() *MemoryDemandStore {
	return &MemoryDemandStore{
		demand: map[demandKey]QueuedDemand{}, libvirtRunners: map[string]memoryLibvirtLifecycle{},
		runners: map[string]RunnerLifecycle{},
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

func (store *MemoryDemandStore) RegisterLibvirtRunner(_ context.Context, runner LibvirtRunnerLifecycle) error {
	key := runnerLifecycleKey(runner)
	if existing, exists := store.libvirtRunners[key]; exists {
		if existing.runner.RequestID != runner.RequestID || existing.reapRequested {
			return errors.New("libvirt runner identity remains reserved by another lifecycle")
		}
		return nil
	}
	store.libvirtRunners[key] = memoryLibvirtLifecycle{runner: runner}
	return nil
}

func (store *MemoryDemandStore) CompleteJob(_ context.Context, completion JobCompletion) error {
	key, lifecycle, err := store.resolveMemoryLibvirtRunner(
		completion.ScaleSet, completion.Repository, completion.RunnerIdentity, anyMemoryLibvirtRunner,
	)
	if err != nil {
		return err
	}
	lifecycle.reapRequested = true
	store.libvirtRunners[key] = lifecycle
	delete(store.demand, demandKey{Repository: completion.Repository, RequestID: completion.RequestID})
	return nil
}

func (store *MemoryDemandStore) PendingLibvirtReaps(
	_ context.Context,
	scaleSet string,
	repository string,
) ([]LibvirtRunnerLifecycle, error) {
	result := make([]LibvirtRunnerLifecycle, 0)
	for _, lifecycle := range store.libvirtRunners {
		if lifecycle.reapRequested && lifecycle.runner.ScaleSet == scaleSet &&
			(repository == "" || lifecycle.runner.Repository == repository) {
			result = append(result, lifecycle.runner)
		}
	}
	return result, nil
}

func (store *MemoryDemandStore) ConfirmLibvirtReaped(_ context.Context, runner LibvirtRunnerLifecycle) error {
	key, _, err := store.resolveMemoryLibvirtRunner(
		runner.ScaleSet, runner.Repository, runner.Identity(), pendingMemoryLibvirtRunner,
	)
	if err != nil {
		return err
	}
	delete(store.libvirtRunners, key)
	return nil
}

type memoryLibvirtEligibility func(memoryLibvirtLifecycle) bool

func anyMemoryLibvirtRunner(memoryLibvirtLifecycle) bool { return true }

func pendingMemoryLibvirtRunner(lifecycle memoryLibvirtLifecycle) bool {
	return lifecycle.reapRequested
}

func (store *MemoryDemandStore) resolveMemoryLibvirtRunner(
	scaleSet, repository string,
	identity RunnerIdentity,
	eligible memoryLibvirtEligibility,
) (string, memoryLibvirtLifecycle, error) {
	if err := identity.Validate(); err != nil {
		return "", memoryLibvirtLifecycle{}, err
	}
	selection := memoryIdentitySelection{}
	selectedKey := ""
	selectedLifecycle := memoryLibvirtLifecycle{}
	for key, lifecycle := range store.libvirtRunners {
		runner := lifecycle.runner
		if runner.ScaleSet != scaleSet || runner.Repository != repository || !eligible(lifecycle) {
			continue
		}
		if selection.observe(identity, runner.RequestID, runner.RunnerName) {
			selectedKey = key
			selectedLifecycle = lifecycle
		}
	}
	if err := selection.result(identity); err != nil {
		return "", memoryLibvirtLifecycle{}, err
	}
	return selectedKey, selectedLifecycle, nil
}

func runnerLifecycleKey(runner LibvirtRunnerLifecycle) string {
	return runner.ScaleSet + "\x00" + runner.Repository + "\x00" + runner.RunnerName
}

type memoryLibvirtLifecycle struct {
	runner        LibvirtRunnerLifecycle
	reapRequested bool
}
