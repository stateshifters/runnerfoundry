package scaler

import (
	"context"
	"errors"
	"sort"
	"time"
)

func (store *MemoryDemandStore) RequestRunnerReap(
	_ context.Context, scaleSet, repository string, identity RunnerIdentity, reason string, requestedAt time.Time,
) error {
	return store.transitionRunnerToReap(scaleSet, repository, identity, reason, requestedAt)
}

func (store *MemoryDemandStore) transitionRunnerToReap(
	scaleSet, repository string, identity RunnerIdentity, reason string, requestedAt time.Time,
) error {
	key, runner, err := store.resolveMemoryRunner(scaleSet, repository, identity, anyMemoryRunner)
	if err != nil {
		return err
	}
	var libvirtKey string
	var libvirtLifecycle memoryLibvirtLifecycle
	if runner.Provider == RunnerProviderLibvirt {
		libvirtKey, libvirtLifecycle, err = store.resolveMemoryLibvirtRunner(
			scaleSet, repository, identity, anyMemoryLibvirtRunner,
		)
		if err != nil && !errors.Is(err, ErrRunnerIdentityNotFound) {
			return err
		}
	}
	runner.Phase = RunnerReapPending
	runner.ReapReason = reason
	runner.UpdatedAt = requestedAt
	store.runners[key] = runner
	if libvirtKey != "" {
		libvirtLifecycle.reapRequested = true
		store.libvirtRunners[libvirtKey] = libvirtLifecycle
	}
	return nil
}

func (store *MemoryDemandStore) RequestExpiredRunnerReaps(
	_ context.Context, scaleSet, repository string, deadline, requestedAt time.Time,
) (int, error) {
	changed := 0
	for key, runner := range store.runners {
		if runner.ScaleSet == scaleSet && (repository == "" || runner.Repository == repository) &&
			runner.Phase == RunnerWaitingForJob && !runner.WaitingSince.IsZero() && !runner.WaitingSince.After(deadline) {
			runner.Phase = RunnerReapPending
			runner.ReapReason = "job_start_timeout"
			runner.UpdatedAt = requestedAt
			store.runners[key] = runner
			if runner.Provider == RunnerProviderLibvirt {
				legacyKey := runnerLifecycleKey(LibvirtRunnerLifecycle{
					ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerName: runner.RunnerName,
				})
				legacy := store.libvirtRunners[legacyKey]
				legacy.reapRequested = true
				store.libvirtRunners[legacyKey] = legacy
			}
			changed++
		}
	}
	return changed, nil
}

func (store *MemoryDemandStore) RequestExcessWaitingRunnerReaps(
	_ context.Context, scaleSet, repository string, retain int, reason string, requestedAt time.Time,
) (int, error) {
	waiting, busy := store.waitingAndBusyRunners(scaleSet, repository)
	sort.Slice(waiting, func(left, right int) bool {
		if waiting[left].WaitingSince.Equal(waiting[right].WaitingSince) {
			return waiting[left].RunnerName > waiting[right].RunnerName
		}
		return waiting[left].WaitingSince.After(waiting[right].WaitingSince)
	})
	if retain < 0 {
		retain = 0
	}
	retain = max(0, retain-busy)
	changed := 0
	for _, runner := range waiting[min(retain, len(waiting)):] {
		store.markMemoryRunnerReapPending(runner, reason, requestedAt)
		changed++
	}
	return changed, nil
}

func (store *MemoryDemandStore) waitingAndBusyRunners(scaleSet, repository string) ([]RunnerLifecycle, int) {
	waiting := make([]RunnerLifecycle, 0)
	busy := 0
	for _, runner := range store.runners {
		if runner.ScaleSet != scaleSet || runner.Repository != repository {
			continue
		}
		if runner.Phase == RunnerBusy {
			busy++
		}
		if runner.RequestID == 0 && runner.Phase == RunnerWaitingForJob {
			waiting = append(waiting, runner)
		}
	}
	return waiting, busy
}

func (store *MemoryDemandStore) markMemoryRunnerReapPending(runner RunnerLifecycle, reason string, requestedAt time.Time) {
	runner.Phase = RunnerReapPending
	runner.ReapReason = reason
	runner.UpdatedAt = requestedAt
	store.runners[genericRunnerLifecycleKey(runner.ScaleSet, runner.Repository, runner.RunnerName)] = runner
	if runner.Provider != RunnerProviderLibvirt {
		return
	}
	legacyKey := runnerLifecycleKey(LibvirtRunnerLifecycle{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerName: runner.RunnerName,
	})
	legacy := store.libvirtRunners[legacyKey]
	legacy.reapRequested = true
	store.libvirtRunners[legacyKey] = legacy
}

func (store *MemoryDemandStore) PendingRunnerReaps(_ context.Context, scaleSet, repository string) ([]RunnerLifecycle, error) {
	result := make([]RunnerLifecycle, 0)
	for _, runner := range store.runners {
		if runner.ScaleSet == scaleSet && (repository == "" || runner.Repository == repository) && runner.Phase == RunnerReapPending {
			result = append(result, runner)
		}
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Repository == result[right].Repository {
			return result[left].RunnerName < result[right].RunnerName
		}
		return result[left].Repository < result[right].Repository
	})
	return result, nil
}

func (store *MemoryDemandStore) ConfirmRunnerReaped(_ context.Context, runner RunnerLifecycle) error {
	key, existing, err := store.resolveMemoryRunner(
		runner.ScaleSet, runner.Repository, runner.Identity(), func(candidate RunnerLifecycle) bool {
			return candidate.Phase == RunnerReapPending
		},
	)
	if err != nil {
		return err
	}
	delete(store.runners, key)
	if existing.Provider == RunnerProviderLibvirt {
		libvirtKey, _, resolveErr := store.resolveMemoryLibvirtRunner(
			runner.ScaleSet, runner.Repository, runner.Identity(), pendingMemoryLibvirtRunner,
		)
		if resolveErr != nil && !errors.Is(resolveErr, ErrRunnerIdentityNotFound) {
			store.runners[key] = existing
			return resolveErr
		}
		if resolveErr == nil {
			delete(store.libvirtRunners, libvirtKey)
		}
	}
	return nil
}
