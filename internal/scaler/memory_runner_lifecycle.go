package scaler

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"
)

func (store *MemoryDemandStore) RegisterRunner(_ context.Context, runner RunnerLifecycle) error {
	key := genericRunnerLifecycleKey(runner.ScaleSet, runner.Repository, runner.RunnerName)
	if existing, exists := store.runners[key]; exists {
		if existing.Provider != runner.Provider || existing.RequestID != runner.RequestID ||
			existing.AssignmentID != runner.AssignmentID || existing.Phase == RunnerReapPending {
			return errors.New("runner identity remains reserved by another lifecycle")
		}
		return nil
	}
	if runner.Phase == "" {
		runner.Phase = RunnerProvisioning
	}
	store.runners[key] = runner
	if runner.Provider == RunnerProviderLibvirt {
		legacy := LibvirtRunnerLifecycle{
			ScaleSet: runner.ScaleSet, Repository: runner.Repository, RequestID: runner.RequestID, RunnerName: runner.RunnerName,
		}
		store.libvirtRunners[runnerLifecycleKey(legacy)] = memoryLibvirtLifecycle{
			runner: legacy, reapRequested: runner.Phase == RunnerReapPending,
		}
	}
	return nil
}

func (store *MemoryDemandStore) CompleteRunnerLifecycleJob(
	_ context.Context,
	completion JobCompletion,
	completedAt time.Time,
) error {
	store.transitionRunnerToReap(completion.ScaleSet, completion.Repository, completion.RequestID,
		completion.RunnerName, "job_completed", completedAt)
	if completion.RequestID > 0 {
		delete(store.demand, demandKey{Repository: completion.Repository, RequestID: completion.RequestID})
	}
	return nil
}

func (store *MemoryDemandStore) ConfirmRunnerWaiting(_ context.Context, runner RunnerLifecycle, confirmedAt time.Time) error {
	key := genericRunnerLifecycleKey(runner.ScaleSet, runner.Repository, runner.RunnerName)
	existing, exists := store.runners[key]
	if !exists || existing.Phase != RunnerProvisioning {
		return sql.ErrNoRows
	}
	existing.Phase = RunnerWaitingForJob
	existing.GitHubRunnerID = runner.GitHubRunnerID
	existing.WaitingSince = confirmedAt
	existing.UpdatedAt = confirmedAt
	store.runners[key] = existing
	return nil
}

func (store *MemoryDemandStore) MarkRunnerBusy(
	_ context.Context, scaleSet, repository string, requestID int64, runnerName string, githubRunnerID int64, startedAt time.Time,
) error {
	changed := false
	for key, runner := range store.runners {
		if runner.ScaleSet != scaleSet || runner.Repository != repository || runner.Phase == RunnerReapPending ||
			!runnerMatchesLifecycleIdentity(runner, requestID, runnerName) {
			continue
		}
		runner.Phase = RunnerBusy
		if runner.RequestID == 0 && requestID > 0 {
			runner.RequestID = requestID
		}
		if githubRunnerID > 0 {
			runner.GitHubRunnerID = githubRunnerID
		}
		runner.UpdatedAt = startedAt
		store.runners[key] = runner
		changed = true
	}
	if !changed {
		return sql.ErrNoRows
	}
	return nil
}

func (store *MemoryDemandStore) RequestRunnerReap(
	_ context.Context, scaleSet, repository string, requestID int64, runnerName, reason string, requestedAt time.Time,
) error {
	store.transitionRunnerToReap(scaleSet, repository, requestID, runnerName, reason, requestedAt)
	return nil
}

func (store *MemoryDemandStore) transitionRunnerToReap(
	scaleSet, repository string, requestID int64, runnerName, reason string, requestedAt time.Time,
) {
	for key, runner := range store.runners {
		if runner.ScaleSet == scaleSet && runner.Repository == repository &&
			runnerMatchesLifecycleIdentity(runner, requestID, runnerName) {
			runner.Phase = RunnerReapPending
			runner.ReapReason = reason
			runner.UpdatedAt = requestedAt
			store.runners[key] = runner
		}
	}
}

func runnerMatchesLifecycleIdentity(runner RunnerLifecycle, requestID int64, runnerName string) bool {
	return (runnerName != "" && runner.RunnerName == runnerName) || (requestID > 0 && runner.RequestID == requestID)
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

func (store *MemoryDemandStore) RunnerLifecycles(_ context.Context, scaleSet, repository string) ([]RunnerLifecycle, error) {
	result := make([]RunnerLifecycle, 0)
	for _, runner := range store.runners {
		if runner.ScaleSet == scaleSet && (repository == "" || runner.Repository == repository) {
			result = append(result, runner)
		}
	}
	return result, nil
}

func (store *MemoryDemandStore) ConfirmRunnerReaped(_ context.Context, runner RunnerLifecycle) error {
	key := genericRunnerLifecycleKey(runner.ScaleSet, runner.Repository, runner.RunnerName)
	if _, exists := store.runners[key]; !exists {
		return sql.ErrNoRows
	}
	delete(store.runners, key)
	if runner.Provider == RunnerProviderLibvirt {
		delete(store.libvirtRunners, runnerLifecycleKey(LibvirtRunnerLifecycle{
			ScaleSet: runner.ScaleSet, Repository: runner.Repository, RequestID: runner.RequestID, RunnerName: runner.RunnerName,
		}))
	}
	return nil
}

func genericRunnerLifecycleKey(scaleSet, repository, runnerName string) string {
	return scaleSet + "\x00" + repository + "\x00" + runnerName
}
