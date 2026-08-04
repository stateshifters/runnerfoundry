package scaler

import (
	"context"
	"database/sql"
	"errors"
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
	err := store.transitionRunnerToReap(completion.ScaleSet, completion.Repository,
		RunnerIdentity{RequestID: completion.RequestID, RunnerName: completion.RunnerName}, "job_completed", completedAt)
	if err != nil {
		return err
	}
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

func (store *MemoryDemandStore) RunnerLifecycles(_ context.Context, scaleSet, repository string) ([]RunnerLifecycle, error) {
	result := make([]RunnerLifecycle, 0)
	for _, runner := range store.runners {
		if runner.ScaleSet == scaleSet && (repository == "" || runner.Repository == repository) {
			result = append(result, runner)
		}
	}
	return result, nil
}

func genericRunnerLifecycleKey(scaleSet, repository, runnerName string) string {
	return scaleSet + "\x00" + repository + "\x00" + runnerName
}
