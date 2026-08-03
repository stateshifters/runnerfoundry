package scaler

import (
	"context"
	"fmt"
	"time"
)

func (coordinator *Coordinator) assignedController(repository string) *AssignedJobController {
	controller := coordinator.assignedControllers[repository]
	if controller == nil {
		controller = NewAssignedJobController(coordinatorAssignedJobBackend{
			coordinator: coordinator, repository: repository,
		})
		coordinator.assignedControllers[repository] = controller
	}
	return controller
}

type coordinatorAssignedJobBackend struct {
	coordinator *Coordinator
	repository  string
}

func (backend coordinatorAssignedJobBackend) Prepare(
	ctx context.Context,
	assignedJobs int,
) (AssignedJobCapacity, error) {
	if err := backend.requestExcessWaitingRunnerCleanup(ctx, assignedJobs); err != nil {
		return AssignedJobCapacity{}, err
	}
	capacity, err := backend.currentCapacity(ctx)
	if err != nil {
		return AssignedJobCapacity{}, err
	}
	worker := backend.coordinator.workers[backend.repository]
	deficit := assignedJobs - capacity.ActiveRunners
	if err = reconcileBlockedRepositoryCapacity(
		ctx, worker, backend.repository, deficit, capacity.AvailableCapacity,
	); err != nil {
		return AssignedJobCapacity{}, err
	}
	return capacity, nil
}

func (backend coordinatorAssignedJobBackend) requestExcessWaitingRunnerCleanup(
	ctx context.Context,
	assignedJobs int,
) error {
	store, supported := backend.coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return nil
	}
	_, err := store.RequestExcessWaitingRunnerReaps(
		ctx, backend.coordinator.scaleSet, backend.repository, assignedJobs,
		"assigned_jobs_decreased", time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("request excess waiting runner cleanup: %w", err)
	}
	return nil
}

func (backend coordinatorAssignedJobBackend) currentCapacity(ctx context.Context) (AssignedJobCapacity, error) {
	runningCount, repositoryRunningCounts, err := backend.coordinator.runningCounts(ctx)
	if err != nil {
		return AssignedJobCapacity{}, err
	}
	pendingReaps, err := backend.coordinator.loadPendingRunnerCapacity(ctx)
	if err != nil {
		return AssignedJobCapacity{}, err
	}
	runningCount, err = backend.coordinator.countPendingRunnerCapacity(ctx, pendingReaps, runningCount)
	if err != nil {
		return AssignedJobCapacity{}, err
	}
	activeRunners := repositoryRunningCounts[backend.repository]
	availableCapacity := backend.coordinator.maxRunners - runningCount
	return AssignedJobCapacity{ActiveRunners: activeRunners, AvailableCapacity: availableCapacity}, nil
}

func (backend coordinatorAssignedJobBackend) StartAssigned(ctx context.Context) (bool, error) {
	worker := backend.coordinator.workers[backend.repository]
	if worker == nil {
		return false, nil
	}
	if err := worker.StartAssigned(ctx); err != nil {
		return false, fmt.Errorf("repository %q: %w", backend.repository, err)
	}
	return true, nil
}
