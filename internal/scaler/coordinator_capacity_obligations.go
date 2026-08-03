package scaler

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const capacityObligationOperationTimeout = 30 * time.Second

var capacityObligationRetryDelays = [...]time.Duration{
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
	40 * time.Second,
	60 * time.Second,
}

type assignedObligationWorker interface {
	StartAssignedObligation(ctx context.Context, obligationID int64) error
}

// ObserveAssignedCapacity durably translates TotalAssignedJobs into capacity
// obligations. Persistence completes before the caller may acknowledge the
// message that carried the statistic.
func (coordinator *Coordinator) ObserveAssignedCapacity(
	ctx context.Context,
	repository string,
	assignedJobs int,
	observedAt time.Time,
) (AssignedObligationDecision, error) {
	store, supported := coordinator.store.(CapacityObligationStore)
	if !supported {
		coordinator.ObserveAssigned(repository, assignedJobs)
		return AssignedObligationDecision{}, nil
	}
	coordinator.mutex.Lock()
	epoch := coordinator.controllerEpoch
	coordinator.mutex.Unlock()
	activeCapacity, err := coordinator.repositoryActiveCapacity(ctx, repository)
	if err != nil {
		return AssignedObligationDecision{}, err
	}
	decision, err := store.ObserveAssignedCapacity(ctx, AssignedCapacityObservation{
		ScaleSet: coordinator.scaleSet, Repository: repository, ControllerEpoch: epoch,
		AssignedJobs: assignedJobs, ObservedAt: observedAt,
	}, activeCapacity)
	if err != nil {
		return AssignedObligationDecision{}, fmt.Errorf("persist assigned capacity observation: %w", err)
	}
	return decision, nil
}

func (coordinator *Coordinator) repositoryActiveCapacity(ctx context.Context, repository string) (int, error) {
	if inventory, supported := coordinator.store.(RunnerLifecycleInventory); supported {
		lifecycles, err := inventory.RunnerLifecycles(ctx, coordinator.scaleSet, repository)
		if err != nil {
			return 0, fmt.Errorf("load repository runner capacity: %w", err)
		}
		active := 0
		for _, lifecycle := range lifecycles {
			if lifecycle.Phase != RunnerReapPending {
				active++
			}
		}
		return active, nil
	}
	coordinator.mutex.Lock()
	worker := coordinator.workers[repository]
	coordinator.mutex.Unlock()
	if worker == nil {
		return 0, nil
	}
	active, err := worker.Running(ctx)
	if err != nil {
		return 0, fmt.Errorf("inspect repository runner capacity: %w", err)
	}
	return active, nil
}

func (coordinator *Coordinator) scheduleCapacityObligations(
	ctx context.Context,
	store CapacityObligationStore,
) error {
	if err := coordinator.acquireProvisioning(ctx); err != nil {
		return err
	}
	defer coordinator.releaseProvisioning()
	if coordinator.provisioningSuspended.Load() {
		return nil
	}
	if err := coordinator.deleteLifecycleOwnedObligations(ctx); err != nil {
		return fmt.Errorf("complete lifecycle-owned capacity obligations: %w", err)
	}
	now := coordinator.now().UTC()
	obligations, err := store.OrderedReady(ctx, coordinator.scaleSet, now)
	if err != nil {
		return fmt.Errorf("load runnable capacity obligations: %w", err)
	}
	available, err := coordinator.availableObligationCapacity(ctx)
	if err != nil {
		return err
	}
	coordinator.launchRunnableObligations(ctx, store, obligations, available)
	return nil
}

func (coordinator *Coordinator) deleteLifecycleOwnedObligations(ctx context.Context) error {
	store, supported := coordinator.store.(LifecycleOwnedObligationStore)
	if !supported {
		return nil
	}
	if err := store.DeleteLifecycleOwnedObligations(ctx, coordinator.scaleSet); err != nil {
		return fmt.Errorf("delete lifecycle-owned capacity obligations: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) launchRunnableObligations(
	ctx context.Context,
	store CapacityObligationStore,
	obligations []QueuedDemand,
	available int,
) {
	for _, obligation := range obligations {
		if available <= 0 {
			return
		}
		if coordinator.claimObligation(obligation) {
			available--
			coordinator.background.Add(1)
			go coordinator.executeCapacityObligation(ctx, store, obligation)
		}
	}
}

func (coordinator *Coordinator) availableObligationCapacity(ctx context.Context) (int, error) {
	if inventory, supported := coordinator.store.(RunnerLifecycleInventory); supported {
		active, err := coordinator.activeLifecycleCapacity(ctx, inventory)
		return coordinator.remainingObligationCapacity(active), err
	}
	active, err := coordinator.activeWorkerCapacity(ctx)
	return coordinator.remainingObligationCapacity(active), err
}

func (coordinator *Coordinator) activeLifecycleCapacity(
	ctx context.Context,
	inventory RunnerLifecycleInventory,
) (int, error) {
	lifecycles, err := inventory.RunnerLifecycles(ctx, coordinator.scaleSet, "")
	if err != nil {
		return 0, fmt.Errorf("load scale-set runner capacity: %w", err)
	}
	active := 0
	for _, lifecycle := range lifecycles {
		if lifecycle.Phase != RunnerReapPending {
			active++
		}
	}
	return active, nil
}

func (coordinator *Coordinator) activeWorkerCapacity(ctx context.Context) (int, error) {
	coordinator.mutex.Lock()
	workers := make([]RepositoryWorker, 0, len(coordinator.workers))
	for _, worker := range coordinator.workers {
		workers = append(workers, worker)
	}
	coordinator.mutex.Unlock()
	active := 0
	for _, worker := range workers {
		running, err := worker.Running(ctx)
		if err != nil {
			return 0, fmt.Errorf("inspect scale-set runner capacity: %w", err)
		}
		active += running
	}
	return active, nil
}

func (coordinator *Coordinator) remainingObligationCapacity(active int) int {
	coordinator.mutex.Lock()
	inFlight := len(coordinator.inFlightObligations)
	coordinator.mutex.Unlock()
	return coordinator.maxRunners - active - inFlight
}

func (coordinator *Coordinator) claimObligation(obligation QueuedDemand) bool {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if coordinator.workers[obligation.Repository] == nil {
		return false
	}
	if obligation.ObligationID <= 0 {
		return false
	}
	if obligation.Source == CapacityObligationAssignedCount && obligation.ControllerEpoch != coordinator.controllerEpoch {
		return false
	}
	if _, claimed := coordinator.inFlightObligations[obligation.ObligationID]; claimed {
		return false
	}
	coordinator.inFlightObligations[obligation.ObligationID] = struct{}{}
	return true
}

func (coordinator *Coordinator) executeCapacityObligation(
	parentCtx context.Context,
	store CapacityObligationStore,
	obligation QueuedDemand,
) {
	defer coordinator.background.Done()
	defer func() {
		coordinator.mutex.Lock()
		delete(coordinator.inFlightObligations, obligation.ObligationID)
		coordinator.mutex.Unlock()
	}()
	coordinator.mutex.Lock()
	worker := coordinator.workers[obligation.Repository]
	coordinator.mutex.Unlock()
	if worker == nil {
		return
	}
	err := coordinator.executeObligation(parentCtx, store, worker, obligation)
	if err == nil {
		return
	}
	retryAt := coordinator.now().UTC().Add(capacityObligationRetryDelay(obligation.Attempt))
	recordCtx, cancelRecord := coordinator.capacityOperationContext(parentCtx)
	recordErr := store.RecordObligationFailure(recordCtx, obligation.ObligationID, retryAt)
	cancelRecord()
	if recordErr != nil {
		coordinator.reportSchedulingError(fmt.Errorf("record capacity obligation retry: %w", errors.Join(err, recordErr)))
		return
	}
	coordinator.reportSchedulingError(err)
}

func (coordinator *Coordinator) capacityOperationContext(parentCtx context.Context) (context.Context, context.CancelFunc) {
	operationCtx, cancelTimeout := context.WithTimeout(context.WithoutCancel(parentCtx), capacityObligationOperationTimeout)
	coordinator.mutex.Lock()
	executionDone := coordinator.executionDone
	coordinator.mutex.Unlock()
	if executionDone == nil {
		return operationCtx, cancelTimeout
	}
	fencedCtx, cancelFence := context.WithCancel(operationCtx)
	go func() {
		select {
		case <-executionDone:
			cancelFence()
		case <-fencedCtx.Done():
		}
	}()
	return fencedCtx, func() {
		cancelFence()
		cancelTimeout()
	}
}

func (coordinator *Coordinator) executeObligation(
	ctx context.Context,
	store CapacityObligationStore,
	worker RepositoryWorker,
	obligation QueuedDemand,
) error {
	if obligation.Source == CapacityObligationAssignedCount {
		return coordinator.executeAssignedObligation(ctx, store, worker, obligation)
	}
	return coordinator.executeExactObligation(ctx, store, worker, obligation)
}

func (coordinator *Coordinator) executeAssignedObligation(
	ctx context.Context,
	store CapacityObligationStore,
	worker RepositoryWorker,
	obligation QueuedDemand,
) error {
	assignedWorker, supported := worker.(assignedObligationWorker)
	if !supported {
		return fmt.Errorf("repository %q does not support durable assigned obligations", obligation.Repository)
	}
	startCtx, cancelStart := coordinator.capacityOperationContext(ctx)
	err := assignedWorker.StartAssignedObligation(startCtx, obligation.ObligationID)
	cancelStart()
	if err != nil {
		return fmt.Errorf("start assigned runner for repository %q: %w", obligation.Repository, err)
	}
	deleteCtx, cancelDelete := coordinator.capacityOperationContext(ctx)
	err = store.DeleteObligation(deleteCtx, obligation.ObligationID)
	cancelDelete()
	if err != nil {
		return fmt.Errorf("complete assigned capacity obligation: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) executeExactObligation(
	ctx context.Context,
	store CapacityObligationStore,
	worker RepositoryWorker,
	obligation QueuedDemand,
) error {
	exists, err := coordinator.exactRunnerExists(ctx, worker, obligation)
	if err != nil || exists {
		return err
	}
	if err = coordinator.acquireExactObligation(ctx, store, worker, &obligation); err != nil {
		return err
	}
	startCtx, cancelStart := coordinator.capacityOperationContext(ctx)
	err = worker.StartOne(startCtx, obligation.RequestID)
	cancelStart()
	if err != nil {
		return fmt.Errorf("start runner for repository %q: %w", obligation.Repository, err)
	}
	return nil
}

func (coordinator *Coordinator) exactRunnerExists(
	ctx context.Context,
	worker RepositoryWorker,
	obligation QueuedDemand,
) (bool, error) {
	existsCtx, cancelExists := coordinator.capacityOperationContext(ctx)
	exists, err := worker.Exists(existsCtx, obligation.RequestID)
	cancelExists()
	if err != nil {
		return false, fmt.Errorf("inspect runner for job %d in repository %q: %w", obligation.RequestID, obligation.Repository, err)
	}
	return exists, nil
}

func (coordinator *Coordinator) acquireExactObligation(
	ctx context.Context,
	store CapacityObligationStore,
	worker RepositoryWorker,
	obligation *QueuedDemand,
) error {
	if obligation.Acquired {
		return nil
	}
	acquireCtx, cancelAcquire := coordinator.capacityOperationContext(ctx)
	err := worker.Acquire(acquireCtx, obligation.RequestID)
	cancelAcquire()
	if err != nil {
		return fmt.Errorf("acquire job %d for repository %q: %w", obligation.RequestID, obligation.Repository, err)
	}
	obligation.Acquired = true
	persistCtx, cancelPersist := coordinator.capacityOperationContext(ctx)
	err = store.Apply(persistCtx, coordinator.scaleSet, []JobDemand{obligation.JobDemand}, nil)
	cancelPersist()
	if err != nil {
		return fmt.Errorf("persist acquired job %d for repository %q: %w", obligation.RequestID, obligation.Repository, err)
	}
	return nil
}

func capacityObligationRetryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= len(capacityObligationRetryDelays) {
		return capacityObligationRetryDelays[len(capacityObligationRetryDelays)-1]
	}
	return capacityObligationRetryDelays[attempt]
}
