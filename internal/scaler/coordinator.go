package scaler

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
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

type runnerUsabilityReconciler interface {
	ReconcileRunnerUsability(ctx context.Context) (int, error)
}

type runnerIdentityInspector interface {
	RunnerPresent(ctx context.Context, runnerName string) (bool, error)
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
	maxRunners            int
	scaleSet              string
	store                 DemandStore
	workers               map[string]RepositoryWorker
	assigned              map[string]int
	provisioningGate      chan struct{}
	provisioningSuspended atomic.Bool
	mutex                 sync.Mutex
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
	coordinator := &Coordinator{
		maxRunners:       maxRunners,
		scaleSet:         scaleSet,
		store:            store,
		workers:          map[string]RepositoryWorker{},
		assigned:         map[string]int{},
		provisioningGate: make(chan struct{}, 1),
	}
	coordinator.provisioningGate <- struct{}{}
	return coordinator
}

func (coordinator *Coordinator) AddRepository(repository string, worker RepositoryWorker) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.workers[repository] = worker
}

// DetachRepository stops scheduling through the repository worker while retaining
// its durable demand and assigned-job observation for a later session generation.
func (coordinator *Coordinator) DetachRepository(repository string) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	delete(coordinator.workers, repository)
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

func (coordinator *Coordinator) Apply(ctx context.Context, observed []JobDemand, removed []DemandRemoval) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if err := coordinator.store.Apply(ctx, coordinator.scaleSet, observed, removed); err != nil {
		return fmt.Errorf("apply demand changes: %w", err)
	}
	return nil
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
	_, _, err := coordinator.reconcileProvisioning(ctx)
	return err
}

// SuspendProvisioning waits for the current scheduling pass and prevents later
// passes from starting runners. Demand observation remains available.
func (coordinator *Coordinator) SuspendProvisioning(ctx context.Context) error {
	if err := coordinator.acquireProvisioning(ctx); err != nil {
		return err
	}
	defer coordinator.releaseProvisioning()
	coordinator.provisioningSuspended.Store(true)
	return nil
}

// ResumeProvisioning re-enables runner starts without waiting on coordinator
// work. Retained demand is handled by the next scheduling pass.
func (coordinator *Coordinator) ResumeProvisioning() {
	coordinator.provisioningSuspended.Store(false)
}

func (coordinator *Coordinator) reconcileProvisioning(
	ctx context.Context,
) (map[string]AssignedReconciliation, bool, error) {
	if err := coordinator.acquireProvisioning(ctx); err != nil {
		return nil, false, err
	}
	defer coordinator.releaseProvisioning()
	if coordinator.provisioningSuspended.Load() {
		return nil, false, nil
	}
	reconciliations, err := coordinator.reconcileCapacity(ctx)
	return reconciliations, true, err
}

func (coordinator *Coordinator) acquireProvisioning(ctx context.Context) error {
	select {
	case <-coordinator.provisioningGate:
		return nil
	default:
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for provisioning boundary: %w", ctx.Err())
	case <-coordinator.provisioningGate:
		return nil
	}
}

func (coordinator *Coordinator) releaseProvisioning() {
	coordinator.provisioningGate <- struct{}{}
}

func (coordinator *Coordinator) reconcileCapacity(ctx context.Context) (map[string]AssignedReconciliation, error) {
	runningCount, repositoryRunningCounts, err := coordinator.runningCounts(ctx)
	if err != nil {
		return nil, err
	}
	pendingReaps, err := coordinator.loadPendingRunnerCapacity(ctx)
	if err != nil {
		return nil, err
	}
	runningCount, err = coordinator.countPendingRunnerCapacity(ctx, pendingReaps, runningCount)
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
	remainingDemand = fencePendingRunnerRequests(remainingDemand, pendingReaps)
	runningCount, err = coordinator.admitRemainingDemand(ctx, remainingDemand, runningCount, repositoryRunningCounts)
	if err != nil {
		return nil, err
	}
	return coordinator.reconcileRetainedAssigned(ctx, runningCount, repositoryRunningCounts)
}

func (coordinator *Coordinator) countPendingRunnerCapacity(
	ctx context.Context,
	pendingReaps []RunnerLifecycle,
	runningCount int,
) (int, error) {
	for _, runner := range pendingReaps {
		worker := coordinator.workers[runner.Repository]
		inspector, supported := worker.(runnerIdentityInspector)
		if !supported {
			runningCount++
			continue
		}
		present, inspectErr := inspector.RunnerPresent(ctx, runner.RunnerName)
		if inspectErr != nil {
			return 0, fmt.Errorf("inspect pending runner capacity %q: %w", runner.RunnerName, inspectErr)
		}
		if !present {
			runningCount++
		}
	}
	return runningCount, nil
}

func (coordinator *Coordinator) loadPendingRunnerCapacity(ctx context.Context) ([]RunnerLifecycle, error) {
	if store, supported := coordinator.store.(RunnerLifecycleStore); supported {
		pending, err := loadGenericPendingRunnerCapacity(ctx, store, coordinator.scaleSet)
		if err != nil {
			return nil, err
		}
		if len(pending) > 0 {
			return pending, nil
		}
	}
	legacy, err := coordinator.loadPendingLibvirtCapacity(ctx)
	if err != nil {
		return nil, err
	}
	pending := make([]RunnerLifecycle, 0, len(legacy))
	for _, runner := range legacy {
		pending = append(pending, RunnerLifecycle{
			ScaleSet: runner.ScaleSet, Repository: runner.Repository, Provider: RunnerProviderLibvirt,
			RunnerName: runner.RunnerName, RequestID: runner.RequestID, Phase: RunnerReapPending,
		})
	}
	return pending, nil
}

func loadGenericPendingRunnerCapacity(
	ctx context.Context,
	store RunnerLifecycleStore,
	scaleSet string,
) ([]RunnerLifecycle, error) {
	pending, err := store.PendingRunnerReaps(ctx, scaleSet, "")
	if err != nil {
		return nil, fmt.Errorf("load pending runner capacity: %w", err)
	}
	return pending, nil
}

func fencePendingRunnerRequests(demand []QueuedDemand, pending []RunnerLifecycle) []QueuedDemand {
	identities := make(map[demandKey]struct{}, len(pending))
	for _, runner := range pending {
		identities[demandKey{Repository: runner.Repository, RequestID: runner.RequestID}] = struct{}{}
	}
	result := demand[:0]
	for _, queued := range demand {
		if _, fenced := identities[demandKey{Repository: queued.Repository, RequestID: queued.RequestID}]; !fenced {
			result = append(result, queued)
		}
	}
	return result
}

func (coordinator *Coordinator) loadPendingLibvirtCapacity(ctx context.Context) ([]LibvirtRunnerLifecycle, error) {
	store, supported := coordinator.store.(LibvirtLifecycleStore)
	if !supported {
		return nil, nil
	}
	pending, err := store.PendingLibvirtReaps(ctx, coordinator.scaleSet, "")
	if err != nil {
		return nil, fmt.Errorf("load pending libvirt capacity: %w", err)
	}
	return pending, nil
}

// ReconcileAssigned starts enough identity-free runners for one repository's
// assigned-job statistic without exceeding shared scale-set capacity.
func (coordinator *Coordinator) ReconcileAssigned(ctx context.Context, repository string, assignedJobs int) (AssignedReconciliation, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.assigned[repository] = assignedJobs
	if store, supported := coordinator.store.(RunnerLifecycleStore); supported {
		if _, err := store.RequestExcessWaitingRunnerReaps(
			ctx, coordinator.scaleSet, repository, assignedJobs, "assigned_jobs_decreased", time.Now().UTC(),
		); err != nil {
			return AssignedReconciliation{}, fmt.Errorf("request excess waiting runner cleanup: %w", err)
		}
	}
	reconciliations, provisioningEnabled, err := coordinator.reconcileProvisioning(ctx)
	if !provisioningEnabled || err != nil {
		if err != nil {
			return AssignedReconciliation{}, err
		}
		return AssignedReconciliation{AssignedJobs: assignedJobs}, nil
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
	if err := reconcileBlockedRepositoryCapacity(ctx, worker, repository, missingAssignedRunners, availableCapacity); err != nil {
		return AssignedReconciliation{}, err
	}
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

func reconcileBlockedRepositoryCapacity(
	ctx context.Context,
	worker RepositoryWorker,
	repository string,
	missingAssignedRunners, availableCapacity int,
) error {
	if missingAssignedRunners <= 0 || availableCapacity > 0 {
		return nil
	}
	usability, supported := worker.(runnerUsabilityReconciler)
	if !supported {
		return nil
	}
	if _, err := usability.ReconcileRunnerUsability(ctx); err != nil {
		return fmt.Errorf("reconcile blocked runner usability for repository %q: %w", repository, err)
	}
	return nil
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
