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
	ObligationID          int64
	Source                CapacityObligationSource
	AssignedObservationID int64
	ControllerEpoch       int64
	Attempt               int
	NextAttemptAt         time.Time
	observationOrder      uint64
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

// CapacityObligationStore extends positive-request demand with durable,
// source-aware assigned-count obligations. Coordinators can retain DemandStore
// compatibility while selecting this capability for unified scheduling.
type CapacityObligationStore interface {
	DemandStore
	ObserveAssignedCapacity(ctx context.Context, observation AssignedCapacityObservation, activeCapacity int) (AssignedObligationDecision, error)
	OrderedReady(ctx context.Context, scaleSet string, cutoff time.Time) ([]QueuedDemand, error)
	RecordObligationFailure(ctx context.Context, obligationID int64, nextAttemptAt time.Time) error
	DeleteObligation(ctx context.Context, obligationID int64) error
}

// LifecycleOwnedObligationStore completes obligation-to-lifecycle ownership
// handoffs independently from scheduling operations.
type LifecycleOwnedObligationStore interface {
	DeleteLifecycleOwnedObligations(ctx context.Context, scaleSet string) error
}

type Coordinator struct {
	maxRunners              int
	scaleSet                string
	store                   DemandStore
	workers                 map[string]RepositoryWorker
	assigned                map[string]int
	assignedControllers     map[string]*AssignedJobController
	controllerAppID         int64
	controllerEpoch         int64
	inFlightObligations     map[int64]struct{}
	background              sync.WaitGroup
	safetyLoop              sync.Once
	executionDone           <-chan struct{}
	schedulingErrorHandler  func(error)
	pendingSchedulingErrors []error
	now                     func() time.Time
	provisioningGate        chan struct{}
	provisioningSuspended   atomic.Bool
	mutex                   sync.Mutex
}

func NewCoordinator(maxRunners int) *Coordinator {
	return NewDurableCoordinator("", maxRunners, createMemoryDemandStore())
}

func NewDurableCoordinator(scaleSet string, maxRunners int, store DemandStore) *Coordinator {
	coordinator := &Coordinator{
		maxRunners:          maxRunners,
		scaleSet:            scaleSet,
		store:               store,
		workers:             map[string]RepositoryWorker{},
		assigned:            map[string]int{},
		assignedControllers: map[string]*AssignedJobController{},
		inFlightObligations: map[int64]struct{}{},
		now:                 time.Now,
		provisioningGate:    make(chan struct{}, 1),
	}
	coordinator.provisioningGate <- struct{}{}
	return coordinator
}

// SetControllerEpoch fences standalone capacity observations and execution to
// the current repository-controller term.
func (coordinator *Coordinator) SetControllerEpoch(epoch int64) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.controllerEpoch = epoch
}

// SetControllerIdentity fences durable message ingestion to the active
// repository-controller term.
func (coordinator *Coordinator) SetControllerIdentity(appID int64, epoch int64) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.controllerAppID = appID
	coordinator.controllerEpoch = epoch
}

func (coordinator *Coordinator) AddRepository(repository string, worker RepositoryWorker) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.workers[repository] = worker
	coordinator.assignedController(repository)
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
	delete(coordinator.assignedControllers, repository)
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
	if store, supported := coordinator.store.(CapacityObligationStore); supported {
		return coordinator.scheduleCapacityObligations(ctx, store)
	}
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	_, _, err := coordinator.reconcileProvisioning(ctx)
	return err
}

// SetSchedulingErrorHandler installs the observer for asynchronous obligation
// execution and safety-loop failures. Errors are retained until an observer is
// installed, so background failures are never silently discarded.
func (coordinator *Coordinator) SetSchedulingErrorHandler(handler func(error)) {
	coordinator.mutex.Lock()
	pending := coordinator.pendingSchedulingErrors
	coordinator.pendingSchedulingErrors = nil
	coordinator.schedulingErrorHandler = handler
	coordinator.mutex.Unlock()
	if handler != nil {
		for _, err := range pending {
			handler(err)
		}
	}
}

func (coordinator *Coordinator) reportSchedulingError(err error) {
	if err == nil {
		return
	}
	coordinator.mutex.Lock()
	handler := coordinator.schedulingErrorHandler
	if handler == nil {
		coordinator.pendingSchedulingErrors = append(coordinator.pendingSchedulingErrors, err)
	}
	coordinator.mutex.Unlock()
	if handler != nil {
		handler(err)
	}
}

// WaitForScheduling waits until all capacity-obligation executions launched by
// this coordinator have completed. It is primarily useful at shutdown and in tests.
func (coordinator *Coordinator) WaitForScheduling() {
	coordinator.background.Wait()
}

// UsesDurableCapacityObligations reports whether this coordinator's store can
// persist aggregate capacity alongside exact GitHub demand.
func (coordinator *Coordinator) UsesDurableCapacityObligations() bool {
	_, supported := coordinator.store.(CapacityObligationStore)
	return supported
}

// StartSchedulingSafetyLoop retries due obligations even when a wake-up signal
// was lost. Repeated calls retain a single loop for the coordinator.
func (coordinator *Coordinator) StartSchedulingSafetyLoop(ctx context.Context, interval time.Duration) {
	coordinator.safetyLoop.Do(func() {
		coordinator.mutex.Lock()
		coordinator.executionDone = ctx.Done()
		coordinator.mutex.Unlock()
		coordinator.background.Add(1)
		go func() {
			defer coordinator.background.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					coordinator.reportSchedulingError(coordinator.Schedule(ctx))
				}
			}
		}()
	})
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
	_, err = coordinator.admitRemainingDemand(ctx, remainingDemand, runningCount, repositoryRunningCounts)
	if err != nil {
		return nil, err
	}
	return coordinator.reconcileRetainedAssigned(ctx)
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
	store, supported := coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return nil, nil
	}
	return loadGenericPendingRunnerCapacity(ctx, store, coordinator.scaleSet)
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

// ReconcileAssigned starts enough identity-free runners for one repository's
// assigned-job statistic without exceeding shared scale-set capacity.
func (coordinator *Coordinator) ReconcileAssigned(ctx context.Context, repository string, assignedJobs int) (AssignedReconciliation, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.assigned[repository] = assignedJobs
	reconciliations, provisioningEnabled, err := coordinator.reconcileProvisioning(ctx)
	if !provisioningEnabled || err != nil {
		if err != nil {
			return AssignedReconciliation{}, err
		}
		return AssignedReconciliation{AssignedJobs: assignedJobs}, nil
	}
	return reconciliations[repository], nil
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
) (map[string]AssignedReconciliation, error) {
	repositories := make([]string, 0, len(coordinator.assigned))
	for repository := range coordinator.assigned {
		repositories = append(repositories, repository)
	}
	sort.Strings(repositories)
	reconciliations := make(map[string]AssignedReconciliation, len(repositories))
	for _, repository := range repositories {
		if coordinator.workers[repository] == nil {
			reconciliations[repository] = AssignedReconciliation{AssignedJobs: coordinator.assigned[repository]}
			continue
		}
		result, err := coordinator.assignedController(repository).Reconcile(ctx, coordinator.assigned[repository])
		if err != nil {
			return nil, err
		}
		reconciliations[repository] = result
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
