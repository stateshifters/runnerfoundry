package scaler

import (
	"context"
	"errors"
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
	maxRunners int
	scaleSet   string
	store      DemandStore
	workers    map[string]RepositoryWorker
	assigned   map[string]int
	mutex      sync.Mutex
}

func (coordinator *Coordinator) ConfigureRunnerLifecycle(
	reconciler *Reconciler,
	repository string,
	provider RunnerProvider,
) error {
	store, supported := coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return errors.New("demand store does not support runner lifecycle")
	}
	reconciler.ScaleSet.Repository = repository
	reconciler.ScaleSet.ProviderKind = provider
	reconciler.ScaleSet.LifecycleScaleSet = coordinator.scaleSet
	reconciler.ScaleSet.Lifecycle = store
	return nil
}

func (coordinator *Coordinator) HasRunnerLifecycle() bool {
	_, supported := coordinator.store.(RunnerLifecycleStore)
	return supported
}

func (coordinator *Coordinator) MarkRunnerBusy(
	ctx context.Context,
	repository string,
	requestID int64,
	runnerName string,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	store, supported := coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return errors.New("demand store does not support runner lifecycle")
	}
	if err := store.MarkRunnerBusy(ctx, coordinator.scaleSet, repository, requestID, runnerName, githubRunnerID, startedAt); err != nil {
		return fmt.Errorf("mark runner busy: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) CompleteRunnerLifecycleJob(
	ctx context.Context,
	completion JobCompletion,
	completedAt time.Time,
) error {
	store, supported := coordinator.store.(RunnerJobCompletionStore)
	if !supported {
		return errors.New("demand store does not support atomic runner job completion")
	}
	completion.ScaleSet = coordinator.scaleSet
	if err := store.CompleteRunnerLifecycleJob(ctx, completion, completedAt); err != nil {
		return fmt.Errorf("complete runner lifecycle job: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) RequestExpiredRunnerReaps(
	ctx context.Context,
	repository string,
	jobStartTimeout time.Duration,
	now time.Time,
) (int, error) {
	store, supported := coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return 0, errors.New("demand store does not support runner lifecycle")
	}
	changed, err := store.RequestExpiredRunnerReaps(ctx, coordinator.scaleSet, repository, now.Add(-jobStartTimeout), now)
	if err != nil {
		return 0, fmt.Errorf("request expired runner reaps: %w", err)
	}
	return changed, nil
}

func (coordinator *Coordinator) PendingRunnerReaps(ctx context.Context, repository string) ([]RunnerLifecycle, error) {
	store, supported := coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return nil, errors.New("demand store does not support runner lifecycle")
	}
	pending, err := store.PendingRunnerReaps(ctx, coordinator.scaleSet, repository)
	if err != nil {
		return nil, fmt.Errorf("load pending runner reaps: %w", err)
	}
	return pending, nil
}

func (coordinator *Coordinator) ConfirmRunnerReaped(ctx context.Context, runner RunnerLifecycle) error {
	store, supported := coordinator.store.(RunnerLifecycleStore)
	if !supported {
		return errors.New("demand store does not support runner lifecycle")
	}
	if err := store.ConfirmRunnerReaped(ctx, runner); err != nil {
		return fmt.Errorf("confirm runner reaped: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) RegisterLibvirtRunner(ctx context.Context, runner LibvirtRunnerLifecycle) error {
	store, supported := coordinator.store.(LibvirtLifecycleStore)
	if !supported {
		return errors.New("demand store does not support libvirt runner lifecycle")
	}
	runner.ScaleSet = coordinator.scaleSet
	if err := store.RegisterLibvirtRunner(ctx, runner); err != nil {
		return fmt.Errorf("register libvirt runner: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) CompleteJob(ctx context.Context, completion JobCompletion) error {
	store, supported := coordinator.store.(LibvirtLifecycleStore)
	if !supported {
		return errors.New("demand store does not support libvirt runner lifecycle")
	}
	completion.ScaleSet = coordinator.scaleSet
	if err := store.CompleteJob(ctx, completion); err != nil {
		return fmt.Errorf("complete libvirt job: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) PendingLibvirtReaps(
	ctx context.Context,
	repository string,
) ([]LibvirtRunnerLifecycle, error) {
	store, supported := coordinator.store.(LibvirtLifecycleStore)
	if !supported {
		return nil, errors.New("demand store does not support libvirt runner lifecycle")
	}
	pending, err := store.PendingLibvirtReaps(ctx, coordinator.scaleSet, repository)
	if err != nil {
		return nil, fmt.Errorf("load pending libvirt reaps: %w", err)
	}
	return pending, nil
}

func (coordinator *Coordinator) ConfirmLibvirtReaped(ctx context.Context, runner LibvirtRunnerLifecycle) error {
	store, supported := coordinator.store.(LibvirtLifecycleStore)
	if !supported {
		return errors.New("demand store does not support libvirt runner lifecycle")
	}
	if err := store.ConfirmLibvirtReaped(ctx, runner); err != nil {
		return fmt.Errorf("confirm libvirt runner reaped: %w", err)
	}
	return nil
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
	return &Coordinator{
		maxRunners: maxRunners,
		scaleSet:   scaleSet,
		store:      store,
		workers:    map[string]RepositoryWorker{},
		assigned:   map[string]int{},
	}
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
	_, err := coordinator.reconcileCapacity(ctx)
	return err
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
	reconciliations, err := coordinator.reconcileCapacity(ctx)
	if err != nil {
		return AssignedReconciliation{}, err
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
	if completion.RunnerName != "" {
		runner := LibvirtRunnerLifecycle(completion)
		store.libvirtRunners[runnerLifecycleKey(runner)] = memoryLibvirtLifecycle{runner: runner, reapRequested: true}
	}
	for key, lifecycle := range store.libvirtRunners {
		if lifecycle.runner.ScaleSet == completion.ScaleSet && lifecycle.runner.Repository == completion.Repository &&
			lifecycle.runner.RequestID == completion.RequestID {
			lifecycle.reapRequested = true
			store.libvirtRunners[key] = lifecycle
		}
	}
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
	delete(store.libvirtRunners, runnerLifecycleKey(runner))
	return nil
}

func runnerLifecycleKey(runner LibvirtRunnerLifecycle) string {
	return runner.ScaleSet + "\x00" + runner.Repository + "\x00" + runner.RunnerName
}

type memoryLibvirtLifecycle struct {
	runner        LibvirtRunnerLifecycle
	reapRequested bool
}
