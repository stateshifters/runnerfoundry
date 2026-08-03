package scaler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCoordinatorReportsRetainedAndLiveSchedulingErrors(t *testing.T) {
	coordinator := NewCoordinator(1)
	coordinator.reportSchedulingError(nil)
	coordinator.reportSchedulingError(errors.New("retained"))
	reported := make([]string, 0, 2)
	coordinator.SetSchedulingErrorHandler(func(err error) { reported = append(reported, err.Error()) })
	coordinator.reportSchedulingError(errors.New("live"))
	coordinator.SetSchedulingErrorHandler(nil)
	if len(reported) != 2 || reported[0] != "retained" || reported[1] != "live" {
		t.Fatalf("reported errors = %#v", reported)
	}
	if coordinator.UsesDurableCapacityObligations() {
		t.Fatal("memory coordinator unexpectedly reports durable obligations")
	}
}

func TestCoordinatorSafetyLoopReportsSchedulingFailureAndStops(t *testing.T) {
	store := newRecordingCapacityObligationStore(nil)
	store.orderedErr = errors.New("queue unavailable")
	coordinator := NewDurableCoordinator("linux", 1, store)
	reported := make(chan error, 1)
	coordinator.SetSchedulingErrorHandler(func(err error) { reported <- err })
	controlCtx, cancelControl := context.WithCancel(context.Background())
	coordinator.StartSchedulingSafetyLoop(controlCtx, time.Millisecond)
	coordinator.StartSchedulingSafetyLoop(controlCtx, time.Millisecond)
	select {
	case <-reported:
	case <-time.After(time.Second):
		t.Fatal("safety scheduling failure was not reported")
	}
	cancelControl()
	coordinator.WaitForScheduling()
	if !coordinator.UsesDurableCapacityObligations() {
		t.Fatal("capacity store was not detected")
	}
}

func TestCoordinatorObservationUsesFallbackAndLifecycleCapacity(t *testing.T) {
	now := time.Now().UTC()
	fallbackStore := newRecordingCapacityObligationStore(nil)
	fallbackWorker := &capacityObservationWorker{running: 2}
	fallback := NewDurableCoordinator("linux", 3, fallbackStore)
	fallback.SetControllerEpoch(4)
	fallback.AddRepository("repo", fallbackWorker)
	if _, err := fallback.ObserveAssignedCapacity(context.Background(), "repo", 3, now); err != nil {
		t.Fatal(err)
	}
	assertRecordedObservation(t, fallbackStore, 4, 2)

	inventoryStore := &inventoryRecordingCapacityStore{
		recordingCapacityObligationStore: newRecordingCapacityObligationStore(nil),
		lifecycles:                       []RunnerLifecycle{{Phase: RunnerBusy}, {Phase: RunnerReapPending}},
	}
	inventory := NewDurableCoordinator("linux", 3, inventoryStore)
	inventory.SetControllerEpoch(5)
	if _, supported := inventory.store.(RunnerLifecycleInventory); !supported {
		t.Fatal("inventory capability was not retained")
	}
	if _, err := inventory.ObserveAssignedCapacity(context.Background(), "repo", 2, now); err != nil {
		t.Fatal(err)
	}
	if available, err := inventory.availableObligationCapacity(context.Background()); err != nil || available != 2 {
		t.Fatalf("inventory capacity = %d, %v", available, err)
	}
	assertRecordedObservation(t, inventoryStore.recordingCapacityObligationStore, 5, 1)
}

func assertRecordedObservation(t *testing.T, store *recordingCapacityObligationStore, epoch int64, active int) {
	t.Helper()
	if store.observation.ControllerEpoch != epoch || store.activeCapacity != active {
		t.Fatalf("observation = %#v active=%d", store.observation, store.activeCapacity)
	}
}

func TestCoordinatorObservationFailurePaths(t *testing.T) {
	now := time.Now().UTC()
	legacy := NewCoordinator(1)
	if _, err := legacy.ObserveAssignedCapacity(context.Background(), "repo", 1, now); err != nil {
		t.Fatal(err)
	}
	if legacy.assigned["repo"] != 1 {
		t.Fatal("legacy observation was not retained")
	}

	runningFailure := NewDurableCoordinator("linux", 1, newRecordingCapacityObligationStore(nil))
	runningFailure.AddRepository("repo", &capacityObservationWorker{runningErr: errors.New("provider")})
	if _, err := runningFailure.ObserveAssignedCapacity(context.Background(), "repo", 1, now); err == nil {
		t.Fatal("expected provider capacity failure")
	}

	inventoryStore := &inventoryRecordingCapacityStore{
		recordingCapacityObligationStore: newRecordingCapacityObligationStore(nil), inventoryErr: errors.New("inventory"),
	}
	inventoryFailure := NewDurableCoordinator("linux", 1, inventoryStore)
	if _, err := inventoryFailure.ObserveAssignedCapacity(context.Background(), "repo", 1, now); err == nil {
		t.Fatal("expected lifecycle inventory failure")
	}

	observeStore := newRecordingCapacityObligationStore(nil)
	observeStore.observeErr = errors.New("persist")
	observeFailure := NewDurableCoordinator("linux", 1, observeStore)
	if _, err := observeFailure.ObserveAssignedCapacity(context.Background(), "repo", 1, now); err == nil {
		t.Fatal("expected observation persistence failure")
	}
}

func TestCoordinatorScheduleFailureAndClaimBranches(t *testing.T) {
	store := newRecordingCapacityObligationStore(nil)
	coordinator := NewDurableCoordinator("linux", 1, store)
	coordinator.SetControllerEpoch(7)
	<-coordinator.provisioningGate
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := coordinator.Schedule(canceledCtx); err == nil {
		t.Fatal("expected scheduling boundary cancellation")
	}
	coordinator.releaseProvisioning()

	coordinator.provisioningSuspended.Store(true)
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	coordinator.provisioningSuspended.Store(false)
	assertCoordinatorClaimBranches(t, coordinator)
}

func assertCoordinatorClaimBranches(t *testing.T, coordinator *Coordinator) {
	t.Helper()
	if coordinator.claimObligation(capacityTestDemand(1, "missing", 0, CapacityObligationAssignedCount, time.Now())) {
		t.Fatal("claimed obligation without worker")
	}
	coordinator.AddRepository("repo", &recordingCapacityWorker{})
	invalid := capacityTestDemand(0, "repo", 0, CapacityObligationAssignedCount, time.Now())
	if coordinator.claimObligation(invalid) {
		t.Fatal("claimed zero obligation ID")
	}
	stale := capacityTestDemand(2, "repo", 0, CapacityObligationAssignedCount, time.Now())
	stale.ControllerEpoch = 6
	if coordinator.claimObligation(stale) {
		t.Fatal("claimed stale obligation")
	}
	valid := capacityTestDemand(3, "repo", 0, CapacityObligationAssignedCount, time.Now())
	if !coordinator.claimObligation(valid) || coordinator.claimObligation(valid) {
		t.Fatal("single-flight claim contract failed")
	}
}

func TestCoordinatorScheduleStoreAndCapacityFailures(t *testing.T) {
	orderedStore := newRecordingCapacityObligationStore(nil)
	orderedStore.orderedErr = errors.New("ordered")
	if err := NewDurableCoordinator("linux", 1, orderedStore).Schedule(context.Background()); err == nil {
		t.Fatal("expected ordered failure")
	}

	capacityStore := newRecordingCapacityObligationStore(nil)
	capacity := NewDurableCoordinator("linux", 1, capacityStore)
	capacity.AddRepository("repo", &capacityObservationWorker{runningErr: errors.New("running")})
	if err := capacity.Schedule(context.Background()); err == nil {
		t.Fatal("expected capacity inspection failure")
	}

	fullStore := newRecordingCapacityObligationStore([]QueuedDemand{
		capacityTestDemand(1, "repo", 0, CapacityObligationAssignedCount, time.Now()),
	})
	full := NewDurableCoordinator("linux", 0, fullStore)
	full.SetControllerEpoch(7)
	full.AddRepository("repo", &recordingCapacityWorker{})
	if err := full.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorExecutionFailureBranches(t *testing.T) {
	store := newRecordingCapacityObligationStore(nil)
	coordinator := NewDurableCoordinator("linux", 1, store)
	aggregate := capacityTestDemand(1, "repo", 0, CapacityObligationAssignedCount, time.Now())
	if err := coordinator.executeAssignedObligation(context.Background(), &plainCapacityWorker{}, aggregate); err == nil {
		t.Fatal("expected unsupported aggregate worker")
	}
	if err := coordinator.executeAssignedObligation(context.Background(), &recordingCapacityWorker{assignedErr: errors.New("start")}, aggregate); err == nil {
		t.Fatal("expected aggregate start failure")
	}
	assertExactExecutionBranches(t, coordinator, store)
}

func assertExactExecutionBranches(t *testing.T, coordinator *Coordinator, store *recordingCapacityObligationStore) {
	t.Helper()
	store.deleteErr = nil
	exact := capacityTestDemand(2, "repo", 42, CapacityObligationJobAvailable, time.Now())
	assertExactExecutionFailure(t, coordinator, store, exact, &plainCapacityWorker{existsErr: errors.New("exists")})
	existing := &plainCapacityWorker{exists: true}
	if err := coordinator.executeExactObligation(context.Background(), store, existing, exact); err != nil {
		t.Fatal(err)
	}
	assertExactExecutionFailure(t, coordinator, store, exact, &plainCapacityWorker{acquireErr: errors.New("acquire")})
	store.applyErr = errors.New("apply")
	assertExactExecutionFailure(t, coordinator, store, exact, &plainCapacityWorker{})
	store.applyErr = nil
	assertExactExecutionFailure(t, coordinator, store, exact, &plainCapacityWorker{startErr: errors.New("start")})
	exact.Acquired = true
	if err := coordinator.executeExactObligation(context.Background(), store, &plainCapacityWorker{}, exact); err != nil {
		t.Fatal(err)
	}
}

func assertExactExecutionFailure(
	t *testing.T,
	coordinator *Coordinator,
	store CapacityObligationStore,
	obligation QueuedDemand,
	worker RepositoryWorker,
) {
	t.Helper()
	if err := coordinator.executeExactObligation(context.Background(), store, worker, obligation); err == nil {
		t.Fatal("expected exact execution failure")
	}
}

func TestCoordinatorExecutionOutlivesTriggerAndUsesIndependentContexts(t *testing.T) {
	store := newRecordingCapacityObligationStore([]QueuedDemand{
		capacityTestDemand(1, "repo", 42, CapacityObligationJobAvailable, time.Now()),
	})
	worker := &contextInspectingCapacityWorker{acquireContextHealthy: make(chan bool, 1)}
	coordinator := NewDurableCoordinator("linux", 1, store)
	coordinator.SetControllerEpoch(7)
	coordinator.AddRepository("repo", worker)
	controlCtx, cancelControl := context.WithCancel(context.Background())
	coordinator.StartSchedulingSafetyLoop(controlCtx, time.Hour)
	triggerCtx, cancelTrigger := context.WithCancel(context.Background())
	if err := coordinator.Schedule(triggerCtx); err != nil {
		t.Fatal(err)
	}
	cancelTrigger()
	select {
	case healthy := <-worker.acquireContextHealthy:
		if !healthy {
			t.Fatal("acquire inherited canceled trigger or prior operation context")
		}
	case <-time.After(time.Second):
		t.Fatal("exact execution did not progress")
	}
	cancelControl()
	coordinator.WaitForScheduling()
}

func TestCapacityObligationRetryDelayBounds(t *testing.T) {
	if capacityObligationRetryDelay(-1) != 5*time.Second ||
		capacityObligationRetryDelay(2) != 20*time.Second ||
		capacityObligationRetryDelay(99) != 60*time.Second {
		t.Fatal("retry delays are not bounded")
	}
}

func TestCoordinatorRemainingCapacityObligationBranches(t *testing.T) {
	baseStore := newRecordingCapacityObligationStore(nil)
	inventoryStore := &inventoryRecordingCapacityStore{
		recordingCapacityObligationStore: baseStore, inventoryErr: errors.New("inventory"),
	}
	inventoryCoordinator := NewDurableCoordinator("linux", 1, inventoryStore)
	if _, err := inventoryCoordinator.activeLifecycleCapacity(context.Background(), inventoryStore); err == nil {
		t.Fatal("expected lifecycle capacity failure")
	}
	assertCapacityExecutionWrapperBranches(t, baseStore)
	assertCapacityOperationFence(t)
}

func assertCapacityExecutionWrapperBranches(t *testing.T, baseStore *recordingCapacityObligationStore) {
	t.Helper()
	missingWorker := NewDurableCoordinator("linux", 1, baseStore)
	missingWorker.background.Add(1)
	missingWorker.executeCapacityObligation(context.Background(), baseStore,
		capacityTestDemand(1, "missing", 0, CapacityObligationAssignedCount, time.Now()))

	retryStore := newRecordingCapacityObligationStore(nil)
	retryStore.retryErr = errors.New("retry persistence")
	retryCoordinator := NewDurableCoordinator("linux", 1, retryStore)
	retryCoordinator.AddRepository("repo", &recordingCapacityWorker{assignedErr: errors.New("start")})
	reported := make(chan error, 1)
	retryCoordinator.SetSchedulingErrorHandler(func(err error) { reported <- err })
	retryCoordinator.background.Add(1)
	retryCoordinator.executeCapacityObligation(context.Background(), retryStore,
		capacityTestDemand(2, "repo", 0, CapacityObligationAssignedCount, time.Now()))
	select {
	case <-reported:
	default:
		t.Fatal("retry persistence failure was not reported")
	}
}

func assertCapacityOperationFence(t *testing.T) {
	t.Helper()
	controlCtx, cancelControl := context.WithCancel(context.Background())
	fenced := NewCoordinator(1)
	fenced.StartSchedulingSafetyLoop(controlCtx, time.Hour)
	operationCtx, cancelOperation := fenced.capacityOperationContext(context.Background())
	cancelControl()
	select {
	case <-operationCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("control shutdown did not fence operation context")
	}
	cancelOperation()
	fenced.WaitForScheduling()
}

type capacityObservationWorker struct {
	running    int
	runningErr error
}

func (worker *capacityObservationWorker) Running(context.Context) (int, error) {
	return worker.running, worker.runningErr
}
func (*capacityObservationWorker) Exists(context.Context, int64) (bool, error) { return false, nil }
func (*capacityObservationWorker) Acquire(context.Context, int64) error        { return nil }
func (*capacityObservationWorker) StartOne(context.Context, int64) error       { return nil }
func (*capacityObservationWorker) StartAssigned(context.Context) error         { return nil }

type inventoryRecordingCapacityStore struct {
	*recordingCapacityObligationStore
	lifecycles   []RunnerLifecycle
	inventoryErr error
}

func (store *inventoryRecordingCapacityStore) RunnerLifecycles(context.Context, string, string) ([]RunnerLifecycle, error) {
	return store.lifecycles, store.inventoryErr
}

type plainCapacityWorker struct {
	exists     bool
	existsErr  error
	acquireErr error
	startErr   error
}

func (*plainCapacityWorker) Running(context.Context) (int, error) { return 0, nil }
func (worker *plainCapacityWorker) Exists(context.Context, int64) (bool, error) {
	return worker.exists, worker.existsErr
}
func (worker *plainCapacityWorker) Acquire(context.Context, int64) error  { return worker.acquireErr }
func (worker *plainCapacityWorker) StartOne(context.Context, int64) error { return worker.startErr }
func (*plainCapacityWorker) StartAssigned(context.Context) error          { return nil }

type contextInspectingCapacityWorker struct {
	acquireContextHealthy chan bool
}

func (*contextInspectingCapacityWorker) Running(context.Context) (int, error) { return 0, nil }
func (*contextInspectingCapacityWorker) Exists(context.Context, int64) (bool, error) {
	return false, nil
}
func (worker *contextInspectingCapacityWorker) Acquire(ctx context.Context, _ int64) error {
	worker.acquireContextHealthy <- ctx.Err() == nil
	return nil
}
func (*contextInspectingCapacityWorker) StartOne(context.Context, int64) error { return nil }
func (*contextInspectingCapacityWorker) StartAssigned(context.Context) error   { return nil }
