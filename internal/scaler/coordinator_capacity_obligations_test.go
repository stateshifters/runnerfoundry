package scaler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCoordinatorExecutesSourceSpecificCapacityObligations(t *testing.T) {
	now := time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC)
	store := newRecordingCapacityObligationStore([]QueuedDemand{
		capacityTestDemand(1, "aggregate", 0, CapacityObligationAssignedCount, now),
		capacityTestDemand(2, "exact", 42, CapacityObligationJobAvailable, now.Add(time.Second)),
	})
	aggregate := &recordingCapacityWorker{}
	exact := &recordingCapacityWorker{}
	coordinator := NewDurableCoordinator("linux", 2, store)
	coordinator.SetControllerEpoch(7)
	coordinator.now = func() time.Time { return now.Add(2 * time.Second) }
	coordinator.AddRepository("aggregate", aggregate)
	coordinator.AddRepository("exact", exact)

	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	coordinator.WaitForScheduling()

	assertAggregateObligationExecution(t, aggregate, store)
	assertExactObligationExecution(t, exact)
}

func assertAggregateObligationExecution(
	t *testing.T,
	worker *recordingCapacityWorker,
	store *recordingCapacityObligationStore,
) {
	t.Helper()
	if len(worker.assignedObligations) != 1 || worker.assignedObligations[0] != 1 {
		t.Fatalf("assigned obligation starts = %#v, want [1]", worker.assignedObligations)
	}
	if len(worker.acquired) != 0 || len(worker.namedStarts) != 0 {
		t.Fatalf("aggregate obligation used exact path: acquired=%#v named=%#v", worker.acquired, worker.namedStarts)
	}
	if store.wasDeleted(1) {
		t.Fatal("successful aggregate obligation was removed before terminal completion")
	}
}

func assertExactObligationExecution(t *testing.T, worker *recordingCapacityWorker) {
	t.Helper()
	if len(worker.acquired) != 1 || worker.acquired[0] != 42 {
		t.Fatalf("exact acquisition = %#v, want [42]", worker.acquired)
	}
	if len(worker.namedStarts) != 1 || worker.namedStarts[0] != 42 {
		t.Fatalf("exact named starts = %#v, want [42]", worker.namedStarts)
	}
}

func TestCoordinatorBacksOffFailureWithoutBlockingLaterObligation(t *testing.T) {
	now := time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC)
	store := newRecordingCapacityObligationStore([]QueuedDemand{
		capacityTestDemand(1, "broken", 0, CapacityObligationAssignedCount, now),
		capacityTestDemand(2, "healthy", 0, CapacityObligationAssignedCount, now.Add(time.Second)),
	})
	broken := &recordingCapacityWorker{assignedErr: errors.New("provider unavailable")}
	healthy := &recordingCapacityWorker{}
	coordinator := NewDurableCoordinator("linux", 2, store)
	coordinator.SetControllerEpoch(7)
	coordinator.now = func() time.Time { return now.Add(2 * time.Second) }
	coordinator.AddRepository("broken", broken)
	coordinator.AddRepository("healthy", healthy)

	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	coordinator.WaitForScheduling()

	if len(healthy.assignedObligations) != 1 {
		t.Fatalf("later runnable obligation did not progress: %#v", healthy.assignedObligations)
	}
	if retryAt := store.retryTime(1); !retryAt.Equal(now.Add(7 * time.Second)) {
		t.Fatalf("first retry = %s, want %s", retryAt, now.Add(7*time.Second))
	}
}

func capacityTestDemand(
	obligationID int64,
	repository string,
	requestID int64,
	source CapacityObligationSource,
	queueTime time.Time,
) QueuedDemand {
	return QueuedDemand{
		JobDemand:    JobDemand{Repository: repository, RequestID: requestID, QueueTime: queueTime},
		ObligationID: obligationID, Source: source, ControllerEpoch: 7, NextAttemptAt: queueTime,
	}
}

type recordingCapacityWorker struct {
	mutex               sync.Mutex
	acquired            []int64
	namedStarts         []int64
	assignedObligations []int64
	assignedErr         error
}

func (*recordingCapacityWorker) Running(context.Context) (int, error)        { return 0, nil }
func (*recordingCapacityWorker) Exists(context.Context, int64) (bool, error) { return false, nil }
func (worker *recordingCapacityWorker) Acquire(_ context.Context, requestID int64) error {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()
	worker.acquired = append(worker.acquired, requestID)
	return nil
}
func (worker *recordingCapacityWorker) StartOne(_ context.Context, requestID int64) error {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()
	worker.namedStarts = append(worker.namedStarts, requestID)
	return nil
}
func (*recordingCapacityWorker) StartAssigned(context.Context) error { return nil }
func (worker *recordingCapacityWorker) StartAssignedObligation(_ context.Context, obligationID int64) error {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()
	worker.assignedObligations = append(worker.assignedObligations, obligationID)
	return worker.assignedErr
}

type recordingCapacityObligationStore struct {
	mutex          sync.Mutex
	obligations    []QueuedDemand
	deleted        map[int64]bool
	retries        map[int64]time.Time
	observation    AssignedCapacityObservation
	activeCapacity int
	observeErr     error
	orderedErr     error
	applyErr       error
	deleteErr      error
	cleanupErr     error
	retryErr       error
}

func newRecordingCapacityObligationStore(obligations []QueuedDemand) *recordingCapacityObligationStore {
	return &recordingCapacityObligationStore{
		obligations: obligations, deleted: map[int64]bool{}, retries: map[int64]time.Time{},
	}
}

func (store *recordingCapacityObligationStore) Apply(context.Context, string, []JobDemand, []DemandRemoval) error {
	return store.applyErr
}
func (*recordingCapacityObligationStore) RemoveRepository(context.Context, string, string) error {
	return nil
}
func (*recordingCapacityObligationStore) RemoveScaleSet(context.Context, string) error { return nil }
func (store *recordingCapacityObligationStore) Ordered(ctx context.Context, scaleSet string) ([]QueuedDemand, error) {
	return store.OrderedReady(ctx, scaleSet, time.Time{})
}
func (store *recordingCapacityObligationStore) ObserveAssignedCapacity(
	_ context.Context,
	observation AssignedCapacityObservation,
	activeCapacity int,
) (AssignedObligationDecision, error) {
	store.observation = observation
	store.activeCapacity = activeCapacity
	return AssignedObligationDecision{}, store.observeErr
}
func (store *recordingCapacityObligationStore) OrderedReady(_ context.Context, _ string, cutoff time.Time) ([]QueuedDemand, error) {
	if store.orderedErr != nil {
		return nil, store.orderedErr
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	result := make([]QueuedDemand, 0, len(store.obligations))
	for _, obligation := range store.obligations {
		if store.deleted[obligation.ObligationID] || (!cutoff.IsZero() && obligation.NextAttemptAt.After(cutoff)) {
			continue
		}
		result = append(result, obligation)
	}
	return result, nil
}
func (store *recordingCapacityObligationStore) RecordObligationFailure(_ context.Context, obligationID int64, retryAt time.Time) error {
	if store.retryErr != nil {
		return store.retryErr
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.retries[obligationID] = retryAt
	return nil
}
func (store *recordingCapacityObligationStore) DeleteObligation(_ context.Context, obligationID int64) error {
	if store.deleteErr != nil {
		return store.deleteErr
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.deleted[obligationID] = true
	return nil
}
func (store *recordingCapacityObligationStore) DeleteLifecycleOwnedObligations(context.Context, string) error {
	return store.cleanupErr
}
func (store *recordingCapacityObligationStore) wasDeleted(obligationID int64) bool {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.deleted[obligationID]
}
func (store *recordingCapacityObligationStore) retryTime(obligationID int64) time.Time {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.retries[obligationID]
}
