package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteAssignedObservationCreatesAndCancelsDurableObligations(t *testing.T) {
	store := openSQLiteCapacityStore(t)
	ctx := context.Background()
	observedAt := time.Unix(10, 20).UTC()
	observation := AssignedCapacityObservation{
		ScaleSet: "linux", Repository: "repo", ControllerEpoch: 3,
		AssignedJobs: 3, ObservedAt: observedAt,
	}

	assertAssignedObligationsCreated(ctx, t, store, observation, 2)
	observation.AssignedJobs = 1
	observation.ObservedAt = observedAt.Add(time.Second)
	decision, err := store.ObserveAssignedCapacity(ctx, observation, 1)
	if err != nil || decision.Cancel != 2 {
		t.Fatalf("lower observation = %#v, %v", decision, err)
	}
	demand, err := store.OrderedReady(ctx, "linux", observation.ObservedAt)
	if err != nil || len(demand) != 0 {
		t.Fatalf("remaining assigned demand = %#v, %v", demand, err)
	}
}

func assertAssignedObligationsCreated(
	ctx context.Context,
	t *testing.T,
	store *SQLiteDemandStore,
	observation AssignedCapacityObservation,
	want int,
) {
	t.Helper()
	decision, err := store.ObserveAssignedCapacity(ctx, observation, 1)
	if err != nil || decision.Create != want {
		t.Fatalf("observe assigned capacity = %#v, %v", decision, err)
	}
	demand, err := store.OrderedReady(ctx, observation.ScaleSet, observation.ObservedAt)
	if err != nil || len(demand) != want {
		t.Fatalf("assigned demand = %#v, %v", demand, err)
	}
	for _, obligation := range demand {
		assertAssignedObligation(t, obligation, observation.ControllerEpoch)
	}
}

func assertAssignedObligation(t *testing.T, obligation QueuedDemand, epoch int64) {
	t.Helper()
	if obligation.Source != CapacityObligationAssignedCount || obligation.RequestID != 0 ||
		obligation.AssignedObservationID == 0 || obligation.ControllerEpoch != epoch {
		t.Fatalf("assigned obligation = %#v", obligation)
	}
}

func TestSQLiteCapacityObligationRetrySurvivesStoreReopen(t *testing.T) {
	path, retryAt := createDeferredSQLiteObligation(t)
	store, err := OpenSQLiteDemandStore(path)
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteStoreDuringCleanup(t, store)
	ctx := context.Background()
	if ready, readyErr := store.OrderedReady(ctx, "linux", retryAt.Add(-time.Nanosecond)); readyErr != nil || len(ready) != 0 {
		t.Fatalf("premature retry = %#v, %v", ready, readyErr)
	}
	ready, err := store.OrderedReady(ctx, "linux", retryAt)
	if err != nil || len(ready) != 1 || ready[0].Attempt != 1 || ready[0].NextAttemptAt != retryAt {
		t.Fatalf("durable retry = %#v, %v", ready, err)
	}
}

func createDeferredSQLiteObligation(t *testing.T) (string, time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenSQLiteDemandStore(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	observedAt := time.Unix(30, 0).UTC()
	_, err = store.ObserveAssignedCapacity(ctx, AssignedCapacityObservation{
		ScaleSet: "linux", Repository: "repo", ControllerEpoch: 1,
		AssignedJobs: 1, ObservedAt: observedAt,
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	demand, err := store.OrderedReady(ctx, "linux", observedAt)
	if err != nil || len(demand) != 1 {
		t.Fatalf("initial demand = %#v, %v", demand, err)
	}
	retryAt := observedAt.Add(time.Minute)
	if err = store.RecordObligationFailure(ctx, demand[0].ObligationID, retryAt); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	return path, retryAt
}

func TestSQLiteRepositoryAndScaleSetRemovalDiscardAssignedObservationAuthority(t *testing.T) {
	for _, remove := range []func(context.Context, *SQLiteDemandStore) error{
		func(ctx context.Context, store *SQLiteDemandStore) error {
			return store.RemoveRepository(ctx, "linux", "repo")
		},
		func(ctx context.Context, store *SQLiteDemandStore) error { return store.RemoveScaleSet(ctx, "linux") },
	} {
		store := openSQLiteCapacityStore(t)
		ctx := context.Background()
		newer := AssignedCapacityObservation{
			ScaleSet: "linux", Repository: "repo", ControllerEpoch: 2,
			AssignedJobs: 1, ObservedAt: time.Unix(50, 0).UTC(),
		}
		if _, err := store.ObserveAssignedCapacity(ctx, newer, 0); err != nil {
			t.Fatal(err)
		}
		if err := remove(ctx, store); err != nil {
			t.Fatal(err)
		}
		olderReplacement := newer
		olderReplacement.ObservedAt = newer.ObservedAt.Add(-time.Second)
		decision, err := store.ObserveAssignedCapacity(ctx, olderReplacement, 0)
		if err != nil || decision.Create != 1 {
			t.Fatalf("replacement observation after removal = %#v, %v", decision, err)
		}
	}
}

func TestSQLiteRunnableFIFOAllowsLaterSourceToOvertakeBackoff(t *testing.T) {
	store := openSQLiteCapacityStore(t)
	ctx := context.Background()
	assignedAt := time.Unix(60, 0).UTC()
	if _, err := store.ObserveAssignedCapacity(ctx, AssignedCapacityObservation{
		ScaleSet: "linux", Repository: "assigned", ControllerEpoch: 1,
		AssignedJobs: 1, ObservedAt: assignedAt,
	}, 0); err != nil {
		t.Fatal(err)
	}
	availableAt := assignedAt.Add(time.Second)
	if err := store.Apply(ctx, "linux", []JobDemand{{
		Repository: "available", RequestID: 42, QueueTime: availableAt,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	ordered, err := store.OrderedReady(ctx, "linux", availableAt)
	if err != nil || len(ordered) != 2 || ordered[0].Source != CapacityObligationAssignedCount ||
		ordered[1].Source != CapacityObligationJobAvailable {
		t.Fatalf("mixed FIFO = %#v, %v", ordered, err)
	}
	if err = store.RecordObligationFailure(ctx, ordered[0].ObligationID, availableAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	runnable, err := store.OrderedReady(ctx, "linux", availableAt)
	if err != nil || len(runnable) != 1 || runnable[0].RequestID != 42 {
		t.Fatalf("runnable FIFO during backoff = %#v, %v", runnable, err)
	}
}

func TestSQLiteAssignedObservationFencesStaleEpochAndTimestamp(t *testing.T) {
	store := openSQLiteCapacityStore(t)
	ctx := context.Background()
	latest := AssignedCapacityObservation{
		ScaleSet: "linux", Repository: "repo", ControllerEpoch: 2,
		AssignedJobs: 1, ObservedAt: time.Unix(70, 0).UTC(),
	}
	if _, err := store.ObserveAssignedCapacity(ctx, latest, 0); err != nil {
		t.Fatal(err)
	}
	for _, stale := range []AssignedCapacityObservation{
		{ScaleSet: "linux", Repository: "repo", ControllerEpoch: 1, AssignedJobs: 4, ObservedAt: latest.ObservedAt.Add(time.Hour)},
		{ScaleSet: "linux", Repository: "repo", ControllerEpoch: 2, AssignedJobs: 4, ObservedAt: latest.ObservedAt.Add(-time.Second)},
	} {
		decision, err := store.ObserveAssignedCapacity(ctx, stale, 0)
		if err != nil || decision != (AssignedObligationDecision{}) {
			t.Fatalf("stale observation = %#v, %v", decision, err)
		}
	}
	obligations, err := store.OrderedReady(ctx, "linux", latest.ObservedAt.Add(time.Hour))
	if err != nil || len(obligations) != 1 || obligations[0].ControllerEpoch != 2 {
		t.Fatalf("fenced obligations = %#v, %v", obligations, err)
	}
}

func openSQLiteCapacityStore(t *testing.T) *SQLiteDemandStore {
	t.Helper()
	store, err := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteStoreDuringCleanup(t, store)
	return store
}

func closeSQLiteStoreDuringCleanup(t *testing.T, store *SQLiteDemandStore) {
	t.Helper()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close SQLite store: %v", err)
		}
	})
}
