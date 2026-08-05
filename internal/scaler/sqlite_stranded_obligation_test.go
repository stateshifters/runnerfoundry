package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteStrandedAssignedCapacityStaysBlockedUntilStatisticsRecover(t *testing.T) {
	store := openSQLiteCapacityStore(t)
	ctx := context.Background()
	observation := AssignedCapacityObservation{
		ScaleSet: "windows", Repository: "repo", ControllerEpoch: 3,
		AssignedJobs: 1, BusyRunners: 1, ObservedAt: time.Unix(10, 20).UTC(),
	}

	decision, err := store.ObserveAssignedCapacity(ctx, observation, 0)
	if err != nil || decision.Create != 1 || decision.Block != 1 {
		t.Fatalf("stranded observation = %#v, %v", decision, err)
	}
	ready, err := store.OrderedReady(ctx, observation.ScaleSet, observation.ObservedAt)
	if err != nil || len(ready) != 0 {
		t.Fatalf("stranded capacity became runnable = %#v, %v", ready, err)
	}

	observation.BusyRunners = 0
	observation.ObservedAt = observation.ObservedAt.Add(time.Second)
	decision, err = store.ObserveAssignedCapacity(ctx, observation, 0)
	if err != nil || decision.Unblock != 1 {
		t.Fatalf("recovered observation = %#v, %v", decision, err)
	}
	ready, err = store.OrderedReady(ctx, observation.ScaleSet, observation.ObservedAt)
	if err != nil || len(ready) != 1 {
		t.Fatalf("recovered capacity = %#v, %v", ready, err)
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
