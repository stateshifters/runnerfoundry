package scaler

import (
	"context"
	"testing"
	"time"
)

func TestSQLiteNewEpochAdoptsAssignedObligationsWithoutResettingFIFOOrRetry(t *testing.T) {
	store := openSQLiteCapacityStore(t)
	ctx := context.Background()
	observedAt := time.Unix(80, 0).UTC()
	epochThirteen, before := createEpochThirteenObligations(ctx, t, store, observedAt)
	retryAt := observedAt.Add(time.Minute)
	if err := store.RecordObligationFailure(ctx, before[0].ObligationID, retryAt); err != nil {
		t.Fatal(err)
	}
	observeReplacementEpoch(ctx, t, store, epochThirteen, 2, 0)
	after, err := store.OrderedReady(ctx, "linux", retryAt)
	if err != nil || len(after) != 2 {
		t.Fatalf("adopted obligations = %#v, %v", after, err)
	}
	assertAdoptedObligation(t, before[0], after[0], 1, retryAt)
	assertAdoptedObligation(t, before[1], after[1], 0, before[1].NextAttemptAt)
}

func TestSQLiteLowerNewEpochCancelsNewestAdoptedUnclaimedObligation(t *testing.T) {
	store := openSQLiteCapacityStore(t)
	ctx := context.Background()
	observedAt := time.Unix(90, 0).UTC()
	observation, obligations := createEpochThirteenObligations(ctx, t, store, observedAt)
	if err := store.RegisterRunner(ctx, RunnerLifecycle{
		NodeID: "local", ScaleSet: "linux", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "linux-1", RequestID: 0, AssignmentID: obligations[0].ObligationID,
		Phase: RunnerWaitingForJob, CreatedAt: observedAt,
	}); err != nil {
		t.Fatal(err)
	}
	observeReplacementEpoch(ctx, t, store, observation, 1, 1)
	assertLifecycleBackedObligationRetained(ctx, t, store, obligations[0].ObligationID, observedAt.Add(time.Second))
}

func TestSQLiteEpochAdoptionLeavesExactObligationUnchanged(t *testing.T) {
	store := openSQLiteCapacityStore(t)
	ctx := context.Background()
	observedAt := time.Unix(95, 0).UTC()
	if err := store.Apply(ctx, "linux", []JobDemand{{Repository: "repo", RequestID: 42, QueueTime: observedAt}}, nil); err != nil {
		t.Fatal(err)
	}
	observation := AssignedCapacityObservation{
		ScaleSet: "linux", Repository: "repo", ControllerEpoch: 13,
		AssignedJobs: 2, ObservedAt: observedAt.Add(time.Second),
	}
	if _, err := store.ObserveAssignedCapacity(ctx, observation, 0); err != nil {
		t.Fatal(err)
	}
	observeReplacementEpoch(ctx, t, store, observation, 2, 0)
	ordered, err := store.OrderedReady(ctx, "linux", observedAt.Add(time.Minute))
	if err != nil || len(ordered) != 3 || ordered[0].Source != CapacityObligationJobAvailable ||
		ordered[0].RequestID != 42 || ordered[0].ControllerEpoch != 0 {
		t.Fatalf("exact obligation changed during epoch adoption: %#v, %v", ordered, err)
	}
}

func createEpochThirteenObligations(
	ctx context.Context,
	t *testing.T,
	store *SQLiteDemandStore,
	observedAt time.Time,
) (AssignedCapacityObservation, []QueuedDemand) {
	t.Helper()
	observation := AssignedCapacityObservation{
		ScaleSet: "linux", Repository: "repo", ControllerEpoch: 13,
		AssignedJobs: 2, ObservedAt: observedAt,
	}
	if _, err := store.ObserveAssignedCapacity(ctx, observation, 0); err != nil {
		t.Fatal(err)
	}
	obligations, err := store.OrderedReady(ctx, "linux", observedAt)
	if err != nil || len(obligations) != 2 {
		t.Fatalf("epoch 13 obligations = %#v, %v", obligations, err)
	}
	return observation, obligations
}

func observeReplacementEpoch(
	ctx context.Context,
	t *testing.T,
	store *SQLiteDemandStore,
	observation AssignedCapacityObservation,
	assignedJobs int,
	activeCapacity int,
) {
	t.Helper()
	observation.ControllerEpoch = 14
	observation.AssignedJobs = assignedJobs
	observation.ObservedAt = observation.ObservedAt.Add(time.Second)
	decision, err := store.ObserveAssignedCapacity(ctx, observation, activeCapacity)
	want := AssignedObligationDecision{}
	if assignedJobs == 1 {
		want.Cancel = 1
	}
	if err != nil || decision != want {
		t.Fatalf("replacement epoch observation = %#v, %v", decision, err)
	}
}

func assertAdoptedObligation(t *testing.T, before, after QueuedDemand, attempt int, nextAttemptAt time.Time) {
	t.Helper()
	if after.ObligationID != before.ObligationID || after.QueueTime != before.QueueTime ||
		after.Attempt != attempt || after.NextAttemptAt != nextAttemptAt || after.ControllerEpoch != 14 {
		t.Fatalf("obligation changed during adoption: before=%#v after=%#v", before, after)
	}
}

func assertLifecycleBackedObligationRetained(
	ctx context.Context,
	t *testing.T,
	store *SQLiteDemandStore,
	wantID int64,
	runnableAt time.Time,
) {
	t.Helper()
	var remainingID, remainingEpoch int64
	err := store.database.QueryRowContext(ctx, `SELECT demand_id, controller_epoch FROM demand
		WHERE source = 'assigned_count'`).Scan(&remainingID, &remainingEpoch)
	if err != nil || remainingID != wantID || remainingEpoch != 14 {
		t.Fatalf("remaining lifecycle-backed obligation = id %d epoch %d, %v", remainingID, remainingEpoch, err)
	}
	ready, err := store.OrderedReady(ctx, "linux", runnableAt)
	if err != nil || len(ready) != 0 {
		t.Fatalf("lifecycle-backed obligation became runnable: %#v, %v", ready, err)
	}
}
