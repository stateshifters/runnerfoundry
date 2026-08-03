package scaler

import (
	"context"
	"testing"
	"time"
)

func TestSQLiteClaimedAssignedObligationIsNotRunnableOrCanceled(t *testing.T) {
	store := openSQLiteCapacityStore(t)
	ctx := context.Background()
	now := time.Unix(40, 0).UTC()
	createLifecycleOwnedObligation(ctx, t, store, now)
	assertNoRunnableObligations(ctx, t, store, now)
	observation := AssignedCapacityObservation{
		ScaleSet: "linux", Repository: "repo", ControllerEpoch: 1,
		AssignedJobs: 0, ObservedAt: now.Add(time.Second),
	}
	decision, err := store.ObserveAssignedCapacity(ctx, observation, 1)
	if err != nil || decision.Cancel != 0 {
		t.Fatalf("claimed obligation cancellation = %#v, %v", decision, err)
	}
}

func TestSQLiteLifecycleOwnedObligationCleanupIsRestartRetriable(t *testing.T) {
	store := openSQLiteCapacityStore(t)
	ctx := context.Background()
	now := time.Unix(45, 0).UTC()
	runner := createLifecycleOwnedObligation(ctx, t, store, now)
	if err := store.DeleteLifecycleOwnedObligations(ctx, "linux"); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfirmRunnerReaped(ctx, runner); err != nil {
		t.Fatal(err)
	}
	assertNoRunnableObligations(ctx, t, store, now)
}

func createLifecycleOwnedObligation(
	ctx context.Context,
	t *testing.T,
	store *SQLiteDemandStore,
	now time.Time,
) RunnerLifecycle {
	t.Helper()
	if _, err := store.ObserveAssignedCapacity(ctx, AssignedCapacityObservation{
		ScaleSet: "linux", Repository: "repo", ControllerEpoch: 1, AssignedJobs: 1, ObservedAt: now,
	}, 0); err != nil {
		t.Fatal(err)
	}
	obligations, err := store.OrderedReady(ctx, "linux", now)
	if err != nil || len(obligations) != 1 {
		t.Fatalf("queued obligations = %#v, %v", obligations, err)
	}
	runner := RunnerLifecycle{
		ScaleSet: "linux", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "linux-1", AssignmentID: obligations[0].ObligationID,
		Phase: RunnerReapPending, CreatedAt: now,
	}
	if err = store.RegisterRunner(ctx, runner); err != nil {
		t.Fatal(err)
	}
	return runner
}

func assertNoRunnableObligations(ctx context.Context, t *testing.T, store *SQLiteDemandStore, cutoff time.Time) {
	t.Helper()
	if remaining, loadErr := store.OrderedReady(ctx, "linux", cutoff); loadErr != nil || len(remaining) != 0 {
		t.Fatalf("lifecycle-owned obligations = %#v, %v", remaining, loadErr)
	}
}
