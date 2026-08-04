package scaler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLiteRunnerIdentityBindingIsExactAndIdempotent(t *testing.T) {
	store := openSQLiteCapacityStore(t)
	registerSQLiteIdentityTestRunner(t, store, RunnerIdentity{RunnerName: "runner-one"})
	registerSQLiteIdentityTestRunner(t, store, RunnerIdentity{RequestID: 42, RunnerName: "runner-two"})
	pair := RunnerIdentity{RequestID: 7, RunnerName: "runner-one"}
	for attempt := 0; attempt < 2; attempt++ {
		if err := store.BindRunnerIdentity(context.Background(), "set", "repo", pair, 99, time.Unix(2, 0).UTC()); err != nil {
			t.Fatalf("BindRunnerIdentity() attempt %d error = %v", attempt+1, err)
		}
	}
	if err := store.BindRunnerIdentity(context.Background(), "set", "repo",
		RunnerIdentity{RequestID: 42, RunnerName: "runner-one"}, 99, time.Unix(2, 0).UTC()); !errors.Is(err, ErrRunnerIdentityMismatch) {
		t.Fatalf("conflicting BindRunnerIdentity() error = %v, want mismatch", err)
	}
}

func TestSQLiteMessageIdentityFailureRollsBackAndNotFoundIsIdempotent(t *testing.T) {
	store, controllerEpoch := newControlledSQLiteIdentityStore(t)
	seedSQLiteMessageIdentityRunners(t, store)
	update := mismatchedSQLiteIdentityMessage(controllerEpoch)
	assertSQLiteIdentityMessageRollsBack(t, store, update)
	update.Started = []RunnerMessageIdentity{{RunnerIdentity: RunnerIdentity{RequestID: 404}}}
	assertSQLiteIdentityMessageCommits(t, store, update)
}

func newControlledSQLiteIdentityStore(t *testing.T) (*SQLiteDemandStore, int64) {
	t.Helper()
	ctx := context.Background()
	store := openSQLiteCapacityStore(t)
	control, err := store.StartController(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	return store, control.Control.ControllerEpoch
}

func seedSQLiteMessageIdentityRunners(t *testing.T, store *SQLiteDemandStore) {
	t.Helper()
	registerSQLiteIdentityTestRunner(t, store, RunnerIdentity{RequestID: 1, RunnerName: "runner-one"})
	registerSQLiteIdentityTestRunner(t, store, RunnerIdentity{RequestID: 2, RunnerName: "runner-two"})
}

func mismatchedSQLiteIdentityMessage(controllerEpoch int64) MessageUpdate {
	observedAt := time.Unix(2, 0).UTC()
	return MessageUpdate{
		AppID: 7, ControllerEpoch: controllerEpoch,
		ScaleSet: "set", Repository: "repo", ObservedAt: observedAt,
		Available: []JobDemand{{Repository: "repo", RequestID: 3, QueueTime: observedAt}},
		Started:   []RunnerMessageIdentity{{RunnerIdentity: RunnerIdentity{RequestID: 1, RunnerName: "runner-two"}}},
	}
}

func assertSQLiteIdentityMessageRollsBack(t *testing.T, store *SQLiteDemandStore, update MessageUpdate) {
	t.Helper()
	_, err := store.ApplyMessageUpdate(context.Background(), update)
	if !errors.Is(err, ErrRunnerIdentityMismatch) {
		t.Fatalf("ApplyMessageUpdate() error = %v, want mismatch", err)
	}
	queued, err := store.Ordered(context.Background(), "set")
	if err != nil || len(queued) != 0 {
		t.Fatalf("rolled-back demand = %#v, %v", queued, err)
	}
	assertSQLiteIdentityTestRunnersRemainWaiting(t, store)
}

func assertSQLiteIdentityMessageCommits(t *testing.T, store *SQLiteDemandStore, update MessageUpdate) {
	t.Helper()
	_, err := store.ApplyMessageUpdate(context.Background(), update)
	if err != nil {
		t.Fatalf("not-found message error = %v", err)
	}
	queued, err := store.Ordered(context.Background(), "set")
	if err != nil || len(queued) != 1 {
		t.Fatalf("committed idempotent demand = %#v, %v", queued, err)
	}
}

func registerSQLiteIdentityTestRunner(t *testing.T, store *SQLiteDemandStore, identity RunnerIdentity) {
	t.Helper()
	err := store.RegisterRunner(context.Background(), RunnerLifecycle{
		ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: identity.RunnerName, RequestID: identity.RequestID, Phase: RunnerWaitingForJob,
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertSQLiteIdentityTestRunnersRemainWaiting(t *testing.T, store *SQLiteDemandStore) {
	t.Helper()
	runners, err := store.RunnerLifecycles(context.Background(), "set", "repo")
	if err != nil {
		t.Fatal(err)
	}
	for _, runner := range runners {
		if runner.Phase != RunnerWaitingForJob {
			t.Fatalf("runner mutated after rejected identity: %#v", runner)
		}
	}
}
