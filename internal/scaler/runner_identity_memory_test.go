package scaler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryRunnerIdentityPairCannotMutateDifferentRunners(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDemandStore()
	registerIdentityTestRunner(t, store, RunnerIdentity{RequestID: 1, RunnerName: "runner-one"})
	registerIdentityTestRunner(t, store, RunnerIdentity{RequestID: 2, RunnerName: "runner-two"})

	err := store.MarkRunnerBusy(ctx, "set", "repo", RunnerIdentity{RequestID: 1, RunnerName: "runner-two"}, 99,
		time.Unix(2, 0).UTC())
	if !errors.Is(err, ErrRunnerIdentityMismatch) {
		t.Fatalf("MarkRunnerBusy() error = %v, want identity mismatch", err)
	}
	assertIdentityTestRunnersRemainWaiting(t, store)
}

func TestMemoryRunnerIdentityDuplicateRollsBackMutation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDemandStore()
	registerIdentityTestRunner(t, store, RunnerIdentity{RequestID: 1, RunnerName: "runner-one"})
	registerIdentityTestRunner(t, store, RunnerIdentity{RequestID: 1, RunnerName: "runner-two"})

	err := store.MarkRunnerBusy(ctx, "set", "repo", RunnerIdentity{RequestID: 1}, 99, time.Unix(2, 0).UTC())
	if !errors.Is(err, ErrRunnerIdentityInvariant) {
		t.Fatalf("MarkRunnerBusy() error = %v, want identity invariant", err)
	}
	assertIdentityTestRunnersRemainWaiting(t, store)
}

func TestMemoryRunnerIdentityBindingIsExactAndIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDemandStore()
	registerIdentityTestRunner(t, store, RunnerIdentity{RunnerName: "runner-one"})
	registerIdentityTestRunner(t, store, RunnerIdentity{RequestID: 42, RunnerName: "runner-two"})
	pair := RunnerIdentity{RequestID: 7, RunnerName: "runner-one"}

	if err := store.BindRunnerIdentity(ctx, "set", "repo", pair, 99, time.Unix(2, 0).UTC()); err != nil {
		t.Fatalf("BindRunnerIdentity() error = %v", err)
	}
	if err := store.BindRunnerIdentity(ctx, "set", "repo", pair, 99, time.Unix(2, 0).UTC()); err != nil {
		t.Fatalf("idempotent BindRunnerIdentity() error = %v", err)
	}
	if err := store.BindRunnerIdentity(ctx, "set", "repo",
		RunnerIdentity{RequestID: 42, RunnerName: "runner-one"}, 99, time.Unix(2, 0).UTC()); !errors.Is(err, ErrRunnerIdentityMismatch) {
		t.Fatalf("conflicting BindRunnerIdentity() error = %v, want identity mismatch", err)
	}

	runners, err := store.RunnerLifecycles(ctx, "set", "repo")
	if err != nil {
		t.Fatal(err)
	}
	for _, runner := range runners {
		if runner.RunnerName == "runner-one" && (runner.RequestID != 7 || runner.Phase != RunnerBusy) {
			t.Fatalf("bound runner = %#v", runner)
		}
	}
}

func TestMemoryMessageIdentityFailureRollsBackAndNotFoundIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDemandStore()
	registerIdentityTestRunner(t, store, RunnerIdentity{RequestID: 1, RunnerName: "runner-one"})
	registerIdentityTestRunner(t, store, RunnerIdentity{RequestID: 2, RunnerName: "runner-two"})
	observedAt := time.Unix(2, 0).UTC()
	update := MessageUpdate{
		ScaleSet: "set", Repository: "repo", ObservedAt: observedAt,
		Available: []JobDemand{{Repository: "repo", RequestID: 3, QueueTime: observedAt}},
		Started:   []RunnerMessageIdentity{{RunnerIdentity: RunnerIdentity{RequestID: 1, RunnerName: "runner-two"}}},
	}
	if _, err := store.ApplyMessageUpdate(ctx, update); !errors.Is(err, ErrRunnerIdentityMismatch) {
		t.Fatalf("ApplyMessageUpdate() error = %v, want mismatch", err)
	}
	queued, err := store.Ordered(ctx, "set")
	if err != nil || len(queued) != 0 {
		t.Fatalf("rolled-back demand = %#v, %v", queued, err)
	}
	assertIdentityTestRunnersRemainWaiting(t, store)

	update.Started = []RunnerMessageIdentity{{RunnerIdentity: RunnerIdentity{RequestID: 404}}}
	if _, err = store.ApplyMessageUpdate(ctx, update); err != nil {
		t.Fatalf("not-found message error = %v", err)
	}
	queued, err = store.Ordered(ctx, "set")
	if err != nil || len(queued) != 1 {
		t.Fatalf("committed idempotent demand = %#v, %v", queued, err)
	}
}

func registerIdentityTestRunner(t *testing.T, store *MemoryDemandStore, identity RunnerIdentity) {
	t.Helper()
	err := store.RegisterRunner(context.Background(), RunnerLifecycle{
		ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: identity.RunnerName, RequestID: identity.RequestID, Phase: RunnerWaitingForJob,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertIdentityTestRunnersRemainWaiting(t *testing.T, store *MemoryDemandStore) {
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
