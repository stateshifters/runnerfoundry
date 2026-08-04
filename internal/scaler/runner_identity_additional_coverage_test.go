package scaler

import (
	"context"
	"errors"
	"testing"
	"time"
)

type coordinatorBindingErrorStore struct {
	*MemoryDemandStore
	err error
}

func (store *coordinatorBindingErrorStore) BindRunnerIdentity(
	context.Context,
	string,
	string,
	RunnerIdentity,
	int64,
	time.Time,
) error {
	return store.err
}

func TestCoordinatorBindRunnerIdentityCoversSuccessAndFailure(t *testing.T) {
	t.Run("success", assertCoordinatorRunnerIdentityBindingSucceeds)
	t.Run("failure", assertCoordinatorRunnerIdentityBindingWrapsFailure)
}

func assertCoordinatorRunnerIdentityBindingSucceeds(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	store := NewMemoryDemandStore()
	runner := RunnerLifecycle{
		ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "runner", Phase: RunnerWaitingForJob, CreatedAt: time.Unix(1, 0).UTC(),
	}
	if err := store.RegisterRunner(ctx, runner); err != nil {
		t.Fatal(err)
	}
	coordinator := NewDurableCoordinator("set", 1, store)
	identity := RunnerIdentity{RequestID: 42, RunnerName: "runner"}
	if err := coordinator.BindRunnerIdentity(ctx, "repo", identity, 99, time.Unix(2, 0).UTC()); err != nil {
		t.Fatal(err)
	}
}

func assertCoordinatorRunnerIdentityBindingWrapsFailure(t *testing.T) {
	t.Helper()
	failure := errors.New("failed")
	store := &coordinatorBindingErrorStore{MemoryDemandStore: NewMemoryDemandStore(), err: failure}
	coordinator := NewDurableCoordinator("set", 1, store)
	identity := RunnerIdentity{RequestID: 42, RunnerName: "runner"}
	err := coordinator.BindRunnerIdentity(context.Background(), "repo", identity, 99, time.Unix(2, 0).UTC())
	if !errors.Is(err, failure) {
		t.Fatalf("BindRunnerIdentity() error = %v, want wrapped failure", err)
	}
}
