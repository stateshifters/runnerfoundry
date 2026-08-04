package scaler

import (
	"context"
	"errors"
	"testing"
)

func TestCoordinatorDelegatesLibvirtLifecycle(t *testing.T) {
	store := NewMemoryDemandStore()
	coordinator := NewDurableCoordinator("linux", 1, store)
	runner := LibvirtRunnerLifecycle{Repository: "repo", RequestID: 42, RunnerName: "linux-42"}
	if err := coordinator.RegisterLibvirtRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CompleteJob(context.Background(), JobCompletion{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerIdentity: runner.Identity(),
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := coordinator.PendingLibvirtReaps(context.Background(), runner.Repository)
	if err != nil || len(pending) != 1 || pending[0].ScaleSet != "linux" {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	if err = coordinator.ConfirmLibvirtReaped(context.Background(), pending[0]); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorReportsUnsupportedLibvirtLifecycle(t *testing.T) {
	unsupported := NewDurableCoordinator("linux", 1, &failingDemandStore{})
	runner := LibvirtRunnerLifecycle{Repository: "repo", RequestID: 42, RunnerName: "linux-42"}
	if err := unsupported.RegisterLibvirtRunner(context.Background(), runner); err == nil {
		t.Fatal("expected unsupported registration")
	}
	if err := unsupported.CompleteJob(context.Background(), JobCompletion{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerIdentity: runner.Identity(),
	}); err == nil {
		t.Fatal("expected unsupported completion")
	}
	if _, err := unsupported.PendingLibvirtReaps(context.Background(), "repo"); err == nil {
		t.Fatal("expected unsupported pending lookup")
	}
	if err := unsupported.ConfirmLibvirtReaped(context.Background(), runner); err == nil {
		t.Fatal("expected unsupported confirmation")
	}
}

func TestCoordinatorReportsFailingLibvirtLifecycle(t *testing.T) {
	runner := LibvirtRunnerLifecycle{Repository: "repo", RequestID: 42, RunnerName: "linux-42"}
	failing := NewDurableCoordinator("linux", 1, &failingLifecycleDemandStore{
		MemoryDemandStore: NewMemoryDemandStore(), lifecycleErr: errors.New("lifecycle failed"),
	})
	if err := failing.RegisterLibvirtRunner(context.Background(), runner); err == nil {
		t.Fatal("expected registration failure")
	}
	if err := failing.CompleteJob(context.Background(), JobCompletion{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerIdentity: runner.Identity(),
	}); err == nil {
		t.Fatal("expected completion failure")
	}
	if _, err := failing.PendingLibvirtReaps(context.Background(), "repo"); err == nil {
		t.Fatal("expected pending failure")
	}
	if err := failing.ConfirmLibvirtReaped(context.Background(), runner); err == nil {
		t.Fatal("expected confirmation failure")
	}
	if err := failing.Schedule(context.Background()); err == nil {
		t.Fatal("expected pending capacity failure")
	}
}

func TestMemoryLibvirtRegistrationIsIdempotentUntilReapRequested(t *testing.T) {
	store := NewMemoryDemandStore()
	runner := LibvirtRunnerLifecycle{ScaleSet: "linux", Repository: "repo", RequestID: 42, RunnerName: "linux-42"}
	if err := store.RegisterLibvirtRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterLibvirtRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	runner.RequestID++
	if err := store.RegisterLibvirtRunner(context.Background(), runner); err == nil {
		t.Fatal("expected identity conflict")
	}
}

type failingLifecycleDemandStore struct {
	*MemoryDemandStore
	lifecycleErr error
}

func (store *failingLifecycleDemandStore) RegisterLibvirtRunner(context.Context, LibvirtRunnerLifecycle) error {
	return store.lifecycleErr
}
func (store *failingLifecycleDemandStore) CompleteJob(context.Context, JobCompletion) error {
	return store.lifecycleErr
}
func (store *failingLifecycleDemandStore) PendingLibvirtReaps(context.Context, string, string) ([]LibvirtRunnerLifecycle, error) {
	return nil, store.lifecycleErr
}
func (store *failingLifecycleDemandStore) ConfirmLibvirtReaped(context.Context, LibvirtRunnerLifecycle) error {
	return store.lifecycleErr
}
