package daemon

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestStandaloneLibvirtReaperRetriesOnInjectedTicks(t *testing.T) {
	memory := scaler.NewMemoryDemandStore()
	store := &confirmingLifecycleStore{MemoryDemandStore: memory, confirmed: make(chan struct{}, 1)}
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	runner := scaler.LibvirtRunnerLifecycle{
		ScaleSet: "linux", Repository: "repo", RequestID: 42, RunnerName: "linux-42",
	}
	if err := coordinator.RegisterLibvirtRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CompleteJob(context.Background(), scaler.JobCompletion{
		Repository: runner.Repository, RequestID: runner.RequestID, RunnerName: runner.RunnerName,
	}); err != nil {
		t.Fatal(err)
	}
	instance := scaler.RunnerInstance{ID: "domain", RunnerName: runner.RunnerName, State: scaler.RunnerStateRunning}
	provider := &notifyingStandaloneReaperProvider{
		lists: [][]scaler.RunnerInstance{{instance}, {instance}, nil}, inspected: make(chan int, 3),
	}
	lifecycle := coordinatedLifecycleConfig{
		Logger: slog.Default(), RepositoryURL: runner.Repository,
		ScaleSetConfig: config.ScaleSetConfig{Name: "linux", Libvirt: &config.LibvirtConfig{}},
		Coordinator:    coordinator,
		Reconciler:     scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: provider}},
	}
	ticks := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runLibvirtReaper(ctx, lifecycle, ticks)
	}()

	ticks <- time.Time{}
	if call := <-provider.inspected; call != 1 {
		t.Fatalf("first inspection call = %d", call)
	}
	if call := <-provider.inspected; call != 2 {
		t.Fatalf("verification call = %d", call)
	}
	select {
	case <-store.confirmed:
		t.Fatal("runner was confirmed while verification still found it")
	default:
	}
	ticks <- time.Time{}
	<-store.confirmed
	cancel()
	<-done
}

type confirmingLifecycleStore struct {
	*scaler.MemoryDemandStore
	confirmed chan struct{}
}

func (store *confirmingLifecycleStore) ConfirmLibvirtReaped(ctx context.Context, runner scaler.LibvirtRunnerLifecycle) error {
	if err := store.MemoryDemandStore.ConfirmLibvirtReaped(ctx, runner); err != nil {
		return err
	}
	store.confirmed <- struct{}{}
	return nil
}

type notifyingStandaloneReaperProvider struct {
	lists     [][]scaler.RunnerInstance
	inspected chan int
	listCalls int
}

func (provider *notifyingStandaloneReaperProvider) List(context.Context, string) ([]scaler.RunnerInstance, error) {
	provider.listCalls++
	provider.inspected <- provider.listCalls
	instances := provider.lists[0]
	provider.lists = provider.lists[1:]
	return instances, nil
}

func (*notifyingStandaloneReaperProvider) Start(context.Context, scaler.RunnerStartRequest) error {
	return nil
}
func (*notifyingStandaloneReaperProvider) Destroy(context.Context, scaler.RunnerInstance) error {
	return nil
}
func (*notifyingStandaloneReaperProvider) DestroyTerminal(context.Context, scaler.RunnerInstance) error {
	return nil
}
