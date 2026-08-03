package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type lifecycleUnsupportedDemandStore struct{}

func (lifecycleUnsupportedDemandStore) Apply(context.Context, string, []scaler.JobDemand, []scaler.DemandRemoval) error {
	return nil
}
func (lifecycleUnsupportedDemandStore) RemoveRepository(context.Context, string, string) error {
	return nil
}
func (lifecycleUnsupportedDemandStore) RemoveScaleSet(context.Context, string) error { return nil }
func (lifecycleUnsupportedDemandStore) Ordered(context.Context, string) ([]scaler.QueuedDemand, error) {
	return nil, nil
}

func TestStandaloneLibvirtReaperRetriesOnInjectedTicks(t *testing.T) {
	harness := newStandaloneLibvirtReaperHarness(t)
	defer harness.stop()

	harness.tick()
	harness.expectInspection(t, 1, "first inspection")
	harness.expectInspection(t, 2, "verification")
	harness.expectNotConfirmed(t)
	harness.tick()
	<-harness.store.confirmed
}

func TestStandaloneLibvirtLifecycleRegistersBeforeStartingRunner(t *testing.T) {
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{Repository: "repo", RequestID: 42}}, nil); err != nil {
		t.Fatal(err)
	}
	provider := &fakeScalerProvider{}
	ctx, cancel := context.WithCancel(context.Background())
	lifecycle, err := startCoordinatedLifecycle(ctx, coordinatedLifecycleConfig{
		Logger: slog.Default(), RepositoryURL: "repo",
		ScaleSetConfig: config.ScaleSetConfig{Name: "linux", MaxRunners: 1, Libvirt: &config.LibvirtConfig{}},
		Coordinator:    coordinator, MessageClient: scaleSetMessageClient{Client: &fakeMessageClient{
			session: upstream.RunnerScaleSetSession{Statistics: &upstream.RunnerScaleSetStatistic{}},
		}},
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "linux", MaxRunners: 1, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	cancel()
	lifecycle.StopPolling()
	if len(provider.started) != 1 || provider.started[0].RunnerName != "linux-42" {
		t.Fatalf("started runners = %#v", provider.started)
	}
}

func TestStandaloneLifecycleContinuesWhenStoreCannotPersistRunnerLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	coordinator := scaler.NewDurableCoordinator("linux", 1, lifecycleUnsupportedDemandStore{})
	lifecycle, err := startCoordinatedLifecycle(ctx, coordinatedLifecycleConfig{
		Logger: slog.Default(), RepositoryURL: "repo", ScaleSetConfig: config.ScaleSetConfig{Name: "linux", JobStartTimeout: "10m"},
		Coordinator: coordinator, MessageClient: scaleSetMessageClient{Client: &fakeMessageClient{
			session: upstream.RunnerScaleSetSession{Statistics: &upstream.RunnerScaleSetStatistic{}},
		}},
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &fakeScalerProvider{}, JITConfig: fakeJITConfigGenerator{}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	lifecycle.StopPolling()
}

func TestStandaloneLibvirtReaperRetainsFailuresForRetry(t *testing.T) {
	runner := scaler.LibvirtRunnerLifecycle{ScaleSet: "linux", Repository: "repo", RequestID: 42, RunnerName: "linux-42"}
	t.Run("load", func(t *testing.T) {
		store := &failingStandaloneLifecycleStore{MemoryDemandStore: scaler.NewMemoryDemandStore(), pendingErr: errors.New("load failed")}
		reapPendingLibvirtRunners(context.Background(), standaloneFailureLifecycle(store, &notifyingStandaloneReaperProvider{}))
	})
	t.Run("reap", func(t *testing.T) {
		store := pendingStandaloneLifecycleStore(t, runner)
		provider := &notifyingStandaloneReaperProvider{listErr: errors.New("list failed")}
		reapPendingLibvirtRunners(context.Background(), standaloneFailureLifecycle(store, provider))
	})
	t.Run("confirm", func(t *testing.T) {
		store := pendingStandaloneLifecycleStore(t, runner)
		store.confirmErr = errors.New("confirm failed")
		provider := &notifyingStandaloneReaperProvider{lists: [][]scaler.RunnerInstance{{}}}
		reapPendingLibvirtRunners(context.Background(), standaloneFailureLifecycle(store, provider))
	})
}

type failingStandaloneLifecycleStore struct {
	*scaler.MemoryDemandStore
	pendingErr error
	confirmErr error
}

func (store *failingStandaloneLifecycleStore) PendingLibvirtReaps(
	ctx context.Context,
	scaleSet string,
	repository string,
) ([]scaler.LibvirtRunnerLifecycle, error) {
	if store.pendingErr != nil {
		return nil, store.pendingErr
	}
	runners, err := store.MemoryDemandStore.PendingLibvirtReaps(ctx, scaleSet, repository)
	if err != nil {
		return nil, fmt.Errorf("load in-memory reaps: %w", err)
	}
	return runners, nil
}

func (store *failingStandaloneLifecycleStore) ConfirmLibvirtReaped(
	ctx context.Context,
	runner scaler.LibvirtRunnerLifecycle,
) error {
	if store.confirmErr != nil {
		return store.confirmErr
	}
	if err := store.MemoryDemandStore.ConfirmLibvirtReaped(ctx, runner); err != nil {
		return fmt.Errorf("confirm in-memory reap: %w", err)
	}
	return nil
}

func pendingStandaloneLifecycleStore(
	t *testing.T,
	runner scaler.LibvirtRunnerLifecycle,
) *failingStandaloneLifecycleStore {
	t.Helper()
	store := &failingStandaloneLifecycleStore{MemoryDemandStore: scaler.NewMemoryDemandStore()}
	if err := store.RegisterLibvirtRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteJob(context.Background(), scaler.JobCompletion(runner)); err != nil {
		t.Fatal(err)
	}
	return store
}

func standaloneFailureLifecycle(
	store scaler.DemandStore,
	provider scaler.Provider,
) coordinatedLifecycleConfig {
	return coordinatedLifecycleConfig{
		Logger: slog.Default(), RepositoryURL: "repo",
		ScaleSetConfig: config.ScaleSetConfig{Name: "linux", Libvirt: &config.LibvirtConfig{}},
		Coordinator:    scaler.NewDurableCoordinator("linux", 1, store),
		Reconciler:     scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: provider}},
	}
}

type standaloneLibvirtReaperHarness struct {
	store    *confirmingLifecycleStore
	provider *notifyingStandaloneReaperProvider
	ticks    chan time.Time
	cancel   context.CancelFunc
	done     chan struct{}
}

func newStandaloneLibvirtReaperHarness(t *testing.T) standaloneLibvirtReaperHarness {
	t.Helper()
	store, coordinator, runner := prepareStandaloneLibvirtLifecycle(t)
	provider, lifecycle := standaloneLibvirtReaperLifecycle(coordinator, runner)
	ticks, cancel, done := startStandaloneLibvirtReaper(lifecycle)
	return standaloneLibvirtReaperHarness{store: store, provider: provider, ticks: ticks, cancel: cancel, done: done}
}

func prepareStandaloneLibvirtLifecycle(
	t *testing.T,
) (*confirmingLifecycleStore, *scaler.Coordinator, scaler.LibvirtRunnerLifecycle) {
	t.Helper()
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
	return store, coordinator, runner
}

func standaloneLibvirtReaperLifecycle(
	coordinator *scaler.Coordinator,
	runner scaler.LibvirtRunnerLifecycle,
) (*notifyingStandaloneReaperProvider, coordinatedLifecycleConfig) {
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
	return provider, lifecycle
}

func startStandaloneLibvirtReaper(
	lifecycle coordinatedLifecycleConfig,
) (chan time.Time, context.CancelFunc, chan struct{}) {
	ticks := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runLibvirtReaper(ctx, lifecycle, ticks)
	}()
	return ticks, cancel, done
}

func (harness standaloneLibvirtReaperHarness) tick() {
	harness.ticks <- time.Time{}
}

func (harness standaloneLibvirtReaperHarness) expectInspection(t *testing.T, expected int, phase string) {
	t.Helper()
	if call := <-harness.provider.inspected; call != expected {
		t.Fatalf("%s call = %d, want %d", phase, call, expected)
	}
}

func (harness standaloneLibvirtReaperHarness) expectNotConfirmed(t *testing.T) {
	t.Helper()
	select {
	case <-harness.store.confirmed:
		t.Fatal("runner was confirmed while verification still found it")
	default:
	}
}

func (harness standaloneLibvirtReaperHarness) stop() {
	harness.cancel()
	<-harness.done
}

type confirmingLifecycleStore struct {
	*scaler.MemoryDemandStore
	confirmed chan struct{}
}

func (store *confirmingLifecycleStore) ConfirmLibvirtReaped(ctx context.Context, runner scaler.LibvirtRunnerLifecycle) error {
	if err := store.MemoryDemandStore.ConfirmLibvirtReaped(ctx, runner); err != nil {
		return fmt.Errorf("confirm in-memory libvirt reap: %w", err)
	}
	store.confirmed <- struct{}{}
	return nil
}

type notifyingStandaloneReaperProvider struct {
	lists     [][]scaler.RunnerInstance
	inspected chan int
	listCalls int
	listErr   error
}

func (provider *notifyingStandaloneReaperProvider) List(context.Context, string) ([]scaler.RunnerInstance, error) {
	provider.listCalls++
	if provider.listErr != nil {
		return nil, provider.listErr
	}
	if provider.inspected != nil {
		provider.inspected <- provider.listCalls
	}
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
