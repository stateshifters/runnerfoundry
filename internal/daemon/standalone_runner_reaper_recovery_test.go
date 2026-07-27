package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestCanceledAssignedRunnerIsReapedBeforeReplacementConsumesCapacity(t *testing.T) {
	fixture := incidentLifecycleFixture(t, 1)
	fixture.observeAssigned(t, 1)
	fixture.observeAssigned(t, 0)
	fixture.reapPending()
	fixture.assertStaleRunnerRemoved(t)
	fixture.observeAssigned(t, 1)
	fixture.assertFreshRunnerStarted(t)
}

type incidentLifecycleTestFixture struct {
	handler     coordinatedScaleSetHandler
	coordinator *scaler.Coordinator
	reconciler  scaler.Reconciler
	provider    *incidentLifecycleProvider
	store       *scaler.MemoryDemandStore
}

func (fixture incidentLifecycleTestFixture) observeAssigned(t *testing.T, assigned int) {
	t.Helper()
	if err := fixture.handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: assigned},
	}); err != nil {
		t.Fatal(err)
	}
}

func (fixture incidentLifecycleTestFixture) reapPending() {
	reapPendingRunners(context.Background(), coordinatedLifecycleConfig{
		RepositoryURL: "repo", ScaleSetConfig: config.ScaleSetConfig{Name: "linux", JobStartTimeout: "10m"},
		Coordinator: fixture.coordinator, Reconciler: fixture.reconciler,
	})
}

func (fixture incidentLifecycleTestFixture) assertStaleRunnerRemoved(t *testing.T) {
	t.Helper()
	if len(fixture.provider.destroyed) != 1 || len(fixture.provider.instances) != 0 {
		t.Fatalf("stale runner was not removed: destroyed=%#v instances=%#v",
			fixture.provider.destroyed, fixture.provider.instances)
	}
}

func (fixture incidentLifecycleTestFixture) assertFreshRunnerStarted(t *testing.T) {
	t.Helper()
	if len(fixture.provider.started) != 2 || fixture.provider.started[0].ID == fixture.provider.started[1].ID {
		t.Fatalf("fresh runner was not started: %#v", fixture.provider.started)
	}
}

func incidentLifecycleFixture(t *testing.T, maxRunners int) incidentLifecycleTestFixture {
	t.Helper()
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", maxRunners, store)
	provider := &incidentLifecycleProvider{}
	registry := newIncidentJITRegistry()
	reconciler := scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "local", MaxRunners: maxRunners, Provider: provider, JITConfig: registry,
	}}
	if err := coordinator.ConfigureRunnerLifecycle(&reconciler, "repo", scaler.RunnerProviderDocker); err != nil {
		t.Fatal(err)
	}
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{Reconciler: reconciler})
	return incidentLifecycleTestFixture{
		handler:     coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo", ScaleSet: "linux"},
		coordinator: coordinator, reconciler: reconciler, provider: provider, store: store,
	}
}

func TestExpiredStandaloneWaitingRunnerIsReapedForEveryProvider(t *testing.T) {
	for name, providerKind := range map[string]scaler.RunnerProvider{
		"docker": scaler.RunnerProviderDocker, "libvirt": scaler.RunnerProviderLibvirt,
	} {
		t.Run(name, func(t *testing.T) {
			assertExpiredStandaloneRunnerReaped(t, providerKind)
		})
	}
}

func assertExpiredStandaloneRunnerReaped(t *testing.T, providerKind scaler.RunnerProvider) {
	t.Helper()
	ctx := context.Background()
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	provider := &incidentLifecycleProvider{}
	oldStart := time.Now().UTC().Add(-time.Hour)
	reconciler := scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "local", MaxRunners: 1,
		Provider: provider, JITConfig: newIncidentJITRegistry(), Now: func() time.Time { return oldStart }}}
	if err := coordinator.ConfigureRunnerLifecycle(&reconciler, "repo", providerKind); err != nil {
		t.Fatal(err)
	}
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{Reconciler: reconciler})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo", ScaleSet: "linux"}
	if err := handler.HandleMessage(ctx, &upstream.RunnerScaleSetMessage{Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1}}); err != nil {
		t.Fatal(err)
	}
	reapPendingRunners(ctx, coordinatedLifecycleConfig{RepositoryURL: "repo",
		ScaleSetConfig: config.ScaleSetConfig{Name: "linux", JobStartTimeout: "10m"}, Coordinator: coordinator, Reconciler: reconciler})
	if len(provider.destroyed) != 1 {
		t.Fatalf("expired runner was not reaped: %#v", provider.instances)
	}
}

type faultLifecycleDemandStore struct {
	*scaler.MemoryDemandStore
	expireErr    error
	pendingErr   error
	confirmErr   error
	orderedErr   error
	busyErr      error
	completeErr  error
	inventoryErr error
}

func (store *faultLifecycleDemandStore) RequestExpiredRunnerReaps(ctx context.Context, scaleSet, repository string, deadline, at time.Time) (int, error) {
	if store.expireErr != nil {
		return 0, store.expireErr
	}
	changed, err := store.MemoryDemandStore.RequestExpiredRunnerReaps(ctx, scaleSet, repository, deadline, at)
	if err != nil {
		return 0, fmt.Errorf("request expired runners: %w", err)
	}
	return changed, nil
}

func (store *faultLifecycleDemandStore) PendingRunnerReaps(ctx context.Context, scaleSet, repository string) ([]scaler.RunnerLifecycle, error) {
	if store.pendingErr != nil {
		return nil, store.pendingErr
	}
	pending, err := store.MemoryDemandStore.PendingRunnerReaps(ctx, scaleSet, repository)
	if err != nil {
		return nil, fmt.Errorf("pending runner reaps: %w", err)
	}
	return pending, nil
}

func (store *faultLifecycleDemandStore) ConfirmRunnerReaped(ctx context.Context, runner scaler.RunnerLifecycle) error {
	if store.confirmErr != nil {
		return store.confirmErr
	}
	if err := store.MemoryDemandStore.ConfirmRunnerReaped(ctx, runner); err != nil {
		return fmt.Errorf("confirm runner reaped: %w", err)
	}
	return nil
}

func (store *faultLifecycleDemandStore) Ordered(ctx context.Context, scaleSet string) ([]scaler.QueuedDemand, error) {
	if store.orderedErr != nil {
		return nil, store.orderedErr
	}
	demand, err := store.MemoryDemandStore.Ordered(ctx, scaleSet)
	if err != nil {
		return nil, fmt.Errorf("load demand: %w", err)
	}
	return demand, nil
}

func (store *faultLifecycleDemandStore) MarkRunnerBusy(ctx context.Context, scaleSet, repository string, requestID int64, runnerName string, runnerID int64, at time.Time) error {
	if store.busyErr != nil {
		return store.busyErr
	}
	if err := store.MemoryDemandStore.MarkRunnerBusy(ctx, scaleSet, repository, requestID, runnerName, runnerID, at); err != nil {
		return fmt.Errorf("mark runner busy: %w", err)
	}
	return nil
}

func (store *faultLifecycleDemandStore) CompleteRunnerLifecycleJob(ctx context.Context, completion scaler.JobCompletion, at time.Time) error {
	if store.completeErr != nil {
		return store.completeErr
	}
	if err := store.MemoryDemandStore.CompleteRunnerLifecycleJob(ctx, completion, at); err != nil {
		return fmt.Errorf("complete runner job: %w", err)
	}
	return nil
}

func (store *faultLifecycleDemandStore) RunnerLifecycles(ctx context.Context, scaleSet, repository string) ([]scaler.RunnerLifecycle, error) {
	if store.inventoryErr != nil {
		return nil, store.inventoryErr
	}
	lifecycles, err := store.MemoryDemandStore.RunnerLifecycles(ctx, scaleSet, repository)
	if err != nil {
		return nil, fmt.Errorf("load runner lifecycles: %w", err)
	}
	return lifecycles, nil
}

var standaloneRunnerReapBoundaryCases = []struct {
	name      string
	configure func(*faultLifecycleDemandStore, *config.ScaleSetConfig, *scaler.Reconciler)
}{
	{name: "invalid timeout", configure: func(_ *faultLifecycleDemandStore, scaleSet *config.ScaleSetConfig, _ *scaler.Reconciler) {
		scaleSet.JobStartTimeout = "invalid"
	}},
	{name: "expire", configure: func(store *faultLifecycleDemandStore, _ *config.ScaleSetConfig, _ *scaler.Reconciler) {
		store.expireErr = errors.New("expire")
	}},
	{name: "load", configure: func(store *faultLifecycleDemandStore, _ *config.ScaleSetConfig, _ *scaler.Reconciler) {
		store.pendingErr = errors.New("load")
	}},
	{name: "reap", configure: func(_ *faultLifecycleDemandStore, _ *config.ScaleSetConfig, reconciler *scaler.Reconciler) {
		reconciler.ScaleSet.Provider = &clusterReapingProvider{listErr: errors.New("list")}
	}},
	{name: "incomplete", configure: func(_ *faultLifecycleDemandStore, _ *config.ScaleSetConfig, reconciler *scaler.Reconciler) {
		instance := scaler.RunnerInstance{ID: "id", RunnerName: "linux-1"}
		reconciler.ScaleSet.Provider = &clusterReapingProvider{lists: [][]scaler.RunnerInstance{{instance}, {instance}}}
	}},
	{name: "confirm", configure: func(store *faultLifecycleDemandStore, _ *config.ScaleSetConfig, _ *scaler.Reconciler) {
		store.confirmErr = errors.New("confirm")
	}},
	{name: "replacement", configure: func(store *faultLifecycleDemandStore, _ *config.ScaleSetConfig, _ *scaler.Reconciler) {
		store.orderedErr = errors.New("schedule")
	}},
}

func TestStandaloneRunnerReaperRetainsCleanupAtBoundaries(t *testing.T) {
	runner := scaler.RunnerLifecycle{ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker,
		RunnerName: "linux-1", Phase: scaler.RunnerReapPending}
	for _, test := range standaloneRunnerReapBoundaryCases {
		t.Run(test.name, func(t *testing.T) {
			store := &faultLifecycleDemandStore{MemoryDemandStore: scaler.NewMemoryDemandStore()}
			if err := store.RegisterRunner(context.Background(), runner); err != nil {
				t.Fatal(err)
			}
			coordinator := scaler.NewDurableCoordinator("linux", 1, store)
			reconciler := scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &incidentLifecycleProvider{}, JITConfig: newIncidentJITRegistry()}}
			scaleSet := config.ScaleSetConfig{Name: "linux", JobStartTimeout: "10m"}
			test.configure(store, &scaleSet, &reconciler)
			reapPendingRunners(context.Background(), coordinatedLifecycleConfig{RepositoryURL: "repo", ScaleSetConfig: scaleSet,
				Coordinator: coordinator, Reconciler: reconciler})
		})
	}
}

func TestRunnerReaperStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		runRunnerReaper(ctx, coordinatedLifecycleConfig{}, make(chan time.Time))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runner reaper did not stop")
	}
}

func TestRunnerReaperProcessesTickBeforeStopping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	ticks := make(chan time.Time, 1)
	done := make(chan struct{})
	go func() {
		runRunnerReaper(ctx, coordinatedLifecycleConfig{RepositoryURL: "repo",
			ScaleSetConfig: config.ScaleSetConfig{Name: "linux", JobStartTimeout: "10m"}, Coordinator: coordinator}, ticks)
		close(done)
	}()
	ticks <- time.Now()
	time.Sleep(10 * time.Millisecond)
	cancel()
	<-done
}
