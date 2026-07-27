package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/scaler"
	scalesetpoller "github.com/meza/RunnerFoundry/internal/scaleset"
)

func TestScaleSetHandlerReconcilesAssignedJobs(t *testing.T) {
	provider := &fakeScalerProvider{}
	handler := scaleSetHandler{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name:       "linux",
		MaxRunners: 3,
		Provider:   provider,
		JITConfig:  fakeJITConfigGenerator{},
	}}}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2},
	})
	if err != nil {
		t.Fatalf("expected handler success, got %v", err)
	}
	if len(provider.started) != 2 {
		t.Fatalf("expected two starts, got %#v", provider.started)
	}
}

func TestCoordinatedScaleSetHandlerQueuesAvailableAndAssignedJobsAndRemovesCompleted(t *testing.T) {
	provider := &fakeScalerProvider{trackStarted: true}
	coordinator := scaler.NewCoordinator(2)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{
		Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "local", MaxRunners: 2, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}},
		MessageClient: scaleSetMessageClient{Client: &fakeMessageClient{}},
	})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	queueTime := time.Now()
	message := &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 1, QueueTime: queueTime}}},
		JobAssignedMessages:  []*upstream.JobAssigned{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 2, QueueTime: queueTime.Add(time.Second)}}},
		JobCompletedMessages: []*upstream.JobCompleted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 2}}},
	}
	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatalf("expected message scheduling, got %v", err)
	}
	if len(provider.started) != 1 {
		t.Fatalf("expected only non-completed demand to start, got %#v", provider.started)
	}
}

func TestCoordinatedScaleSetHandlerUsesStatisticsForZeroIdentityAssignedJobs(t *testing.T) {
	fixture := newAssignedStatisticsFixture()
	if err := fixture.handler.HandleMessage(context.Background(), fixture.message); err != nil {
		t.Fatalf("expected assigned capacity reconciliation, got %v", err)
	}
	if len(fixture.provider.started) != 2 {
		t.Fatalf("expected two assigned runners, got %#v", fixture.provider.started)
	}
	for _, startedRunner := range fixture.provider.started {
		if strings.HasSuffix(startedRunner.RunnerName, "-0") {
			t.Fatalf("expected assigned runner without zero request identity, got %q", startedRunner.RunnerName)
		}
	}
	if !strings.Contains(fixture.logs.String(), "assigned capacity reconciled") ||
		!strings.Contains(fixture.logs.String(), "startedRunners=2") ||
		strings.Contains(fixture.logs.String(), "requestID=0") {
		t.Fatalf("expected aggregate assigned-capacity log without fabricated identity, got %q", fixture.logs.String())
	}
}

type assignedStatisticsFixture struct {
	logs     *bytes.Buffer
	provider *fakeScalerProvider
	handler  coordinatedScaleSetHandler
	message  *upstream.RunnerScaleSetMessage
}

func newAssignedStatisticsFixture() assignedStatisticsFixture {
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	provider := &fakeScalerProvider{trackStarted: true}
	coordinator := scaler.NewCoordinator(2)
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "local", MaxRunners: 2, Provider: provider, JITConfig: fakeJITConfigGenerator{},
	}}})
	return assignedStatisticsFixture{
		logs: logs, provider: provider,
		handler: coordinatedScaleSetHandler{Logger: logger, ScaleSet: "self-hosted", Coordinator: coordinator, Repository: "repo"},
		message: &upstream.RunnerScaleSetMessage{
			Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2}, JobAssignedMessages: []*upstream.JobAssigned{{}, {}},
		},
	}
}

func TestCoordinatedScaleSetHandlerAppliesLowerAssignedCountBeforeScheduling(t *testing.T) {
	capacityHolder := &recordingRecoveryWorker{running: 1}
	waitingWorker := &recordingRecoveryWorker{}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("capacity-holder", capacityHolder)
	coordinator.AddRepository("waiting", waitingWorker)
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "waiting"}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	}); err != nil {
		t.Fatal(err)
	}

	capacityHolder.running = 0
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
	}); err != nil {
		t.Fatal(err)
	}
	if waitingWorker.assigned != 0 {
		t.Fatalf("expected current zero assigned count to prevent stale provisioning, got %d", waitingWorker.assigned)
	}
}

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

func TestCoordinatedWorkerReportsUsabilityFailure(t *testing.T) {
	store := &faultLifecycleDemandStore{MemoryDemandStore: scaler.NewMemoryDemandStore(), inventoryErr: errors.New("inventory")}
	worker := coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "linux", Repository: "repo", Provider: &incidentLifecycleProvider{}, JITConfig: newIncidentJITRegistry(), Lifecycle: store,
	}}}
	if _, err := worker.ReconcileRunnerUsability(context.Background()); err == nil {
		t.Fatal("expected usability failure")
	}
}

func TestCoordinatedHandlerReportsLifecyclePersistenceFailures(t *testing.T) {
	store := &faultLifecycleDemandStore{MemoryDemandStore: scaler.NewMemoryDemandStore(), busyErr: errors.New("busy")}
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo", ScaleSet: "linux"}
	message := &upstream.RunnerScaleSetMessage{Statistics: &upstream.RunnerScaleSetStatistic{},
		JobStartedMessages: []*upstream.JobStarted{{RunnerName: "runner"}}}
	if err := handler.HandleMessage(context.Background(), message); err == nil {
		t.Fatal("expected busy persistence failure")
	}
	store.busyErr = nil
	store.completeErr = errors.New("complete")
	if err := handler.completeJob(context.Background(), &upstream.JobCompleted{RunnerName: "runner"}); err == nil {
		t.Fatal("expected cleanup persistence failure")
	}
	fallback := coordinatedScaleSetHandler{Coordinator: scaler.NewDurableCoordinator("linux", 1, lifecycleUnsupportedDemandStore{}), Repository: "repo"}
	if err := fallback.completeJob(context.Background(), &upstream.JobCompleted{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 7}}); err != nil {
		t.Fatal(err)
	}
}

func TestMissingGitHubRegistrationIsReapedWhenItBlocksDemand(t *testing.T) {
	ctx := context.Background()
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	provider := &incidentLifecycleProvider{}
	jit := newIncidentJITRegistry()
	reconciler := scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "local", MaxRunners: 1, Provider: provider, JITConfig: jit,
	}}
	if err := coordinator.ConfigureRunnerLifecycle(&reconciler, "repo", scaler.RunnerProviderDocker); err != nil {
		t.Fatal(err)
	}
	coordinator.AddRepository("repo", coordinatedRepositoryWorker{Reconciler: reconciler, Repository: "repo", ScaleSet: "linux"})
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo", ScaleSet: "linux"}
	if err := handler.HandleMessage(ctx, &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	}); err != nil {
		t.Fatal(err)
	}
	delete(jit.runners, provider.started[0].RunnerName)
	if err := handler.HandleMessage(ctx, &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2},
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingRunnerReaps(ctx, "linux", "repo")
	if err != nil || len(pending) != 1 || pending[0].ReapReason != "github_registration_absent" {
		t.Fatalf("missing registration pending reaps = %#v, %v", pending, err)
	}
}

func TestJobStartedMarksExactRunnerBusyBeforeMessageSucceeds(t *testing.T) {
	ctx := context.Background()
	fixture := incidentLifecycleFixture(t, 1)
	handler, provider, store := fixture.handler, fixture.provider, fixture.store
	if err := handler.HandleMessage(ctx, &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := handler.HandleMessage(ctx, &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobStartedMessages: []*upstream.JobStarted{{
			RunnerID: 99, RunnerName: provider.started[0].RunnerName,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	lifecycles, err := store.RunnerLifecycles(ctx, "linux", "repo")
	if err != nil || len(lifecycles) != 1 || lifecycles[0].Phase != scaler.RunnerBusy || lifecycles[0].GitHubRunnerID != 99 {
		t.Fatalf("started lifecycle = %#v, %v", lifecycles, err)
	}
}

func TestCanceledCompletionsReapIdentityBoundAndIdentityFreeRunners(t *testing.T) {
	ctx := context.Background()
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 2, store)
	now := time.Now().UTC()
	seedCanceledCompletionRunners(t, store, now)
	if err := coordinator.Apply(ctx, []scaler.JobDemand{{Repository: "repo", RequestID: 42, QueueTime: now}}, nil); err != nil {
		t.Fatal(err)
	}
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo", ScaleSet: "linux"}
	if err := handler.HandleMessage(ctx, &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{
			{Result: "canceled", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42}},
			{Result: "canceled", RunnerName: "linux-1"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingRunnerReaps(ctx, "linux", "repo")
	if err != nil || len(pending) != 2 {
		t.Fatalf("canceled pending reaps = %#v, %v", pending, err)
	}
}

func seedCanceledCompletionRunners(t *testing.T, store *scaler.MemoryDemandStore, now time.Time) {
	t.Helper()
	for _, runner := range []scaler.RunnerLifecycle{
		{ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker, RunnerName: "linux-42", RequestID: 42, Phase: scaler.RunnerWaitingForJob, CreatedAt: now},
		{ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker, RunnerName: "linux-1", Phase: scaler.RunnerWaitingForJob, CreatedAt: now},
	} {
		if err := store.RegisterRunner(context.Background(), runner); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCoordinatedScaleSetHandlerRejectsMalformedAvailableDemand(t *testing.T) {
	worker := &recordingRecoveryWorker{}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}

	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{}},
	})
	if err == nil || !strings.Contains(err.Error(), "positive runner request ID and queue time") {
		t.Fatalf("expected malformed available demand rejection, got %v", err)
	}
	if len(worker.acquired) != 0 || len(worker.started) != 0 {
		t.Fatalf("expected no malformed demand side effects, acquired=%#v started=%#v", worker.acquired, worker.started)
	}
}

type incidentLifecycleProvider struct {
	instances []scaler.RunnerInstance
	started   []scaler.RunnerInstance
	destroyed []string
}

func (provider *incidentLifecycleProvider) List(context.Context, string) ([]scaler.RunnerInstance, error) {
	return append([]scaler.RunnerInstance(nil), provider.instances...), nil
}

func (provider *incidentLifecycleProvider) Start(_ context.Context, request scaler.RunnerStartRequest) error {
	instance := scaler.RunnerInstance{
		ID: fmt.Sprintf("instance-%d", len(provider.started)+1), RunnerName: request.RunnerName, State: scaler.RunnerStateRunning,
	}
	provider.started = append(provider.started, instance)
	provider.instances = append(provider.instances, instance)
	return nil
}

func (provider *incidentLifecycleProvider) Destroy(_ context.Context, instance scaler.RunnerInstance) error {
	provider.destroyed = append(provider.destroyed, instance.ID)
	for index := range provider.instances {
		if provider.instances[index].ID == instance.ID {
			provider.instances = append(provider.instances[:index], provider.instances[index+1:]...)
			break
		}
	}
	return nil
}

type incidentJITRegistry struct {
	nextID  int64
	runners map[string]scaler.JITRunner
}

func newIncidentJITRegistry() *incidentJITRegistry {
	return &incidentJITRegistry{runners: map[string]scaler.JITRunner{}}
}

func (registry *incidentJITRegistry) Generate(_ context.Context, runnerName string) (scaler.JITRunnerConfig, error) {
	registry.nextID++
	runner := scaler.JITRunner{ID: registry.nextID, Name: runnerName}
	registry.runners[runnerName] = runner
	return scaler.JITRunnerConfig{Runner: runner, JITConfig: "jit"}, nil
}

func (registry *incidentJITRegistry) Remove(_ context.Context, runner scaler.JITRunner) error {
	delete(registry.runners, runner.Name)
	return nil
}

func (registry *incidentJITRegistry) Find(_ context.Context, runnerName string) (scaler.JITRunner, bool, error) {
	runner, found := registry.runners[runnerName]
	return runner, found, nil
}

func TestCoordinatedScaleSetHandlerRequiresStatisticsAndStartsAssignedWorker(t *testing.T) {
	worker := &recordingRecoveryWorker{}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{}); err == nil {
		t.Fatal("expected missing statistics failure")
	}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if worker.assigned != 1 {
		t.Fatalf("expected assigned worker start, got %d", worker.assigned)
	}
}

func TestCoordinatedScaleSetHandlerIgnoresZeroIdentityCompletion(t *testing.T) {
	worker := &recordingRecoveryWorker{running: 1}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		Repository: "repo", RequestID: 48, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobStartedMessages:   []*upstream.JobStarted{{}},
		JobCompletedMessages: []*upstream.JobCompleted{{}},
	}); err != nil {
		t.Fatal(err)
	}
	worker.running = 0
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.started) != 1 || worker.started[0] != 48 {
		t.Fatalf("expected valid demand to survive zero-ID completion, got %#v", worker.started)
	}
}

func TestCoordinatedScaleSetHandlerRemovesRepeatedPositiveIdentityCompletionIdempotently(t *testing.T) {
	worker := &recordingRecoveryWorker{running: 1}
	coordinator := scaler.NewCoordinator(1)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		Repository: "repo", RequestID: 49, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	handler := coordinatedScaleSetHandler{Coordinator: coordinator, Repository: "repo"}
	completion := &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobCompletedMessages: []*upstream.JobCompleted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 49}}},
	}
	for range 2 {
		if err := handler.HandleMessage(context.Background(), completion); err != nil {
			t.Fatal(err)
		}
	}
	worker.running = 0
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.started) != 0 {
		t.Fatalf("expected positive completion identity to remove demand, got %#v", worker.started)
	}
}

func TestCoordinatedScaleSetHandlerPersistsLibvirtReapBeforeCompletionSucceeds(t *testing.T) {
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", &recordingRecoveryWorker{})
	handler := coordinatedScaleSetHandler{
		Coordinator: coordinator,
		Repository:  "repo",
		ScaleSet:    "linux",
		ReapLibvirt: true,
	}
	message := &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{
			Result:         "canceled",
			RunnerName:     "repo-linux-42",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42},
		}},
	}

	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingLibvirtReaps(context.Background(), "linux", "repo")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].RunnerName != "repo-linux-42" {
		t.Fatalf("pending reaps = %#v", pending)
	}
}

func TestCoordinatedScaleSetHandlerPersistsIdentityFreeLibvirtCompletionByRunnerName(t *testing.T) {
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", &recordingRecoveryWorker{})
	handler := coordinatedScaleSetHandler{
		Coordinator: coordinator, Repository: "repo", ScaleSet: "linux", ReapLibvirt: true,
	}
	message := &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: "repo-linux-1"}},
	}

	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingLibvirtReaps(context.Background(), "linux", "repo")
	if err != nil || len(pending) != 1 || pending[0].RunnerName != "repo-linux-1" {
		t.Fatalf("pending reaps = %#v, %v", pending, err)
	}
}

func TestLibvirtCompletionIsNotAcknowledgedUntilCleanupIntentPersists(t *testing.T) {
	harness := newLibvirtCompletionAcknowledgementHarness()
	harness.expectPersistenceFailureWithoutAcknowledgement(t)
	harness.retryWithDurablePersistence(t)
	harness.expectPendingReapAcknowledged(t)
}

type libvirtCompletionAcknowledgementHarness struct {
	store   *failingCompletionLifecycleStore
	client  *ackRecordingMessageClient
	poller  scalesetpoller.Poller
	message *upstream.RunnerScaleSetMessage
}

func newLibvirtCompletionAcknowledgementHarness() libvirtCompletionAcknowledgementHarness {
	store := &failingCompletionLifecycleStore{MemoryDemandStore: scaler.NewMemoryDemandStore(), fail: true}
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", &recordingRecoveryWorker{})
	handler := coordinatedScaleSetHandler{
		Coordinator: coordinator, Repository: "repo", ScaleSet: "linux", ReapLibvirt: true,
	}
	message := &upstream.RunnerScaleSetMessage{
		MessageID: 7, Statistics: &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{
			RunnerName: "linux-42", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42},
		}},
	}
	client := &ackRecordingMessageClient{message: message}
	poller := scalesetpoller.Poller{Client: client, Handler: handler}
	return libvirtCompletionAcknowledgementHarness{store: store, client: client, poller: poller, message: message}
}

func (harness libvirtCompletionAcknowledgementHarness) expectPersistenceFailureWithoutAcknowledgement(t *testing.T) {
	t.Helper()
	if err := harness.poller.PollOnce(context.Background()); err == nil {
		t.Fatal("expected durable completion failure")
	}
	if len(harness.client.acknowledged) != 0 {
		t.Fatalf("failed persistence acknowledged messages %#v", harness.client.acknowledged)
	}
}

func (harness libvirtCompletionAcknowledgementHarness) retryWithDurablePersistence(t *testing.T) {
	t.Helper()
	harness.store.fail = false
	harness.client.message = harness.message
	if err := harness.poller.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func (harness libvirtCompletionAcknowledgementHarness) expectPendingReapAcknowledged(t *testing.T) {
	t.Helper()
	pending, err := harness.store.PendingLibvirtReaps(context.Background(), "linux", "repo")
	acknowledged := harness.client.acknowledged
	if err != nil || len(pending) != 1 || len(acknowledged) != 1 || acknowledged[0] != 7 {
		t.Fatalf("pending=%#v acknowledged=%#v error=%v", pending, acknowledged, err)
	}
}

type failingCompletionLifecycleStore struct {
	*scaler.MemoryDemandStore
	fail bool
}

func (store *failingCompletionLifecycleStore) CompleteJob(ctx context.Context, completion scaler.JobCompletion) error {
	if store.fail {
		return errors.New("persistence failed")
	}
	if err := store.MemoryDemandStore.CompleteJob(ctx, completion); err != nil {
		return fmt.Errorf("complete in-memory job: %w", err)
	}
	return nil
}

type ackRecordingMessageClient struct {
	message      *upstream.RunnerScaleSetMessage
	acknowledged []int
}

func (client *ackRecordingMessageClient) GetMessage(context.Context, int, int) (*upstream.RunnerScaleSetMessage, error) {
	message := client.message
	client.message = nil
	if message == nil {
		return nil, scalesetpoller.ErrNoMessage
	}
	return message, nil
}

func (client *ackRecordingMessageClient) DeleteMessage(_ context.Context, messageID int) error {
	client.acknowledged = append(client.acknowledged, messageID)
	return nil
}
