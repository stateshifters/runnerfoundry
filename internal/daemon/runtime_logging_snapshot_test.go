package daemon

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/config"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestHermeticRuntimeCompositionReportsReadinessAndRuntimeProgress(t *testing.T) {
	fixture := newRuntimeLoggingFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := fixture.start(ctx)
	<-fixture.discoverer.started
	waitForSupervisorSignal(t, fixture.notifier.ready, "systemd readiness while discovery is blocked")
	close(fixture.discoverer.release)
	assertOperationalHealth(t, fixture.notifier.statuses)
	<-fixture.cycleCompleted
	waitForSnapshotTerm(t, fixture.source)
	runSnapshotCycle(t, fixture.ticker, fixture.cycleCompleted)
	waitForSnapshotTerm(t, fixture.source)
	<-fixture.sessionOpened
	runSnapshotCycle(t, fixture.ticker, fixture.cycleCompleted)
	<-fixture.pollStarted
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	assertRuntimeLogSnapshot(t, fixture.destination.String())
}

type runtimeLoggingFixture struct {
	destination    *memoryLogDestination
	notifier       *runtimeSnapshotNotifier
	discoverer     *blockingRepositoryDiscoverer
	reporter       *productionRuntimeReporter
	generation     *maintenanceGeneration
	source         *githubSourceRuntime
	ticker         *manualCycleTicker
	cycleCompleted chan struct{}
	sessionOpened  chan struct{}
	pollStarted    chan struct{}
}

func newRuntimeLoggingFixture(t *testing.T) runtimeLoggingFixture {
	t.Helper()
	destination := &memoryLogDestination{}
	logging, err := newLoggingManager(config.LogConfig{Format: "text", Level: "debug"},
		singleLogDestinationFactory{destination: destination})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTestLogging(t, logging) })
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	notifier := &runtimeSnapshotNotifier{ready: make(chan struct{}, 1), statuses: make(chan operationalStatus, 2)}
	reporter := newProductionRuntimeReporter(logging, &productionState{member: "member-a"}, func() time.Time { return now })
	reporter.notifier = notifier
	discoverer := &blockingRepositoryDiscoverer{started: make(chan struct{}), release: make(chan struct{})}
	store := newBuilderStoreFake()
	sessionOpened := make(chan struct{})
	pollStarted := make(chan struct{})
	manager := newRuntimeSnapshotManager(t, discoverer, store, reporter, now, sessionOpened, pollStarted)
	ticker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	runtimeGeneration, source, cycleCompleted := newRuntimeSnapshotGeneration(manager, store, reporter, now, ticker)
	return runtimeLoggingFixture{destination: destination, notifier: notifier, discoverer: discoverer,
		reporter: reporter, generation: runtimeGeneration, source: source, ticker: ticker,
		cycleCompleted: cycleCompleted,
		sessionOpened:  sessionOpened, pollStarted: pollStarted}
}

func newRuntimeSnapshotManager(t *testing.T, discoverer githubsource.RepositoryDiscoverer, store *builderStoreFake,
	reporter *productionRuntimeReporter, now time.Time, sessionOpened chan struct{}, pollStarted chan struct{},
) *githubsource.Manager {
	t.Helper()
	manager, err := githubsource.NewManager(discoverer,
		runtimeSnapshotClientFactory(&runtimeSnapshotSession{pollStarted: pollStarted}, sessionOpened), store,
		newGitHubManagerObserver(reporter.reportMaintenance, func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func newRuntimeSnapshotGeneration(manager *githubsource.Manager, store *builderStoreFake,
	reporter *productionRuntimeReporter, now time.Time, ticker *manualCycleTicker,
) (*maintenanceGeneration, *githubSourceRuntime, chan struct{}) {
	capacity := runtimeengine.NewCapacitySnapshotService(store, func() time.Time { return now }, time.Minute)
	source := newGitHubSourceRuntime(manager, capacity, "member-a", eventHandlerFake{}, githubsource.ManagerConfig{
		RunnerPools: []githubsource.RunnerPoolConfig{{RunnerPool: "linux", RunnerGroup: "default"}},
	}, time.Hour, func() time.Time { return now })
	source.report = reporter.reportMaintenance
	events := make([]string, 0)
	engine := NewUnifiedEngine(UnifiedEngineConfig{
		CurrentTime: func() time.Time { return now }, MemberTTL: time.Minute,
		Membership: membershipFake{events: &events}, Leadership: leadershipFake{leader: true, events: &events},
		Source: source, Pools: []RunnerPoolRuntime{{RunnerPool: "linux", runtime: poolRuntimeFake{events: &events}}},
		Inventory: inventoryRuntimeFake{events: &events}, Report: reporter.reportMaintenance,
	})
	cycleCompleted := make(chan struct{}, 1)
	observedEngine := &cycleCompletionEngine{cycleEngine: newGatedCycleEngine(
		engine, leadershipFake{leader: true, events: &events}), completed: cycleCompleted}
	runtimeGeneration := newMaintenanceGeneration(observedEngine,
		ticker, reporter.reportMaintenance, nil, nil)
	runtimeGeneration.currentTime = func() time.Time { return now }
	return runtimeGeneration, source, cycleCompleted
}

func runSnapshotCycle(t *testing.T, ticker *manualCycleTicker, completed <-chan struct{}) {
	t.Helper()
	ticker.ticks <- time.Time{}
	<-completed
}

func waitForSnapshotTerm(t *testing.T, source *githubSourceRuntime) {
	t.Helper()
	if err := source.waitForTerm(context.Background(), source.term); err != nil {
		t.Fatalf("wait for GitHub background work: %v", err)
	}
}

type cycleCompletionEngine struct {
	cycleEngine
	completed chan<- struct{}
}

func (engine *cycleCompletionEngine) Cycle(ctx context.Context) error {
	err := engine.cycleEngine.Cycle(ctx)
	engine.completed <- struct{}{}
	if err != nil {
		return fmt.Errorf("run observed cycle: %w", err)
	}
	return nil
}

func (fixture runtimeLoggingFixture) start(ctx context.Context) <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- supervise(ctx, nil, func() (config.Config, error) { return config.Config{}, nil },
			func(context.Context, config.Config) (generation, error) { return fixture.generation, nil }, fixture.notifier,
			fixture.reporter.reportMaintenance, LeaveActiveRunners)
	}()
	return result
}

func assertOperationalHealth(t *testing.T, statuses <-chan operationalStatus) {
	t.Helper()
	select {
	case status := <-statuses:
		if status.health != healthOperational {
			t.Fatalf("health = %q, want operational", status.health)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for operational health")
	}
}

func assertRuntimeLogSnapshot(t *testing.T, output string) {
	t.Helper()
	for _, fragment := range []string{
		"operation=maintenance_cycle member=member-a stage=immediate outcome=started",
		"operation=source_reconciliation member=member-a stage=reconciliation outcome=started",
		"operation=source_sessions member=member-a stage=open_sessions outcome=completed",
		"operation=capacity_publication member=member-a stage=runner_pool outcome=published runner_pool=linux",
	} {
		if !strings.Contains(output, fragment) {
			t.Fatalf("runtime log snapshot missing %q from %q", fragment, output)
		}
	}
}

func runtimeSnapshotClientFactory(
	session githubsource.ManagedSession,
	sessionOpened chan struct{},
) githubsource.ManagerClientFactoryFunc {
	var opened sync.Once
	return func(context.Context, transport.Repository) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{
			ResolveRunnerGroup: func(context.Context, string) (transport.RunnerGroupID, error) {
				return 7, nil
			},
			EnsureRunnerPool: func(context.Context, string, string) (transport.Registration, error) {
				return transport.Registration{ID: 11, Name: "linux", RunnerGroupID: 7}, nil
			},
			OpenSession: func(context.Context, transport.Registration, string) (githubsource.ManagedSession, error) {
				opened.Do(func() { close(sessionOpened) })
				return session, nil
			},
			GenerateBootstrap: func(context.Context, transport.Registration, string) ([]byte, error) { return nil, nil },
			PruneRunnerPools: func(context.Context, transport.RunnerGroupID, []transport.OwnedRunnerPool, []transport.OwnedRunnerPool) (transport.PruneResult, error) {
				return transport.PruneResult{}, nil
			},
		}, nil
	}
}

type blockingRepositoryDiscoverer struct {
	started chan struct{}
	release chan struct{}
}

func (discoverer *blockingRepositoryDiscoverer) Discover(ctx context.Context) (transport.DiscoveryResult, error) {
	close(discoverer.started)
	select {
	case <-discoverer.release:
		return transport.DiscoveryResult{Repositories: []transport.Repository{{InstallationID: 1, URL: "https://github.com/example/project"}}}, nil
	case <-ctx.Done():
		return transport.DiscoveryResult{}, fmt.Errorf("wait to release discovery: %w", ctx.Err())
	}
}

type runtimeSnapshotSession struct {
	pollStarted chan struct{}
	pollOnce    sync.Once
}

func (session *runtimeSnapshotSession) GetMessage(ctx context.Context, _, _ int) (*upstream.RunnerScaleSetMessage, error) {
	session.pollOnce.Do(func() { close(session.pollStarted) })
	<-ctx.Done()
	return nil, fmt.Errorf("wait for poll cancellation: %w", ctx.Err())
}
func (*runtimeSnapshotSession) DeleteMessage(context.Context, int) error { return nil }
func (*runtimeSnapshotSession) AcquireRequests(_ context.Context, requestIDs []int64) ([]int64, error) {
	return requestIDs, nil
}
func (*runtimeSnapshotSession) Close(context.Context) error { return nil }

type runtimeSnapshotNotifier struct {
	ready    chan struct{}
	statuses chan operationalStatus
}

func (notifier *runtimeSnapshotNotifier) Ready(context.Context) error {
	notifier.ready <- struct{}{}
	return nil
}
func (*runtimeSnapshotNotifier) Reloading(context.Context) error { return nil }
func (notifier *runtimeSnapshotNotifier) Status(_ context.Context, status operationalStatus) error {
	notifier.statuses <- status
	return nil
}
