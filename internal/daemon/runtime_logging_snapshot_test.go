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
	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	githubsupervisor "github.com/stateshifters/runnerfoundry/internal/source/github/supervisor"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestHermeticRuntimeCompositionReportsReadinessAndRuntimeProgress(t *testing.T) {
	fixture := newRuntimeLoggingFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := fixture.start(ctx)
	<-fixture.discoverer.started
	waitForSupervisorSignal(t, fixture.notifier.ready, "systemd readiness while discovery is blocked")
	close(fixture.discoverer.release)
	<-fixture.discoverer.finished
	assertOperationalHealth(t, fixture.notifier.statuses)
	<-fixture.sourceCompleted
	runSnapshotSourceUntil(t, fixture.sourceTicker, fixture.sourceCompleted, fixture.sessionOpened)
	runSnapshotSourceUntil(t, fixture.sourceTicker, fixture.sourceCompleted, fixture.pollStarted)
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	assertRuntimeLogSnapshot(t, fixture.destination.String())
}

type runtimeLoggingFixture struct {
	destination     *memoryLogDestination
	notifier        *runtimeSnapshotNotifier
	discoverer      *blockingRepositoryDiscoverer
	reporter        *productionRuntimeReporter
	generation      *maintenanceGeneration
	sourceTicker    *manualCycleTicker
	sourceCompleted chan struct{}
	sessionOpened   chan struct{}
	pollStarted     chan struct{}
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
	discoverer := &blockingRepositoryDiscoverer{started: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	store := newBuilderStoreFake()
	sessionOpened := make(chan struct{})
	pollStarted := make(chan struct{})
	manager := newRuntimeSnapshotManager(t, discoverer, store, reporter, now, sessionOpened, pollStarted)
	sourceTicker := &manualCycleTicker{ticks: make(chan time.Time, 1)}
	runtimeGeneration, sourceCompleted := newRuntimeSnapshotGeneration(manager, store, reporter, now, sourceTicker)
	return runtimeLoggingFixture{destination: destination, notifier: notifier, discoverer: discoverer,
		reporter: reporter, generation: runtimeGeneration, sourceTicker: sourceTicker,
		sourceCompleted: sourceCompleted,
		sessionOpened:   sessionOpened, pollStarted: pollStarted}
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
	reporter *productionRuntimeReporter, now time.Time, sourceTicker *manualCycleTicker,
) (*maintenanceGeneration, chan struct{}) {
	engine := newRuntimeSnapshotEngine(manager, store, reporter, now)
	return newObservedRuntimeSnapshotGeneration(engine, reporter, now, sourceTicker)
}

func newRuntimeSnapshotEngine(
	manager *githubsource.Manager,
	store *builderStoreFake,
	reporter *productionRuntimeReporter,
	now time.Time,
) *UnifiedEngine {
	capacity := runtimeengine.NewCapacitySnapshotService(store, func() time.Time { return now }, time.Minute)
	source := githubsupervisor.New(manager, capacity, "member-a", runtimeSnapshotEventHandler{}, githubsource.ManagerConfig{
		RunnerPools: []githubsource.RunnerPoolConfig{{RunnerPool: "linux", RunnerGroup: "default"}},
	}, time.Hour, func() time.Time { return now }, newGitHubManagerObserver(reporter.reportMaintenance, func() time.Time { return now }))
	membershipEvents := make([]string, 0)
	executionEvents := make([]string, 0)
	engine := NewUnifiedEngine(UnifiedEngineConfig{
		CurrentTime: func() time.Time { return now }, MemberTTL: time.Minute,
		Membership: membershipFake{events: &membershipEvents}, Leadership: permanentLeadership{},
		Source: source, Pools: []RunnerPoolRuntime{{RunnerPool: "linux", runtime: poolRuntimeFake{events: &executionEvents}}},
		Inventory: inventoryRuntimeFake{events: &executionEvents}, Report: reporter.reportMaintenance,
	})
	return engine
}

func newObservedRuntimeSnapshotGeneration(
	engine *UnifiedEngine,
	reporter *productionRuntimeReporter,
	now time.Time,
	sourceTicker *manualCycleTicker,
) (*maintenanceGeneration, chan struct{}) {
	sourceCompleted := make(chan struct{}, 1)
	gatedEngine := newGatedMaintenanceEngine(engine, (&localLeadershipGate{}).NewLeadership())
	runtimeGeneration := newIndependentMaintenanceGeneration(
		gatedEngine,
		gatedEngine,
		maintenanceLaneTickers{
			membership: &manualCycleTicker{ticks: make(chan time.Time)},
			source:     sourceTicker,
			execution:  &manualCycleTicker{ticks: make(chan time.Time)},
		},
		reporter.reportMaintenance,
		nil,
		nil,
	)
	runtimeGeneration.maintenanceLaneCompleted = func(laneName string, _ cycleTicker) {
		if laneName == "source" {
			sourceCompleted <- struct{}{}
		}
	}
	runtimeGeneration.currentTime = func() time.Time { return now }
	return runtimeGeneration, sourceCompleted
}

func runSnapshotSource(t *testing.T, ticker *manualCycleTicker, completed <-chan struct{}) {
	t.Helper()
	ticker.ticks <- time.Time{}
	<-completed
}

func runSnapshotSourceUntil(t *testing.T, ticker *manualCycleTicker, completed <-chan struct{}, signal <-chan struct{}) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case <-signal:
			return
		default:
		}
		runSnapshotSource(t, ticker, completed)
		select {
		case <-signal:
			return
		case <-deadline:
			t.Fatal("timed out waiting for GitHub poll")
		default:
		}
	}
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
		"operation=maintenance_source member=member-a stage=immediate outcome=started",
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
	started  chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func (discoverer *blockingRepositoryDiscoverer) Discover(ctx context.Context) (transport.DiscoveryResult, error) {
	close(discoverer.started)
	select {
	case <-discoverer.release:
		close(discoverer.finished)
		return transport.DiscoveryResult{Repositories: []transport.Repository{{InstallationID: 1, URL: "https://github.com/example/project"}}}, nil
	case <-ctx.Done():
		return transport.DiscoveryResult{}, fmt.Errorf("wait to release discovery: %w", ctx.Err())
	}
}

type runtimeSnapshotEventHandler struct{}

func (runtimeSnapshotEventHandler) ReconcileAssignedDemand(
	_ context.Context,
	demand githubsource.AssignedDemand,
) (githubsource.AssignedDemandResult, error) {
	return githubsource.AssignedDemandResult{ActiveJobs: demand.DesiredJobs}, nil
}

func (runtimeSnapshotEventHandler) ObserveTerminalIfUnaccepted(
	context.Context,
	core.JobIdentity,
) (core.ConditionalTerminalResult, error) {
	return core.ConditionalTerminalResult{Outcome: core.ConditionalTerminalProtected}, nil
}

func (runtimeSnapshotEventHandler) IngestDemand(context.Context, core.JobRequest) (core.EnqueueResult, error) {
	return core.EnqueueResult{Outcome: core.QueueQueued}, nil
}

func (runtimeSnapshotEventHandler) ObserveRunnerAcceptance(
	context.Context,
	core.RunnerObservation,
) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}

func (runtimeSnapshotEventHandler) ObserveTerminal(
	context.Context,
	core.RunnerObservation,
) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
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
