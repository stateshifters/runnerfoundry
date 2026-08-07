package daemon

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestHermeticRuntimeCompositionReportsReadinessAndRuntimeProgress(t *testing.T) {
	fixture := newRuntimeLoggingFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := fixture.start(ctx)
	<-fixture.discoverer.started
	waitForSupervisorSignal(t, fixture.notifier.ready, "systemd readiness while discovery is blocked")
	close(fixture.discoverer.release)
	assertOperationalHealth(t, fixture.notifier.statuses)
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	assertRuntimeLogSnapshot(t, fixture.destination.String())
}

type runtimeLoggingFixture struct {
	destination *memoryLogDestination
	notifier    *runtimeSnapshotNotifier
	discoverer  *blockingRepositoryDiscoverer
	reporter    *productionRuntimeReporter
	generation  *maintenanceGeneration
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
	manager := newRuntimeSnapshotManager(t, discoverer, store, reporter, now)
	runtimeGeneration := newRuntimeSnapshotGeneration(manager, store, reporter, now)
	return runtimeLoggingFixture{destination: destination, notifier: notifier, discoverer: discoverer,
		reporter: reporter, generation: runtimeGeneration}
}

func newRuntimeSnapshotManager(t *testing.T, discoverer githubsource.RepositoryDiscoverer, store *builderStoreFake,
	reporter *productionRuntimeReporter, now time.Time,
) *githubsource.Manager {
	t.Helper()
	manager, err := githubsource.NewManager(discoverer, runtimeSnapshotClientFactory(&runtimeSnapshotSession{}), store,
		newGitHubManagerObserver(reporter.reportMaintenance, func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func newRuntimeSnapshotGeneration(manager *githubsource.Manager, store *builderStoreFake,
	reporter *productionRuntimeReporter, now time.Time,
) *maintenanceGeneration {
	source := newGitHubSourceRuntime(manager, store, "member-a", eventHandlerFake{}, githubsource.ManagerConfig{
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
	runtimeGeneration := newMaintenanceGeneration(newGatedCycleEngine(engine, leadershipFake{leader: true, events: &events}),
		&manualCycleTicker{ticks: make(chan time.Time)}, reporter.reportMaintenance, nil, nil)
	runtimeGeneration.currentTime = func() time.Time { return now }
	return runtimeGeneration
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

func runtimeSnapshotClientFactory(session githubsource.ManagedSession) githubsource.ManagerClientFactoryFunc {
	return func(context.Context, transport.Repository) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{
			EnsureRunnerPool: func(context.Context, string, string) (transport.Registration, error) {
				return transport.Registration{ID: 11, Name: "linux", RunnerGroupID: 7}, nil
			},
			OpenSession: func(context.Context, transport.Registration, string) (githubsource.ManagedSession, error) {
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

type runtimeSnapshotSession struct{}

func (*runtimeSnapshotSession) GetMessage(ctx context.Context, _, _ int) (*upstream.RunnerScaleSetMessage, error) {
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
