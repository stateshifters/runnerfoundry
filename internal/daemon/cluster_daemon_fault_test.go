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
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestClusteredStartupRequiresOperatingSystemHostname(t *testing.T) {
	failure := errors.New("failed")
	for name, hostname := range map[string]func() (string, error){
		"missing reader": nil,
		"lookup failure": func() (string, error) { return "", failure },
		"empty hostname": func() (string, error) { return "  ", nil },
	} {
		t.Run(name, func(t *testing.T) {
			assertClusteredStartupFailure(t, func(_ *config.Config, daemon *clusteredDaemon, _ *fakeClusterState) {
				daemon.hostname = hostname
			})
		})
	}
}

func TestClusteredStartupRequiresMigratedPostgreSQLState(t *testing.T) {
	failure := errors.New("failed")
	tests := map[string]func(*config.Config, *clusteredDaemon, *fakeClusterState){
		"configured backend": func(configured *config.Config, _ *clusteredDaemon, _ *fakeClusterState) {
			configured.State.Postgres = nil
		},
		"open connection": func(_ *config.Config, daemon *clusteredDaemon, _ *fakeClusterState) {
			daemon.openStore = func(postgresconfig.Settings) (clusterStoreConnection, error) {
				return clusterStoreConnection{}, failure
			}
		},
		"migration status": func(_ *config.Config, _ *clusteredDaemon, state *fakeClusterState) {
			state.migrationStatusErr = failure
		},
		"upward migration": func(_ *config.Config, _ *clusteredDaemon, state *fakeClusterState) {
			state.migrateErr = failure
		},
	}
	for name, configure := range tests {
		t.Run(name, func(t *testing.T) { assertClusteredStartupFailure(t, configure) })
	}
}

func TestClusteredStartupRequiresGitHubCredentials(t *testing.T) {
	assertClusteredStartupFailure(t, func(configured *config.Config, _ *clusteredDaemon, _ *fakeClusterState) {
		configured.GitHub.App.PrivateKeyFile = "missing-private-key"
	})
}

func TestClusteredStartupRequiresRepositoryDiscovery(t *testing.T) {
	assertClusteredStartupFailure(t, func(_ *config.Config, daemon *clusteredDaemon, _ *fakeClusterState) {
		daemon.newDiscoverer = func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return nil, errors.New("failed")
		}
	})
}

func TestClusteredStartupRequiresPublicationBeforeReadiness(t *testing.T) {
	failure := errors.New("failed")
	tests := map[string]func(*config.Config, *clusteredDaemon, *fakeClusterState){
		"publication": func(_ *config.Config, _ *clusteredDaemon, state *fakeClusterState) { state.publishErr = failure },
		"readiness": func(_ *config.Config, daemon *clusteredDaemon, _ *fakeClusterState) {
			daemon.notifier = failingNotifier{readyErr: failure}
		},
	}
	for name, configure := range tests {
		t.Run(name, func(t *testing.T) { assertClusteredStartupFailure(t, configure) })
	}
}

func assertClusteredStartupFailure(
	t *testing.T,
	configure func(*config.Config, *clusteredDaemon, *fakeClusterState),
) {
	t.Helper()
	configured := clusteredConfig(t)
	state := &fakeClusterState{}
	daemon := clusteredDaemonForTest(state)
	configure(&configured, daemon, state)
	if err := runClusteredDaemon(context.Background(), Options{}, daemon, configured); err == nil {
		t.Fatal("expected clustered startup failure")
	}
}

func TestClusterEventLoopDispatchesHeartbeatWorker(t *testing.T) {
	ctx, fixture := clusterEventFixtureForTest(t)
	fixture.state.onPublish = fixture.cancel
	heartbeats := make(chan time.Time, 1)
	heartbeats <- time.Now()
	if err := fixture.daemon.runEvents(ctx, Options{}, clusterEvents{heartbeat: heartbeats}); err != nil {
		t.Fatal(err)
	}
}

func TestClusterEventLoopDispatchesLeadershipWorker(t *testing.T) {
	ctx, fixture := clusterEventFixtureForTest(t)
	fixture.state.onScaleSets = fixture.cancel
	leadership := make(chan leadershipResult, 1)
	leadership <- leadershipResult{lease: &fakeClusterLease{alive: true}}
	if err := fixture.daemon.runEvents(ctx, Options{}, clusterEvents{leadership: leadership}); err != nil {
		t.Fatal(err)
	}
}

func TestClusterEventLoopDispatchesAssignmentWorker(t *testing.T) {
	ctx, fixture := clusterEventFixtureForTest(t)
	fixture.state.onClaimReady = fixture.cancel
	executor := make(chan time.Time, 1)
	executor <- time.Now()
	if err := fixture.daemon.runEvents(ctx, Options{}, clusterEvents{executor: executor}); err != nil {
		t.Fatal(err)
	}
}

type clusterEventFixture struct {
	state  *fakeClusterState
	daemon *clusteredDaemon
	cancel context.CancelFunc
}

func clusterEventFixtureForTest(t *testing.T) (context.Context, clusterEventFixture) {
	t.Helper()
	state := &fakeClusterState{}
	daemon := clusteredDaemonForTest(state)
	daemon.config = clusteredConfig(t)
	daemon.discoverer = staticDiscoverer{}
	daemon.logger = slog.Default()
	daemon.nodes = state
	daemon.assignments = state
	daemon.tryLeader = fakeTryLeader(state)
	daemon.waitForLeader = fakeWaitForLeader(state)
	ctx, cancel := context.WithCancel(context.Background())
	return ctx, clusterEventFixture{state: state, daemon: daemon, cancel: cancel}
}

func TestClusterEventLoopReturnsReloadFailure(t *testing.T) {
	reloadRequests := make(chan struct{}, 1)
	reloadRequests <- struct{}{}
	state := &fakeClusterState{}
	daemon := clusteredDaemon{
		config: clusteredConfig(t), logger: slog.Default(), nodes: state, assignments: state,
		loadConfig: func(string) (config.Config, error) { return config.Config{}, errors.New("reload failed") },
	}
	if err := daemon.runEvents(context.Background(), Options{ReloadRequests: reloadRequests}, clusterEvents{}); err == nil {
		t.Fatal("expected reload event failure")
	}
}

func TestSlowProviderHealthDoesNotBlockAssignmentExecution(t *testing.T) {
	fixture := newSlowProviderHealthFixture(t)
	heartbeatEvents := make(chan time.Time, 1)
	executorEvents := make(chan time.Time, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- fixture.daemon.runEvents(ctx, Options{}, clusterEvents{heartbeat: heartbeatEvents, executor: executorEvents})
	}()
	heartbeatEvents <- time.Now()
	<-fixture.healthCheckStarted
	executorEvents <- time.Now()
	select {
	case <-fixture.assignmentClaimed:
	case <-time.After(time.Second):
		t.Fatal("assignment execution was blocked by provider health")
	}
	close(fixture.releaseHealthCheck)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type slowProviderHealthFixture struct {
	daemon             *clusteredDaemon
	healthCheckStarted chan struct{}
	releaseHealthCheck chan struct{}
	assignmentClaimed  chan struct{}
}

func newSlowProviderHealthFixture(t *testing.T) *slowProviderHealthFixture {
	t.Helper()
	fixture := &slowProviderHealthFixture{
		healthCheckStarted: make(chan struct{}), releaseHealthCheck: make(chan struct{}), assignmentClaimed: make(chan struct{}),
	}
	state := &fakeClusterState{onClaimReady: func() { close(fixture.assignmentClaimed) }}
	fixture.daemon = clusteredDaemonForTest(state)
	fixture.daemon.config = clusteredConfig(t)
	fixture.daemon.discoverer = staticDiscoverer{}
	fixture.daemon.logger = slog.Default()
	fixture.daemon.providerHealthy = fixture.providerHealth
	fixture.daemon.nodes = state
	fixture.daemon.assignments = state
	fixture.daemon.tryLeader = fakeTryLeader(state)
	fixture.daemon.waitForLeader = fakeWaitForLeader(state)
	return fixture
}

func (fixture *slowProviderHealthFixture) providerHealth(ctx context.Context, _ config.ScaleSetConfig) error {
	close(fixture.healthCheckStarted)
	select {
	case <-fixture.releaseHealthCheck:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("provider health check canceled: %w", ctx.Err())
	}
}

func TestBlockedReconciliationStopsWithDaemonContext(t *testing.T) {
	reconciliationStarted := make(chan struct{})
	state := &fakeClusterState{}
	daemonFixture := clusteredDaemonForTest(state)
	daemon := clusteredDaemon{
		config: clusteredConfig(t), logger: slog.Default(), loadConfig: daemonFixture.loadConfig, newDiscoverer: daemonFixture.newDiscoverer,
		newGitHubClient: daemonFixture.newGitHubClient, providerHealthy: daemonFixture.providerHealthy,
		buildReconciler: daemonFixture.buildReconciler, notifier: daemonFixture.notifier,
		discoverer: blockingDiscoverer{started: reconciliationStarted},
		nodes:      state, assignments: state, tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
	}
	leadership := make(chan leadershipResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- daemon.runEvents(ctx, Options{}, clusterEvents{leadership: leadership})
	}()
	leadership <- leadershipResult{lease: &fakeClusterLease{alive: true}}
	<-reconciliationStarted
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked reconciliation prevented daemon shutdown")
	}
}

type blockingDiscoverer struct {
	started chan<- struct{}
}

func (discoverer blockingDiscoverer) Discover(ctx context.Context) (githubapp.DiscoveryResult, error) {
	close(discoverer.started)
	<-ctx.Done()
	return githubapp.DiscoveryResult{}, fmt.Errorf("blocked discovery: %w", ctx.Err())
}

func TestClusterHeartbeatLogsPublicationFailure(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{publishErr: errors.New("heartbeat failed")}
	daemonFixture := clusteredDaemonForTest(state)
	daemon := clusteredDaemon{
		nodeID: "spock", config: clusteredConfig(t), discoverer: staticDiscoverer{},
		logger: newClusterLogSnapshotLogger(logs), loadConfig: daemonFixture.loadConfig, newDiscoverer: daemonFixture.newDiscoverer,
		newGitHubClient: daemonFixture.newGitHubClient, providerHealthy: daemonFixture.providerHealthy,
		buildReconciler: daemonFixture.buildReconciler, notifier: daemonFixture.notifier,
		nodes: state, assignments: state, tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
	}
	daemon.heartbeat(context.Background())
	assertClusterLogSnapshot(t, logs,
		`{"level":"ERROR","msg":"cluster heartbeat failed","hostname":"spock","error":"publish clustered node contributions: heartbeat failed"}`)
}

func TestClusterLeadershipFailureRemainsRetryable(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{tryLeaderErr: errors.New("election failed")}
	daemon := clusteredDaemon{
		nodeID:    "spock",
		logger:    newClusterLogSnapshotLogger(logs),
		tryLeader: fakeTryLeader(state),
	}
	daemon.tryLeadership(context.Background())
	if daemon.leader != nil {
		t.Fatalf("expected retryable election failure, leader=%#v", daemon.leader)
	}
	assertClusterLogSnapshot(t, logs,
		`{"level":"ERROR","msg":"leader election failed","hostname":"spock","error":"election failed"}`)
}

func TestClusterStatusNotificationFailureLogsNodeIdentity(t *testing.T) {
	logs := &bytes.Buffer{}
	daemon := clusteredDaemon{
		nodeID: "spock", logger: newClusterLogSnapshotLogger(logs), notifier: failingStatusNotifier{},
	}

	daemon.notifyStatus(context.Background(), clusterServiceStatusLeaderDegraded)

	assertClusterLogSnapshot(t, logs,
		`{"level":"WARN","msg":"notify clustered service status failed","hostname":"spock","error":"status unavailable"}`)
}

func TestClusterHeartbeatPreservesLiveLeaderLease(t *testing.T) {
	state := &fakeClusterState{}
	daemon := clusteredDaemon{
		config: clusteredConfig(t), logger: slog.Default(), nodes: state,
		providerHealthy: func(context.Context, config.ScaleSetConfig) error { return nil },
	}
	lease := &fakeClusterLease{alive: true}
	daemon.leader = newClusterLeader(&daemon, lease)
	daemon.heartbeat(context.Background())
	if lease.closed {
		t.Fatal("live leader lease was replaced")
	}
}

func TestClusterHeartbeatLogsInvoluntaryLeadershipLoss(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{}
	daemon := clusteredDaemon{
		nodeID: "spock", config: clusteredConfig(t), logger: newClusterLogSnapshotLogger(logs),
		nodes: state, providerHealthy: func(context.Context, config.ScaleSetConfig) error { return nil },
		waitForLeader: fakeWaitForLeader(state),
	}
	lease := &fakeClusterLease{alive: false}
	daemon.leader = newClusterLeader(&daemon, lease)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	daemon.heartbeat(ctx)
	daemon.stopLeadershipContender(context.Background())

	if daemon.currentLeader() != nil || !lease.closed {
		t.Fatalf("leader=%#v leaseClosed=%t", daemon.currentLeader(), lease.closed)
	}
	assertClusterLogSnapshot(t, logs,
		`{"level":"WARN","msg":"leader_election","action":"leader_election","phase":"lost","hostname":"spock"}`)
}

func TestClusterReloadValidatesAuthorityAndPublishesReplacement(t *testing.T) {
	configured := clusteredConfig(t)
	daemon, state := newClusterReloadFixture(t, clusterReloadFixtureConfig{initial: configured, loaded: configured})
	if err := daemon.reload(context.Background(), "config.yaml"); err != nil {
		t.Fatalf("expected reload success, got %v", err)
	}
	if len(state.publications) != 1 {
		t.Fatalf("replacement publications = %d, want 1", len(state.publications))
	}
}

func TestClusterReloadReportsConfigurationLoadFailure(t *testing.T) {
	daemon, _ := newClusterReloadFixture(t, clusterReloadFixtureConfig{initial: clusteredConfig(t), loadErr: errors.New("failed")})
	if err := daemon.reload(context.Background(), "config.yaml"); err == nil {
		t.Fatal("expected configuration load failure")
	}
}

func TestClusterReloadRejectsNonPostgreSQLBackend(t *testing.T) {
	daemon, _ := newClusterReloadFixture(t, clusterReloadFixtureConfig{initial: clusteredConfig(t), loaded: runnableConfig(t)})
	if err := daemon.reload(context.Background(), "config.yaml"); err == nil {
		t.Fatal("expected backend authority failure")
	}
}

func TestClusterReloadRejectsChangedPostgreSQLConnection(t *testing.T) {
	configured := clusteredConfig(t)
	loaded := withPostgreSQLDSN(configured, "postgres://replacement")
	daemon, _ := newClusterReloadFixture(t, clusterReloadFixtureConfig{initial: configured, loaded: loaded})
	if err := daemon.reload(context.Background(), "config.yaml"); err == nil {
		t.Fatal("expected connection authority failure")
	}
}

func TestClusterReloadRejectsInvalidCredentials(t *testing.T) {
	configured := clusteredConfig(t)
	loaded := withPrivateKey(configured, "missing-key")
	daemon, _ := newClusterReloadFixture(t, clusterReloadFixtureConfig{initial: configured, loaded: loaded})
	if err := daemon.reload(context.Background(), "config.yaml"); err == nil {
		t.Fatal("expected credential validation failure")
	}
}

func TestClusterReloadReportsDiscovererConstructionFailure(t *testing.T) {
	configured := clusteredConfig(t)
	daemon, _ := newClusterReloadFixture(t, clusterReloadFixtureConfig{
		initial: configured, loaded: configured, discovererErr: errors.New("failed"),
	})
	if err := daemon.reload(context.Background(), "config.yaml"); err == nil {
		t.Fatal("expected discoverer construction failure")
	}
}

func TestClusterReloadReportsReplacementPublicationFailure(t *testing.T) {
	configured := clusteredConfig(t)
	daemon, _ := newClusterReloadFixture(t, clusterReloadFixtureConfig{
		initial: configured, loaded: configured, publishErr: errors.New("failed"),
	})
	if err := daemon.reload(context.Background(), "config.yaml"); err == nil {
		t.Fatal("expected replacement publication failure")
	}
}

type clusterReloadFixtureConfig struct {
	initial       config.Config
	loaded        config.Config
	loadErr       error
	discovererErr error
	publishErr    error
}

func newClusterReloadFixture(t *testing.T, fixture clusterReloadFixtureConfig) (*clusteredDaemon, *fakeClusterState) {
	t.Helper()
	state := &fakeClusterState{publishErr: fixture.publishErr}
	daemon := clusteredDaemonForTest(state)
	daemon.config = fixture.initial
	daemon.credentials = testAppCredentials(t)
	daemon.logger = slog.Default()
	daemon.loadConfig = func(string) (config.Config, error) { return fixture.loaded, fixture.loadErr }
	daemon.newDiscoverer = func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
		return staticDiscoverer{}, fixture.discovererErr
	}
	daemon.nodes = state
	daemon.assignments = state
	daemon.tryLeader = fakeTryLeader(state)
	daemon.waitForLeader = fakeWaitForLeader(state)
	return daemon, state
}

func TestLeaderReconciliationReportsDiscoveryFailure(t *testing.T) {
	logs := &bytes.Buffer{}
	daemon := activeClusteredDaemon(t, &fakeClusterState{})
	daemon.nodeID = "spock"
	daemon.discoverer = staticDiscoverer{err: errors.New("failed")}
	daemon.logger = newClusterLogSnapshotLogger(logs)

	if newClusterLeader(&daemon, &fakeClusterLease{}).reconcile(context.Background()) {
		t.Fatal("expected reconciliation failure")
	}
	assertClusterLogSnapshot(t, logs,
		`{"level":"ERROR","msg":"leader repository discovery failed","hostname":"spock","error":"failed"}`)
}

func TestLeaderReconciliationReportsCapacityAggregationFailure(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{statusesErr: errors.New("failed")}
	daemon := activeClusteredDaemon(t, state)
	daemon.nodeID = "spock"
	daemon.logger = newClusterLogSnapshotLogger(logs)

	if newClusterLeader(&daemon, &fakeClusterLease{}).reconcile(context.Background()) {
		t.Fatal("expected reconciliation failure")
	}
	assertClusterLogSnapshot(t, logs,
		`{"level":"ERROR","msg":"leader scale-set aggregation failed","hostname":"spock","error":"failed"}`)
}

func TestLeaderLeavesFailedRepositorySessionAbsent(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errors.New("failed")
	}
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	err := leader.startMissingSessions(context.Background(), map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{
		key: {Name: "linux", RunnerGroup: "Default", Compatible: true, EffectiveCapacity: 1},
	}, map[string]githubapp.Repository{"repo": {URL: "repo"}})
	if err == nil {
		t.Fatal("expected repository session start failure")
	}
	if _, exists := leader.sessions[key]; exists {
		t.Fatal("failed repository session was retained")
	}
}

func TestSchedulerReleasesAssignmentWithoutRepositorySession(t *testing.T) {
	state := &fakeClusterState{assignments: []*cluster.Assignment{{ID: 1, Repository: "repo", ScaleSet: "linux", RequestID: 2}}}
	scheduler := &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}}
	outcome, err := scheduler.scheduleOne(context.Background(), "linux")
	if err != nil || outcome != scheduleProgressed || len(state.released) != 1 || state.released[0] != 1 {
		t.Fatalf("release outcome=%v released=%v error=%v", outcome, state.released, err)
	}
}

func TestSchedulerReportsAssignmentSelectionFailure(t *testing.T) {
	scheduler := &clusterScheduler{store: &fakeClusterState{assignErr: errors.New("failed")}}
	if _, err := scheduler.scheduleOne(context.Background(), "linux"); err == nil {
		t.Fatal("expected assignment selection failure")
	}
}

func TestSchedulerReportsOrphanedAssignmentReleaseFailure(t *testing.T) {
	state := &fakeClusterState{
		assignments: []*cluster.Assignment{{ID: 2, Repository: "repo", ScaleSet: "linux", RequestID: 3}},
		releaseErr:  errors.New("failed"),
	}
	scheduler := &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}}
	if _, err := scheduler.scheduleOne(context.Background(), "linux"); err == nil {
		t.Fatal("expected orphaned assignment release failure")
	}
}

func TestSchedulerReportsGitHubAcquisitionFailure(t *testing.T) {
	failure := errors.New("failed")
	state := &fakeClusterState{assignments: []*cluster.Assignment{{ID: 3, Repository: "repo", ScaleSet: "linux", RequestID: 4}}}
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	scheduler := &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{
		key: {key: key, messageClient: scaleSetMessageClient{Client: &fakeMessageClient{acquireErr: failure}}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 1}},
	}}
	if _, err := scheduler.scheduleOne(context.Background(), "linux"); !errors.Is(err, failure) {
		t.Fatalf("expected acquisition failure, got %v", err)
	}
}

func TestSchedulerRetriesOnlyAcquisitionConfirmation(t *testing.T) {
	failure := errors.New("failed")
	state := &fakeClusterState{
		assignments:              []*cluster.Assignment{{ID: 4, Repository: "repo", ScaleSet: "linux", RequestID: 5}},
		confirmFailuresRemaining: 1,
		confirmFailure:           failure,
	}
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	messageClient := &fakeMessageClient{}
	scheduler := &clusterScheduler{
		store: state, acquisitionConfirmationWait: time.Nanosecond,
		sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{
			key: {key: key, messageClient: scaleSetMessageClient{Client: messageClient}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 1}},
		},
	}
	if _, err := scheduler.scheduleOne(context.Background(), "linux"); err != nil {
		t.Fatalf("expected acquisition confirmation recovery, got %v", err)
	}
	if len(messageClient.acquiredIDs) != 1 || messageClient.acquiredIDs[0] != 5 {
		t.Fatalf("GitHub acquisitions = %v, want [5]", messageClient.acquiredIDs)
	}
}

func TestClusterMessageRequiresStatisticsAndPositiveRequestIdentity(t *testing.T) {
	state := &fakeClusterState{}
	handler := clusterMessageHandlerForTest(state)
	tests := map[string]*upstream.RunnerScaleSetMessage{
		"statistics":         {},
		"available identity": clusterMessage(&upstream.JobAvailable{}),
		"nil started":        {Statistics: &upstream.RunnerScaleSetStatistic{}, JobStartedMessages: []*upstream.JobStarted{nil}},
		"empty started":      {Statistics: &upstream.RunnerScaleSetStatistic{}, JobStartedMessages: []*upstream.JobStarted{{}}},
		"nil completed":      {Statistics: &upstream.RunnerScaleSetStatistic{}, JobCompletedMessages: []*upstream.JobCompleted{nil}},
		"empty completed":    {Statistics: &upstream.RunnerScaleSetStatistic{}, JobCompletedMessages: []*upstream.JobCompleted{{}}},
	}
	for name, message := range tests {
		t.Run(name, func(t *testing.T) {
			if err := handler.HandleMessage(context.Background(), message); err == nil {
				t.Fatal("expected malformed message to be rejected")
			}
		})
	}
}

func TestClusterMessageReportsDemandPersistenceFailure(t *testing.T) {
	failure := errors.New("failed")
	tests := map[string]struct {
		state   *fakeClusterState
		message *upstream.RunnerScaleSetMessage
	}{
		"available": {
			state: &fakeClusterState{enqueueErr: failure},
			message: clusterMessage(&upstream.JobAvailable{JobMessageBase: upstream.JobMessageBase{
				RunnerRequestID: 1, QueueTime: time.Now(),
			}}),
		},
		"completed": {
			state: &fakeClusterState{removeErr: failure},
			message: &upstream.RunnerScaleSetMessage{Statistics: &upstream.RunnerScaleSetStatistic{},
				JobCompletedMessages: []*upstream.JobCompleted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 1}}}},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if err := clusterMessageHandlerForTest(test.state).HandleMessage(context.Background(), test.message); !errors.Is(err, failure) {
				t.Fatalf("expected persistence failure, got %v", err)
			}
		})
	}
}

func clusterMessageHandlerForTest(state *fakeClusterState) clusterMessageHandler {
	return clusterMessageHandler{
		store: state,
		scheduler: &clusterScheduler{
			store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{},
		},
		session: &repositoryScaleSetSession{
			key: repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"},
		},
	}
}

func TestLeadershipIgnoresNilLease(t *testing.T) {
	state := &fakeClusterState{leases: []cluster.Lease{nil}}
	daemon := clusteredDaemon{logger: slog.Default(), tryLeader: fakeTryLeader(state)}
	daemon.tryLeadership(context.Background())
	if daemon.leader != nil {
		t.Fatal("nil lease unexpectedly elected a leader")
	}
}

func TestAssignmentStartReportsPostStartInspectionFailure(t *testing.T) {
	failure := errors.New("failed")
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	provider := &postStartInspectionFailureProvider{failure: failure}
	daemon.buildReconciler = reconcilerWithProvider(provider)
	if err := daemon.executeAssignment(context.Background(), &cluster.Assignment{
		ScaleSet: "linux", Repository: "repo", RequestID: 1,
	}); err == nil || !strings.Contains(err.Error(), "verify assigned runner after start") {
		t.Fatalf("expected runner verification failure, got %v", err)
	}
}

func TestLeaderKeepsSessionWhenCapacityIsUnchanged(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
	session := &repositoryScaleSetSession{
		key: repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}, capacity: 1,
	}
	leader.sessions[session.key] = session
	if err := leader.stopOutdatedSessions(context.Background(), map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{
		session.key: {Name: "linux", EffectiveCapacity: 1},
	}, []cluster.ScaleSetStatus{{Name: "linux"}}); err != nil {
		t.Fatal(err)
	}
	if leader.sessions[session.key] != session {
		t.Fatal("unchanged session was replaced")
	}
}

func TestSchedulerReportsAcquisitionAndReleaseFailure(t *testing.T) {
	failure := errors.New("failed")
	state := &fakeClusterState{
		assignments: []*cluster.Assignment{{ID: 1, Repository: "repo", ScaleSet: "linux", RequestID: 1}},
		releaseErr:  failure,
	}
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	scheduler := &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{
		key: {
			key:            key,
			messageClient:  scaleSetMessageClient{Client: &fakeMessageClient{acquireErr: failure}},
			runnerScaleSet: &upstream.RunnerScaleSet{ID: 1},
		},
	}}
	if _, err := scheduler.scheduleOne(context.Background(), "linux"); err == nil || !strings.Contains(err.Error(), "release unacquired") {
		t.Fatalf("expected joined acquisition and release failure, got %v", err)
	}
}

func TestRepositoryScaleSetSessionFailureIsReturned(t *testing.T) {
	state := &fakeClusterState{}
	daemonUnderTest := clusteredDaemonForTest(state)
	daemonUnderTest.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return &fakeScaleSetClient{fakeScaleSetSessionClient: fakeScaleSetSessionClient{
			fakeRunnerGroupClient: fakeRunnerGroupClient{getRunnerGroupByName: func(context.Context, string) (*upstream.RunnerGroup, error) {
				return nil, errors.New("group failed")
			}},
		}}, nil
	}
	daemon := clusteredDaemon{
		config: clusteredConfig(t), credentials: testAppCredentials(t), logger: slog.Default(), loadConfig: daemonUnderTest.loadConfig, newDiscoverer: daemonUnderTest.newDiscoverer,
		newGitHubClient: daemonUnderTest.newGitHubClient, providerHealthy: daemonUnderTest.providerHealthy,
		buildReconciler: daemonUnderTest.buildReconciler, notifier: daemonUnderTest.notifier,
		nodes: state, assignments: state, tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
	}
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
	if _, err := leader.startSession(context.Background(), githubapp.Repository{URL: "repo"}, cluster.ScaleSetStatus{
		Name: "linux", RunnerGroup: "Default", Compatible: true,
	}); err == nil {
		t.Fatal("expected session failure")
	}
}

func activeClusteredDaemon(t *testing.T, state *fakeClusterState) clusteredDaemon {
	t.Helper()
	boundaries := clusteredDaemonForTest(state)
	return clusteredDaemon{
		config: clusteredConfig(t), credentials: testAppCredentials(t), discoverer: staticDiscoverer{}, logger: slog.Default(),
		loadConfig: boundaries.loadConfig, newDiscoverer: boundaries.newDiscoverer,
		newGitHubClient: boundaries.newGitHubClient, providerHealthy: boundaries.providerHealthy,
		buildReconciler: boundaries.buildReconciler, notifier: boundaries.notifier,
		nodes: state, assignments: state, tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
	}
}

func reconcilerWithProvider(provider scaler.Provider) func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
	return func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "linux", MaxRunners: 10, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}}, nil
	}
}

type postStartInspectionFailureProvider struct {
	started bool
	failure error
}

func (provider *postStartInspectionFailureProvider) List(context.Context, string) ([]scaler.RunnerInstance, error) {
	if provider.started {
		return nil, provider.failure
	}
	return nil, nil
}

func (provider *postStartInspectionFailureProvider) Start(context.Context, scaler.RunnerStartRequest) error {
	provider.started = true
	return nil
}

func (*postStartInspectionFailureProvider) Destroy(context.Context, scaler.RunnerInstance) error {
	return nil
}

func withPostgreSQLDSN(configured config.Config, dsn string) config.Config {
	configured.State.Postgres = &postgresconfig.Settings{DSN: dsn}
	return configured
}

func withPrivateKey(configured config.Config, path string) config.Config {
	configured.GitHub.App.PrivateKeyFile = path
	return configured
}

func clusterMessage(available *upstream.JobAvailable) *upstream.RunnerScaleSetMessage {
	return &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{}, JobAvailableMessages: []*upstream.JobAvailable{available},
	}
}
