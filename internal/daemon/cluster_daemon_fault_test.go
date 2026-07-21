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
	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	"github.com/meza/github-scaler/internal/scaler"
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
			daemon.openStore = func(config.PostgreSQLStateConfig) (clusterStoreConnection, error) {
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

func TestClusterEventLoopDispatchesIndependentWorkers(t *testing.T) {
	configured := clusteredConfig(t)
	tests := []struct {
		name      string
		events    func() clusterEvents
		configure func(*fakeClusterState, context.CancelFunc)
	}{
		{name: "heartbeat", events: func() clusterEvents {
			channel := make(chan time.Time, 1)
			channel <- time.Now()
			return clusterEvents{heartbeat: channel}
		}, configure: func(state *fakeClusterState, cancel context.CancelFunc) { state.onPublish = cancel }},
		{name: "leadership", events: func() clusterEvents {
			channel := make(chan leadershipResult, 1)
			channel <- leadershipResult{lease: &fakeClusterLease{alive: true}}
			return clusterEvents{leadership: channel}
		}, configure: func(state *fakeClusterState, cancel context.CancelFunc) { state.onScaleSets = cancel }},
		{name: "executor", events: func() clusterEvents {
			channel := make(chan time.Time, 1)
			channel <- time.Now()
			return clusterEvents{executor: channel}
		}, configure: func(state *fakeClusterState, cancel context.CancelFunc) { state.onClaimReady = cancel }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := &fakeClusterState{}
			daemon := clusteredDaemon{
				config: configured, discoverer: staticDiscoverer{}, logger: slog.Default(),
				loadConfig: clusteredDaemonForTest(state).loadConfig, newDiscoverer: clusteredDaemonForTest(state).newDiscoverer,
				newGitHubClient: clusteredDaemonForTest(state).newGitHubClient, providerHealthy: clusteredDaemonForTest(state).providerHealthy,
				buildReconciler: clusteredDaemonForTest(state).buildReconciler, notifier: clusteredDaemonForTest(state).notifier, nodes: state, assignments: state, tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
			}
			ctx, cancel := context.WithCancel(context.Background())
			test.configure(state, cancel)
			if err := daemon.runEvents(ctx, Options{}, test.events()); err != nil {
				t.Fatal(err)
			}
		})
	}
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
	healthCheckStarted := make(chan struct{})
	releaseHealthCheck := make(chan struct{})
	assignmentClaimed := make(chan struct{})
	state := &fakeClusterState{onClaimReady: func() { close(assignmentClaimed) }}
	daemonUnderTest := clusteredDaemonForTest(state)
	daemonUnderTest.providerHealthy = func(ctx context.Context, _ config.ScaleSetConfig) error {
		close(healthCheckStarted)
		select {
		case <-releaseHealthCheck:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	daemon := clusteredDaemon{
		config: clusteredConfig(t), discoverer: staticDiscoverer{}, logger: slog.Default(),
		loadConfig: daemonUnderTest.loadConfig, newDiscoverer: daemonUnderTest.newDiscoverer,
		newGitHubClient: daemonUnderTest.newGitHubClient, providerHealthy: daemonUnderTest.providerHealthy,
		buildReconciler: daemonUnderTest.buildReconciler, notifier: daemonUnderTest.notifier, nodes: state, assignments: state, tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
	}
	heartbeatEvents := make(chan time.Time, 1)
	executorEvents := make(chan time.Time, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- daemon.runEvents(ctx, Options{}, clusterEvents{heartbeat: heartbeatEvents, executor: executorEvents})
	}()
	heartbeatEvents <- time.Now()
	<-healthCheckStarted
	executorEvents <- time.Now()
	select {
	case <-assignmentClaimed:
	case <-time.After(time.Second):
		t.Fatal("assignment execution was blocked by provider health")
	}
	close(releaseHealthCheck)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBlockedReconciliationStopsWithDaemonContext(t *testing.T) {
	reconciliationStarted := make(chan struct{})
	state := &fakeClusterState{}
	daemon := clusteredDaemon{
		config: clusteredConfig(t), logger: slog.Default(), loadConfig: clusteredDaemonForTest(state).loadConfig, newDiscoverer: clusteredDaemonForTest(state).newDiscoverer,
		newGitHubClient: clusteredDaemonForTest(state).newGitHubClient, providerHealthy: clusteredDaemonForTest(state).providerHealthy,
		buildReconciler: clusteredDaemonForTest(state).buildReconciler, notifier: clusteredDaemonForTest(state).notifier,
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
	daemon := clusteredDaemon{
		nodeID: "spock", config: clusteredConfig(t), discoverer: staticDiscoverer{},
		logger: newClusterLogSnapshotLogger(logs), loadConfig: clusteredDaemonForTest(state).loadConfig, newDiscoverer: clusteredDaemonForTest(state).newDiscoverer,
		newGitHubClient: clusteredDaemonForTest(state).newGitHubClient, providerHealthy: clusteredDaemonForTest(state).providerHealthy,
		buildReconciler: clusteredDaemonForTest(state).buildReconciler, notifier: clusteredDaemonForTest(state).notifier,
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

func TestClusterReloadValidatesAuthorityAndPublishesReplacement(t *testing.T) {
	configured := clusteredConfig(t)
	failure := errors.New("failed")
	tests := []struct {
		name          string
		loaded        config.Config
		loadErr       error
		discovererErr error
		publishErr    error
		wantErr       bool
	}{
		{name: "load", loadErr: failure, wantErr: true},
		{name: "backend", loaded: runnableConfig(t), wantErr: true},
		{name: "connection", loaded: withPostgreSQLDSN(configured, "postgres://replacement"), wantErr: true},
		{name: "credentials", loaded: withPrivateKey(configured, "missing-key"), wantErr: true},
		{name: "discoverer", loaded: configured, discovererErr: failure, wantErr: true},
		{name: "publish", loaded: configured, publishErr: failure, wantErr: true},
		{name: "success", loaded: configured},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := &fakeClusterState{publishErr: test.publishErr}
			daemonUnderTest := clusteredDaemonForTest(state)
			daemonUnderTest.loadConfig = func(string) (config.Config, error) { return test.loaded, test.loadErr }
			daemonUnderTest.newDiscoverer = func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
				return staticDiscoverer{}, test.discovererErr
			}
			daemon := clusteredDaemon{
				config: configured, credentials: testAppCredentials(t), logger: slog.Default(), loadConfig: daemonUnderTest.loadConfig, newDiscoverer: daemonUnderTest.newDiscoverer,
				newGitHubClient: daemonUnderTest.newGitHubClient, providerHealthy: daemonUnderTest.providerHealthy,
				buildReconciler: daemonUnderTest.buildReconciler, notifier: daemonUnderTest.notifier,
				nodes: state, assignments: state, tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
			}
			err := daemon.reload(context.Background(), "config.yaml")
			if test.wantErr != (err != nil) {
				t.Fatalf("reload error = %v, wantErr=%t", err, test.wantErr)
			}
		})
	}
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
	daemon.newGitHubClient = func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errors.New("failed")
	}
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	leader.startMissingSessions(context.Background(), map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{
		key: {Name: "linux", RunnerGroup: "Default", Compatible: true, EffectiveCapacity: 1},
	}, map[string]githubapp.Repository{"repo": {URL: "repo"}})
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
		"started": {
			state: &fakeClusterState{removeErr: failure},
			message: &upstream.RunnerScaleSetMessage{Statistics: &upstream.RunnerScaleSetStatistic{},
				JobStartedMessages: []*upstream.JobStarted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 1}}}},
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
	daemonUnderTest.newGitHubClient = func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
		return &fakeScaleSetClient{runnerGroupErr: errors.New("group failed")}, nil
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

func reconcilerWithProvider(provider scaler.Provider) func(context.Context, string, config.ScaleSetConfig, scaleSetClient, int) (scaler.Reconciler, error) {
	return func(context.Context, string, config.ScaleSetConfig, scaleSetClient, int) (scaler.Reconciler, error) {
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
	configured.State.Postgres = &config.PostgreSQLStateConfig{DSN: dsn}
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
