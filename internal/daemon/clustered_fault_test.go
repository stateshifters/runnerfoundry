package daemon

import (
	"bytes"
	"context"
	"errors"
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

func TestClusteredStartupReportsEveryPreflightFailure(t *testing.T) {
	configured := clusteredConfig(t)
	failure := errors.New("failed")
	tests := []struct {
		name      string
		configure func(*config.Config, *runtime, *fakeClusterState)
	}{
		{name: "missing postgres", configure: func(configured *config.Config, _ *runtime, _ *fakeClusterState) { configured.State.Postgres = nil }},
		{name: "hostname dependency", configure: func(_ *config.Config, daemonRuntime *runtime, _ *fakeClusterState) {
			daemonRuntime.Hostname = nil
		}},
		{name: "hostname lookup", configure: func(_ *config.Config, daemonRuntime *runtime, _ *fakeClusterState) {
			daemonRuntime.Hostname = func() (string, error) { return "", failure }
		}},
		{name: "empty hostname", configure: func(_ *config.Config, daemonRuntime *runtime, _ *fakeClusterState) {
			daemonRuntime.Hostname = func() (string, error) { return "  ", nil }
		}},
		{name: "open state", configure: func(_ *config.Config, daemonRuntime *runtime, _ *fakeClusterState) {
			daemonRuntime.OpenClusterStore = func(config.PostgreSQLStateConfig) (clusterState, error) { return nil, failure }
		}},
		{name: "migrate", configure: func(_ *config.Config, _ *runtime, state *fakeClusterState) { state.migrateErr = failure }},
		{name: "migration status", configure: func(_ *config.Config, _ *runtime, state *fakeClusterState) { state.migrationStatusErr = failure }},
		{name: "credentials", configure: func(configured *config.Config, _ *runtime, _ *fakeClusterState) {
			configured.GitHub.App.PrivateKeyFile = "missing-private-key"
		}},
		{name: "discoverer", configure: func(_ *config.Config, daemonRuntime *runtime, _ *fakeClusterState) {
			daemonRuntime.NewDiscoverer = func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return nil, failure }
		}},
		{name: "publish", configure: func(_ *config.Config, _ *runtime, state *fakeClusterState) { state.publishErr = failure }},
		{name: "notifier", configure: func(_ *config.Config, daemonRuntime *runtime, _ *fakeClusterState) {
			daemonRuntime.Notifier = failingNotifier{readyErr: failure}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := &fakeClusterState{}
			daemonRuntime := clusteredRuntime(state)
			candidate := configured
			test.configure(&candidate, &daemonRuntime, state)
			if err := runClusteredDaemon(context.Background(), Options{}, daemonRuntime, candidate); err == nil {
				t.Fatal("expected clustered startup failure")
			}
		})
	}
}

func TestRunWithRuntimeRoutesBothStateBackends(t *testing.T) {
	failure := errors.New("load failed")
	if err := runWithRuntime(context.Background(), Options{}, runtime{LoadConfig: func(string) (config.Config, error) {
		return config.Config{}, failure
	}}); !errors.Is(err, failure) {
		t.Fatalf("load failure = %v", err)
	}

	clustered := clusteredConfig(t)
	state := &fakeClusterState{migrateErr: failure}
	daemonRuntime := clusteredRuntime(state)
	daemonRuntime.LoadConfig = func(string) (config.Config, error) { return clustered, nil }
	if err := runWithRuntime(context.Background(), Options{}, daemonRuntime); err == nil {
		t.Fatal("expected clustered route to reach migration")
	}

	standalone := runnableConfig(t)
	standalone.State.Path = "state.db"
	ctx, cancel := context.WithCancel(context.Background())
	daemonRuntime = withRuntimeDefaults(runtime{
		LoadConfig:    func(string) (config.Config, error) { return standalone, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return staticDiscoverer{}, nil },
		NewLogger:     func(string, string) *slog.Logger { return slog.Default() },
		Notifier:      cancellingNotifier{cancel: cancel},
	})
	if err := runWithRuntime(ctx, Options{}, daemonRuntime); err != nil {
		t.Fatalf("standalone route failed: %v", err)
	}
}

func TestClusterEventLoopHandlesEveryEvent(t *testing.T) {
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
				runtime: clusteredRuntime(state), store: state,
			}
			ctx, cancel := context.WithCancel(context.Background())
			test.configure(state, cancel)
			if err := daemon.runEvents(ctx, Options{}, test.events()); err != nil {
				t.Fatal(err)
			}
		})
	}

	reloadRequests := make(chan struct{}, 1)
	reloadRequests <- struct{}{}
	daemon := clusteredDaemon{
		config: configured, logger: slog.Default(), store: &fakeClusterState{},
		runtime: runtime{LoadConfig: func(string) (config.Config, error) { return config.Config{}, errors.New("reload failed") }},
	}
	if err := daemon.runEvents(context.Background(), Options{ReloadRequests: reloadRequests}, clusterEvents{}); err == nil {
		t.Fatal("expected reload event failure")
	}
}

func TestClusterHeartbeatAndLeadershipFailuresRemainRetryable(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{publishErr: errors.New("heartbeat failed"), tryLeaderErr: errors.New("election failed")}
	daemon := clusteredDaemon{
		config: clusteredConfig(t), discoverer: staticDiscoverer{err: errors.New("discovery failed")},
		logger: slog.New(slog.NewTextHandler(logs, nil)), runtime: clusteredRuntime(state), store: state,
	}
	daemon.leader = newClusterLeader(&daemon, &fakeClusterLease{alive: true})
	daemon.heartbeat(context.Background())
	daemon.leader = nil
	daemon.tryLeadership(context.Background())
	if !strings.Contains(logs.String(), "cluster heartbeat failed") || !strings.Contains(logs.String(), "leader election failed") {
		t.Fatalf("retryable failures were not logged: %q", logs.String())
	}

	lease := &fakeClusterLease{alive: true}
	daemon.leader = newClusterLeader(&daemon, lease)
	daemon.heartbeat(context.Background())
	if lease.closed {
		t.Fatal("live leader lease was replaced")
	}
}

func TestClusterExecutorReportsAndRetriesFailures(t *testing.T) {
	failure := errors.New("failed")
	configured := clusteredConfig(t)
	localAssignment := &cluster.Assignment{ID: 1, ScaleSet: "linux", Repository: "repo", RequestID: 2}
	tests := []struct {
		name      string
		state     *fakeClusterState
		configure func(*runtime)
	}{
		{name: "claim", state: &fakeClusterState{claimErr: failure}},
		{name: "release removed", state: &fakeClusterState{readyAssignments: []*cluster.Assignment{{ID: 1, ScaleSet: "remote"}}, releaseErr: failure}},
		{name: "client", state: &fakeClusterState{readyAssignments: []*cluster.Assignment{localAssignment}}, configure: func(daemonRuntime *runtime) {
			daemonRuntime.NewGitHubClient = func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) { return nil, failure }
		}},
		{name: "build", state: &fakeClusterState{readyAssignments: []*cluster.Assignment{localAssignment}}, configure: func(daemonRuntime *runtime) {
			daemonRuntime.BuildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetClient, int) (scaler.Reconciler, error) {
				return scaler.Reconciler{}, failure
			}
		}},
		{name: "start", state: &fakeClusterState{readyAssignments: []*cluster.Assignment{localAssignment}}, configure: func(daemonRuntime *runtime) {
			daemonRuntime.BuildReconciler = reconcilerWithProvider(&fakeScalerProvider{listErr: failure})
		}},
		{name: "missing runner", state: &fakeClusterState{readyAssignments: []*cluster.Assignment{localAssignment}}, configure: func(daemonRuntime *runtime) {
			daemonRuntime.BuildReconciler = reconcilerWithProvider(&fakeScalerProvider{})
		}},
		{name: "record started", state: &fakeClusterState{readyAssignments: []*cluster.Assignment{localAssignment}, startedErr: failure}, configure: func(daemonRuntime *runtime) {
			daemonRuntime.BuildReconciler = reconcilerWithProvider(&fakeScalerProvider{trackStarted: true})
		}},
		{name: "retry", state: &fakeClusterState{readyAssignments: []*cluster.Assignment{localAssignment}, retryErr: failure}, configure: func(daemonRuntime *runtime) {
			daemonRuntime.BuildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetClient, int) (scaler.Reconciler, error) {
				return scaler.Reconciler{}, failure
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			daemonRuntime := clusteredRuntime(test.state)
			if test.configure != nil {
				test.configure(&daemonRuntime)
			}
			daemon := clusteredDaemon{
				config: configured, credentials: testAppCredentials(t), logger: slog.Default(), runtime: daemonRuntime, store: test.state,
			}
			daemon.executeNextAssignment(context.Background())
		})
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
			daemonRuntime := clusteredRuntime(state)
			daemonRuntime.LoadConfig = func(string) (config.Config, error) { return test.loaded, test.loadErr }
			daemonRuntime.NewDiscoverer = func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
				return staticDiscoverer{}, test.discovererErr
			}
			daemon := clusteredDaemon{
				config: configured, credentials: testAppCredentials(t), logger: slog.Default(), runtime: daemonRuntime, store: state,
			}
			err := daemon.reload(context.Background(), "config.yaml")
			if test.wantErr != (err != nil) {
				t.Fatalf("reload error = %v, wantErr=%t", err, test.wantErr)
			}
		})
	}
}

func TestClusterLeaderAndSchedulerErrorPaths(t *testing.T) {
	failure := errors.New("failed")
	configured := clusteredConfig(t)
	logs := &bytes.Buffer{}
	state := &fakeClusterState{statusesErr: failure}
	daemon := clusteredDaemon{
		config: configured, discoverer: staticDiscoverer{result: githubapp.DiscoveryResult{}},
		logger: slog.New(slog.NewTextHandler(logs, nil)), runtime: clusteredRuntime(state), store: state,
	}
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
	leader.reconcile(context.Background())
	daemon.discoverer = staticDiscoverer{err: failure}
	leader.reconcile(context.Background())

	daemon.discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{Repositories: []githubapp.Repository{{URL: "repo"}}}}
	state.statusesErr = nil
	state.statuses = []cluster.ScaleSetStatus{{Name: "linux", RunnerGroup: "Default", Compatible: true, EffectiveCapacity: 1}}
	daemon.runtime.NewGitHubClient = func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) { return nil, failure }
	leader.startMissingManagers(context.Background(), map[clusterManagerKey]cluster.ScaleSetStatus{
		{repository: "repo", scaleSet: "linux"}: state.statuses[0],
	}, map[string]githubapp.Repository{"repo": {URL: "repo"}})

	key := clusterManagerKey{repository: "repo", scaleSet: "linux"}
	state.assignments = []*cluster.Assignment{{ID: 1, Repository: "repo", ScaleSet: "linux", RequestID: 2}}
	scheduler := &clusterScheduler{store: state, managers: map[clusterManagerKey]*clusterManager{}}
	if _, err := scheduler.scheduleOne(context.Background(), "linux"); err != nil {
		t.Fatal(err)
	}
	state.assignErr = failure
	if _, err := scheduler.scheduleOne(context.Background(), "linux"); err == nil {
		t.Fatal("expected assignment failure")
	}
	state.assignErr = nil
	state.assignments = []*cluster.Assignment{{ID: 2, Repository: "repo", ScaleSet: "linux", RequestID: 3}}
	state.releaseErr = failure
	if _, err := scheduler.scheduleOne(context.Background(), "linux"); err == nil {
		t.Fatal("expected release failure")
	}
	state.releaseErr = nil
	messageClient := &fakeMessageClient{acquireErr: failure}
	scheduler.managers[key] = &clusterManager{
		key: key, messageClient: scaleSetMessageClient{Client: messageClient}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 1},
	}
	state.assignments = []*cluster.Assignment{{ID: 3, Repository: "repo", ScaleSet: "linux", RequestID: 4}}
	if _, err := scheduler.scheduleOne(context.Background(), "linux"); err == nil {
		t.Fatal("expected acquisition failure")
	}
	state.assignments = []*cluster.Assignment{{ID: 4, Repository: "repo", ScaleSet: "linux", RequestID: 5}}
	state.readyErr = failure
	messageClient.acquireErr = nil
	if _, err := scheduler.scheduleOne(context.Background(), "linux"); err == nil {
		t.Fatal("expected ready failure")
	}
}

func TestClusterMessageValidationAndPersistenceFailures(t *testing.T) {
	failure := errors.New("failed")
	manager := &clusterManager{key: clusterManagerKey{repository: "repo", scaleSet: "linux"}}
	tests := []struct {
		name    string
		state   *fakeClusterState
		message *upstream.RunnerScaleSetMessage
	}{
		{name: "statistics", state: &fakeClusterState{}, message: &upstream.RunnerScaleSetMessage{}},
		{name: "available identity", state: &fakeClusterState{}, message: clusterMessage(&upstream.JobAvailable{})},
		{name: "enqueue", state: &fakeClusterState{enqueueErr: failure}, message: clusterMessage(&upstream.JobAvailable{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 1, QueueTime: time.Now()}})},
		{name: "started", state: &fakeClusterState{removeErr: failure}, message: &upstream.RunnerScaleSetMessage{Statistics: &upstream.RunnerScaleSetStatistic{}, JobStartedMessages: []*upstream.JobStarted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 1}}}}},
		{name: "completed", state: &fakeClusterState{removeErr: failure}, message: &upstream.RunnerScaleSetMessage{Statistics: &upstream.RunnerScaleSetStatistic{}, JobCompletedMessages: []*upstream.JobCompleted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 1}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheduler := &clusterScheduler{store: test.state, managers: map[clusterManagerKey]*clusterManager{}}
			handler := clusterMessageHandler{store: test.state, scheduler: scheduler, manager: manager}
			if err := handler.HandleMessage(context.Background(), test.message); err == nil {
				t.Fatal("expected message handling failure")
			}
		})
	}

	state := &fakeClusterState{}
	handler := clusterMessageHandler{store: state, scheduler: &clusterScheduler{store: state, managers: map[clusterManagerKey]*clusterManager{}}, manager: manager}
	message := &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{}, JobStartedMessages: []*upstream.JobStarted{nil, {}},
		JobCompletedMessages: []*upstream.JobCompleted{nil, {}},
	}
	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
}

func TestProductionClusterRuntimeWiringAndDefaults(t *testing.T) {
	production := productionRuntime()
	state, err := production.OpenClusterStore(config.PostgreSQLStateConfig{Password: "file:///secret-file"})
	if err != nil || state == nil {
		t.Fatalf("production cluster store = %#v, %v", state, err)
	}
	if healthErr := production.ProviderHealthy(context.Background(), config.ScaleSetConfig{Name: "missing"}); healthErr == nil {
		t.Fatal("production provider health did not validate configuration")
	}
	defaults := withRuntimeDefaults(runtime{})
	state, err = defaults.OpenClusterStore(config.PostgreSQLStateConfig{})
	if err != nil || state == nil {
		t.Fatalf("default cluster store = %#v, %v", state, err)
	}
	if err := runDaemon(context.Background(), Options{}, runtime{
		LoadConfig: func(string) (config.Config, error) { return config.Config{}, errors.New("load failed") },
	}); err == nil {
		t.Fatal("expected standalone configuration load failure")
	}
}

func TestRemainingClusterCleanupAndRecoveryBranches(t *testing.T) {
	failure := errors.New("failed")
	configured := clusteredConfig(t)
	state := &fakeClusterState{}
	daemon := clusteredDaemon{
		config: configured, discoverer: staticDiscoverer{}, logger: slog.Default(), runtime: clusteredRuntime(state), store: state,
	}
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
	daemon.leader = leader
	daemon.heartbeat(context.Background())

	daemon.leader = nil
	state.leases = []cluster.Lease{nil}
	daemon.tryLeadership(context.Background())
	if daemon.leader != nil {
		t.Fatal("nil lease unexpectedly elected a leader")
	}

	provider := &sequenceListProvider{listResults: []sequenceListResult{{}, {err: failure}}}
	daemon.runtime.BuildReconciler = reconcilerWithProvider(provider)
	if err := daemon.executeAssignment(context.Background(), &cluster.Assignment{
		ScaleSet: "linux", Repository: "repo", RequestID: 1,
	}); err == nil || !strings.Contains(err.Error(), "verify assigned runner") {
		t.Fatalf("expected runner verification failure, got %v", err)
	}

	lease := &fakeClusterLease{closeErr: failure}
	leader = newClusterLeader(&daemon, lease)
	messageClient := &fakeMessageClient{closeErr: failure}
	githubClient := &fakeScaleSetClient{deleteScaleSetErr: failure}
	manager := &clusterManager{
		key: clusterManagerKey{repository: "repo", scaleSet: "linux"}, capacity: 1,
		client: githubClient, messageClient: scaleSetMessageClient{Client: messageClient},
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 1}, stopPolling: func() {}, pollerDone: closedChannel(),
	}
	leader.managers[manager.key] = manager
	leader.stopOutdatedManagers(context.Background(), map[clusterManagerKey]cluster.ScaleSetStatus{
		manager.key: {Name: "linux", EffectiveCapacity: 1},
	}, []cluster.ScaleSetStatus{{Name: "linux"}})
	if len(leader.managers) != 1 {
		t.Fatal("unchanged manager was restarted")
	}
	leader.startMissingManagers(context.Background(), map[clusterManagerKey]cluster.ScaleSetStatus{
		manager.key: {Name: "linux"},
	}, map[string]githubapp.Repository{"repo": {URL: "repo"}})
	if !hasClusterStatus([]cluster.ScaleSetStatus{{Name: "linux"}}, "linux") {
		t.Fatal("declared scale set was not found")
	}
	leader.stopManager(context.Background(), manager, removeGitHubScaleSet)
	leader.managers = map[clusterManagerKey]*clusterManager{}
	leader.stop(context.Background())
	if !lease.closed {
		t.Fatal("leader lease close was not attempted")
	}

	state.publishErr = failure
	daemon.leader = newClusterLeader(&daemon, &fakeClusterLease{})
	daemon.shutdown(context.Background())

	state.assignments = []*cluster.Assignment{{ID: 1, Repository: "repo", ScaleSet: "linux", RequestID: 1}}
	state.releaseErr = failure
	scheduler := &clusterScheduler{store: state, managers: map[clusterManagerKey]*clusterManager{
		{repository: "repo", scaleSet: "linux"}: {
			key:           clusterManagerKey{repository: "repo", scaleSet: "linux"},
			messageClient: scaleSetMessageClient{Client: &fakeMessageClient{acquireErr: failure}},
		},
	}}
	if _, err := scheduler.scheduleOne(context.Background(), "linux"); err == nil || !strings.Contains(err.Error(), "release unacquired") {
		t.Fatalf("expected joined acquisition and release failure, got %v", err)
	}
}

func TestClusterManagerSessionFailureIsReturned(t *testing.T) {
	state := &fakeClusterState{}
	daemonRuntime := clusteredRuntime(state)
	daemonRuntime.NewGitHubClient = func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
		return &fakeScaleSetClient{runnerGroupErr: errors.New("group failed")}, nil
	}
	daemon := clusteredDaemon{
		config: clusteredConfig(t), credentials: testAppCredentials(t), logger: slog.Default(), runtime: daemonRuntime, store: state,
	}
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
	if _, err := leader.startManager(context.Background(), githubapp.Repository{URL: "repo"}, cluster.ScaleSetStatus{
		Name: "linux", RunnerGroup: "Default", Compatible: true,
	}); err == nil {
		t.Fatal("expected manager session failure")
	}
}

func reconcilerWithProvider(provider scaler.Provider) func(context.Context, string, config.ScaleSetConfig, scaleSetClient, int) (scaler.Reconciler, error) {
	return func(context.Context, string, config.ScaleSetConfig, scaleSetClient, int) (scaler.Reconciler, error) {
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "linux", MaxRunners: 10, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}}, nil
	}
}

type sequenceListResult struct {
	instances []scaler.RunnerInstance
	err       error
}

type sequenceListProvider struct {
	listResults []sequenceListResult
}

func (provider *sequenceListProvider) List(context.Context, string) ([]scaler.RunnerInstance, error) {
	result := provider.listResults[0]
	provider.listResults = provider.listResults[1:]
	return result.instances, result.err
}

func (provider *sequenceListProvider) Start(_ context.Context, request scaler.RunnerStartRequest) error {
	provider.listResults[0].instances = []scaler.RunnerInstance{{RunnerName: request.RunnerName, State: scaler.RunnerStateRunning}}
	return nil
}

func (*sequenceListProvider) Destroy(context.Context, scaler.RunnerInstance) error { return nil }

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
