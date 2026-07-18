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

func TestRunClusteredDaemonPublishesStartupAndShutdown(t *testing.T) {
	configured := clusteredConfig(t)
	state := &fakeClusterState{}
	ctx, cancel := context.WithCancel(context.Background())
	daemonRuntime := clusteredRuntime(state)
	daemonRuntime.Notifier = cancellingNotifier{cancel: cancel}

	if err := runClusteredDaemon(ctx, Options{}, daemonRuntime, configured); err != nil {
		t.Fatal(err)
	}
	if state.migrations != 1 || len(state.publications) != 2 {
		t.Fatalf("migrations=%d publications=%#v", state.migrations, state.publications)
	}
	if !state.publications[0].healthy || state.publications[1].healthy {
		t.Fatalf("startup and shutdown health was not published: %#v", state.publications)
	}
	if state.publications[0].nodeID != "spock" || state.publications[1].nodeID != "spock" {
		t.Fatalf("operating-system hostname was not used as node identity: %#v", state.publications)
	}
	contribution := state.publications[0].contributions[0]
	if contribution.ScaleSet != "linux" || contribution.MaxRunners != 10 || !contribution.Healthy {
		t.Fatalf("host-local contribution was not published: %#v", contribution)
	}
}

func TestRunClusteredDaemonReconcilesImmediatelyWhenElected(t *testing.T) {
	configured := clusteredConfig(t)
	scaleSetReads := 0
	state := &fakeClusterState{
		leases:      []cluster.Lease{&fakeClusterLease{alive: true}},
		statuses:    []cluster.ScaleSetStatus{{Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: false}},
		onScaleSets: func() { scaleSetReads++ },
	}
	ctx, cancel := context.WithCancel(context.Background())
	daemonRuntime := clusteredRuntime(state)
	daemonRuntime.Notifier = cancellingNotifier{cancel: cancel}
	if err := runClusteredDaemon(ctx, Options{}, daemonRuntime, configured); err != nil {
		t.Fatal(err)
	}
	if scaleSetReads != 1 {
		t.Fatalf("startup reconciliation read scale sets %d times", scaleSetReads)
	}
}

func TestLeadershipContenderDeliveryRetryAndShutdown(t *testing.T) {
	t.Run("retry then acquire", func(t *testing.T) {
		attempts := 0
		state := &fakeClusterState{waitForLeader: func(context.Context) (*cluster.LeaderLease, error) {
			attempts++
			if attempts == 1 {
				return nil, errors.New("temporary failure")
			}
			return &cluster.LeaderLease{}, nil
		}}
		results := make(chan leadershipResult, 1)
		done := make(chan struct{})
		daemon := clusteredDaemon{store: state, logger: slog.Default()}
		daemon.contendForLeadership(context.Background(), results, done)
		if result := <-results; result.lease == nil || attempts != 2 {
			t.Fatalf("result=%#v attempts=%d", result, attempts)
		}
	})

	t.Run("canceled contender", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		done := make(chan struct{})
		daemon := clusteredDaemon{store: &fakeClusterState{}, logger: slog.Default()}
		daemon.contendForLeadership(ctx, make(chan leadershipResult, 1), done)
	})

	t.Run("delivery closes canceled lease", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		lease := &fakeClusterLease{closeErr: errors.New("close failed")}
		daemon := clusteredDaemon{logger: slog.Default()}
		daemon.deliverLeadership(ctx, make(chan leadershipResult), lease)
		if !lease.closed {
			t.Fatal("canceled delivery did not close lease")
		}
	})

	t.Run("start once and close unclaimed lease", func(t *testing.T) {
		lease := &fakeClusterLease{closeErr: errors.New("close failed")}
		done := make(chan struct{})
		close(done)
		results := make(chan leadershipResult, 1)
		results <- leadershipResult{lease: lease}
		daemon := clusteredDaemon{
			logger: slog.Default(), leadershipResults: results,
			stopContender: func() {}, contenderDone: done,
		}
		daemon.startContender(context.Background())
		daemon.stopLeadershipContender(context.Background())
		if !lease.closed {
			t.Fatal("unclaimed lease was not closed")
		}
		daemon.stopLeadershipContender(context.Background())
	})

	t.Run("retry cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if waitForLeadershipRetry(ctx) {
			t.Fatal("canceled retry waited")
		}
	})

	t.Run("contender cancellation during retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		state := &fakeClusterState{waitForLeader: func(context.Context) (*cluster.LeaderLease, error) {
			return nil, errors.New("temporary failure")
		}}
		time.AfterFunc(10*time.Millisecond, cancel)
		done := make(chan struct{})
		daemon := clusteredDaemon{store: state, logger: slog.Default()}
		daemon.contendForLeadership(ctx, make(chan leadershipResult, 1), done)
	})
}

func TestClusteredDaemonPublishesProviderHealth(t *testing.T) {
	configured := clusteredConfig(t)
	configured.ScaleSets = append(configured.ScaleSets, config.ScaleSetConfig{
		Name: "unhealthy", RunnerGroup: "Default", MaxRunners: 2,
		Docker: &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
	})
	state := &fakeClusterState{}
	daemon := clusteredDaemon{
		nodeID: "spock",
		config: configured, logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), store: state,
		runtime: clusteredRuntime(state),
	}
	daemon.runtime.ProviderHealthy = func(_ context.Context, scaleSet config.ScaleSetConfig) error {
		if scaleSet.Name == "unhealthy" {
			return errors.New("provider unavailable")
		}
		return nil
	}
	if err := daemon.publish(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if len(state.publications) != 1 || state.publications[0].nodeID != "spock" ||
		!state.publications[0].contributions[0].Healthy || state.publications[0].contributions[1].Healthy {
		t.Fatalf("provider health was not reflected in contributions: %#v", state.publications)
	}
}

func TestClusteredExecutorUsesOnlyLocalScaleSets(t *testing.T) {
	configured := clusteredConfig(t)
	state := &fakeClusterState{readyAssignments: []*cluster.Assignment{
		{ID: 1, NodeID: "spock", ScaleSet: "remote-only", Repository: "repo", RequestID: 1},
		{ID: 2, NodeID: "spock", ScaleSet: "linux", Repository: "repo", RequestID: 2, InstallationID: 3, GitHubScaleSetID: 4},
	}}
	provider := &fakeScalerProvider{trackStarted: true}
	daemonRuntime := clusteredRuntime(state)
	daemonRuntime.BuildReconciler = func(
		context.Context, string, config.ScaleSetConfig, scaleSetClient, int,
	) (scaler.Reconciler, error) {
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "linux", MaxRunners: 10, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}}, nil
	}
	daemon := clusteredDaemon{
		nodeID: "spock",
		config: configured, credentials: testAppCredentials(t), logger: slog.Default(), runtime: daemonRuntime, store: state,
	}
	daemon.executeAssignments(context.Background())
	if len(state.released) != 1 || state.released[0] != 1 {
		t.Fatalf("remote-only assignment was not released: %#v", state.released)
	}
	if len(state.started) != 1 || state.started[0] != 2 || len(provider.started) != 1 {
		t.Fatalf("local assignment was not executed: started=%#v provider=%#v", state.started, provider.started)
	}
	for _, nodeID := range state.claimedNodeIDs {
		if nodeID != "spock" {
			t.Fatalf("assignment was claimed for node %q", nodeID)
		}
	}
}

func TestClusterLeaderManagesRemoteScaleSetAndRetainsItDuringFailover(t *testing.T) {
	configured := clusteredConfig(t)
	state := &fakeClusterState{statuses: []cluster.ScaleSetStatus{{
		Name: "remote-only", RunnerGroup: "Default", EffectiveCapacity: 12, Compatible: true,
	}}}
	lease := &fakeClusterLease{alive: true}
	state.leases = []cluster.Lease{lease}
	messageClient := &blockingMessageClient{entered: make(chan struct{}), exited: make(chan struct{})}
	githubClient := &fakeScaleSetClient{
		runnerGroup: &upstream.RunnerGroup{ID: 1}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 20},
		messageClient: scaleSetMessageClient{Client: messageClient},
	}
	daemonRuntime := clusteredRuntime(state)
	daemonRuntime.NewGitHubClient = func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) { return githubClient, nil }
	daemon := clusteredDaemon{
		config: configured, credentials: testAppCredentials(t),
		discoverer: staticDiscoverer{result: githubapp.DiscoveryResult{Repositories: []githubapp.Repository{{URL: "repo", InstallationID: 7}}}},
		logger:     slog.Default(), runtime: daemonRuntime, store: state,
	}
	daemon.tryLeadership(context.Background())
	daemon.leader.reconcile(context.Background())
	if daemon.leader == nil || len(daemon.leader.managers) != 1 {
		t.Fatalf("leader did not manage remote-only scale set: %#v", daemon.leader)
	}
	manager := daemon.leader.managers[clusterManagerKey{repository: "repo", scaleSet: "remote-only"}]
	if manager == nil || manager.capacity != 12 {
		t.Fatalf("aggregate capacity was not used by leader: %#v", manager)
	}
	lease.alive = false
	ctx, cancel := context.WithCancel(context.Background())
	daemon.heartbeat(ctx)
	cancel()
	daemon.stopLeadershipContender(ctx)
	if len(githubClient.deletedScaleSetIDs) != 0 {
		t.Fatalf("leader failover deleted a declared scale set: %#v", githubClient.deletedScaleSetIDs)
	}
	if !lease.closed {
		t.Fatal("leader failover did not close its lease")
	}
}

func TestClusterLeaderRejectsConflictsAndDeletesFinalWithdrawal(t *testing.T) {
	configured := clusteredConfig(t)
	state := &fakeClusterState{statuses: []cluster.ScaleSetStatus{{Name: "linux", Compatible: false}}}
	logs := &bytes.Buffer{}
	daemon := clusteredDaemon{
		config: configured, credentials: testAppCredentials(t),
		discoverer: staticDiscoverer{result: githubapp.DiscoveryResult{Repositories: []githubapp.Repository{{URL: "repo", InstallationID: 7}}}},
		logger:     slog.New(slog.NewTextHandler(logs, nil)), runtime: clusteredRuntime(state), store: state,
	}
	leader := newClusterLeader(&daemon, &fakeClusterLease{alive: true})
	leader.reconcile(context.Background())
	if len(leader.managers) != 0 || !strings.Contains(logs.String(), "conflicting clustered scale-set declarations") {
		t.Fatalf("conflict was not rejected clearly: managers=%#v logs=%q", leader.managers, logs.String())
	}

	messageClient := &fakeMessageClient{}
	githubClient := &fakeScaleSetClient{messageClient: scaleSetMessageClient{Client: messageClient}}
	manager := &clusterManager{
		key: clusterManagerKey{repository: "repo", scaleSet: "linux"}, client: githubClient,
		messageClient: scaleSetMessageClient{Client: messageClient}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 20},
		stopPolling: func() {}, pollerDone: closedChannel(),
	}
	leader.managers[manager.key] = manager
	state.statuses = nil
	leader.reconcile(context.Background())
	if len(githubClient.deletedScaleSetIDs) != 1 || githubClient.deletedScaleSetIDs[0] != 20 {
		t.Fatalf("final explicit withdrawal did not delete scale set: %#v", githubClient.deletedScaleSetIDs)
	}
}

func TestClusterMessageHandlerPersistsDemandBeforeScheduling(t *testing.T) {
	now := time.Now()
	state := &fakeClusterState{assignments: []*cluster.Assignment{{
		ID: 8, NodeID: "spock", ScaleSet: "linux", Repository: "repo", RequestID: 9,
	}}}
	messageClient := &fakeMessageClient{}
	manager := &clusterManager{
		key: clusterManagerKey{repository: "repo", scaleSet: "linux"}, installationID: 3,
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 4}, messageClient: scaleSetMessageClient{Client: messageClient},
	}
	scheduler := &clusterScheduler{store: state, managers: map[clusterManagerKey]*clusterManager{manager.key: manager}}
	handler := clusterMessageHandler{store: state, scheduler: scheduler, manager: manager}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 9, QueueTime: now}}},
		JobStartedMessages:   []*upstream.JobStarted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 7}}},
		JobCompletedMessages: []*upstream.JobCompleted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 6}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(state.demand) != 1 || len(state.removedDemand) != 2 || len(state.ready) != 1 || len(messageClient.acquiredIDs) != 1 {
		t.Fatalf("cluster message was not applied atomically before scheduling: %#v", state)
	}
}

func clusteredConfig(t *testing.T) config.Config {
	t.Helper()
	configured := runnableConfig(t)
	configured.State.Postgres = &config.PostgreSQLStateConfig{DSN: "postgres://unused", Role: "github_scaler"}
	configured.ScaleSets[0].RunnerGroup = "Default"
	configured.ScaleSets[0].MaxRunners = 10
	return configured
}

func clusteredRuntime(state clusterState) runtime {
	return withRuntimeDefaults(runtime{
		Hostname:      func() (string, error) { return "spock", nil },
		NewLogger:     func(string, string) *slog.Logger { return slog.Default() },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return staticDiscoverer{}, nil },
		NewGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		OpenClusterStore: func(config.PostgreSQLStateConfig) (clusterState, error) { return state, nil },
	})
}

type clusterPublication struct {
	nodeID        string
	healthy       bool
	contributions []cluster.Contribution
}

type removedClusterDemand struct {
	scaleSet   string
	repository string
	requestID  int64
}

type fakeClusterState struct {
	migrations         int
	publications       []clusterPublication
	statuses           []cluster.ScaleSetStatus
	demand             []cluster.Demand
	removedDemand      []removedClusterDemand
	assignments        []*cluster.Assignment
	readyAssignments   []*cluster.Assignment
	claimedNodeIDs     []string
	ready              []int64
	retried            []int64
	started            []int64
	released           []int64
	leases             []cluster.Lease
	tryLeaderErr       error
	migrateErr         error
	migrationStatusErr error
	publishErr         error
	statusesErr        error
	enqueueErr         error
	assignErr          error
	readyErr           error
	retryErr           error
	startedErr         error
	claimErr           error
	removeErr          error
	releaseErr         error
	onPublish          func()
	onTryLeader        func()
	onScaleSets        func()
	onClaimReady       func()
	waitForLeader      func(context.Context) (*cluster.LeaderLease, error)
}

func (state *fakeClusterState) Migrate(context.Context) error {
	state.migrations++
	return state.migrateErr
}

func (state *fakeClusterState) MigrationStatus(context.Context) (cluster.MigrationStatus, error) {
	return cluster.MigrationStatus{Target: 1}, state.migrationStatusErr
}

func (state *fakeClusterState) Publish(
	_ context.Context,
	nodeID string,
	healthy bool,
	contributions []cluster.Contribution,
	_ time.Time,
) error {
	state.publications = append(state.publications, clusterPublication{
		nodeID: nodeID, healthy: healthy, contributions: append([]cluster.Contribution(nil), contributions...),
	})
	if state.onPublish != nil {
		state.onPublish()
	}
	return state.publishErr
}

func (state *fakeClusterState) ScaleSets(context.Context, time.Time, time.Duration) ([]cluster.ScaleSetStatus, error) {
	if state.onScaleSets != nil {
		state.onScaleSets()
	}
	return state.statuses, state.statusesErr
}

func (state *fakeClusterState) Enqueue(_ context.Context, demand cluster.Demand) error {
	state.demand = append(state.demand, demand)
	return state.enqueueErr
}

func (state *fakeClusterState) AssignOldest(context.Context, string, time.Time, time.Duration) (*cluster.Assignment, error) {
	if state.assignErr != nil {
		return nil, state.assignErr
	}
	if len(state.assignments) == 0 {
		return nil, cluster.ErrNoAssignment
	}
	assignment := state.assignments[0]
	state.assignments = state.assignments[1:]
	return assignment, nil
}

func (state *fakeClusterState) Ready(_ context.Context, assignmentID int64, _ int64, _ int) error {
	state.ready = append(state.ready, assignmentID)
	return state.readyErr
}

func (state *fakeClusterState) Retry(_ context.Context, assignmentID int64) error {
	state.retried = append(state.retried, assignmentID)
	return state.retryErr
}

func (state *fakeClusterState) Started(_ context.Context, assignmentID int64) error {
	state.started = append(state.started, assignmentID)
	return state.startedErr
}

func (state *fakeClusterState) ClaimReady(_ context.Context, nodeID string) (*cluster.Assignment, error) {
	state.claimedNodeIDs = append(state.claimedNodeIDs, nodeID)
	if state.onClaimReady != nil {
		state.onClaimReady()
	}
	if state.claimErr != nil {
		return nil, state.claimErr
	}
	if len(state.readyAssignments) == 0 {
		return nil, cluster.ErrNoReadyAssignment
	}
	assignment := state.readyAssignments[0]
	state.readyAssignments = state.readyAssignments[1:]
	return assignment, nil
}

func (state *fakeClusterState) RemoveDemand(_ context.Context, scaleSet string, repository string, requestID int64) error {
	state.removedDemand = append(state.removedDemand, removedClusterDemand{scaleSet: scaleSet, repository: repository, requestID: requestID})
	return state.removeErr
}

func (state *fakeClusterState) Release(_ context.Context, assignmentID int64) error {
	state.released = append(state.released, assignmentID)
	return state.releaseErr
}

func (state *fakeClusterState) TryLeader(context.Context) (cluster.Lease, error) { //nolint:ireturn // The fake mirrors the production state seam.
	if state.onTryLeader != nil {
		state.onTryLeader()
	}
	if state.tryLeaderErr != nil {
		return nil, state.tryLeaderErr
	}
	if len(state.leases) == 0 {
		return nil, cluster.ErrNotLeader
	}
	lease := state.leases[0]
	state.leases = state.leases[1:]
	return lease, nil
}

func (state *fakeClusterState) WaitForLeader(ctx context.Context) (*cluster.LeaderLease, error) {
	if state.waitForLeader != nil {
		return state.waitForLeader(ctx)
	}
	<-ctx.Done()
	return nil, fmt.Errorf("wait for leader: %w", ctx.Err())
}

type fakeClusterLease struct {
	alive    bool
	closed   bool
	closeErr error
}

func (lease *fakeClusterLease) Alive(context.Context) bool { return lease.alive }

func (lease *fakeClusterLease) Close(context.Context) error {
	lease.closed = true
	return lease.closeErr
}

func closedChannel() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
