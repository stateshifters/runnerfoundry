package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	"github.com/meza/github-scaler/internal/scaler"
)

func TestAssignmentExecutionPhasesRetainStructuredLogValues(t *testing.T) {
	if assignmentExecutionPhaseStart != "start" {
		t.Fatalf("start phase = %q, want start", assignmentExecutionPhaseStart)
	}
	if assignmentExecutionPhaseRepair != "repair" {
		t.Fatalf("repair phase = %q, want repair", assignmentExecutionPhaseRepair)
	}
}

func TestClusteredDaemonPublishesProviderHealth(t *testing.T) {
	configured := clusteredConfig(t)
	configured.ScaleSets = append(configured.ScaleSets, config.ScaleSetConfig{
		Name: "unhealthy", RunnerGroup: "Default", MaxRunners: 2,
		Docker: &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
	})
	state := &fakeClusterState{}
	daemonFixture := clusteredDaemonForTest(state)
	daemon := clusteredDaemon{
		nodeID: "spock",
		config: configured, logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		nodes: state, assignments: state, tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
		loadConfig: daemonFixture.loadConfig, newDiscoverer: daemonFixture.newDiscoverer,
		newGitHubClient: daemonFixture.newGitHubClient, providerHealthy: daemonFixture.providerHealthy,
		buildReconciler: daemonFixture.buildReconciler, notifier: daemonFixture.notifier,
	}
	daemon.providerHealthy = func(_ context.Context, scaleSet config.ScaleSetConfig) error {
		if scaleSet.Name == "unhealthy" {
			return errors.New("provider unavailable")
		}
		return nil
	}
	if err := daemon.publish(context.Background(), cluster.NodeHealthy); err != nil {
		t.Fatal(err)
	}
	if len(state.publications) != 1 || state.publications[0].nodeID != "spock" ||
		!state.publications[0].contributions[0].Healthy || state.publications[0].contributions[1].Healthy {
		t.Fatalf("provider health was not reflected in contributions: %#v", state.publications)
	}
}

func TestClusteredExecutorUsesOnlyLocalScaleSets(t *testing.T) {
	daemon, state, provider := clusteredAssignmentExecutorFixture(t)
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

func clusteredAssignmentExecutorFixture(t *testing.T) (*clusteredDaemon, *fakeClusterState, *fakeScalerProvider) {
	t.Helper()
	configured := clusteredConfig(t)
	state := &fakeClusterState{readyAssignments: []*cluster.Assignment{
		{ID: 1, NodeID: "spock", ScaleSet: "remote-only", Repository: "repo", RequestID: 1},
		{ID: 2, NodeID: "spock", ScaleSet: "linux", Repository: "repo", RequestID: 2, InstallationID: 3, GitHubScaleSetID: 4},
	}}
	provider := &fakeScalerProvider{trackStarted: true}
	daemonUnderTest := clusteredDaemonForTest(state)
	daemonUnderTest.buildReconciler = func(
		context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int,
	) (scaler.Reconciler, error) {
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "linux", MaxRunners: 10, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}}, nil
	}
	daemonUnderTest.nodeID = "spock"
	daemonUnderTest.config = configured
	daemonUnderTest.credentials = testAppCredentials(t)
	daemonUnderTest.logger = slog.Default()
	daemonUnderTest.nodes = state
	daemonUnderTest.assignments = state
	daemonUnderTest.tryLeader = fakeTryLeader(state)
	daemonUnderTest.waitForLeader = fakeWaitForLeader(state)
	return daemonUnderTest, state, provider
}

func TestClusterLeaderManagesRemoteScaleSetAtAggregateCapacity(t *testing.T) {
	fixture := newRemoteScaleSetLeaderFixture(t)
	fixture.daemon.tryLeadership(context.Background())
	fixture.daemon.leader.reconcile(context.Background())
	session := fixture.daemon.leader.sessions[repositoryScaleSetSessionKey{repository: "repo", scaleSet: "remote-only"}]
	if session == nil || session.capacity != 12 {
		t.Fatalf("aggregate capacity was not used by leader: %#v", session)
	}
}

func TestClusterLeaderFailoverRetainsDeclaredScaleSetAndClosesLease(t *testing.T) {
	fixture := newRemoteScaleSetLeaderFixture(t)
	fixture.daemon.tryLeadership(context.Background())
	fixture.daemon.leader.reconcile(context.Background())
	fixture.lease.alive = false
	ctx, cancel := context.WithCancel(context.Background())
	fixture.daemon.heartbeat(ctx)
	cancel()
	fixture.daemon.stopLeadershipContender(ctx)
	if len(fixture.deletedScaleSetIDs) != 0 {
		t.Fatalf("leader failover deleted a declared scale set: %#v", fixture.deletedScaleSetIDs)
	}
	if !fixture.lease.closed {
		t.Fatal("leader failover did not close its lease")
	}
}

type remoteScaleSetLeaderFixture struct {
	daemon             *clusteredDaemon
	lease              *fakeClusterLease
	deletedScaleSetIDs []int
}

func newRemoteScaleSetLeaderFixture(t *testing.T) *remoteScaleSetLeaderFixture {
	t.Helper()
	configured := clusteredConfig(t)
	state := &fakeClusterState{statuses: []cluster.ScaleSetStatus{{
		Name: "remote-only", RunnerGroup: "Default", EffectiveCapacity: 12, Compatible: true,
	}}}
	fixture := &remoteScaleSetLeaderFixture{lease: &fakeClusterLease{alive: true}}
	state.leases = []cluster.Lease{fixture.lease}
	githubClient := remoteScaleSetGitHubClient(fixture)
	daemonUnderTest := clusteredDaemonForTest(state)
	daemonUnderTest.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return githubClient, nil
	}
	fixture.daemon = daemonUnderTest
	fixture.daemon.config = configured
	fixture.daemon.credentials = testAppCredentials(t)
	fixture.daemon.discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{
		Repositories: []githubapp.Repository{{URL: "repo", InstallationID: 7}},
	}}
	fixture.daemon.logger = slog.Default()
	fixture.daemon.nodes = state
	fixture.daemon.assignments = state
	fixture.daemon.tryLeader = fakeTryLeader(state)
	fixture.daemon.waitForLeader = fakeWaitForLeader(state)
	return fixture
}

func remoteScaleSetGitHubClient(fixture *remoteScaleSetLeaderFixture) *fakeScaleSetClient {
	messageClient := &blockingMessageClient{entered: make(chan struct{}), exited: make(chan struct{})}
	client := fakeExistingScaleSetClient(
		&upstream.RunnerGroup{ID: 1}, &upstream.RunnerScaleSet{ID: 20},
		scaleSetMessageClient{Client: messageClient},
	)
	client.fakeScaleSetDeletionClient = fakeScaleSetDeletionClient{deleteRunnerScaleSet: func(_ context.Context, scaleSetID int) error {
		fixture.deletedScaleSetIDs = append(fixture.deletedScaleSetIDs, scaleSetID)
		return nil
	}}
	return client
}

func TestClusterLeaderRejectsConflictingDeclarations(t *testing.T) {
	leader, logs, _ := conflictingScaleSetLeaderFixture(t)
	leader.reconcile(context.Background())
	if len(leader.sessions) != 0 || !strings.Contains(logs.String(), "conflicting clustered scale-set declarations") {
		t.Fatalf("conflict was not rejected clearly: sessions=%#v logs=%q", leader.sessions, logs.String())
	}
}

func conflictingScaleSetLeaderFixture(t *testing.T) (leader *clusterLeader, logs *bytes.Buffer, state *fakeClusterState) {
	t.Helper()
	configured := clusteredConfig(t)
	state = &fakeClusterState{statuses: []cluster.ScaleSetStatus{{Name: "linux", Compatible: false}}}
	logs = &bytes.Buffer{}
	daemon := clusteredDaemonForTest(state)
	daemon.config = configured
	daemon.credentials = testAppCredentials(t)
	daemon.discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{
		Repositories: []githubapp.Repository{{URL: "repo", InstallationID: 7}},
	}}
	daemon.logger = slog.New(slog.NewTextHandler(logs, nil))
	daemon.nodes = state
	daemon.assignments = state
	daemon.tryLeader = fakeTryLeader(state)
	daemon.waitForLeader = fakeWaitForLeader(state)
	return newClusterLeader(daemon, &fakeClusterLease{alive: true}), logs, state
}

func TestClusterLeaderDeletesScaleSetAfterFinalExplicitWithdrawal(t *testing.T) {
	leader, state, deletedScaleSetIDs := withdrawnScaleSetLeaderFixture(t)
	state.statuses = nil
	leader.reconcile(context.Background())
	if len(*deletedScaleSetIDs) != 1 || (*deletedScaleSetIDs)[0] != 20 {
		t.Fatalf("final explicit withdrawal did not delete scale set: %#v", *deletedScaleSetIDs)
	}
}

func withdrawnScaleSetLeaderFixture(t *testing.T) (*clusterLeader, *fakeClusterState, *[]int) {
	t.Helper()
	leader, _, state := conflictingScaleSetLeaderFixture(t)
	messageClient := &fakeMessageClient{}
	deletedScaleSetIDs := []int{}
	githubClient := fakeScaleSetDeletionClient{deleteRunnerScaleSet: func(_ context.Context, runnerScaleSetID int) error {
		deletedScaleSetIDs = append(deletedScaleSetIDs, runnerScaleSetID)
		return nil
	}}
	session := &repositoryScaleSetSession{
		key: repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}, client: githubClient,
		messageClient: scaleSetMessageClient{Client: messageClient}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 20},
		stopPolling: func() {}, pollerDone: closedChannel(),
	}
	leader.sessions[session.key] = session
	return leader, state, &deletedScaleSetIDs
}
