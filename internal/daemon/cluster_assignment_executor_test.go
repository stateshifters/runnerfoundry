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
	configured := clusteredConfig(t)
	state := &fakeClusterState{readyAssignments: []*cluster.Assignment{
		{ID: 1, NodeID: "spock", ScaleSet: "remote-only", Repository: "repo", RequestID: 1},
		{ID: 2, NodeID: "spock", ScaleSet: "linux", Repository: "repo", RequestID: 2, InstallationID: 3, GitHubScaleSetID: 4},
	}}
	provider := &fakeScalerProvider{trackStarted: true}
	daemonUnderTest := clusteredDaemonForTest(state)
	daemonUnderTest.buildReconciler = func(
		context.Context, string, config.ScaleSetConfig, scaleSetClient, int,
	) (scaler.Reconciler, error) {
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name: "linux", MaxRunners: 10, Provider: provider, JITConfig: fakeJITConfigGenerator{},
		}}, nil
	}
	daemon := clusteredDaemon{
		nodeID: "spock",
		config: configured, credentials: testAppCredentials(t), logger: slog.Default(), loadConfig: daemonUnderTest.loadConfig, newDiscoverer: daemonUnderTest.newDiscoverer,
		newGitHubClient: daemonUnderTest.newGitHubClient, providerHealthy: daemonUnderTest.providerHealthy,
		buildReconciler: daemonUnderTest.buildReconciler, notifier: daemonUnderTest.notifier,
		nodes: state, assignments: state, tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
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
	daemonUnderTest := clusteredDaemonForTest(state)
	daemonUnderTest.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return githubClient, nil
	}
	daemon := clusteredDaemon{
		config: configured, credentials: testAppCredentials(t),
		discoverer: staticDiscoverer{result: githubapp.DiscoveryResult{Repositories: []githubapp.Repository{{URL: "repo", InstallationID: 7}}}},
		logger:     slog.Default(), loadConfig: daemonUnderTest.loadConfig, newDiscoverer: daemonUnderTest.newDiscoverer,
		newGitHubClient: daemonUnderTest.newGitHubClient, providerHealthy: daemonUnderTest.providerHealthy,
		buildReconciler: daemonUnderTest.buildReconciler, notifier: daemonUnderTest.notifier, nodes: state, assignments: state,
		tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
	}
	daemon.tryLeadership(context.Background())
	daemon.leader.reconcile(context.Background())
	if daemon.leader == nil || len(daemon.leader.sessions) != 1 {
		t.Fatalf("leader did not manage remote-only scale set: %#v", daemon.leader)
	}
	session := daemon.leader.sessions[repositoryScaleSetSessionKey{repository: "repo", scaleSet: "remote-only"}]
	if session == nil || session.capacity != 12 {
		t.Fatalf("aggregate capacity was not used by leader: %#v", session)
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
	daemonFixture := clusteredDaemonForTest(state)
	daemon := clusteredDaemon{
		config: configured, credentials: testAppCredentials(t),
		discoverer: staticDiscoverer{result: githubapp.DiscoveryResult{Repositories: []githubapp.Repository{{URL: "repo", InstallationID: 7}}}},
		logger:     slog.New(slog.NewTextHandler(logs, nil)), loadConfig: daemonFixture.loadConfig, newDiscoverer: daemonFixture.newDiscoverer,
		newGitHubClient: daemonFixture.newGitHubClient, providerHealthy: daemonFixture.providerHealthy,
		buildReconciler: daemonFixture.buildReconciler, notifier: daemonFixture.notifier,
		nodes: state, assignments: state, tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
	}
	leader := newClusterLeader(&daemon, &fakeClusterLease{alive: true})
	leader.reconcile(context.Background())
	if len(leader.sessions) != 0 || !strings.Contains(logs.String(), "conflicting clustered scale-set declarations") {
		t.Fatalf("conflict was not rejected clearly: sessions=%#v logs=%q", leader.sessions, logs.String())
	}

	messageClient := &fakeMessageClient{}
	githubClient := &fakeScaleSetClient{messageClient: scaleSetMessageClient{Client: messageClient}}
	session := &repositoryScaleSetSession{
		key: repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}, client: githubClient,
		messageClient: scaleSetMessageClient{Client: messageClient}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 20},
		stopPolling: func() {}, pollerDone: closedChannel(),
	}
	leader.sessions[session.key] = session
	state.statuses = nil
	leader.reconcile(context.Background())
	if len(githubClient.deletedScaleSetIDs) != 1 || githubClient.deletedScaleSetIDs[0] != 20 {
		t.Fatalf("final explicit withdrawal did not delete scale set: %#v", githubClient.deletedScaleSetIDs)
	}
}
