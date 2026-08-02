package daemon

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
)

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
		&upstream.RunnerGroup{ID: 1}, &upstream.RunnerScaleSet{ID: 20, Name: "remote-only"},
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
	return newClusterLeader(context.Background(), daemon, &fakeClusterLease{alive: true}), logs, state
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
