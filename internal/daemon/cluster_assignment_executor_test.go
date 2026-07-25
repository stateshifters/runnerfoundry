package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
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

func TestClusteredExecutorRegistersLibvirtRunnerBeforeStart(t *testing.T) {
	daemon, state, provider := clusteredAssignmentExecutorFixture(t)
	daemon.config.ScaleSets[0].Docker = nil
	daemon.config.ScaleSets[0].Libvirt = &config.LibvirtConfig{ConnectionURI: "test", DomainXMLTemplate: "test"}
	daemon.executeAssignments(context.Background())

	if len(provider.started) != 1 || len(state.registeredLibvirtRunners) != 1 {
		t.Fatalf("started=%#v registered=%#v", provider.started, state.registeredLibvirtRunners)
	}
	registered := state.registeredLibvirtRunners[0]
	if registered.NodeID != "spock" || registered.RequestID != 2 || registered.RunnerName != provider.started[0].RunnerName {
		t.Fatalf("registered lifecycle = %#v; start = %#v", registered, provider.started[0])
	}
}

func TestClusteredExecutorRetainsReapUntilSecondInspectionConfirmsAbsence(t *testing.T) {
	configured := clusteredConfig(t)
	configured.ScaleSets[0].Docker = nil
	configured.ScaleSets[0].Libvirt = &config.LibvirtConfig{ConnectionURI: "test", DomainXMLTemplate: "test"}
	runner := cluster.LibvirtRunnerLifecycle{
		NodeID: "spock", ScaleSet: "linux", Repository: "repo", RequestID: 2, RunnerName: "linux-2",
	}
	state := &fakeClusterState{pendingLibvirtReaps: []cluster.LibvirtRunnerLifecycle{runner}}
	instance := scaler.RunnerInstance{ID: "domain", RunnerName: runner.RunnerName, State: scaler.RunnerStateRunning}
	provider := &clusterReapingProvider{lists: [][]scaler.RunnerInstance{{instance}, {instance}, nil}}
	daemon := clusteredDaemonForTest(state)
	daemon.nodeID, daemon.config, daemon.logger, daemon.assignments = "spock", configured, slog.Default(), state
	daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: provider}}, nil
	}

	daemon.executeAssignments(context.Background())
	if len(state.confirmedLibvirtReaps) != 0 || provider.destroyed != 1 {
		t.Fatalf("first tick confirmations=%#v destroys=%d", state.confirmedLibvirtReaps, provider.destroyed)
	}
	daemon.executeAssignments(context.Background())
	if len(state.confirmedLibvirtReaps) != 1 || state.confirmedLibvirtReaps[0] != runner {
		t.Fatalf("second tick confirmations=%#v", state.confirmedLibvirtReaps)
	}
}

func TestClusteredExecutorRetainsReapWhenLocalLibvirtConfigurationIsMissing(t *testing.T) {
	configured := clusteredConfig(t)
	runner := cluster.LibvirtRunnerLifecycle{
		NodeID: "spock", ScaleSet: "missing", Repository: "repo", RequestID: 2, RunnerName: "missing-2",
	}
	state := &fakeClusterState{pendingLibvirtReaps: []cluster.LibvirtRunnerLifecycle{runner}}
	daemon := clusteredDaemonForTest(state)
	daemon.nodeID, daemon.config, daemon.logger, daemon.assignments = "spock", configured, slog.Default(), state

	daemon.executeAssignments(context.Background())
	if len(state.confirmedLibvirtReaps) != 0 {
		t.Fatalf("missing configuration confirmed reaps %#v", state.confirmedLibvirtReaps)
	}
}

type clusterReapingProvider struct {
	lists     [][]scaler.RunnerInstance
	destroyed int
	listErr   error
}

func (provider *clusterReapingProvider) List(context.Context, string) ([]scaler.RunnerInstance, error) {
	if provider.listErr != nil {
		return nil, provider.listErr
	}
	if len(provider.lists) == 0 {
		return nil, nil
	}
	instances := provider.lists[0]
	provider.lists = provider.lists[1:]
	return instances, nil
}

func TestClusteredExecutorRetainsReapsOnBoundaryFailures(t *testing.T) {
	runClusterReapBoundaryFailure(t, "load", func(_ *clusteredDaemon, state *fakeClusterState) {
		state.pendingLibvirtReapsErr = errors.New("load failed")
	})
	runClusterReapBoundaryFailure(t, "build", func(daemon *clusteredDaemon, _ *fakeClusterState) {
		daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
			return scaler.Reconciler{}, errors.New("build failed")
		}
	})
	runClusterReapBoundaryFailure(t, "reap", func(daemon *clusteredDaemon, _ *fakeClusterState) {
		daemon.buildReconciler = failingClusterReaperBuilder
	})
	runClusterReapBoundaryFailure(t, "confirm", func(daemon *clusteredDaemon, state *fakeClusterState) {
		state.confirmLibvirtReapErr = errors.New("confirm failed")
		daemon.buildReconciler = absentClusterReaperBuilder
	})
}

func runClusterReapBoundaryFailure(
	t *testing.T,
	name string,
	configure func(*clusteredDaemon, *fakeClusterState),
) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		runner := cluster.LibvirtRunnerLifecycle{NodeID: "spock", ScaleSet: "linux", Repository: "repo", RequestID: 2, RunnerName: "linux-2"}
		state := &fakeClusterState{pendingLibvirtReaps: []cluster.LibvirtRunnerLifecycle{runner}}
		daemon := clusteredDaemonForTest(state)
		configured := clusteredConfig(t)
		configured.ScaleSets[0].Docker = nil
		configured.ScaleSets[0].Libvirt = &config.LibvirtConfig{}
		daemon.nodeID, daemon.config, daemon.logger, daemon.assignments = "spock", configured, slog.Default(), state
		configure(daemon, state)
		daemon.executeAssignments(context.Background())
	})
}

func failingClusterReaperBuilder(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
	return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &clusterReapingProvider{listErr: errors.New("list failed")}}}, nil
}

func absentClusterReaperBuilder(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
	return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &clusterReapingProvider{}}}, nil
}

func (*clusterReapingProvider) Start(context.Context, scaler.RunnerStartRequest) error { return nil }
func (*clusterReapingProvider) Destroy(context.Context, scaler.RunnerInstance) error   { return nil }
func (provider *clusterReapingProvider) DestroyTerminal(context.Context, scaler.RunnerInstance) error {
	provider.destroyed++
	return nil
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
