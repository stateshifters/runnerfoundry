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
	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type lifecycleClusterState struct {
	*fakeClusterState
	store      *scaler.MemoryDemandStore
	pending    []scaler.RunnerLifecycle
	expireErr  error
	pendingErr error
	confirmErr error
	busyErr    error
	confirmed  []scaler.RunnerLifecycle
}

func newLifecycleClusterState(state *fakeClusterState) *lifecycleClusterState {
	return &lifecycleClusterState{fakeClusterState: state, store: scaler.NewMemoryDemandStore()}
}

func (state *lifecycleClusterState) RegisterRunner(ctx context.Context, runner scaler.RunnerLifecycle) error {
	if err := state.store.RegisterRunner(ctx, runner); err != nil {
		return fmt.Errorf("register runner: %w", err)
	}
	return nil
}
func (state *lifecycleClusterState) ConfirmRunnerWaiting(ctx context.Context, runner scaler.RunnerLifecycle, at time.Time) error {
	if err := state.store.ConfirmRunnerWaiting(ctx, runner, at); err != nil {
		return fmt.Errorf("confirm runner waiting: %w", err)
	}
	return nil
}
func (state *lifecycleClusterState) MarkRunnerBusy(ctx context.Context, scaleSet, repository string, requestID int64, runnerName string, runnerID int64, at time.Time) error {
	if state.busyErr != nil {
		return state.busyErr
	}
	if err := state.store.MarkRunnerBusy(ctx, scaleSet, repository, requestID, runnerName, runnerID, at); err != nil {
		return fmt.Errorf("mark runner busy: %w", err)
	}
	return nil
}
func (state *lifecycleClusterState) RequestRunnerReap(ctx context.Context, scaleSet, repository string, requestID int64, runnerName, reason string, at time.Time) error {
	if err := state.store.RequestRunnerReap(ctx, scaleSet, repository, requestID, runnerName, reason, at); err != nil {
		return fmt.Errorf("request runner reap: %w", err)
	}
	return nil
}
func (state *lifecycleClusterState) RequestExpiredRunnerReaps(ctx context.Context, scaleSet, repository string, deadline, at time.Time) (int, error) {
	if state.expireErr != nil {
		return 0, state.expireErr
	}
	changed, err := state.store.RequestExpiredRunnerReaps(ctx, scaleSet, repository, deadline, at)
	if err != nil {
		return 0, fmt.Errorf("request expired runners: %w", err)
	}
	return changed, nil
}
func (state *lifecycleClusterState) RequestExcessWaitingRunnerReaps(ctx context.Context, scaleSet, repository string, retain int, reason string, at time.Time) (int, error) {
	changed, err := state.store.RequestExcessWaitingRunnerReaps(ctx, scaleSet, repository, retain, reason, at)
	if err != nil {
		return 0, fmt.Errorf("request excess runners: %w", err)
	}
	return changed, nil
}
func (state *lifecycleClusterState) PendingRunnerReaps(ctx context.Context, scaleSet, repository string) ([]scaler.RunnerLifecycle, error) {
	pending, err := state.store.PendingRunnerReaps(ctx, scaleSet, repository)
	if err != nil {
		return nil, fmt.Errorf("pending runner reaps: %w", err)
	}
	return pending, nil
}
func (state *lifecycleClusterState) PendingNodeRunnerReaps(context.Context, cluster.NodeID) ([]scaler.RunnerLifecycle, error) {
	if state.pendingErr != nil {
		return nil, state.pendingErr
	}
	return append([]scaler.RunnerLifecycle(nil), state.pending...), nil
}
func (state *lifecycleClusterState) ConfirmRunnerReaped(ctx context.Context, runner scaler.RunnerLifecycle) error {
	state.confirmed = append(state.confirmed, runner)
	if state.confirmErr != nil {
		return state.confirmErr
	}
	if err := state.store.ConfirmRunnerReaped(ctx, runner); err != nil {
		return fmt.Errorf("confirm runner reaped: %w", err)
	}
	return nil
}
func (state *lifecycleClusterState) CompleteJob(ctx context.Context, completion cluster.JobCompletion) error {
	return state.fakeClusterState.CompleteJob(ctx, completion)
}

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

func TestClusteredExecutorRegistersProviderIndependentLifecycle(t *testing.T) {
	daemon, base, provider := clusteredAssignmentExecutorFixture(t)
	state := newLifecycleClusterState(base)
	daemon.assignments = state
	daemon.executeAssignments(context.Background())
	lifecycles, err := state.store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil || len(lifecycles) != 1 || lifecycles[0].Provider != scaler.RunnerProviderDocker ||
		lifecycles[0].AssignmentID != 2 || len(provider.started) != 1 {
		t.Fatalf("lifecycles=%#v started=%#v err=%v", lifecycles, provider.started, err)
	}
}

func TestClusteredExecutorRecordsGenericLibvirtProvider(t *testing.T) {
	daemon, base, _ := clusteredAssignmentExecutorFixture(t)
	daemon.config.ScaleSets[0].Docker = nil
	daemon.config.ScaleSets[0].Libvirt = &config.LibvirtConfig{}
	state := newLifecycleClusterState(base)
	daemon.assignments = state
	daemon.executeAssignments(context.Background())
	lifecycles, err := state.store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil || len(lifecycles) != 1 || lifecycles[0].Provider != scaler.RunnerProviderLibvirt {
		t.Fatalf("lifecycles=%#v err=%v", lifecycles, err)
	}
}

func TestClusterMessageHandlerMarksRunnerBusyBeforeAcknowledgement(t *testing.T) {
	handler, state := busyClusterMessageFixture(t)
	if err := handler.removeStarted(context.Background(), []*upstream.JobStarted{{
		JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 7}, RunnerName: "linux-7", RunnerID: 99,
	}}); err != nil {
		t.Fatal(err)
	}
	lifecycles, err := state.store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil {
		t.Fatal(err)
	}
	if len(lifecycles) != 1 || lifecycles[0].Phase != scaler.RunnerBusy {
		t.Fatalf("lifecycles=%#v", lifecycles)
	}
}

func TestClusterMessageHandlerReportsBusyPersistenceBoundaries(t *testing.T) {
	handler, state := busyClusterMessageFixture(t)
	state.busyErr = pgx.ErrNoRows
	if err := handler.removeStarted(context.Background(), []*upstream.JobStarted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 8}}}); err != nil {
		t.Fatal(err)
	}
	state.busyErr = errors.New("busy")
	if err := handler.removeStarted(context.Background(), []*upstream.JobStarted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 9}}}); err == nil {
		t.Fatal("expected busy persistence failure")
	}
}

func busyClusterMessageFixture(t *testing.T) (clusterMessageHandler, *lifecycleClusterState) {
	t.Helper()
	state := newLifecycleClusterState(&fakeClusterState{})
	runner := scaler.RunnerLifecycle{ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker,
		RunnerName: "linux-7", RequestID: 7, Phase: scaler.RunnerWaitingForJob}
	if err := state.store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	return clusterMessageHandler{store: state, session: &repositoryScaleSetSession{
		key: repositoryScaleSetSessionKey{scaleSet: "linux", repository: "repo"},
	}}, state
}

func TestClusteredExecutorProcessesProviderIndependentReaps(t *testing.T) {
	configured := clusteredConfig(t)
	runner := scaler.RunnerLifecycle{NodeID: "spock", ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker,
		RunnerName: "linux-2", InstallationID: 3, GitHubScaleSetID: 4, Phase: scaler.RunnerReapPending}
	base := &fakeClusterState{}
	state := newLifecycleClusterState(base)
	state.pending = []scaler.RunnerLifecycle{runner}
	if err := state.store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	daemon := clusteredDaemonForTest(base)
	daemon.nodeID, daemon.config, daemon.logger, daemon.assignments = "spock", configured, slog.Default(), state
	daemon.credentials = testAppCredentials(t)
	daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &incidentLifecycleProvider{}, JITConfig: newIncidentJITRegistry()}}, nil
	}
	if !daemon.executePendingRunnerReaps(context.Background()) || len(state.confirmed) != 1 {
		t.Fatalf("confirmed=%#v", state.confirmed)
	}
}

var clusteredRunnerReapBoundaryCases = []struct {
	name      string
	configure func(*clusteredDaemon, *lifecycleClusterState)
	want      bool
}{
	{name: "invalid timeout", want: false, configure: func(daemon *clusteredDaemon, _ *lifecycleClusterState) {
		daemon.config.ScaleSets[0].JobStartTimeout = "invalid"
	}},
	{name: "expire", want: false, configure: func(_ *clusteredDaemon, state *lifecycleClusterState) { state.expireErr = errors.New("expire") }},
	{name: "load", want: false, configure: func(_ *clusteredDaemon, state *lifecycleClusterState) { state.pendingErr = errors.New("load") }},
	{name: "missing config", want: true, configure: func(daemon *clusteredDaemon, _ *lifecycleClusterState) { daemon.config.ScaleSets[0].Name = "different" }},
	{name: "client", want: true, configure: func(daemon *clusteredDaemon, _ *lifecycleClusterState) {
		daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return nil, errors.New("client")
		}
	}},
	{name: "build", want: true, configure: func(daemon *clusteredDaemon, _ *lifecycleClusterState) {
		daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
			return scaler.Reconciler{}, errors.New("build")
		}
	}},
	{name: "reap", want: true, configure: func(daemon *clusteredDaemon, _ *lifecycleClusterState) {
		daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
			return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &clusterReapingProvider{listErr: errors.New("list")}, JITConfig: newIncidentJITRegistry()}}, nil
		}
	}},
	{name: "incomplete", want: true, configure: func(daemon *clusteredDaemon, _ *lifecycleClusterState) {
		daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
			instance := scaler.RunnerInstance{ID: "id", RunnerName: "linux-2"}
			return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &clusterReapingProvider{lists: [][]scaler.RunnerInstance{{instance}, {instance}}}, JITConfig: newIncidentJITRegistry()}}, nil
		}
	}},
	{name: "confirm", want: true, configure: func(daemon *clusteredDaemon, state *lifecycleClusterState) {
		state.confirmErr = errors.New("confirm")
		daemon.buildReconciler = func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error) {
			return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{Name: "linux", Provider: &incidentLifecycleProvider{}, JITConfig: newIncidentJITRegistry()}}, nil
		}
	}},
}

func TestClusteredExecutorRetainsProviderIndependentReapsAtBoundaries(t *testing.T) {
	runner := scaler.RunnerLifecycle{NodeID: "spock", ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderDocker,
		RunnerName: "linux-2", InstallationID: 3, GitHubScaleSetID: 4, Phase: scaler.RunnerReapPending}
	for _, test := range clusteredRunnerReapBoundaryCases {
		t.Run(test.name, func(t *testing.T) {
			base := &fakeClusterState{}
			state := newLifecycleClusterState(base)
			state.pending = []scaler.RunnerLifecycle{runner}
			if err := state.store.RegisterRunner(context.Background(), runner); err != nil {
				t.Fatal(err)
			}
			daemon := clusteredDaemonForTest(base)
			daemon.nodeID, daemon.config, daemon.logger, daemon.assignments = "spock", clusteredConfig(t), slog.Default(), state
			daemon.credentials = testAppCredentials(t)
			test.configure(daemon, state)
			if got := daemon.executePendingRunnerReaps(context.Background()); got != test.want {
				t.Fatalf("result=%t want=%t", got, test.want)
			}
		})
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
