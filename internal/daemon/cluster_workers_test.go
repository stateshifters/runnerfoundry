package daemon

import (
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

func TestReconciliationRequestsCoalesce(t *testing.T) {
	state := &fakeClusterState{statuses: []cluster.ScaleSetStatus{{Name: "linux", Compatible: false}}}
	daemonFixture := clusteredDaemonForTest(state)
	daemon := clusteredDaemon{
		config: clusteredConfig(t), credentials: testAppCredentials(t), discoverer: staticDiscoverer{},
		logger: slog.Default(), loadConfig: daemonFixture.loadConfig, newDiscoverer: daemonFixture.newDiscoverer,
		newGitHubClient: daemonFixture.newGitHubClient, providerHealthy: daemonFixture.providerHealthy,
		buildReconciler: daemonFixture.buildReconciler, notifier: daemonFixture.notifier, nodes: state, assignments: state,
	}
	daemon.leader = newClusterLeader(&daemon, &fakeClusterLease{alive: true})
	ctx, cancel := context.WithCancel(context.Background())
	requests := make(chan struct{}, 1)
	state.onScaleSets = func() {
		if state.scaleSetCalls == 1 {
			requestReconciliation(requests)
			return
		}
		cancel()
	}
	daemon.workers.Add(1)
	go daemon.runReconciliationLoop(ctx, requests)
	requestReconciliation(requests)
	daemon.waitForWorkers()
	if state.scaleSetCalls != 2 {
		t.Fatalf("scale-set reads = %d, want 2 coalesced runs", state.scaleSetCalls)
	}
}

func TestReconciliationRequestAfterLeadershipLossDoesNoWork(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.leader = nil
	daemon.reconcileLeader(context.Background())
	if state.scaleSetCalls != 0 {
		t.Fatalf("reconciliation after leadership loss read scale sets %d times", state.scaleSetCalls)
	}
}

func TestStoppedLeaderRejectsNewReconciliation(t *testing.T) {
	discoveryCalls := 0
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.discoverer = &recordingDiscoverer{afterDiscover: func(int) { discoveryCalls++ }}
	leader := newClusterLeader(&daemon, &fakeClusterLease{alive: true})
	leader.stop(context.Background())
	if leader.reconcile(context.Background()) {
		t.Fatal("stopped leader reported successful reconciliation")
	}
	if discoveryCalls != 0 {
		t.Fatalf("stopped leader performed %d repository discoveries", discoveryCalls)
	}
}

func TestRepositorySessionStartFailureRecoversLeaderOperationalStatus(t *testing.T) {
	fixture := repositorySessionRetryFixture(t)
	fixture.daemon.reconcileLeader(context.Background())
	fixture.daemon.reconcileLeader(context.Background())

	expected := "STATUS=leader reconciling\nSTATUS=leader degraded\n" +
		"STATUS=leader reconciling\nSTATUS=leader operational"
	if actual := strings.Join(fixture.notifier.statuses, "\n"); actual != expected {
		t.Fatalf("cluster status snapshot mismatch\nexpected: %s\nactual:   %s", expected, actual)
	}
	if fixture.startAttempts != 2 || len(fixture.leader.sessions) != 1 {
		t.Fatalf("session start attempts=%d active sessions=%d, want 2 attempts and 1 session", fixture.startAttempts, len(fixture.leader.sessions))
	}
	fixture.leader.stop(context.Background())
}

type repositorySessionRetryTestFixture struct {
	daemon        *clusteredDaemon
	leader        *clusterLeader
	notifier      *clusterStatusSnapshotNotifier
	successClient *fakeScaleSetClient
	startAttempts int
}

func repositorySessionRetryFixture(t *testing.T) *repositorySessionRetryTestFixture {
	t.Helper()
	state := &fakeClusterState{statuses: []cluster.ScaleSetStatus{{
		Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: true,
	}}}
	daemon := activeClusteredDaemon(t, state)
	fixture := &repositorySessionRetryTestFixture{daemon: &daemon}
	fixture.daemon.discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{Repositories: []githubapp.Repository{{
		InstallationID: 1, URL: "https://github.com/acme/retry",
	}}}}
	messageClient := &blockingMessageClient{entered: make(chan struct{}), exited: make(chan struct{})}
	fixture.successClient = fakeExistingScaleSetClient(
		&upstream.RunnerGroup{ID: 1},
		&upstream.RunnerScaleSet{ID: 2},
		scaleSetMessageClient{Client: messageClient},
	)
	fixture.daemon.newGitHubClient = func(
		repositoryURL string,
		_ githubapp.InstallationID,
		_ config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		return fixture.clientForRepository(repositoryURL)
	}
	fixture.notifier = &clusterStatusSnapshotNotifier{}
	fixture.daemon.notifier = fixture.notifier
	fixture.leader = newClusterLeader(fixture.daemon, &fakeClusterLease{alive: true})
	fixture.daemon.leader = fixture.leader
	return fixture
}

func (fixture *repositorySessionRetryTestFixture) clientForRepository(string) (*fakeScaleSetClient, error) {
	fixture.startAttempts++
	if fixture.startAttempts == 1 {
		return nil, errors.New("session unavailable")
	}
	return fixture.successClient, nil
}

func TestExistingAssignedRunnerIsNotStartedAgain(t *testing.T) {
	provider := &fakeScalerProvider{instances: []scaler.RunnerInstance{{RunnerName: "linux-9", State: scaler.RunnerStateRunning}}}
	reconciler := scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "linux", MaxRunners: 1, Provider: provider, JITConfig: fakeJITConfigGenerator{},
	}}
	if err := ensureAssignedRunner(context.Background(), reconciler, 9); err != nil {
		t.Fatal(err)
	}
	if len(provider.started) != 0 {
		t.Fatalf("provider starts = %v, want none", provider.started)
	}
}

func TestAssignedRunnerStartFailureIsReturnedWhenRunnerRemainsAbsent(t *testing.T) {
	failureProvider := failingStartProvider{}
	reconciler := scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "linux", MaxRunners: 1, Provider: failureProvider, JITConfig: fakeJITConfigGenerator{},
	}}
	if err := ensureAssignedRunner(context.Background(), reconciler, 9); err == nil || !strings.Contains(err.Error(), "start failed") {
		t.Fatalf("ensureAssignedRunner() error = %v, want provider start failure", err)
	}
}

func TestAssignmentRecoveryStopsAfterLoadOrRepairFailure(t *testing.T) {
	state := &fakeClusterState{startingErr: errors.New("database unavailable")}
	daemon := clusteredDaemon{nodeID: "spock", logger: slog.Default(), assignments: state}
	daemon.executeAssignments(context.Background())
	if len(state.claimedNodeIDs) != 0 {
		t.Fatalf("claims after recovery load failure = %v", state.claimedNodeIDs)
	}

	state = &fakeClusterState{startingAssignments: []cluster.Assignment{{
		ID: 1, NodeID: "spock", ScaleSet: "linux", Repository: "repo",
	}}}
	daemonFixture := clusteredDaemonForTest(state)
	daemon = clusteredDaemon{
		nodeID: "spock", config: clusteredConfig(t), credentials: testAppCredentials(t),
		logger: slog.Default(), loadConfig: daemonFixture.loadConfig, newDiscoverer: daemonFixture.newDiscoverer,
		newGitHubClient: daemonFixture.newGitHubClient, providerHealthy: daemonFixture.providerHealthy,
		buildReconciler: daemonFixture.buildReconciler, notifier: daemonFixture.notifier, assignments: state,
	}
	daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errors.New("client unavailable")
	}
	daemon.executeAssignments(context.Background())
	if len(state.claimedNodeIDs) != 0 {
		t.Fatalf("claims after repair failure = %v", state.claimedNodeIDs)
	}
}

func TestStaleLeadershipAndStatusFailureDoNotChangeDaemonState(t *testing.T) {
	current := newClusterLeader(&clusteredDaemon{}, &fakeClusterLease{alive: true})
	daemon := clusteredDaemon{
		leader: current, logger: slog.Default(),
		notifier: failingStatusNotifier{},
	}
	if daemon.clearLeadership(newClusterLeader(&daemon, &fakeClusterLease{alive: true})) {
		t.Fatal("stale leader cleared current leadership")
	}
	daemon.notifyStatus(context.Background(), clusterServiceStatusLeaderDegraded)
	if daemon.currentLeader() != current {
		t.Fatal("status notification failure changed leadership")
	}
}

func clusteredConfig(t *testing.T) config.Config {
	t.Helper()
	configured := runnableConfig(t)
	configured.State.Postgres = &postgresconfig.Settings{DSN: "postgres://unused", Role: "runnerfoundry"}
	configured.ScaleSets[0].RunnerGroup = "Default"
	configured.ScaleSets[0].MaxRunners = 10
	return configured
}

func clusteredDaemonForTest(state *fakeClusterState) *clusteredDaemon {
	return &clusteredDaemon{
		hostname:      func() (string, error) { return "spock", nil },
		newLogger:     func(string, string) *slog.Logger { return slog.Default() },
		loadConfig:    loadConfig,
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return staticDiscoverer{}, nil },
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		openStore: func(postgresconfig.Settings) (clusterStoreConnection, error) {
			return clusterStoreConnection{
				state: state, tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
			}, nil
		},
		providerHealthy: func(context.Context, config.ScaleSetConfig) error { return nil },
		buildReconciler: buildReconciler,
		notifier:        noopNotifier{}, waitBeforeLeadershipRetry: func(ctx context.Context, _ time.Duration) bool {
			return ctx.Err() == nil
		},
	}
}

type clusterPublication struct {
	nodeID        cluster.NodeID
	healthy       bool
	contributions []cluster.Contribution
}

type removedClusterDemand struct {
	scaleSet   string
	repository string
	requestID  int64
}

type fakeClusterState struct {
	migrations               int
	publications             []clusterPublication
	statuses                 []cluster.ScaleSetStatus
	demand                   []cluster.Demand
	removedDemand            []removedClusterDemand
	assignments              []*cluster.Assignment
	pendingAssignments       []cluster.Assignment
	readyAssignments         []*cluster.Assignment
	startingAssignments      []cluster.Assignment
	claimedNodeIDs           []cluster.NodeID
	begun                    []cluster.AssignmentID
	ready                    []cluster.AssignmentID
	started                  []cluster.AssignmentID
	released                 []cluster.AssignmentID
	leases                   []cluster.Lease
	tryLeaderErr             error
	migrateErr               error
	migrationStatusErr       error
	publishErr               error
	statusesErr              error
	enqueueErr               error
	assignErr                error
	readyErr                 error
	confirmFailuresRemaining int
	confirmFailure           error
	startedErr               error
	claimErr                 error
	removeErr                error
	releaseErr               error
	onPublish                func()
	onTryLeader              func()
	onScaleSets              func()
	onStarting               func()
	onClaimReady             func()
	waitForLeader            func(context.Context) (cluster.Lease, error)
	pendingErr               error
	startingErr              error
	startingCalls            int
	scaleSetCalls            int
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
	nodeID cluster.NodeID,
	health cluster.NodeHealth,
	contributions []cluster.Contribution,
	_ time.Time,
) error {
	state.publications = append(state.publications, clusterPublication{
		nodeID: nodeID, healthy: health == cluster.NodeHealthy, contributions: append([]cluster.Contribution(nil), contributions...),
	})
	if state.onPublish != nil {
		state.onPublish()
	}
	return state.publishErr
}

func (state *fakeClusterState) ScaleSets(context.Context, time.Time, time.Duration) ([]cluster.ScaleSetStatus, error) {
	state.scaleSetCalls++
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

func (state *fakeClusterState) BeginAcquisition(
	_ context.Context,
	assignmentID cluster.AssignmentID,
	_ githubapp.InstallationID,
	_ int,
) error {
	state.begun = append(state.begun, assignmentID)
	return state.readyErr
}

func (state *fakeClusterState) ConfirmAcquisition(_ context.Context, assignmentID cluster.AssignmentID) error {
	state.ready = append(state.ready, assignmentID)
	if state.confirmFailuresRemaining > 0 {
		state.confirmFailuresRemaining--
		return state.confirmFailure
	}
	return state.readyErr
}

func (state *fakeClusterState) PendingAcquisitions(context.Context) ([]cluster.Assignment, error) {
	return append([]cluster.Assignment(nil), state.pendingAssignments...), state.pendingErr
}

func (state *fakeClusterState) ConfirmRunning(
	_ context.Context,
	assignmentID cluster.AssignmentID,
	_ cluster.NodeID,
) error {
	state.started = append(state.started, assignmentID)
	return state.startedErr
}

func (state *fakeClusterState) ClaimForStart(_ context.Context, nodeID cluster.NodeID) (*cluster.Assignment, error) {
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

func (state *fakeClusterState) Starting(context.Context, cluster.NodeID) ([]cluster.Assignment, error) {
	state.startingCalls++
	if state.onStarting != nil {
		state.onStarting()
	}
	return append([]cluster.Assignment(nil), state.startingAssignments...), state.startingErr
}

func (state *fakeClusterState) CompleteDemand(_ context.Context, scaleSet string, repository string, requestID int64) error {
	state.removedDemand = append(state.removedDemand, removedClusterDemand{scaleSet: scaleSet, repository: repository, requestID: requestID})
	return state.removeErr
}

func (state *fakeClusterState) CompleteAssignment(
	_ context.Context,
	assignmentID cluster.AssignmentID,
	_ cluster.NodeID,
	_ cluster.AssignmentState,
) error {
	state.released = append(state.released, assignmentID)
	return state.releaseErr
}

func fakeTryLeader(state *fakeClusterState) func(context.Context) (leadershipResult, error) {
	return func(context.Context) (leadershipResult, error) {
		if state.onTryLeader != nil {
			state.onTryLeader()
		}
		if state.tryLeaderErr != nil {
			return leadershipResult{}, state.tryLeaderErr
		}
		if len(state.leases) == 0 {
			return leadershipResult{}, cluster.ErrNotLeader
		}
		lease := state.leases[0]
		state.leases = state.leases[1:]
		return leadershipResult{lease: lease}, nil
	}
}

func fakeWaitForLeader(state *fakeClusterState) func(context.Context) (leadershipResult, error) {
	return func(ctx context.Context) (leadershipResult, error) {
		if state.waitForLeader != nil {
			lease, err := state.waitForLeader(ctx)
			return leadershipResult{lease: lease}, err
		}
		<-ctx.Done()
		return leadershipResult{}, fmt.Errorf("wait for leader: %w", ctx.Err())
	}
}

type fakeClusterLease struct {
	alive    bool
	closed   bool
	closeErr error
}

type clusterLifecycleNotifier struct {
	ready   bool
	onReady func()
}

func (notifier *clusterLifecycleNotifier) Ready(context.Context) error {
	notifier.ready = true
	notifier.onReady()
	return nil
}

func (*clusterLifecycleNotifier) Reloading(context.Context) error { return nil }

func (*clusterLifecycleNotifier) Status(context.Context, clusterServiceStatus) error { return nil }

type failingStatusNotifier struct{}

func (failingStatusNotifier) Ready(context.Context) error     { return nil }
func (failingStatusNotifier) Reloading(context.Context) error { return nil }
func (failingStatusNotifier) Status(context.Context, clusterServiceStatus) error {
	return errors.New("status unavailable")
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
