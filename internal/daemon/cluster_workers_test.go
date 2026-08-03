package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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
	daemon.leader = newClusterLeader(context.Background(), &daemon, &fakeClusterLease{alive: true})
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

func TestRepositoryRepairTimerIsDisabledWithoutLeadership(t *testing.T) {
	daemon := activeClusteredDaemon(t, &fakeClusterState{})
	daemon.leader = nil
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	if ready := daemon.resetRepositoryRepairTimer(timer); ready != nil {
		t.Fatal("repository repair timer remained enabled without leadership")
	}
}

func TestStoppedLeaderRejectsNewReconciliation(t *testing.T) {
	discoveryCalls := 0
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.discoverer = &recordingDiscoverer{afterDiscover: func(int) { discoveryCalls++ }}
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{alive: true})
	leader.stop(context.Background())
	if leader.reconcile(context.Background()) {
		t.Fatal("stopped leader reported successful reconciliation")
	}
	if discoveryCalls != 0 {
		t.Fatalf("stopped leader performed %d repository discoveries", discoveryCalls)
	}
}

func TestRepositorySessionStartFailureKeepsLeaderOperational(t *testing.T) {
	fixture := repositorySessionRetryFixture(t)
	fixture.daemon.reconcileLeader(context.Background())

	expected := "STATUS=leader reconciling\nSTATUS=leader operational"
	if actual := strings.Join(fixture.notifier.statuses, "\n"); actual != expected {
		t.Fatalf("cluster status snapshot mismatch\nexpected: %s\nactual:   %s", expected, actual)
	}
	if fixture.startAttempts != 1 || len(fixture.leader.sessions) != 0 {
		t.Fatalf("session start attempts=%d active sessions=%d, want one failed attempt", fixture.startAttempts, len(fixture.leader.sessions))
	}
	fixture.leader.stop(context.Background())
}

func TestRepositorySessionFailureIsActivelyRepaired(t *testing.T) {
	fixture := repositorySessionRetryFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	requests := make(chan struct{}, 1)
	fixture.daemon.repositoryRepairRequests = make(chan struct{}, 1)
	fixture.daemon.workers.Add(2)
	go fixture.daemon.runReconciliationLoop(ctx, requests)
	go fixture.daemon.runRepositoryRepairLoop(ctx)
	requestReconciliation(requests)

	select {
	case attempt := <-fixture.attempted:
		if attempt != 2 {
			t.Fatalf("repository attempt = %d, want first retry", attempt)
		}
	case <-time.After(100 * time.Millisecond):
		cancel()
		fixture.daemon.waitForWorkers()
		t.Fatal("failed repository was not actively retried")
	}
	awaitClusterSessionCount(t, fixture.leader, 1)
	cancel()
	fixture.daemon.waitForWorkers()
	fixture.leader.stop(context.Background())
}

func awaitClusterSessionCount(t *testing.T, leader *clusterLeader, expected int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		leader.sessionsMutex.Lock()
		actual := len(leader.sessions)
		leader.sessionsMutex.Unlock()
		if actual == expected {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("active sessions = %d, want %d", actual, expected)
		case <-ticker.C:
		}
	}
}

type repositorySessionRetryTestFixture struct {
	daemon        *clusteredDaemon
	leader        *clusterLeader
	notifier      *clusterStatusSnapshotNotifier
	successClient *fakeScaleSetClient
	startAttempts int
	attempted     chan int
}

func repositorySessionRetryFixture(t *testing.T) *repositorySessionRetryTestFixture {
	t.Helper()
	state := &fakeClusterState{statuses: []cluster.ScaleSetStatus{{
		Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: true,
	}}}
	daemon := activeClusteredDaemon(t, state)
	fixture := &repositorySessionRetryTestFixture{daemon: &daemon, attempted: make(chan int, 1)}
	fixture.daemon.discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{Repositories: []githubapp.Repository{{
		InstallationID: 1, URL: "https://github.com/acme/retry",
	}}}}
	messageClient := &blockingMessageClient{entered: make(chan struct{}), exited: make(chan struct{})}
	fixture.successClient = fakeExistingScaleSetClient(
		&upstream.RunnerGroup{ID: 1},
		&upstream.RunnerScaleSet{ID: 2, Name: "linux"},
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
	fixture.leader = newClusterLeader(context.Background(), fixture.daemon, &fakeClusterLease{alive: true})
	fixture.leader.repositoryRepairDelays = []time.Duration{time.Millisecond}
	fixture.daemon.leader = fixture.leader
	return fixture
}

func (fixture *repositorySessionRetryTestFixture) clientForRepository(string) (*fakeScaleSetClient, error) {
	fixture.startAttempts++
	if fixture.startAttempts == 1 {
		return nil, errors.New("session unavailable")
	}
	fixture.attempted <- fixture.startAttempts
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
	daemon := clusteredDaemon{nodeID: "runner-host-1", logger: slog.Default(), assignments: state}
	daemon.executeAssignments(context.Background())
	if len(state.claimedNodeIDs) != 0 {
		t.Fatalf("claims after recovery load failure = %v", state.claimedNodeIDs)
	}

	state = &fakeClusterState{startingAssignments: []cluster.Assignment{{
		ID: 1, NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo",
	}}}
	daemonFixture := clusteredDaemonForTest(state)
	daemon = clusteredDaemon{
		nodeID: "runner-host-1", config: clusteredConfig(t), credentials: testAppCredentials(t),
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
	current := newClusterLeader(context.Background(), &clusteredDaemon{}, &fakeClusterLease{alive: true})
	daemon := clusteredDaemon{
		leader: current, logger: slog.Default(),
		notifier: failingStatusNotifier{},
	}
	if daemon.clearLeadership(newClusterLeader(context.Background(), &daemon, &fakeClusterLease{alive: true})) {
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
		hostname:      func() (string, error) { return "runner-host-1", nil },
		newLogger:     func(string, string) *slog.Logger { return slog.Default() },
		loadConfig:    loadConfig,
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return staticDiscoverer{}, nil },
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		openStore: func(postgresconfig.Settings) (clusterStoreConnection, error) {
			return clusterStoreConnection{
				state: state, repositoryState: &fakeRepositoryStateStore{},
				tryLeader: fakeTryLeader(state), waitForLeader: fakeWaitForLeader(state),
			}, nil
		},
		providerHealthy: func(context.Context, config.ScaleSetConfig) error { return nil },
		buildReconciler: buildReconciler,
		notifier:        noopNotifier{}, waitBeforeLeadershipRetry: func(ctx context.Context, _ time.Duration) bool {
			return ctx.Err() == nil
		},
	}
}

func TestClusterRegistrationReconciliationRunsAtDiscoveryCadence(t *testing.T) {
	daemon := clusteredDaemonForTest(&fakeClusterState{})
	daemon.config = clusteredConfig(t)
	daemon.config.GitHub.DiscoveryInterval = "1ms"
	daemon.logger = slog.Default()
	requests := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	daemon.workers.Add(1)
	go daemon.runPeriodicReconciliationLoop(ctx, requests)
	select {
	case <-requests:
		cancel()
	case <-time.After(time.Second):
		cancel()
		t.Fatal("periodic registration reconciliation was not requested")
	}
	daemon.workers.Wait()
}

func TestClusterRegistrationReconciliationHandlesInvalidIntervalAndFullRequestQueue(t *testing.T) {
	daemon := clusteredDaemonForTest(&fakeClusterState{})
	daemon.config = clusteredConfig(t)
	daemon.config.GitHub.DiscoveryInterval = "invalid"
	daemon.logger = slog.Default()
	requests := make(chan struct{}, 1)
	requestReconciliation(requests)
	requestReconciliation(requests)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	daemon.workers.Add(1)
	daemon.runPeriodicReconciliationLoop(ctx, requests)
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
	assignedJobsMutex        sync.Mutex
	migrations               int
	publications             []clusterPublication
	statuses                 []cluster.ScaleSetStatus
	demand                   []cluster.Demand
	removedDemand            []removedClusterDemand
	completedJobs            []cluster.JobCompletion
	registeredLibvirtRunners []cluster.LibvirtRunnerLifecycle
	pendingLibvirtReaps      []cluster.LibvirtRunnerLifecycle
	pendingLibvirtReapsErr   error
	confirmedLibvirtReaps    []cluster.LibvirtRunnerLifecycle
	confirmLibvirtReapErr    error
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
	assignedJobCapacity      scaler.AssignedJobCapacity
	assignedJobObservations  []cluster.AssignedJobObservation
	assignedJobAssignments   []cluster.Assignment
	observeAssignedJobsErr   error
	assignAssignedJobErr     error
	onObserveAssignedJobs    func()
	assignedJobEvents        []string
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
	state.assignedJobsMutex.Lock()
	defer state.assignedJobsMutex.Unlock()
	state.demand = append(state.demand, demand)
	state.assignedJobEvents = append(state.assignedJobEvents, "enqueue")
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
	state.assignedJobsMutex.Lock()
	state.assignedJobEvents = append(state.assignedJobEvents, "confirm-exact")
	state.assignedJobsMutex.Unlock()
	if state.confirmFailuresRemaining > 0 {
		state.confirmFailuresRemaining--
		return state.confirmFailure
	}
	return state.readyErr
}

func (state *fakeClusterState) PendingAcquisitions(context.Context) ([]cluster.Assignment, error) {
	return append([]cluster.Assignment(nil), state.pendingAssignments...), state.pendingErr
}

func (state *fakeClusterState) ObserveAssignedJobs(
	_ context.Context,
	observation cluster.AssignedJobObservation,
) (scaler.AssignedJobCapacity, error) {
	state.assignedJobsMutex.Lock()
	state.assignedJobObservations = append(state.assignedJobObservations, observation)
	state.assignedJobEvents = append(state.assignedJobEvents, "observe-assigned")
	onObserve := state.onObserveAssignedJobs
	capacity := state.assignedJobCapacity
	capacity.AvailableCapacity = max(capacity.AvailableCapacity-len(state.assignedJobAssignments), 0)
	err := state.observeAssignedJobsErr
	state.assignedJobsMutex.Unlock()
	if onObserve != nil {
		onObserve()
	}
	return capacity, err
}

func (state *fakeClusterState) AssignAssignedJob(
	_ context.Context,
	observation cluster.AssignedJobObservation,
	_ time.Time,
	_ time.Duration,
) (*cluster.Assignment, error) {
	state.assignedJobsMutex.Lock()
	defer state.assignedJobsMutex.Unlock()
	if state.assignAssignedJobErr != nil {
		return nil, state.assignAssignedJobErr
	}
	assignment := cluster.Assignment{
		ID:       cluster.AssignmentID(len(state.assignedJobAssignments) + 1),
		ScaleSet: observation.ScaleSet, Repository: observation.Repository,
		ControllerEpoch: observation.ControllerEpoch, IdentityFree: true,
	}
	state.assignedJobAssignments = append(state.assignedJobAssignments, assignment)
	state.assignedJobEvents = append(state.assignedJobEvents, "assign-identity-free")
	return &assignment, nil
}

func (state *fakeClusterState) assignedJobsSnapshot() (
	[]cluster.AssignedJobObservation,
	[]cluster.Assignment,
	[]string,
) {
	state.assignedJobsMutex.Lock()
	defer state.assignedJobsMutex.Unlock()
	return append([]cluster.AssignedJobObservation(nil), state.assignedJobObservations...),
		append([]cluster.Assignment(nil), state.assignedJobAssignments...),
		append([]string(nil), state.assignedJobEvents...)
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

func (state *fakeClusterState) CompleteJob(_ context.Context, completion cluster.JobCompletion) error {
	state.completedJobs = append(state.completedJobs, completion)
	if completion.RequestID > 0 {
		state.removedDemand = append(state.removedDemand, removedClusterDemand{
			scaleSet: completion.ScaleSet, repository: completion.Repository, requestID: completion.RequestID,
		})
	}
	return state.removeErr
}

func (state *fakeClusterState) RegisterLibvirtRunner(
	_ context.Context,
	_ cluster.AssignmentID,
	runner cluster.LibvirtRunnerLifecycle,
) error {
	state.registeredLibvirtRunners = append(state.registeredLibvirtRunners, runner)
	return nil
}

func (state *fakeClusterState) PendingLibvirtReaps(context.Context, cluster.NodeID) ([]cluster.LibvirtRunnerLifecycle, error) {
	return append([]cluster.LibvirtRunnerLifecycle(nil), state.pendingLibvirtReaps...), state.pendingLibvirtReapsErr
}

func (state *fakeClusterState) ConfirmLibvirtReaped(_ context.Context, runner cluster.LibvirtRunnerLifecycle) error {
	state.confirmedLibvirtReaps = append(state.confirmedLibvirtReaps, runner)
	return state.confirmLibvirtReapErr
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
