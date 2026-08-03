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
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type inertAssignedJobBackend struct{}

func (inertAssignedJobBackend) Prepare(context.Context, int) (scaler.AssignedJobCapacity, error) {
	return scaler.AssignedJobCapacity{}, nil
}

func (inertAssignedJobBackend) StartAssigned(context.Context) (bool, error) { return false, nil }

func inertAssignedJobRecovery() *clusterAssignedJobRecovery {
	return &clusterAssignedJobRecovery{
		controller: scaler.NewAssignedJobController(inertAssignedJobBackend{}), logger: slog.Default(),
		wake: make(chan struct{}, 1),
	}
}

func TestClusterAssignedJobBackendReportsObservationAndAssignmentFailures(t *testing.T) {
	observationFailure := errors.New("observation unavailable")
	backend := assignedJobBackendForTest(&fakeClusterState{observeAssignedJobsErr: observationFailure})
	if _, err := backend.Prepare(context.Background(), 2); !errors.Is(err, observationFailure) {
		t.Fatalf("prepare error = %v, want observation failure", err)
	}

	backend = assignedJobBackendForTest(&fakeClusterState{assignAssignedJobErr: cluster.ErrNoAssignment})
	if started, err := backend.StartAssigned(context.Background()); err != nil || started {
		t.Fatalf("no-assignment start = %t, %v", started, err)
	}

	assignmentFailure := errors.New("assignment unavailable")
	backend = assignedJobBackendForTest(&fakeClusterState{assignAssignedJobErr: assignmentFailure})
	if started, err := backend.StartAssigned(context.Background()); !errors.Is(err, assignmentFailure) || started {
		t.Fatalf("failed start = %t, %v", started, err)
	}
}

func assignedJobBackendForTest(state *fakeClusterState) *clusterAssignedJobBackend {
	return &clusterAssignedJobBackend{
		state: state, repository: "repo", scaleSet: "linux", now: func() time.Time { return time.Unix(100, 0) },
	}
}

func TestClusterAssignedJobRecoveryFailureHasRetryStateAndNoDuplicateLegacyEvent(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{observeAssignedJobsErr: errors.New("unavailable")}
	recovery := &clusterAssignedJobRecovery{
		controller: scaler.NewAssignedJobController(assignedJobBackendForTest(state)),
		logger:     slog.New(slog.NewTextHandler(logs, nil)), repository: "repo", scaleSet: "linux",
	}
	recovery.remember(&upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1})
	recovery.reconcileLatest(context.Background(), "safety")
	output := logs.String()
	if !strings.Contains(output, "action=assigned_recovery phase=failed") || !strings.Contains(output, "retryAfter=5s") {
		t.Fatalf("assigned recovery failure log = %s", output)
	}
	if strings.Contains(output, "assigned_job_recovery") {
		t.Fatalf("obsolete duplicate recovery event = %s", output)
	}
}

func TestClusterAssignedJobRecoveryHandlesMissingStateAndStatistics(t *testing.T) {
	recovery := inertAssignedJobRecovery()
	recovery.reconcileLatest(context.Background(), "safety")
	if latest := recovery.latest(); latest != nil {
		t.Fatalf("latest statistics = %#v, want nil", latest)
	}
	if err := recovery.reconcile(context.Background(), nil, "session"); err == nil {
		t.Fatal("expected missing statistics failure")
	}
}

func TestClusterMessageHandlerRequiresStatisticsAndAssignedRecovery(t *testing.T) {
	handler := clusterMessageHandler{}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{}); err == nil {
		t.Fatal("expected missing statistics failure")
	}
	state := &fakeClusterState{}
	session := &repositoryScaleSetSession{key: repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}}
	handler = clusterMessageHandler{
		store: state, session: session,
		scheduler: &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}},
	}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
	}); err == nil {
		t.Fatal("expected missing assigned recovery failure")
	}
}

func TestClusterMessageHandlerReportsSchedulingFailure(t *testing.T) {
	expectedErr := errors.New("schedule unavailable")
	state := &fakeClusterState{assignErr: expectedErr}
	session := &repositoryScaleSetSession{
		key: repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}, assignedJobs: inertAssignedJobRecovery(),
	}
	handler := clusterMessageHandler{
		store: state, session: session,
		scheduler: &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}},
	}
	if err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
	}); !errors.Is(err, expectedErr) {
		t.Fatalf("scheduling error = %v, want %v", err, expectedErr)
	}
}

func TestClusterMessageReconcilesAssignedStatisticsWithoutAcquiringZeroIdentity(t *testing.T) {
	state := &fakeClusterState{assignedJobCapacity: scaler.AssignedJobCapacity{AvailableCapacity: 1}}
	observedAt := time.Unix(100, 0).UTC()
	handler, messageClient := assignedJobMessageHandler(state, observedAt)
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		MessageID: 9,
		Statistics: &upstream.RunnerScaleSetStatistic{
			TotalAssignedJobs: 1, TotalAvailableJobs: 2, TotalRegisteredRunners: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertAssignedObservation(t, state, observedAt)
	assertNoIdentityAcquisition(t, state, messageClient)
}

func assertAssignedObservation(t *testing.T, state *fakeClusterState, observedAt time.Time) {
	t.Helper()
	observations, _, _ := state.assignedJobsSnapshot()
	if len(observations) != 1 || observations[0] != (cluster.AssignedJobObservation{
		AppID: 7, Repository: "repo", ScaleSet: "linux", ControllerEpoch: 3,
		AssignedJobs: 1, ObservedAt: observedAt,
	}) {
		t.Fatalf("assigned-job observations = %#v", observations)
	}
}

func assertNoIdentityAcquisition(t *testing.T, state *fakeClusterState, messageClient *fakeMessageClient) {
	t.Helper()
	_, assignments, _ := state.assignedJobsSnapshot()
	if len(assignments) != 1 {
		t.Fatalf("assigned-job assignments = %#v", assignments)
	}
	if len(messageClient.acquiredIDs) != 0 {
		t.Fatalf("zero-identity jobs were acquired through GitHub = %#v", messageClient.acquiredIDs)
	}
}

func TestClusterMessageSchedulesExactDemandBeforeAggregateAssignedRecovery(t *testing.T) {
	state := &fakeClusterState{
		assignedJobCapacity: scaler.AssignedJobCapacity{AvailableCapacity: 1},
		assignments: []*cluster.Assignment{{
			ID: 8, NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", RequestID: 42,
		}},
	}
	handler, messageClient := assignedJobMessageHandler(state, time.Now())
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAvailableMessages: []*upstream.JobAvailable{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42, QueueTime: time.Unix(10, 0)},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertExactDemandPrecedesRecovery(t, state, messageClient)
}

func assignedJobMessageHandler(state *fakeClusterState, observedAt time.Time) (clusterMessageHandler, *fakeMessageClient) {
	daemon := clusteredDaemonForTest(state)
	daemon.assignments = state
	daemon.logger = slog.Default()
	leader := newClusterLeader(context.Background(), daemon, &fakeClusterLease{})
	leader.now = func() time.Time { return observedAt }
	leader.repositoryAppID = 7
	leader.repositoryControllerEpoch = 3
	messageClient := &fakeMessageClient{}
	session := &repositoryScaleSetSession{
		key: repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}, installationID: 11,
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 13}, messageClient: scaleSetMessageClient{Client: messageClient},
	}
	leader.prepareSessionRuntime(session)
	leader.scheduler.add(session)
	return clusterMessageHandler{store: state, scheduler: leader.scheduler, session: session, logger: daemon.logger}, messageClient
}

func assertExactDemandPrecedesRecovery(t *testing.T, state *fakeClusterState, messageClient *fakeMessageClient) {
	t.Helper()
	_, _, assignedJobEvents := state.assignedJobsSnapshot()
	if events := strings.Join(assignedJobEvents, ","); events != "enqueue,confirm-exact,observe-assigned,assign-identity-free" {
		t.Fatalf("cluster scheduling order = %q", events)
	}
	if len(messageClient.acquiredIDs) != 1 || messageClient.acquiredIDs[0] != 42 {
		t.Fatalf("acquired exact job IDs = %#v", messageClient.acquiredIDs)
	}
}

func TestClusterAssignedJobRecoveryUsesSafetyTick(t *testing.T) {
	state := &fakeClusterState{}
	daemon := clusteredDaemonForTest(state)
	daemon.assignments = state
	leader := newClusterLeader(context.Background(), daemon, &fakeClusterLease{})
	leader.repositoryAppID = 7
	leader.repositoryControllerEpoch = 3
	session := &repositoryScaleSetSession{
		key:            repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"},
		installationID: 11, runnerScaleSet: &upstream.RunnerScaleSet{ID: 13},
	}
	recovery := newClusterAssignedJobRecovery(leader, session)
	recovery.remember(&upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2})
	observed := make(chan struct{}, 1)
	state.onObserveAssignedJobs = func() { observed <- struct{}{} }
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time)
	done := make(chan struct{})
	go func() {
		defer close(done)
		recovery.run(ctx, ticks)
	}()
	ticks <- time.Unix(100, 0)
	<-observed
	cancel()
	<-done
	observations, _, _ := state.assignedJobsSnapshot()
	if len(observations) != 1 || observations[0].AssignedJobs != 2 {
		t.Fatalf("safety observations = %#v", observations)
	}
}

func TestSuccessfulClusterHeartbeatSignalsIndependentAssignedJobRecovery(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{alive: true})
	blocked := inertAssignedJobRecovery()
	blocked.signal()
	independent := inertAssignedJobRecovery()
	leader.sessions[repositoryScaleSetSessionKey{repository: "blocked", scaleSet: "linux"}] =
		&repositoryScaleSetSession{assignedJobs: blocked}
	leader.sessions[repositoryScaleSetSessionKey{repository: "independent", scaleSet: "windows"}] =
		&repositoryScaleSetSession{assignedJobs: independent}
	daemon.leader = leader
	daemon.heartbeat(context.Background())
	select {
	case <-independent.wake:
	default:
		t.Fatal("successful heartbeat did not signal independent assigned-job recovery")
	}
}
