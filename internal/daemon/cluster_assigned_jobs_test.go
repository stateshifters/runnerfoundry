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

func inertAssignedJobRecovery() *clusterAssignedJobRecovery {
	return &clusterAssignedJobRecovery{
		backend: &clusterAssignedJobBackend{
			state: &fakeClusterState{}, appID: 1, repository: "repo", scaleSet: "linux",
			controllerEpoch: 1, now: func() time.Time { return time.Unix(100, 0) },
		},
		logger: slog.Default(), repository: "repo", scaleSet: "linux",
	}
}

func TestClusterAssignedJobBackendReportsObservationFailure(t *testing.T) {
	observationFailure := errors.New("observation unavailable")
	backend := &clusterAssignedJobBackend{
		state: &fakeClusterState{observeAssignedJobsErr: observationFailure}, appID: 1,
		repository: "repo", scaleSet: "linux", controllerEpoch: 1,
		now: func() time.Time { return time.Unix(100, 0) },
	}
	if _, err := backend.Prepare(context.Background(), 2); !errors.Is(err, observationFailure) {
		t.Fatalf("prepare error = %v, want observation failure", err)
	}
}

func TestScaleSetStatisticsUsesIngestionSafeTriggerField(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	logScaleSetSessionStatistics(
		context.Background(), logger, "repo", "linux", "message", &upstream.RunnerScaleSetStatistic{},
	)
	output := logs.String()
	if !strings.Contains(output, `"trigger":"message"`) {
		t.Fatalf("session statistics trigger = %s", output)
	}
	if strings.Contains(output, `"source":"message"`) {
		t.Fatalf("session statistics used reserved source field = %s", output)
	}
}

func TestClusterAssignedJobRecoveryHandlesMissingState(t *testing.T) {
	recovery := inertAssignedJobRecovery()
	ctx, cancel := context.WithCancel(context.Background())
	recovery.start(ctx)
	cancel()
	if latest := recovery.latest(); latest != nil {
		t.Fatalf("latest statistics = %#v, want nil", latest)
	}
}

func TestClusterAssignedJobRecoveryRequiresDurableBackend(t *testing.T) {
	recovery := inertAssignedJobRecovery()
	recovery.backend = nil
	statistics := &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1}
	if err := recovery.observe(context.Background(), statistics, "message"); err == nil ||
		!strings.Contains(err.Error(), "observation backend is required") {
		t.Fatalf("observe() error = %v, want missing backend failure", err)
	}
	if latest := recovery.latest(); latest == nil || latest.TotalAssignedJobs != 1 {
		t.Fatalf("latest statistics = %#v, want remembered observation", latest)
	}
}

func TestClusterAssignedJobRecoveryReportsDurableObservationFailure(t *testing.T) {
	persistenceFailure := errors.New("observation unavailable")
	recovery := inertAssignedJobRecovery()
	recovery.backend.state = &fakeClusterState{observeAssignedJobsErr: persistenceFailure}
	if err := recovery.observe(
		context.Background(), &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1}, "message",
	); !errors.Is(err, persistenceFailure) {
		t.Fatalf("observe() error = %v, want %v", err, persistenceFailure)
	}
}

func TestClusterAssignedJobRecoveryFailureLogIncludesRetryContext(t *testing.T) {
	logs := &bytes.Buffer{}
	recovery := inertAssignedJobRecovery()
	recovery.logger = slog.New(slog.NewTextHandler(logs, nil))
	recovery.logStatistics(
		context.Background(), &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		"message", "failed", errors.New("observation unavailable"),
	)
	output := logs.String()
	if !strings.Contains(output, "assigned_recovery") || !strings.Contains(output, "retryAfter=") ||
		!strings.Contains(output, "observation unavailable") {
		t.Fatalf("assigned recovery failure log = %q", output)
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

func TestClusterMessageAcknowledgementWaitsForDurableAssignedObligations(t *testing.T) {
	state := &fakeClusterState{}
	entered := make(chan struct{})
	release := make(chan struct{})
	state.onObserveAssignedJobs = func() {
		close(entered)
		<-release
	}
	recovery := inertAssignedJobRecovery()
	recovery.backend.state = state
	handler := clusterMessageHandler{
		store: state,
		session: &repositoryScaleSetSession{
			key: repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}, assignedJobs: recovery,
		},
		scheduler: &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}},
	}
	result := make(chan error, 1)
	go func() {
		result <- handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
			Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		})
	}()
	<-entered
	select {
	case err := <-result:
		t.Fatalf("message completed before obligations were durable: %v", err)
	default:
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
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
	handler, messageClient := assignedJobMessageHandler(t, state, observedAt)
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		MessageID: 9,
		Statistics: &upstream.RunnerScaleSetStatistic{
			TotalAssignedJobs: 1, TotalAvailableJobs: 2, TotalRegisteredRunners: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	awaitAssignedJobEventCount(t, state, 2)
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
	handler, messageClient := assignedJobMessageHandler(t, state, time.Now())
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAvailableMessages: []*upstream.JobAvailable{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42, QueueTime: time.Unix(10, 0)},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	awaitAssignedJobEventCount(t, state, 4)
	assertExactDemandPrecedesRecovery(t, state, messageClient)
}

func assignedJobMessageHandler(
	t *testing.T,
	state *fakeClusterState,
	observedAt time.Time,
) (clusterMessageHandler, *fakeMessageClient) {
	t.Helper()
	daemon := clusteredDaemonForTest(state)
	daemon.assignments = state
	daemon.logger = slog.Default()
	leader := newClusterLeader(context.Background(), daemon, &fakeClusterLease{})
	t.Cleanup(func() {
		leader.stopRepositoryWorkers()
		leader.repositoryWorkers.wait()
		leader.scheduler.wait()
	})
	leader.now = func() time.Time { return observedAt }
	leader.repositoryAppID = 7
	leader.repositoryControllerEpoch = 3
	runtimeCtx, stopRuntime := context.WithCancel(context.Background())
	messageClient := &fakeMessageClient{}
	session := &repositoryScaleSetSession{
		key: repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}, installationID: 11,
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 13}, messageClient: scaleSetMessageClient{Client: messageClient},
	}
	leader.prepareSessionRuntime(session)
	session.assignedJobs.start(runtimeCtx)
	leader.scheduler.add(session)
	t.Cleanup(stopRuntime)
	return clusterMessageHandler{store: state, scheduler: leader.scheduler, session: session, logger: daemon.logger}, messageClient
}

func awaitAssignedJobEventCount(t *testing.T, state *fakeClusterState, expected int) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		_, _, events := state.assignedJobsSnapshot()
		if len(events) >= expected {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("assigned-job events = %#v, want at least %d", events, expected)
		case <-time.After(time.Millisecond):
		}
	}
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

func TestClusterAssignedJobRecoveryPersistsBeforeScheduling(t *testing.T) {
	state := &fakeClusterState{}
	daemon := clusteredDaemonForTest(state)
	daemon.assignments = state
	ctx, cancel := context.WithCancel(context.Background())
	leader := newClusterLeader(ctx, daemon, &fakeClusterLease{})
	defer func() {
		cancel()
		leader.stopRepositoryWorkers()
		leader.scheduler.wait()
		leader.repositoryWorkers.wait()
	}()
	leader.repositoryAppID = 7
	leader.repositoryControllerEpoch = 3
	session := &repositoryScaleSetSession{
		key:            repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"},
		installationID: 11, runnerScaleSet: &upstream.RunnerScaleSet{ID: 13},
	}
	recovery := newClusterAssignedJobRecovery(leader, session)
	observed := make(chan struct{}, 1)
	state.onObserveAssignedJobs = func() { observed <- struct{}{} }
	if err := recovery.prepareInitial(ctx, &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2}); err != nil {
		t.Fatal(err)
	}
	recovery.start(ctx)
	<-observed
	observations, _, _ := state.assignedJobsSnapshot()
	if len(observations) != 1 || observations[0].AssignedJobs != 2 {
		t.Fatalf("safety observations = %#v", observations)
	}
}

func TestSuccessfulClusterHeartbeatWakesUnifiedObligationScheduling(t *testing.T) {
	state := &fakeClusterState{assignments: []*cluster.Assignment{{
		ID: 1, Repository: "independent", ScaleSet: "windows", IdentityFree: true,
	}}}
	daemon := activeClusteredDaemon(t, state)
	ctx, cancel := context.WithCancel(context.Background())
	leader := newClusterLeader(ctx, &daemon, &fakeClusterLease{alive: true})
	t.Cleanup(func() {
		cancel()
		leader.stopRepositoryWorkers()
		leader.repositoryWorkers.wait()
		leader.scheduler.wait()
	})
	key := repositoryScaleSetSessionKey{repository: "independent", scaleSet: "windows"}
	leader.sessions[key] = &repositoryScaleSetSession{key: key}
	daemon.leader = leader
	daemon.heartbeat(context.Background())
	deadline := time.Now().Add(time.Second)
	for {
		_, assignments, _ := state.assignedJobsSnapshot()
		if len(assignments) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("successful heartbeat did not wake unified obligation scheduling")
		}
		time.Sleep(time.Millisecond)
	}
}
