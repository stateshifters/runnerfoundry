package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/githubapp"
)

type parallelClusterSchedulingState struct {
	*fakeClusterState
	mutex       sync.Mutex
	assignments map[string][]*cluster.Assignment
}

func (state *parallelClusterSchedulingState) AssignOldest(
	_ context.Context,
	scaleSet string,
	_ time.Time,
	_ time.Duration,
) (*cluster.Assignment, error) {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	assignments := state.assignments[scaleSet]
	if len(assignments) == 0 {
		return nil, cluster.ErrNoAssignment
	}
	assignment := assignments[0]
	state.assignments[scaleSet] = assignments[1:]
	return assignment, nil
}

func (state *parallelClusterSchedulingState) BeginAcquisition(
	ctx context.Context,
	assignmentID cluster.AssignmentID,
	installationID githubapp.InstallationID,
	githubScaleSetID int,
) error {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	return state.fakeClusterState.BeginAcquisition(ctx, assignmentID, installationID, githubScaleSetID)
}

func (state *parallelClusterSchedulingState) ConfirmAcquisition(
	ctx context.Context,
	assignmentID cluster.AssignmentID,
) error {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	return state.fakeClusterState.ConfirmAcquisition(ctx, assignmentID)
}

type blockingAcquisitionMessageClient struct {
	fakeMessageClient
	entered chan struct{}
	release <-chan struct{}
}

func (client *blockingAcquisitionMessageClient) AcquireJobs(
	context.Context,
	[]int64,
) ([]int64, error) {
	close(client.entered)
	<-client.release
	return []int64{1}, nil
}

func TestClusterMessageHandlerPersistsDemandBeforeScheduling(t *testing.T) {
	now := time.Now()
	handler, state, messageClient := clusterDemandTestHandler()
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 9, QueueTime: now}}},
		JobStartedMessages:   []*upstream.JobStarted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 7}}},
		JobCompletedMessages: []*upstream.JobCompleted{{Result: "failed", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 6}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertClusterDemandApplied(t, state, messageClient)
}

func clusterDemandTestHandler() (clusterMessageHandler, *fakeClusterState, *fakeMessageClient) {
	state := &fakeClusterState{assignments: []*cluster.Assignment{{
		ID: 8, NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", RequestID: 9,
	}}}
	messageClient := &fakeMessageClient{}
	session := &repositoryScaleSetSession{
		key: repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}, installationID: 3,
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 4}, messageClient: scaleSetMessageClient{Client: messageClient},
		assignedJobs: inertAssignedJobRecovery(),
	}
	scheduler := &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{session.key: session}}
	return clusterMessageHandler{store: state, scheduler: scheduler, session: session}, state, messageClient
}

func assertClusterDemandApplied(t *testing.T, state *fakeClusterState, messageClient *fakeMessageClient) {
	t.Helper()
	if len(state.demand) != 1 || len(state.removedDemand) != 1 || len(state.ready) != 1 || len(messageClient.acquiredIDs) != 1 {
		t.Fatalf("cluster message was not applied atomically before scheduling: %#v", state)
	}
	if state.removedDemand[0].requestID != 6 {
		t.Fatalf("completed demands = %#v, want only request 6", state.removedDemand)
	}
}

func TestClusterSchedulerAllowsUnrelatedScaleSetsToAcquireConcurrently(t *testing.T) {
	fixture := newParallelAcquisitionFixture()
	linuxDone := scheduleAsync(fixture.scheduler, "linux")
	<-fixture.linuxClient.entered
	windowsDone := scheduleAsync(fixture.scheduler, "windows")
	awaitAcquisition(t, fixture.windowsClient.entered)
	close(fixture.linuxRelease)
	assertScheduleSucceeded(t, linuxDone)
	close(fixture.windowsRelease)
	assertScheduleSucceeded(t, windowsDone)
}

type parallelAcquisitionFixture struct {
	scheduler      *clusterScheduler
	linuxClient    *blockingAcquisitionMessageClient
	windowsClient  *blockingAcquisitionMessageClient
	linuxRelease   chan struct{}
	windowsRelease chan struct{}
}

func newParallelAcquisitionFixture() parallelAcquisitionFixture {
	linuxRelease := make(chan struct{})
	windowsRelease := make(chan struct{})
	linuxClient := &blockingAcquisitionMessageClient{entered: make(chan struct{}), release: linuxRelease}
	windowsClient := &blockingAcquisitionMessageClient{entered: make(chan struct{}), release: windowsRelease}
	state := &parallelClusterSchedulingState{
		fakeClusterState: &fakeClusterState{},
		assignments: map[string][]*cluster.Assignment{
			"linux":   {{ID: 1, Repository: "repo", ScaleSet: "linux", RequestID: 1}},
			"windows": {{ID: 2, Repository: "repo", ScaleSet: "windows", RequestID: 1}},
		},
	}
	linuxKey := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	windowsKey := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "windows"}
	scheduler := &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{
		linuxKey: {
			key: linuxKey, runnerScaleSet: &upstream.RunnerScaleSet{ID: 1},
			messageClient: scaleSetMessageClient{Client: linuxClient},
		},
		windowsKey: {
			key: windowsKey, runnerScaleSet: &upstream.RunnerScaleSet{ID: 2},
			messageClient: scaleSetMessageClient{Client: windowsClient},
		},
	}}
	return parallelAcquisitionFixture{
		scheduler: scheduler, linuxClient: linuxClient, windowsClient: windowsClient,
		linuxRelease: linuxRelease, windowsRelease: windowsRelease,
	}
}

func scheduleAsync(scheduler *clusterScheduler, scaleSet string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- scheduler.schedule(context.Background(), scaleSet) }()
	return done
}

func awaitAcquisition(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("unrelated scale set was blocked by the active Linux acquisition")
	}
}

func assertScheduleSucceeded(t *testing.T, done <-chan error) {
	t.Helper()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAcquisitionConfirmationRetryDoesNotAcquireTwice(t *testing.T) {
	state := &fakeClusterState{
		assignments: []*cluster.Assignment{{
			ID: 8, NodeID: "runner-host-1", State: cluster.AssignmentReserved,
			ScaleSet: "linux", Repository: "repo", RequestID: 9,
		}},
		confirmFailuresRemaining: 1,
		confirmFailure:           errors.New("temporary database failure"),
	}
	messageClient := &fakeMessageClient{}
	session := &repositoryScaleSetSession{
		key:            repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"},
		installationID: 3, runnerScaleSet: &upstream.RunnerScaleSet{ID: 4},
		messageClient: scaleSetMessageClient{Client: messageClient},
	}
	scheduler := &clusterScheduler{
		store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{session.key: session},
		acquisitionConfirmationWait: time.Millisecond,
	}
	if _, err := scheduler.scheduleOne(context.Background(), "linux"); err != nil {
		t.Fatal(err)
	}
	if len(messageClient.acquiredIDs) != 1 || len(state.ready) != 2 {
		t.Fatalf("acquisitions=%v confirmations=%v", messageClient.acquiredIDs, state.ready)
	}
}

func TestPendingAcquisitionRecoveryReacquiresOnlyMatchingScaleSet(t *testing.T) {
	state := &fakeClusterState{pendingAssignments: []cluster.Assignment{
		{ID: 1, ScaleSet: "other", State: cluster.AssignmentAcquiring},
		{ID: 2, NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", RequestID: 9, State: cluster.AssignmentAcquiring},
	}}
	messageClient := &fakeMessageClient{}
	session := &repositoryScaleSetSession{
		key:           repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"},
		messageClient: scaleSetMessageClient{Client: messageClient}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 4},
	}
	scheduler := &clusterScheduler{
		store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{session.key: session},
		acquisitionConfirmationWait: time.Millisecond,
	}
	if err := scheduler.schedule(context.Background(), "linux"); err != nil {
		t.Fatal(err)
	}
	if len(messageClient.acquiredIDs) != 1 || messageClient.acquiredIDs[0] != 9 || len(state.ready) != 1 {
		t.Fatalf("acquired=%v confirmed=%v", messageClient.acquiredIDs, state.ready)
	}
}

func TestPendingAcquisitionWithoutSessionIsCompleted(t *testing.T) {
	state := &fakeClusterState{pendingAssignments: []cluster.Assignment{{
		ID: 2, NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", State: cluster.AssignmentAcquiring,
	}}}
	scheduler := &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}}
	if err := scheduler.schedule(context.Background(), "linux"); err != nil {
		t.Fatal(err)
	}
	if len(state.released) != 1 || state.released[0] != 2 {
		t.Fatalf("completed assignments = %v", state.released)
	}
}

func TestPendingAcquisitionLoadFailureIsReturned(t *testing.T) {
	failure := errors.New("failed")
	state := &fakeClusterState{pendingErr: failure}
	scheduler := &clusterScheduler{store: state}
	if err := scheduler.schedule(context.Background(), "linux"); !errors.Is(err, failure) {
		t.Fatalf("schedule() error = %v, want %v", err, failure)
	}
}

func TestPendingAcquisitionReleaseFailureIsReturned(t *testing.T) {
	failure := errors.New("failed")
	state := &fakeClusterState{pendingAssignments: []cluster.Assignment{{
		ID: 2, NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", State: cluster.AssignmentAcquiring,
	}}, releaseErr: failure}
	scheduler := &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}}
	if err := scheduler.schedule(context.Background(), "linux"); !errors.Is(err, failure) {
		t.Fatalf("schedule() error = %v, want %v", err, failure)
	}
}

func TestAcquisitionRejectsInvalidAssignmentState(t *testing.T) {
	session := &repositoryScaleSetSession{runnerScaleSet: &upstream.RunnerScaleSet{ID: 4}}
	state := &fakeClusterState{}
	scheduler := &clusterScheduler{store: state}
	if err := scheduler.acquireAssignment(context.Background(), &cluster.Assignment{State: cluster.AssignmentRunning}, session); err == nil {
		t.Fatal("running assignment was accepted for acquisition")
	}
}

func TestAcquisitionReportsBeginTransitionFailure(t *testing.T) {
	failure := errors.New("failed")
	session := &repositoryScaleSetSession{runnerScaleSet: &upstream.RunnerScaleSet{ID: 4}}
	state := &fakeClusterState{readyErr: failure}
	scheduler := &clusterScheduler{store: state}
	if err := scheduler.acquireAssignment(context.Background(), &cluster.Assignment{ID: 1}, session); !errors.Is(err, failure) {
		t.Fatalf("acquireAssignment() error = %v, want %v", err, failure)
	}
}

func TestAcquisitionConfirmationStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	state := &fakeClusterState{readyErr: errors.New("database unavailable")}
	scheduler := &clusterScheduler{
		store: state,
		waitBeforeConfirmationRetry: func(context.Context, time.Duration) bool {
			cancel()
			return false
		},
	}
	if err := scheduler.confirmAcquisition(ctx, 1); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("confirmAcquisition() error = %v, want cancellation", err)
	}
}

func TestAcquisitionConfirmationReportsAlreadyCanceledContext(t *testing.T) {
	state := &fakeClusterState{readyErr: errors.New("database unavailable")}
	scheduler := &clusterScheduler{store: state}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := scheduler.confirmAcquisition(ctx, 1); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("confirmAcquisition() error = %v, want cancellation", err)
	}
}
