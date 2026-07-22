package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
)

func TestClusterMessageHandlerPersistsDemandBeforeScheduling(t *testing.T) {
	now := time.Now()
	state := &fakeClusterState{assignments: []*cluster.Assignment{{
		ID: 8, NodeID: "spock", ScaleSet: "linux", Repository: "repo", RequestID: 9,
	}}}
	messageClient := &fakeMessageClient{}
	session := &repositoryScaleSetSession{
		key: repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}, installationID: 3,
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 4}, messageClient: scaleSetMessageClient{Client: messageClient},
	}
	scheduler := &clusterScheduler{store: state, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{session.key: session}}
	handler := clusterMessageHandler{store: state, scheduler: scheduler, session: session}
	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 9, QueueTime: now}}},
		JobStartedMessages:   []*upstream.JobStarted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 7}}},
		JobCompletedMessages: []*upstream.JobCompleted{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 6}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(state.demand) != 1 || len(state.removedDemand) != 2 || len(state.ready) != 1 || len(messageClient.acquiredIDs) != 1 {
		t.Fatalf("cluster message was not applied atomically before scheduling: %#v", state)
	}
}

func TestAcquisitionConfirmationRetryDoesNotAcquireTwice(t *testing.T) {
	state := &fakeClusterState{
		assignments: []*cluster.Assignment{{
			ID: 8, NodeID: "spock", State: cluster.AssignmentReserved,
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
		{ID: 2, NodeID: "spock", ScaleSet: "linux", Repository: "repo", RequestID: 9, State: cluster.AssignmentAcquiring},
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
		ID: 2, NodeID: "spock", ScaleSet: "linux", Repository: "repo", State: cluster.AssignmentAcquiring,
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
		ID: 2, NodeID: "spock", ScaleSet: "linux", Repository: "repo", State: cluster.AssignmentAcquiring,
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
