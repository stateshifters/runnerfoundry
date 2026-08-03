package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/scaler"
	scalesetpoller "github.com/meza/RunnerFoundry/internal/scaleset"
)

func TestStaleClusterMessageDoesNotScheduleOrAcknowledge(t *testing.T) {
	assertClusterMessageFailure(t, scaler.ErrNotLeader)
}

func TestTransientClusterMessageFailureDoesNotScheduleOrAcknowledge(t *testing.T) {
	assertClusterMessageFailure(t, errors.New("database unavailable"))
}

func assertClusterMessageFailure(t *testing.T, failure error) {
	t.Helper()
	state := &messageUpdateHandlerState{fakeClusterState: &fakeClusterState{}, applyErr: failure}
	handler := messageUpdateClusterHandler(state)
	client := &ackRecordingMessageClient{message: &upstream.RunnerScaleSetMessage{
		MessageID: 11, Statistics: &upstream.RunnerScaleSetStatistic{},
	}}
	poller := scalesetpoller.Poller{Client: client, Handler: handler}

	err := poller.PollOnce(context.Background())
	if !errors.Is(err, failure) {
		t.Fatalf("poll error = %v, want %v", err, failure)
	}
	if state.applyCalls != 1 || state.scheduleCalls != 0 || len(client.acknowledged) != 0 {
		t.Fatalf("apply=%d schedule=%d acknowledgements=%#v", state.applyCalls, state.scheduleCalls, client.acknowledged)
	}
	if !state.boundedContext {
		t.Fatal("message update did not receive a bounded context")
	}
}

func TestClusterMessageSchedulesOnlyAfterAtomicCommit(t *testing.T) {
	state := &messageUpdateHandlerState{fakeClusterState: &fakeClusterState{}}
	handler := messageUpdateClusterHandler(state)
	message := &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: 42, QueueTime: time.Unix(100, 0).UTC(),
		}}},
	}

	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if state.applyCalls != 1 || state.scheduleCalls != 1 || !state.committed {
		t.Fatalf("apply=%d schedule=%d committed=%t", state.applyCalls, state.scheduleCalls, state.committed)
	}
}

type messageUpdateHandlerState struct {
	*fakeClusterState
	applyErr       error
	applyCalls     int
	scheduleCalls  int
	committed      bool
	boundedContext bool
}

func (state *messageUpdateHandlerState) ApplyMessageUpdate(
	ctx context.Context,
	_ scaler.MessageUpdate,
) (scaler.MessageUpdateResult, error) {
	state.applyCalls++
	deadline, ok := ctx.Deadline()
	state.boundedContext = ok && time.Until(deadline) <= messageUpdateTimeout && time.Until(deadline) > 0
	if state.applyErr != nil {
		return scaler.MessageUpdateResult{}, state.applyErr
	}
	state.committed = true
	return scaler.MessageUpdateResult{}, nil
}

func (state *messageUpdateHandlerState) AssignOldest(
	context.Context,
	string,
	time.Time,
	time.Duration,
) (*cluster.Assignment, error) {
	state.scheduleCalls++
	if !state.committed {
		return nil, errors.New("scheduled before message commit")
	}
	return nil, cluster.ErrNoAssignment
}

func messageUpdateClusterHandler(state *messageUpdateHandlerState) clusterMessageHandler {
	session := &repositoryScaleSetSession{
		key:          repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"},
		assignedJobs: inertAssignedJobRecovery(),
	}
	return clusterMessageHandler{
		store: state, session: session,
		scheduler: &clusterScheduler{
			store: state,
			sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{
				session.key: session,
			},
		},
	}
}
