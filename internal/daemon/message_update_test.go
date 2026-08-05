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

func TestClusterMessagePersistsAssignedCapacityStatistics(t *testing.T) {
	state := &messageUpdateHandlerState{fakeClusterState: &fakeClusterState{}}
	handler := messageUpdateClusterHandler(state)
	message := &upstream.RunnerScaleSetMessage{Statistics: &upstream.RunnerScaleSetStatistic{
		TotalAssignedJobs: 2, TotalRunningJobs: 3,
		TotalRegisteredRunners: 4, TotalBusyRunners: 5,
	}}

	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if state.update.AssignedJobs != 2 || state.update.RunningJobs != 3 ||
		state.update.RegisteredRunners != 4 || state.update.BusyRunners != 5 {
		t.Fatalf("persisted statistics = %#v", state.update)
	}
}

func TestClusterMessageReportsCommittedCapacityObligationState(t *testing.T) {
	oldestRunnableAt := time.Unix(90, 0).UTC()
	logs, handler := capacityObligationMessageHandler(oldestRunnableAt)
	message := &upstream.RunnerScaleSetMessage{Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 4}}

	if err := handler.HandleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	assertCapacityObligationMessageLog(t, logs.String())
}

func TestStrandedAssignmentLogsOnlyStateTransitions(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	update := scaler.MessageUpdate{
		Repository: "repo", ScaleSet: "windows", ControllerEpoch: 1,
		AssignedJobs: 1, BusyRunners: 1, ObservedAt: time.Unix(100, 0).UTC(),
	}
	logCapacityObligationState(context.Background(), logger, 1, update, scaler.MessageUpdateResult{
		AssignedObligations: scaler.AssignedObligationDecision{Block: 1},
	})
	logCapacityObligationState(context.Background(), logger, 1, update, scaler.MessageUpdateResult{})
	update.BusyRunners = 0
	logCapacityObligationState(context.Background(), logger, 1, update, scaler.MessageUpdateResult{
		AssignedObligations: scaler.AssignedObligationDecision{Unblock: 1},
	})

	logged := logs.String()
	if strings.Count(logged, `"action":"assigned_job_stranded"`) != 2 ||
		!strings.Contains(logged, `"phase":"blocked"`) || !strings.Contains(logged, `"phase":"recovered"`) {
		t.Fatalf("stranded transition logs = %s", logged)
	}
}

func capacityObligationMessageHandler(oldestRunnableAt time.Time) (*bytes.Buffer, clusterMessageHandler) {
	state := &messageUpdateHandlerState{
		fakeClusterState: &fakeClusterState{},
		result: scaler.MessageUpdateResult{
			AssignedObligations: scaler.AssignedObligationDecision{Create: 2, Cancel: 1},
			QueueSnapshot: scaler.CapacityObligationQueueSnapshot{
				JobAvailable: 3, AssignedCount: 4, NewlyQueued: 2, Canceled: 1,
				OldestRunnableAt: &oldestRunnableAt,
			},
		},
	}
	logs := &bytes.Buffer{}
	handler := messageUpdateClusterHandler(state)
	handler.logger = slog.New(slog.NewJSONHandler(logs, nil))
	handler.session.assignedJobs.controllerEpoch = 1
	handler.session.assignedJobs.generation = 5
	return logs, handler
}

func assertCapacityObligationMessageLog(t *testing.T, logged string) {
	t.Helper()
	for _, expected := range []string{
		`"action":"capacity_obligation"`, `"phase":"reconciled"`, `"repository":"repo"`,
		`"scaleSet":"linux"`, `"jobAvailable":3`, `"assignedCount":4`, `"newlyQueued":2`,
		`"canceled":1`, `"createdAssigned":2`, `"canceledAssigned":1`, `"epoch":1`,
		`"generation":5`,
	} {
		if !strings.Contains(logged, expected) {
			t.Fatalf("capacity obligation log %q does not contain %q", logged, expected)
		}
	}
}

type messageUpdateHandlerState struct {
	*fakeClusterState
	applyErr       error
	result         scaler.MessageUpdateResult
	applyCalls     int
	scheduleCalls  int
	committed      bool
	boundedContext bool
	update         scaler.MessageUpdate
}

func (state *messageUpdateHandlerState) ApplyMessageUpdate(
	ctx context.Context,
	update scaler.MessageUpdate,
) (scaler.MessageUpdateResult, error) {
	state.applyCalls++
	state.update = update
	deadline, ok := ctx.Deadline()
	state.boundedContext = ok && time.Until(deadline) <= messageUpdateTimeout && time.Until(deadline) > 0
	if state.applyErr != nil {
		return scaler.MessageUpdateResult{}, state.applyErr
	}
	state.committed = true
	return state.result, nil
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
