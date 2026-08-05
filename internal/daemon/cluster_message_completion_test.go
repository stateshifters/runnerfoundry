package daemon

import (
	"bytes"
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
	scalesetpoller "github.com/meza/RunnerFoundry/internal/scaleset"
)

func TestClusterPollerAcknowledgesRunnerNameCompletionOnce(t *testing.T) {
	messageClient := &persistentCompletionMessageClient{message: &upstream.RunnerScaleSetMessage{
		MessageID:  27,
		Statistics: &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{
			RunnerName:     "linux-legacy",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 0},
		}},
	}}
	poller := scalesetpoller.Poller{
		Client:  messageClient,
		Handler: clusterMessageHandlerForTest(&fakeClusterState{}),
	}

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("expected runner-name completion to be acknowledged, got %v", err)
	}
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("expected acknowledged completion not to be redelivered, got %v", err)
	}
	if messageClient.getCalls != 2 || messageClient.deleteCalls != 1 || poller.LastMessageID != 27 {
		t.Fatalf("polls=%d acknowledgements=%d cursor=%d", messageClient.getCalls, messageClient.deleteCalls, poller.LastMessageID)
	}
}

func TestClusterMessageReconcilesIdentityFreeCompletionByRunnerName(t *testing.T) {
	state := &fakeClusterState{}
	handler := clusterMessageHandlerForTest(state)

	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{
			RunnerName:     "linux-legacy",
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 0},
		}},
	})
	if err != nil {
		t.Fatalf("expected identity-free completion reconciliation, got %v", err)
	}
	if len(state.completedJobs) != 1 || state.completedJobs[0].RunnerName != "linux-legacy" ||
		state.completedJobs[0].RequestID != 0 {
		t.Fatalf("completion reconciliation = %#v", state.completedJobs)
	}
	if len(state.removedDemand) != 0 || len(state.released) != 0 {
		t.Fatalf("identity-free completion mutated demand or assignments: demand=%#v assignments=%#v", state.removedDemand, state.released)
	}
}

func TestClusterMessageReportsRunnerNameCompletionAndDropsOnlyUnidentifiedCompletions(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{}
	handler := clusterMessageHandlerForTest(state)
	handler.logger = newClusterLogSnapshotLogger(logs)

	err := handler.HandleMessage(context.Background(), &upstream.RunnerScaleSetMessage{
		Statistics: &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{
			{
				RunnerID:       91,
				RunnerName:     "linux-legacy",
				JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 0},
			},
			{},
			nil,
		},
	})
	if err != nil {
		t.Fatalf("expected invalid completions to be dropped, got %v", err)
	}
	if len(state.completedJobs) != 1 || len(state.removedDemand) != 0 || len(state.released) != 0 {
		t.Fatalf("unexpected clustered mutations: completions=%#v demand=%#v assignments=%#v",
			state.completedJobs, state.removedDemand, state.released)
	}
	assertClusterLogSnapshot(t, logs, `{"level":"INFO","msg":"cluster_job_completion","action":"cluster_job_completion","phase":"completed","repository":"repo","scaleSet":"linux","messageType":"JobCompleted","requestID":0,"runner":"linux-legacy","runnerID":91,"identity":"runner_name"}
{"level":"WARN","msg":"cluster_job_completion_invalid","action":"cluster_job_completion_invalid","phase":"dropped","repository":"repo","scaleSet":"linux","messageType":"JobCompleted","requestID":0,"runner":"","runnerID":0}
{"level":"WARN","msg":"cluster_job_completion_invalid","action":"cluster_job_completion_invalid","phase":"dropped","repository":"repo","scaleSet":"linux","messageType":"JobCompleted","requestID":0,"runner":"","runnerID":0}
{"level":"INFO","msg":"capacity_obligation","action":"capacity_obligation","phase":"reconciled","repository":"repo","scaleSet":"linux","jobAvailable":0,"assignedCount":0,"newlyQueued":0,"canceled":0,"createdAssigned":0,"canceledAssigned":0,"blockedAssigned":0,"unblockedAssigned":0,"assignedJobs":0,"runningJobs":0,"registeredRunners":0,"busyRunners":0,"strandedAssigned":0,"runnableAssigned":0,"generation":0,"epoch":1}`)
}

func TestClusterPollerRetriesTransientRunnerNameCompletionReconciliationFailure(t *testing.T) {
	state := &fakeClusterState{removeErr: errors.New("database unavailable")}
	messageClient := &persistentCompletionMessageClient{message: &upstream.RunnerScaleSetMessage{
		MessageID:            28,
		Statistics:           &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: "linux-legacy"}},
	}}
	poller := scalesetpoller.Poller{Client: messageClient, Handler: clusterMessageHandlerForTest(state)}

	if err := poller.PollOnce(context.Background()); err == nil {
		t.Fatal("expected transient reconciliation failure")
	}
	if messageClient.deleteCalls != 0 || poller.LastMessageID != 0 {
		t.Fatalf("failed reconciliation acknowledgements=%d cursor=%d", messageClient.deleteCalls, poller.LastMessageID)
	}

	state.removeErr = nil
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("expected redelivered completion to succeed, got %v", err)
	}
	if messageClient.deleteCalls != 1 || poller.LastMessageID != 28 {
		t.Fatalf("successful retry acknowledgements=%d cursor=%d", messageClient.deleteCalls, poller.LastMessageID)
	}
}

type persistentCompletionMessageClient struct {
	message      *upstream.RunnerScaleSetMessage
	acknowledged bool
	getCalls     int
	deleteCalls  int
}

func (client *persistentCompletionMessageClient) GetMessage(
	context.Context,
	int,
	int,
) (*upstream.RunnerScaleSetMessage, error) {
	client.getCalls++
	if client.acknowledged {
		return nil, scalesetpoller.ErrNoMessage
	}
	return client.message, nil
}

func (client *persistentCompletionMessageClient) DeleteMessage(context.Context, int) error {
	client.deleteCalls++
	client.acknowledged = true
	return nil
}
