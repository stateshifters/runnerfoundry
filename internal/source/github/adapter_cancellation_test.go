package github_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestAdapterDoesNotHandleMessageReceivedByCancelledPoll(t *testing.T) {
	t.Parallel()

	received := make(chan struct{})
	releaseReceive := make(chan struct{})
	client := &cancellationMessageClient{
		message:        acquiredAvailableMessage(17, 41),
		received:       received,
		releaseReceive: releaseReceive,
	}
	handler := &cancellationEventHandler{}
	adapter := newAdapter(client, handler)
	activities := make([]githubsource.ManagerActivity, 0)
	adapter.Observer = githubsource.ManagerObserver{CurrentTime: adapter.Observer.CurrentTime, Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}
	ctx, cancel := context.WithCancel(context.Background())
	resultReady := make(chan pollOutcome, 1)

	go func() {
		result, err := adapter.Poll(ctx, 1)
		resultReady <- pollOutcome{result: result, err: err}
	}()

	<-received
	cancel()
	close(releaseReceive)
	outcome := <-resultReady

	assertCanceledPoll(t, outcome, githubsource.PollResult{Received: true})
	if handler.requests != 0 || client.acknowledged != 0 {
		t.Fatalf("late work: requests=%d acknowledgements=%d", handler.requests, client.acknowledged)
	}
	assertIntentionalCancellationActivity(t, activities, "handle_message")
}

func TestAdapterReportsCallerCancelledReceiveWithoutOperationalFailure(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(&cancelledReceiveClient{}, &cancellationEventHandler{})
	adapter.Observer = githubsource.ManagerObserver{CurrentTime: adapter.Observer.CurrentTime, Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}

	if _, err := adapter.Poll(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Poll() error = %v, want context canceled", err)
	}
	assertIntentionalCancellationActivity(t, activities, "poll")
}

func TestAdapterReportsTransportCancellationWithLiveCallerAsFailure(t *testing.T) {
	t.Parallel()

	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(&transportCancelledReceiveClient{}, &cancellationEventHandler{})
	adapter.Observer = githubsource.ManagerObserver{CurrentTime: adapter.Observer.CurrentTime, Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}

	if _, err := adapter.Poll(context.Background(), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Poll() error = %v, want transport context cancellation", err)
	}
	failure := findManagerActivity(activities, "poll", "failed")
	if failure.Visibility != githubsource.ActivityDebug || !errors.Is(failure.Err, context.Canceled) {
		t.Fatalf("transport cancellation activity = %#v", failure)
	}
}

func assertIntentionalCancellationActivity(t *testing.T, activities []githubsource.ManagerActivity, stage string) {
	t.Helper()
	foundCanceled := false
	for _, activity := range activities {
		if activity.Outcome == "failed" {
			t.Fatalf("caller cancellation reported as failure: %#v", activity)
		}
		if activity.Stage == stage && activity.Outcome == "canceled" &&
			activity.Visibility == githubsource.ActivityDebug {
			foundCanceled = true
		}
	}
	if !foundCanceled {
		t.Fatalf("activities = %#v, want %s canceled debug activity", activities, stage)
	}
}

type cancelledReceiveClient struct{}

func (*cancelledReceiveClient) GetMessage(ctx context.Context, _, _ int) (*upstream.RunnerScaleSetMessage, error) {
	return nil, fmt.Errorf("receive canceled: %w", ctx.Err())
}

func (*cancelledReceiveClient) AcquireRequests(context.Context, []int64) ([]int64, error) {
	return nil, nil
}
func (*cancelledReceiveClient) DeleteMessage(context.Context, int) error { return nil }

type transportCancelledReceiveClient struct{ cancelledReceiveClient }

func (*transportCancelledReceiveClient) GetMessage(
	context.Context,
	int,
	int,
) (*upstream.RunnerScaleSetMessage, error) {
	return nil, context.Canceled
}

func TestAdapterDoesNotAcknowledgeMessageHandledByCancelledPoll(t *testing.T) {
	t.Parallel()

	handling := make(chan struct{})
	releaseHandling := make(chan struct{})
	client := &cancellationMessageClient{message: acquiredAvailableMessage(17, 41)}
	handler := &cancellationEventHandler{handling: handling, releaseHandling: releaseHandling}
	adapter := newAdapter(client, handler)
	ctx, cancel := context.WithCancel(context.Background())
	resultReady := make(chan pollOutcome, 1)

	go func() {
		result, err := adapter.Poll(ctx, 1)
		resultReady <- pollOutcome{result: result, err: err}
	}()

	<-handling
	cancel()
	close(releaseHandling)
	outcome := <-resultReady

	assertCanceledPoll(t, outcome, githubsource.PollResult{Received: true, Handled: true})
	if handler.requests != 1 || client.acknowledged != 0 {
		t.Fatalf("durable handling and acknowledgement counts = %d, %d; want 1, 0", handler.requests, client.acknowledged)
	}
}

func TestAdapterReportsCancelledDurableHandlingWithoutOperationalFailure(t *testing.T) {
	t.Parallel()

	handling := make(chan struct{})
	releaseHandling := make(chan struct{})
	client := &cancellationMessageClient{message: acquiredAvailableMessage(18, 42)}
	handler := &cancellationEventHandler{
		handling: handling, releaseHandling: releaseHandling, returnContextError: true,
	}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(client, handler)
	adapter.Observer = githubsource.ManagerObserver{CurrentTime: adapter.Observer.CurrentTime, Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}
	ctx, cancel := context.WithCancel(context.Background())
	resultReady := make(chan pollOutcome, 1)

	go func() {
		result, err := adapter.Poll(ctx, 1)
		resultReady <- pollOutcome{result: result, err: err}
	}()
	<-handling
	cancel()
	close(releaseHandling)
	outcome := <-resultReady

	assertCanceledPoll(t, outcome, githubsource.PollResult{Received: true})
	assertIntentionalCancellationActivity(t, activities, "handle_message")
}

func TestAdapterReportsCancelledLifecyclePersistenceWithoutOperationalFailure(t *testing.T) {
	for _, message := range []*upstream.RunnerScaleSetMessage{
		{MessageID: 19, Statistics: &upstream.RunnerScaleSetStatistic{},
			JobStartedMessages: []*upstream.JobStarted{{RunnerName: "external-runner"}}},
		{MessageID: 20, Statistics: &upstream.RunnerScaleSetStatistic{},
			JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: "external-runner"}}},
	} {
		assertCanceledLifecyclePersistence(t, message)
	}
}

func assertCanceledLifecyclePersistence(t *testing.T, message *upstream.RunnerScaleSetMessage) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	handler := &cancelingLifecycleEventHandler{cancel: cancel}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(&cancellationMessageClient{message: message}, handler)
	adapter.Observer = githubsource.ManagerObserver{CurrentTime: adapter.Observer.CurrentTime, Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}
	result, err := adapter.Poll(ctx, 1)
	assertCanceledPoll(t, pollOutcome{result: result, err: err}, githubsource.PollResult{Received: true})
	assertIntentionalCancellationActivity(t, activities, "handle_message")
}

func TestAdapterReportsCancelledAcknowledgementWithoutOperationalFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	client := &cancellationMessageClient{message: acquiredAvailableMessage(21, 43), cancelAcknowledgement: cancel}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(client, &cancellationEventHandler{})
	adapter.Observer = githubsource.ManagerObserver{CurrentTime: adapter.Observer.CurrentTime, Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}
	result, err := adapter.Poll(ctx, 1)
	assertCanceledPoll(t, pollOutcome{result: result, err: err},
		githubsource.PollResult{Received: true, Handled: true})
	assertIntentionalCancellationActivity(t, activities, "acknowledge_message")
}

type pollOutcome struct {
	result githubsource.PollResult
	err    error
}

func assertCanceledPoll(t *testing.T, outcome pollOutcome, expected githubsource.PollResult) {
	t.Helper()
	if !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("Poll() error = %v, want context canceled", outcome.err)
	}
	if outcome.result != expected {
		t.Fatalf("Poll() result = %#v, want %#v", outcome.result, expected)
	}
}

type cancellationMessageClient struct {
	message               *upstream.RunnerScaleSetMessage
	received              chan struct{}
	releaseReceive        chan struct{}
	acknowledged          int
	cancelAcknowledgement context.CancelFunc
}

func (client *cancellationMessageClient) GetMessage(
	context.Context,
	int,
	int,
) (*upstream.RunnerScaleSetMessage, error) {
	if client.received != nil {
		close(client.received)
	}
	if client.releaseReceive != nil {
		<-client.releaseReceive
	}
	return client.message, nil
}

func (*cancellationMessageClient) AcquireRequests(_ context.Context, requestIDs []int64) ([]int64, error) {
	return requestIDs, nil
}

func (client *cancellationMessageClient) DeleteMessage(ctx context.Context, _ int) error {
	if client.cancelAcknowledgement != nil {
		client.cancelAcknowledgement()
		return fmt.Errorf("acknowledgement canceled: %w", ctx.Err())
	}
	client.acknowledged++
	return nil
}

type cancelingLifecycleEventHandler struct{ cancel context.CancelFunc }

func (*cancelingLifecycleEventHandler) IngestDemand(context.Context, core.JobRequest) (core.EnqueueResult, error) {
	return core.EnqueueResult{}, nil
}

func (handler *cancelingLifecycleEventHandler) ObserveRunnerAcceptance(
	ctx context.Context,
	_ core.RunnerObservation,
) (core.ObservationResult, error) {
	handler.cancel()
	return core.ObservationResult{}, fmt.Errorf("runner start canceled: %w", ctx.Err())
}

func (handler *cancelingLifecycleEventHandler) ObserveTerminal(
	ctx context.Context,
	_ core.RunnerObservation,
) (core.ObservationResult, error) {
	handler.cancel()
	return core.ObservationResult{}, fmt.Errorf("terminal observation canceled: %w", ctx.Err())
}

func (*cancelingLifecycleEventHandler) ReconcileAssignedDemand(
	context.Context,
	githubsource.AssignedDemand,
) (githubsource.AssignedDemandResult, error) {
	return githubsource.AssignedDemandResult{}, nil
}

func (*cancelingLifecycleEventHandler) ObserveTerminalIfUnaccepted(
	context.Context,
	core.JobIdentity,
) (core.ConditionalTerminalResult, error) {
	return core.ConditionalTerminalResult{Outcome: core.ConditionalTerminalProtected}, nil
}

type cancellationEventHandler struct {
	requests           int
	handling           chan struct{}
	releaseHandling    chan struct{}
	returnContextError bool
}

func (handler *cancellationEventHandler) IngestDemand(ctx context.Context, _ core.JobRequest) (core.EnqueueResult, error) {
	if handler.handling != nil {
		close(handler.handling)
	}
	if handler.releaseHandling != nil {
		<-handler.releaseHandling
	}
	handler.requests++
	if handler.returnContextError {
		return core.EnqueueResult{}, fmt.Errorf("persist request canceled: %w", ctx.Err())
	}
	return core.EnqueueResult{Outcome: core.QueueQueued}, nil
}

func (*cancellationEventHandler) ObserveRunnerAcceptance(
	context.Context,
	core.RunnerObservation,
) (core.ObservationResult, error) {
	return core.ObservationResult{Outcome: core.ObservationApplied}, nil
}

func (*cancellationEventHandler) ObserveTerminal(
	context.Context,
	core.RunnerObservation,
) (core.ObservationResult, error) {
	return core.ObservationResult{Outcome: core.ObservationApplied}, nil
}

func (*cancellationEventHandler) ReconcileAssignedDemand(
	context.Context,
	githubsource.AssignedDemand,
) (githubsource.AssignedDemandResult, error) {
	return githubsource.AssignedDemandResult{}, nil
}

func (*cancellationEventHandler) ObserveTerminalIfUnaccepted(
	context.Context,
	core.JobIdentity,
) (core.ConditionalTerminalResult, error) {
	return core.ConditionalTerminalResult{Outcome: core.ConditionalTerminalProtected}, nil
}

func acquiredAvailableMessage(messageID int, requestID int64) *upstream.RunnerScaleSetMessage {
	return &upstream.RunnerScaleSetMessage{
		MessageID: messageID, Statistics: &upstream.RunnerScaleSetStatistic{},
		JobAvailableMessages: []*upstream.JobAvailable{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID},
		}},
	}
}
