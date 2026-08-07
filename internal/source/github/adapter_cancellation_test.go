package github_test

import (
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestAdapterDoesNotHandleMessageReceivedByCancelledPoll(t *testing.T) {
	t.Parallel()

	received := make(chan struct{})
	releaseReceive := make(chan struct{})
	client := &cancellationMessageClient{
		message:        assignedMessage(17, 41),
		received:       received,
		releaseReceive: releaseReceive,
	}
	handler := &cancellationEventHandler{}
	adapter := newAdapter(client, handler)
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
}

func TestAdapterDoesNotAcknowledgeMessageHandledByCancelledPoll(t *testing.T) {
	t.Parallel()

	handling := make(chan struct{})
	releaseHandling := make(chan struct{})
	client := &cancellationMessageClient{message: assignedMessage(17, 41)}
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
	message        *upstream.RunnerScaleSetMessage
	received       chan struct{}
	releaseReceive chan struct{}
	acknowledged   int
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

func (*cancellationMessageClient) AcquireRequests(context.Context, []int64) ([]int64, error) {
	return nil, nil
}

func (client *cancellationMessageClient) DeleteMessage(context.Context, int) error {
	client.acknowledged++
	return nil
}

type cancellationEventHandler struct {
	requests        int
	handling        chan struct{}
	releaseHandling chan struct{}
}

func (handler *cancellationEventHandler) Request(context.Context, core.JobRequest) error {
	if handler.handling != nil {
		close(handler.handling)
	}
	if handler.releaseHandling != nil {
		<-handler.releaseHandling
	}
	handler.requests++
	return nil
}

func (*cancellationEventHandler) RunnerStarted(context.Context, core.SourceReference) error {
	return nil
}

func (*cancellationEventHandler) Terminalize(context.Context, core.SourceReference) error { return nil }

func assignedMessage(messageID int, requestID int64) *upstream.RunnerScaleSetMessage {
	return &upstream.RunnerScaleSetMessage{
		MessageID: messageID,
		JobAssignedMessages: []*upstream.JobAssigned{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID},
		}},
	}
}
