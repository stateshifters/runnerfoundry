package github_test

import (
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestPollOnceValidatesTransportResultsAndDependencies(t *testing.T) {
	t.Parallel()

	tests := map[string]githubsource.Adapter{
		"missing client": {
			Handler: &recordingEventHandler{events: &[]string{}},
		},
		"missing handler": {
			Client: &recordingClient{},
		},
		"receive failure": {
			Client:  &recordingClient{receiveErr: errors.New("receive")},
			Handler: &recordingEventHandler{events: &[]string{}},
		},
		"ambiguous nil message": {
			Client:  ambiguousNilClient{},
			Handler: &recordingEventHandler{events: &[]string{}},
		},
	}
	for testName, adapter := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			if err := adapter.PollOnce(context.Background()); err == nil {
				t.Fatal("PollOnce() error = nil, want failure")
			}
		})
	}
}

func TestPollOnceTreatsNoMessageAsSuccess(t *testing.T) {
	t.Parallel()

	adapter := githubsource.Adapter{
		Client:  &recordingClient{},
		Handler: &recordingEventHandler{events: &[]string{}},
	}
	if err := adapter.PollOnce(context.Background()); err != nil {
		t.Fatalf("empty poll: %v", err)
	}
}

func TestAcknowledgementFailureDoesNotAdvanceCursor(t *testing.T) {
	t.Parallel()

	client := &recordingClient{
		messages:  []*upstream.RunnerScaleSetMessage{{MessageID: 23}},
		deleteErr: errors.New("acknowledgement failed"),
	}
	adapter := githubsource.Adapter{
		Client:        client,
		Handler:       &recordingEventHandler{events: &client.events},
		LastMessageID: 22,
	}
	if err := adapter.PollOnce(context.Background()); err == nil {
		t.Fatal("PollOnce() error = nil, want acknowledgement failure")
	}
	if adapter.LastMessageID != 22 {
		t.Fatalf("cursor = %d, want 22", adapter.LastMessageID)
	}
}

func TestPollOnceRejectsNilJobEventsWithoutAcknowledgement(t *testing.T) {
	t.Parallel()

	tests := map[string]*upstream.RunnerScaleSetMessage{
		"available": {MessageID: 24, JobAvailableMessages: []*upstream.JobAvailable{nil}},
		"started":   {MessageID: 25, JobStartedMessages: []*upstream.JobStarted{nil}},
		"completed": {MessageID: 26, JobCompletedMessages: []*upstream.JobCompleted{nil}},
		"unidentified available": {
			MessageID: 29, JobAvailableMessages: []*upstream.JobAvailable{{}},
		},
		"unidentified started": {
			MessageID: 30, JobStartedMessages: []*upstream.JobStarted{{}},
		},
		"unidentified completed": {
			MessageID: 31, JobCompletedMessages: []*upstream.JobCompleted{{}},
		},
	}
	for testName, message := range tests {
		message := message
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
			adapter := githubsource.Adapter{Client: client, Handler: &recordingEventHandler{events: &client.events}}
			if err := adapter.PollOnce(context.Background()); err == nil {
				t.Fatal("PollOnce() error = nil, want invalid event failure")
			}
			if len(client.acknowledged) != 0 {
				t.Fatalf("invalid event acknowledgements = %#v", client.acknowledged)
			}
		})
	}
}

func TestPollOnceDoesNotAcknowledgeFailedStartedObservation(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{
		MessageID: 27,
		JobStartedMessages: []*upstream.JobStarted{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 101},
		}},
	}
	assertFailedObservationNotAcknowledged(t, message, &recordingEventHandler{startedErr: errors.New("started")})
}

func TestPollOnceDoesNotAcknowledgeFailedCompletedObservation(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{
		MessageID: 28,
		JobCompletedMessages: []*upstream.JobCompleted{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 102},
		}},
	}
	assertFailedObservationNotAcknowledged(t, message, &recordingEventHandler{completedErr: errors.New("completed")})
}

func assertFailedObservationNotAcknowledged(
	t *testing.T,
	message *upstream.RunnerScaleSetMessage,
	handler *recordingEventHandler,
) {
	t.Helper()
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler.events = &client.events
	adapter := githubsource.Adapter{
		Client: client, Handler: handler, Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"),
	}
	if err := adapter.PollOnce(context.Background()); err == nil {
		t.Fatal("PollOnce() error = nil, want durable observation failure")
	}
	if len(client.acknowledged) != 0 {
		t.Fatalf("failed observation acknowledgements = %#v", client.acknowledged)
	}
}

type ambiguousNilClient struct {
	message *upstream.RunnerScaleSetMessage
	err     error
}

func (client ambiguousNilClient) GetMessage(context.Context, int, int) (*upstream.RunnerScaleSetMessage, error) {
	return client.message, client.err
}

func (ambiguousNilClient) DeleteMessage(context.Context, int) error {
	return nil
}
