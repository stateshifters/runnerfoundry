package scaleset

import (
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
)

func TestPollerAcknowledgesOnlyAfterHandlingMessage(t *testing.T) {
	client := &recordingMessageClient{
		messages: []*upstream.RunnerScaleSetMessage{
			{MessageID: 41, Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2}},
		},
	}
	handler := &recordingMessageHandler{}
	poller := Poller{Client: client, Handler: handler, MaxCapacity: 3}

	err := poller.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("expected poll success, got %v", err)
	}
	if len(handler.desiredCounts) != 1 || handler.desiredCounts[0] != 2 {
		t.Fatalf("expected desired count handled before ack, got %#v", handler.desiredCounts)
	}
	if len(client.deleted) != 1 || client.deleted[0] != 41 {
		t.Fatalf("expected message 41 acknowledged, got %#v", client.deleted)
	}
}

func TestPollerDoesNotAcknowledgeFailedHandling(t *testing.T) {
	client := &recordingMessageClient{
		messages: []*upstream.RunnerScaleSetMessage{
			{MessageID: 42, Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2}},
		},
	}
	poller := Poller{Client: client, Handler: failingMessageHandler{}, MaxCapacity: 3}

	if err := poller.PollOnce(context.Background()); err == nil {
		t.Fatal("expected handler failure")
	}
	if len(client.deleted) != 0 {
		t.Fatalf("expected no acknowledgement, got %#v", client.deleted)
	}
}

func TestPollerHandlesNilMessageAndValidationFailures(t *testing.T) {
	tests := map[string]Poller{
		"missing client":  {Handler: &recordingMessageHandler{}},
		"missing handler": {Client: &recordingMessageClient{}},
		"nil statistics": {
			Client:  &recordingMessageClient{messages: []*upstream.RunnerScaleSetMessage{{MessageID: 1}}},
			Handler: &recordingMessageHandler{},
		},
	}
	for testName, poller := range tests {
		t.Run(testName, func(t *testing.T) {
			if err := poller.PollOnce(context.Background()); err == nil {
				t.Fatal("expected poller error")
			}
		})
	}

	poller := Poller{Client: &recordingMessageClient{}, Handler: &recordingMessageHandler{}}
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("expected nil message success, got %v", err)
	}
}

func TestPollerSurfacesClientAndAcknowledgementFailures(t *testing.T) {
	if err := (&Poller{Client: failingMessageClient{}, Handler: &recordingMessageHandler{}}).PollOnce(context.Background()); err == nil {
		t.Fatal("expected get message error")
	}
	client := &recordingMessageClient{
		messages:    []*upstream.RunnerScaleSetMessage{{MessageID: 7, Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1}}},
		deleteFails: true,
	}
	if err := (&Poller{Client: client, Handler: &recordingMessageHandler{}}).PollOnce(context.Background()); err == nil {
		t.Fatal("expected acknowledgement error")
	}
}

type recordingMessageClient struct {
	messages    []*upstream.RunnerScaleSetMessage
	deleted     []int
	deleteFails bool
}

func (client *recordingMessageClient) GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	if len(client.messages) == 0 {
		return nil, nil
	}
	message := client.messages[0]
	client.messages = client.messages[1:]
	return message, nil
}

func (client *recordingMessageClient) DeleteMessage(ctx context.Context, messageID int) error {
	if client.deleteFails {
		return errors.New("delete failed")
	}
	client.deleted = append(client.deleted, messageID)
	return nil
}

type recordingMessageHandler struct {
	desiredCounts []int
}

func (handler *recordingMessageHandler) HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	handler.desiredCounts = append(handler.desiredCounts, message.Statistics.TotalAssignedJobs)
	return nil
}

type failingMessageHandler struct{}

func (handler failingMessageHandler) HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	return errors.New("handler failed")
}

type failingMessageClient struct{}

func (client failingMessageClient) GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	return nil, errors.New("get failed")
}

func (client failingMessageClient) DeleteMessage(ctx context.Context, messageID int) error {
	return nil
}
