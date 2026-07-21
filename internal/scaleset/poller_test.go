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

func TestPollerRequiresClientAndHandler(t *testing.T) {
	tests := map[string]Poller{
		"missing client":  {Handler: &recordingMessageHandler{}},
		"missing handler": {Client: &recordingMessageClient{}},
	}
	for testName, poller := range tests {
		t.Run(testName, func(t *testing.T) {
			if err := poller.PollOnce(context.Background()); err == nil {
				t.Fatal("expected poller error")
			}
		})
	}
}

func TestPollerRejectsMessageWithoutStatistics(t *testing.T) {
	poller := Poller{
		Client:  &recordingMessageClient{messages: []*upstream.RunnerScaleSetMessage{{MessageID: 1}}},
		Handler: &recordingMessageHandler{},
	}
	if err := poller.PollOnce(context.Background()); err == nil {
		t.Fatal("expected missing statistics error")
	}
}

func TestPollerTreatsNoMessageAsSuccess(t *testing.T) {
	poller := Poller{Client: &recordingMessageClient{}, Handler: &recordingMessageHandler{}}
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("expected nil message success, got %v", err)
	}
}

func TestPollerReportsMessageReceiveFailure(t *testing.T) {
	if err := (&Poller{Client: failingMessageClient{}, Handler: &recordingMessageHandler{}}).PollOnce(context.Background()); err == nil {
		t.Fatal("expected get message error")
	}
}

func TestPollerReportsAcknowledgementFailure(t *testing.T) {
	client := &recordingMessageClient{
		messages:    []*upstream.RunnerScaleSetMessage{{MessageID: 7, Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1}}},
		deleteFails: true,
	}
	if err := (&Poller{Client: client, Handler: &recordingMessageHandler{}}).PollOnce(context.Background()); err == nil {
		t.Fatal("expected acknowledgement error")
	}
}

func TestPollerRedeliversHandledMessageAfterAcknowledgementFailure(t *testing.T) {
	message := &upstream.RunnerScaleSetMessage{MessageID: 8, Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1}}
	client := &recordingMessageClient{
		messages:       []*upstream.RunnerScaleSetMessage{message, message},
		deleteFailures: 1,
	}
	handler := &recordingMessageHandler{}
	poller := &Poller{Client: client, Handler: handler}

	if err := poller.PollOnce(context.Background()); err == nil {
		t.Fatal("expected first acknowledgement failure")
	}
	if poller.LastMessageID != 0 {
		t.Fatalf("expected failed acknowledgement to leave the cursor unchanged, got %d", poller.LastMessageID)
	}
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("expected redelivery acknowledgement success, got %v", err)
	}
	if len(handler.desiredCounts) != 2 || len(client.deleted) != 1 || client.deleted[0] != 8 || poller.LastMessageID != 8 {
		t.Fatalf("expected handled redelivery to advance only after acknowledgement, handled=%#v deleted=%#v cursor=%d", handler.desiredCounts, client.deleted, poller.LastMessageID)
	}
}

type recordingMessageClient struct {
	messages       []*upstream.RunnerScaleSetMessage
	deleted        []int
	deleteFails    bool
	deleteFailures int
}

func (client *recordingMessageClient) GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	if len(client.messages) == 0 {
		// The upstream client represents an empty poll with a nil message and nil error.
		return nil, nil
	}
	message := client.messages[0]
	client.messages = client.messages[1:]
	return message, nil
}

func (client *recordingMessageClient) DeleteMessage(ctx context.Context, messageID int) error {
	if client.deleteFails || client.deleteFailures > 0 {
		if client.deleteFailures > 0 {
			client.deleteFailures--
		}
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
