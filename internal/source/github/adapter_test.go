package github_test

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestPollOnceAcquiresAvailableWorkBeforePersistingBatchEffects(t *testing.T) {
	t.Parallel()

	queueTime := time.Date(2026, time.August, 5, 10, 30, 0, 0, time.UTC)
	message := messageWithAllEffects(queueTime)
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}, acquired: []int64{42}}
	handler := &recordingEventHandler{events: &client.events}
	adapter := newAdapter(client, handler)

	received, err := adapter.PollOnce(context.Background(), 7)
	if err != nil {
		t.Fatalf("poll message: %v", err)
	}
	if !received {
		t.Fatal("poll did not report received message")
	}
	assertBatchEffects(t, queueTime, client, handler)
}

func messageWithAllEffects(queueTime time.Time) *upstream.RunnerScaleSetMessage {
	return &upstream.RunnerScaleSetMessage{
		MessageID: 17,
		JobAvailableMessages: []*upstream.JobAvailable{
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 41, QueueTime: queueTime}},
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42, QueueTime: queueTime.Add(time.Second)}},
		},
		JobAssignedMessages: []*upstream.JobAssigned{
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 43, QueueTime: queueTime.Add(2 * time.Second)}},
		},
		JobStartedMessages: []*upstream.JobStarted{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 44},
		}},
		JobCompletedMessages: []*upstream.JobCompleted{
			{Result: "succeeded", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 44}},
			{Result: "canceled", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 45}},
		},
	}
}

func assertBatchEffects(
	t *testing.T,
	queueTime time.Time,
	client *recordingClient,
	handler *recordingEventHandler,
) {
	t.Helper()
	if client.maxCapacity != 7 {
		t.Fatalf("poll capacity = %d, want 7", client.maxCapacity)
	}
	if !reflect.DeepEqual(client.requested, [][]int64{{41, 42}}) {
		t.Fatalf("acquisition requests = %#v, want [[41 42]]", client.requested)
	}
	wantRequests := []core.JobRequest{
		jobRequest(t, 42, queueTime.Add(time.Second)),
		jobRequest(t, 43, queueTime.Add(2*time.Second)),
	}
	if !reflect.DeepEqual(handler.requests, wantRequests) {
		t.Fatalf("requests = %#v, want %#v", handler.requests, wantRequests)
	}
	wantCancel := sourceReference(t, 45)
	if !reflect.DeepEqual(handler.cancellations, []core.SourceReference{wantCancel}) {
		t.Fatalf("cancellations = %#v, want [%#v]", handler.cancellations, wantCancel)
	}
	assertEvents(t, client.events, []string{
		"acquire:41,42",
		"request:" + sourceReference(t, 42).ID,
		"request:" + sourceReference(t, 43).ID,
		"cancel:" + sourceReference(t, 45).ID,
		"ack:17",
	})
}

func TestPollOnceRejectsUnknownAcquiredRequestBeforeDurableEffects(t *testing.T) {
	t.Parallel()

	message := availableMessage(18, 51)
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}, acquired: []int64{51, 52}}
	handler := &recordingEventHandler{events: &client.events}
	adapter := newAdapter(client, handler)

	received, err := adapter.PollOnce(context.Background(), 0)
	if err == nil || !received {
		t.Fatalf("poll = (%t, %v), want received failure", received, err)
	}
	if len(handler.requests) != 0 || len(client.acknowledged) != 0 {
		t.Fatalf("unknown acquisition produced effects: requests=%#v acknowledgements=%#v",
			handler.requests, client.acknowledged)
	}
}

func TestPollOnceLeavesMessageWhenAcquisitionOrDurableEffectFails(t *testing.T) {
	t.Parallel()

	for testName, testCase := range durableFailureCases() {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			testCase.client.messages = []*upstream.RunnerScaleSetMessage{testCase.message}
			testCase.handler.events = &testCase.client.events
			adapter := newAdapter(testCase.client, testCase.handler)
			received, err := adapter.PollOnce(context.Background(), 0)
			if err == nil || !received {
				t.Fatalf("poll = (%t, %v), want received failure", received, err)
			}
			if len(testCase.client.acknowledged) != 0 {
				t.Fatalf("failed message acknowledgements = %#v", testCase.client.acknowledged)
			}
		})
	}
}

type durableFailureCase struct {
	message *upstream.RunnerScaleSetMessage
	client  *recordingClient
	handler *recordingEventHandler
}

func durableFailureCases() map[string]durableFailureCase {
	return map[string]durableFailureCase{
		"acquisition": {
			message: availableMessage(19, 61),
			client:  &recordingClient{acquireErr: errors.New("acquire failed")},
			handler: &recordingEventHandler{},
		},
		"request": {
			message: availableMessage(20, 62),
			client:  &recordingClient{acquired: []int64{62}},
			handler: &recordingEventHandler{requestErr: errors.New("persist failed")},
		},
		"assigned request": {
			message: &upstream.RunnerScaleSetMessage{MessageID: 20, JobAssignedMessages: []*upstream.JobAssigned{{
				JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 64},
			}}},
			client:  &recordingClient{},
			handler: &recordingEventHandler{requestErr: errors.New("persist assigned failed")},
		},
		"cancellation": {
			message: &upstream.RunnerScaleSetMessage{MessageID: 21, JobCompletedMessages: []*upstream.JobCompleted{{
				Result: "canceled", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 63},
			}}},
			client:  &recordingClient{},
			handler: &recordingEventHandler{cancelErr: errors.New("cancel failed")},
		},
	}
}

func TestPollOnceValidatesRelevantEventsBeforeAcquisition(t *testing.T) {
	t.Parallel()

	tests := map[string]*upstream.RunnerScaleSetMessage{
		"nil available":        {MessageID: 22, JobAvailableMessages: []*upstream.JobAvailable{nil}},
		"invalid available":    availableMessage(23, 0),
		"nil assigned":         {MessageID: 24, JobAssignedMessages: []*upstream.JobAssigned{nil}},
		"invalid assigned":     {MessageID: 25, JobAssignedMessages: []*upstream.JobAssigned{{}}},
		"nil completion":       {MessageID: 26, JobCompletedMessages: []*upstream.JobCompleted{nil}},
		"invalid cancellation": {MessageID: 27, JobCompletedMessages: []*upstream.JobCompleted{{Result: "canceled"}}},
	}
	for testName, message := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
			adapter := newAdapter(client, &recordingEventHandler{events: &client.events})
			if _, err := adapter.PollOnce(context.Background(), 0); err == nil {
				t.Fatal("PollOnce() error = nil, want invalid event failure")
			}
			if len(client.requested) != 0 || len(client.acknowledged) != 0 {
				t.Fatalf("invalid event effects: acquisitions=%#v acknowledgements=%#v",
					client.requested, client.acknowledged)
			}
		})
	}
}

func TestPollOncePreservesAcknowledgementCursor(t *testing.T) {
	t.Parallel()

	client := &recordingClient{
		messages:  []*upstream.RunnerScaleSetMessage{{MessageID: 28}},
		deleteErr: errors.New("acknowledgement failed"),
	}
	adapter := newAdapter(client, &recordingEventHandler{events: &client.events})
	adapter.LastMessageID = 27
	received, err := adapter.PollOnce(context.Background(), 0)
	if err == nil || !received || adapter.LastMessageID != 27 {
		t.Fatalf("failed acknowledgement = (%t, %v, cursor %d), want received error and cursor 27",
			received, err, adapter.LastMessageID)
	}
	client.deleteErr = nil
	client.messages = []*upstream.RunnerScaleSetMessage{{MessageID: 28}}
	if _, err := adapter.PollOnce(context.Background(), 0); err != nil {
		t.Fatalf("redeliver acknowledged message: %v", err)
	}
	if adapter.LastMessageID != 28 {
		t.Fatalf("cursor = %d, want 28", adapter.LastMessageID)
	}
}

type recordingEventHandler struct {
	events        *[]string
	requests      []core.JobRequest
	cancellations []core.SourceReference
	requestErr    error
	cancelErr     error
}

func (handler *recordingEventHandler) Request(_ context.Context, request core.JobRequest) error {
	handler.requests = append(handler.requests, request)
	if handler.events != nil {
		*handler.events = append(*handler.events, "request:"+request.Source.ID)
	}
	return handler.requestErr
}

func (handler *recordingEventHandler) Cancel(_ context.Context, source core.SourceReference) error {
	handler.cancellations = append(handler.cancellations, source)
	if handler.events != nil {
		*handler.events = append(*handler.events, "cancel:"+source.ID)
	}
	return handler.cancelErr
}

type recordingClient struct {
	messages     []*upstream.RunnerScaleSetMessage
	acquired     []int64
	requested    [][]int64
	acknowledged []int
	events       []string
	receiveErr   error
	acquireErr   error
	deleteErr    error
	maxCapacity  int
}

func (client *recordingClient) GetMessage(_ context.Context, _, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	client.maxCapacity = maxCapacity
	if client.receiveErr != nil {
		return nil, client.receiveErr
	}
	if len(client.messages) == 0 {
		return nil, githubsource.ErrNoMessage
	}
	message := client.messages[0]
	client.messages = client.messages[1:]
	return message, nil
}

func (client *recordingClient) AcquireRequests(_ context.Context, requestIDs []int64) ([]int64, error) {
	client.requested = append(client.requested, append([]int64(nil), requestIDs...))
	values := make([]string, len(requestIDs))
	for index, requestID := range requestIDs {
		values[index] = strconv.FormatInt(requestID, 10)
	}
	client.events = append(client.events, "acquire:"+join(values))
	return append([]int64(nil), client.acquired...), client.acquireErr
}

func (client *recordingClient) DeleteMessage(_ context.Context, messageID int) error {
	if client.deleteErr != nil {
		return client.deleteErr
	}
	client.acknowledged = append(client.acknowledged, messageID)
	client.events = append(client.events, "ack:"+strconv.Itoa(messageID))
	return nil
}

func availableMessage(messageID int, requestID int64) *upstream.RunnerScaleSetMessage {
	return &upstream.RunnerScaleSetMessage{
		MessageID: messageID,
		JobAvailableMessages: []*upstream.JobAvailable{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: requestID},
		}},
	}
}

func newAdapter(client githubsource.MessageClient, handler githubsource.EventHandler) *githubsource.Adapter {
	return &githubsource.Adapter{
		Client: client, Handler: handler, Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"),
	}
}

func jobRequest(t *testing.T, requestID int64, enqueuedAt time.Time) core.JobRequest {
	t.Helper()
	return core.JobRequest{Source: sourceReference(t, requestID), RunnerPool: core.RunnerPool("linux"), EnqueuedAt: enqueuedAt}
}

func sourceReference(t *testing.T, requestID int64) core.SourceReference {
	t.Helper()
	sourceID, err := githubsource.EncodeSourceID(githubsource.SourceIdentity{
		Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"), RequestID: requestID,
	})
	if err != nil {
		t.Fatalf("encode source identity: %v", err)
	}
	return core.SourceReference{Adapter: githubsource.AdapterName, ID: sourceID}
}

func assertEvents(t *testing.T, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %#v, want %#v", got, want)
	}
}

func join(values []string) string {
	if len(values) == 0 {
		return ""
	}
	joined := values[0]
	for _, value := range values[1:] {
		joined += "," + value
	}
	return joined
}
