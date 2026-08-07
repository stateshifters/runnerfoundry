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
	assertPollCapacity(t, client, 7)
	assertAcquisitionRequests(t, client, [][]int64{{41, 42}})
	assertPersistedRequests(t, handler, []core.JobRequest{
		jobRequest(t, 42, queueTime.Add(time.Second)),
		jobRequest(t, 43, queueTime.Add(2*time.Second)),
	})
	assertStartedObservations(t, handler, []core.SourceReference{sourceReference(t, 44)})
	assertTerminalObservations(t, handler, []core.SourceReference{sourceReference(t, 44), sourceReference(t, 45)})
	assertEvents(t, client.events, batchEffectEvents(t))
}

func TestAdapterReportsPollAndDurableMessageActivities(t *testing.T) {
	queueTime := time.Date(2026, time.August, 5, 10, 30, 0, 0, time.UTC)
	message := messageWithAllEffects(queueTime)
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}, acquired: []int64{42}}
	handler := &recordingEventHandler{events: &client.events}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(client, handler)
	adapter.Observer = githubsource.ManagerObserver{
		Report:      func(activity githubsource.ManagerActivity) { activities = append(activities, activity) },
		CurrentTime: func() time.Time { return time.Unix(1_700_000_000, 0) },
	}

	if _, err := adapter.PollOnce(context.Background(), 7); err != nil {
		t.Fatalf("poll message: %v", err)
	}

	infoOutcomes, debugRequests := adapterActivityResults(activities)
	wantInfo := []string{
		"job_ingestion:enqueue_jobs:2",
		"job_observation:runner_started:1",
		"job_observation:terminalized:2",
	}
	if !reflect.DeepEqual(infoOutcomes, wantInfo) || !reflect.DeepEqual(debugRequests, []int64{42, 43}) {
		t.Fatalf("info outcomes=%#v debug requests=%#v", infoOutcomes, debugRequests)
	}
}

func adapterActivityResults(activities []githubsource.ManagerActivity) ([]string, []int64) {
	infoOutcomes := make([]string, 0)
	debugRequests := make([]int64, 0)
	for _, activity := range activities {
		if activity.Visibility == githubsource.ActivityInfo && activity.Outcome == "completed" {
			infoOutcomes = append(infoOutcomes, activity.Operation+":"+activity.Stage+":"+strconv.Itoa(activity.Count))
		}
		if activity.Visibility == githubsource.ActivityDebug && activity.Stage == "persist_job" && activity.Outcome == "completed" {
			debugRequests = append(debugRequests, activity.SourceRequestID)
		}
	}
	return infoOutcomes, debugRequests
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

func assertPollCapacity(t *testing.T, client *recordingClient, want int) {
	t.Helper()
	if client.maxCapacity != want {
		t.Fatalf("poll capacity = %d, want %d", client.maxCapacity, want)
	}
}

func assertAcquisitionRequests(t *testing.T, client *recordingClient, want [][]int64) {
	t.Helper()
	if !reflect.DeepEqual(client.requested, want) {
		t.Fatalf("acquisition requests = %#v, want %#v", client.requested, want)
	}
}

func assertPersistedRequests(t *testing.T, handler *recordingEventHandler, want []core.JobRequest) {
	t.Helper()
	if !reflect.DeepEqual(handler.requests, want) {
		t.Fatalf("requests = %#v, want %#v", handler.requests, want)
	}
}

func assertStartedObservations(t *testing.T, handler *recordingEventHandler, want []core.SourceReference) {
	t.Helper()
	if !reflect.DeepEqual(handler.started, want) {
		t.Fatalf("started = %#v, want %#v", handler.started, want)
	}
}

func assertTerminalObservations(t *testing.T, handler *recordingEventHandler, want []core.SourceReference) {
	t.Helper()
	if !reflect.DeepEqual(handler.completed, want) {
		t.Fatalf("completed = %#v, want %#v", handler.completed, want)
	}
}

func batchEffectEvents(t *testing.T) []string {
	t.Helper()
	return []string{
		"acquire:41,42",
		"request:" + string(sourceReference(t, 42).ID),
		"request:" + string(sourceReference(t, 43).ID),
		"started:" + string(sourceReference(t, 44).ID),
		"completed:" + string(sourceReference(t, 44).ID),
		"completed:" + string(sourceReference(t, 45).ID),
		"ack:17",
	}
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

func TestPollOnceLeavesMessageWhenAcquisitionFails(t *testing.T) {
	t.Parallel()

	assertMessageRemainsAfterFailure(
		t,
		availableMessage(19, 61),
		&recordingClient{acquireErr: errors.New("acquire failed")},
		&recordingEventHandler{},
	)
}

func TestPollOnceLeavesMessageWhenAcquiredRequestCannotBePersisted(t *testing.T) {
	t.Parallel()

	assertMessageRemainsAfterFailure(
		t,
		availableMessage(20, 62),
		&recordingClient{acquired: []int64{62}},
		&recordingEventHandler{requestErr: errors.New("persist failed")},
	)
}

func TestPollOnceLeavesMessageWhenAssignedRequestCannotBePersisted(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 20, JobAssignedMessages: []*upstream.JobAssigned{{
		JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 64},
	}}}
	assertMessageRemainsAfterFailure(
		t,
		message,
		&recordingClient{},
		&recordingEventHandler{requestErr: errors.New("persist assigned failed")},
	)
}

func TestPollOnceLeavesMessageWhenStartedObservationCannotBePersisted(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 21, JobStartedMessages: []*upstream.JobStarted{{
		JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 63},
	}}}
	assertMessageRemainsAfterFailure(
		t,
		message,
		&recordingClient{},
		&recordingEventHandler{startedErr: errors.New("started failed")},
	)
}

func TestPollOnceLeavesMessageWhenCompletionCannotBePersisted(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 21, JobCompletedMessages: []*upstream.JobCompleted{{
		Result: "canceled", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 63},
	}}}
	assertMessageRemainsAfterFailure(
		t,
		message,
		&recordingClient{},
		&recordingEventHandler{completedErr: errors.New("completion failed")},
	)
}

func assertMessageRemainsAfterFailure(
	t *testing.T,
	message *upstream.RunnerScaleSetMessage,
	client *recordingClient,
	handler *recordingEventHandler,
) {
	t.Helper()
	client.messages = []*upstream.RunnerScaleSetMessage{message}
	handler.events = &client.events
	adapter := newAdapter(client, handler)
	received, err := adapter.PollOnce(context.Background(), 0)
	if err == nil || !received {
		t.Fatalf("poll = (%t, %v), want received failure", received, err)
	}
	if len(client.acknowledged) != 0 {
		t.Fatalf("failed message acknowledgements = %#v", client.acknowledged)
	}
}

func TestPollOnceValidatesRelevantEventsBeforeAcquisition(t *testing.T) {
	t.Parallel()

	tests := map[string]*upstream.RunnerScaleSetMessage{
		"nil available":      {MessageID: 22, JobAvailableMessages: []*upstream.JobAvailable{nil}},
		"invalid available":  availableMessage(23, 0),
		"nil assigned":       {MessageID: 24, JobAssignedMessages: []*upstream.JobAssigned{nil}},
		"invalid assigned":   {MessageID: 25, JobAssignedMessages: []*upstream.JobAssigned{{}}},
		"nil started":        {MessageID: 26, JobStartedMessages: []*upstream.JobStarted{nil}},
		"invalid started":    {MessageID: 27, JobStartedMessages: []*upstream.JobStarted{{}}},
		"nil completion":     {MessageID: 28, JobCompletedMessages: []*upstream.JobCompleted{nil}},
		"invalid completion": {MessageID: 29, JobCompletedMessages: []*upstream.JobCompleted{{Result: "succeeded"}}},
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

func TestPollOnceRedeliversDurableEffectsWhenAcknowledgementFails(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{
		MessageID: 30,
		JobStartedMessages: []*upstream.JobStarted{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 71},
		}},
		JobCompletedMessages: []*upstream.JobCompleted{{
			Result: "failed", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 72},
		}},
	}
	client := &recordingClient{
		messages:  []*upstream.RunnerScaleSetMessage{message},
		deleteErr: errors.New("acknowledgement failed"),
	}
	handler := &recordingEventHandler{events: &client.events}
	adapter := newAdapter(client, handler)

	if received, err := adapter.PollOnce(context.Background(), 0); err == nil || !received {
		t.Fatalf("first poll = (%t, %v), want received acknowledgement failure", received, err)
	}
	client.deleteErr = nil
	client.messages = []*upstream.RunnerScaleSetMessage{message}
	if received, err := adapter.PollOnce(context.Background(), 0); err != nil || !received {
		t.Fatalf("redelivery = (%t, %v), want received success", received, err)
	}

	assertEvents(t, client.events, acknowledgementRedeliveryEvents(t))
}

func acknowledgementRedeliveryEvents(t *testing.T) []string {
	t.Helper()
	return []string{
		"started:" + string(sourceReference(t, 71).ID),
		"completed:" + string(sourceReference(t, 72).ID),
		"started:" + string(sourceReference(t, 71).ID),
		"completed:" + string(sourceReference(t, 72).ID),
		"ack:30",
	}
}

func TestPollOnceTerminalizesEveryGitHubCompletionResult(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 31}
	for requestOffset, result := range []string{"", "succeeded", "failed", "canceled", "unknown"} {
		message.JobCompletedMessages = append(message.JobCompletedMessages, &upstream.JobCompleted{
			Result: result,
			JobMessageBase: upstream.JobMessageBase{
				RunnerRequestID: int64(80 + requestOffset),
			},
		})
	}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{events: &client.events}
	adapter := newAdapter(client, handler)

	if received, err := adapter.PollOnce(context.Background(), 0); err != nil || !received {
		t.Fatalf("poll = (%t, %v), want received success", received, err)
	}

	wantCompleted := make([]core.SourceReference, len(message.JobCompletedMessages))
	for requestOffset := range wantCompleted {
		wantCompleted[requestOffset] = sourceReference(t, int64(80+requestOffset))
	}
	if !reflect.DeepEqual(handler.completed, wantCompleted) {
		t.Fatalf("completed = %#v, want %#v", handler.completed, wantCompleted)
	}
}

type recordingEventHandler struct {
	events       *[]string
	requests     []core.JobRequest
	started      []core.SourceReference
	completed    []core.SourceReference
	requestErr   error
	startedErr   error
	completedErr error
}

func (handler *recordingEventHandler) Request(_ context.Context, request core.JobRequest) error {
	handler.requests = append(handler.requests, request)
	if handler.events != nil {
		*handler.events = append(*handler.events, "request:"+string(request.Source.ID))
	}
	return handler.requestErr
}

func (handler *recordingEventHandler) RunnerStarted(_ context.Context, source core.SourceReference) error {
	handler.started = append(handler.started, source)
	if handler.events != nil {
		*handler.events = append(*handler.events, "started:"+string(source.ID))
	}
	return handler.startedErr
}

func (handler *recordingEventHandler) Terminalize(_ context.Context, source core.SourceReference) error {
	handler.completed = append(handler.completed, source)
	if handler.events != nil {
		*handler.events = append(*handler.events, "completed:"+string(source.ID))
	}
	return handler.completedErr
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
