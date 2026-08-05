package github_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestJobAvailableCreatesOneSourceNeutralJobBeforeAcknowledgement(t *testing.T) {
	t.Parallel()

	queueTime := time.Date(2026, time.August, 5, 10, 30, 0, 0, time.UTC)
	message := &upstream.RunnerScaleSetMessage{
		MessageID: 17,
		JobAvailableMessages: []*upstream.JobAvailable{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42, QueueTime: queueTime},
		}},
	}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{events: &client.events}
	adapter := githubsource.Adapter{
		Client: client, Handler: handler, Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"),
	}

	assertReceivedWithCapacity(t, &adapter, client, 7)

	sourceID := mustSourceID(t, 42)
	want := core.JobRequest{
		Source:     core.SourceReference{Adapter: githubsource.AdapterName, ID: sourceID},
		RunnerPool: core.RunnerPool("linux"),
		EnqueuedAt: queueTime,
	}
	assertSingleRequest(t, handler.requests, want)
	assertEvents(t, client.events, []string{"request:github:" + sourceID, "ack:17"})
}

func TestJobStartedAndCompletedTranslateBeforeAcknowledgement(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, time.August, 5, 11, 0, 0, 0, time.UTC)
	message := &upstream.RunnerScaleSetMessage{
		MessageID: 18,
		JobStartedMessages: []*upstream.JobStarted{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 51, RunnerAssignTime: startedAt},
		}},
		JobCompletedMessages: []*upstream.JobCompleted{
			{Result: "succeeded", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 51}},
			{Result: "canceled", JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 52}},
		},
	}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{events: &client.events}
	adapter := githubsource.Adapter{
		Client: client, Handler: handler, Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"),
	}

	if _, err := adapter.PollOnce(context.Background(), 0); err != nil {
		t.Fatalf("poll lifecycle message: %v", err)
	}

	assertEvents(t, client.events, []string{
		"started:github:" + mustSourceID(t, 51) + ":2026-08-05T11:00:00Z",
		"completed:github:" + mustSourceID(t, 51) + ":success:runner",
		"completed:github:" + mustSourceID(t, 52) + ":failure:runner",
		"ack:18",
	})
}

func TestStatisticsAndAssignedCountsDoNotCreateDemand(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{
		MessageID: 19,
		Statistics: &upstream.RunnerScaleSetStatistic{
			TotalAvailableJobs: 7,
			TotalAssignedJobs:  5,
		},
		JobAssignedMessages: []*upstream.JobAssigned{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 61},
		}},
	}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{events: &client.events}
	adapter := githubsource.Adapter{
		Client: client, Handler: handler, Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"),
	}

	if _, err := adapter.PollOnce(context.Background(), 0); err != nil {
		t.Fatalf("poll count-only message: %v", err)
	}

	if len(handler.requests) != 0 {
		t.Fatalf("count-only message created requests %#v", handler.requests)
	}
	assertEvents(t, client.events, []string{"ack:19"})
}

func TestDurableFailureLeavesMessageForRedelivery(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{
		MessageID: 20,
		JobAvailableMessages: []*upstream.JobAvailable{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 71},
		}},
	}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message, message}}
	handler := &recordingEventHandler{events: &client.events, failuresRemaining: 1}
	adapter := githubsource.Adapter{
		Client: client, Handler: handler, Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"),
	}

	if _, err := adapter.PollOnce(context.Background(), 0); err == nil {
		t.Fatal("first delivery succeeded despite durable failure")
	}
	if len(client.acknowledged) != 0 {
		t.Fatalf("failed durable handling acknowledged messages %#v", client.acknowledged)
	}
	if _, err := adapter.PollOnce(context.Background(), 0); err != nil {
		t.Fatalf("redeliver message: %v", err)
	}

	sourceID := mustSourceID(t, 71)
	assertEvents(t, client.events, []string{"request:github:" + sourceID, "request:github:" + sourceID, "ack:20"})
}

func TestAvailableBatchHandlesEveryRequestBeforeReturningError(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{
		MessageID: 21,
		JobAvailableMessages: []*upstream.JobAvailable{
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 72}},
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 73}},
		},
		JobStartedMessages: []*upstream.JobStarted{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 72},
		}},
	}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{events: &client.events, failuresRemaining: 2}
	adapter := githubsource.Adapter{
		Client: client, Handler: handler, Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"),
	}

	assertFailedAvailableBatch(t, &adapter, handler, client)
	wantEvents := []string{
		"request:github:" + mustSourceID(t, 72),
		"request:github:" + mustSourceID(t, 73),
	}
	assertEvents(t, client.events, wantEvents)
}

func TestGitHubResultDeliveryIsRunnerManaged(t *testing.T) {
	t.Parallel()

	adapter := githubsource.Adapter{}
	delivery, err := adapter.ReportResult(context.Background(), core.Job{
		Source: core.SourceReference{Adapter: githubsource.AdapterName, ID: "81"},
	}, core.Result{Outcome: core.OutcomeSuccess, Origin: core.ResultOriginRunner})
	if err != nil {
		t.Fatalf("report GitHub result: %v", err)
	}
	if delivery != core.ResultDeliveryRunnerManaged {
		t.Fatalf("GitHub result delivery = %q, want runner managed", delivery)
	}
}

func TestServiceResultDeliveryIsUnsupported(t *testing.T) {
	t.Parallel()

	adapter := githubsource.Adapter{}
	delivery, err := adapter.ReportResult(context.Background(), core.Job{
		Source: core.SourceReference{Adapter: githubsource.AdapterName, ID: "82"},
	}, core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService})
	if err != nil {
		t.Fatalf("classify service result: %v", err)
	}
	if delivery != core.ResultDeliveryUnsupported {
		t.Fatalf("service result delivery = %q, want unsupported", delivery)
	}
}

func TestForeignResultDeliveryIsUnsupported(t *testing.T) {
	t.Parallel()

	adapter := githubsource.Adapter{}
	delivery, err := adapter.ReportResult(context.Background(), core.Job{
		Source: core.SourceReference{Adapter: "gitlab", ID: "91"},
	}, core.Result{Outcome: core.OutcomeFailure})
	if err != nil {
		t.Fatalf("classify foreign result: %v", err)
	}
	if delivery != core.ResultDeliveryUnsupported {
		t.Fatalf("foreign result delivery = %q, want unsupported", delivery)
	}
}

type recordingEventHandler struct {
	events            *[]string
	requests          []core.JobRequest
	failuresRemaining int
	startedErr        error
	completedErr      error
}

func (handler *recordingEventHandler) Request(_ context.Context, request core.JobRequest) error {
	handler.requests = append(handler.requests, request)
	*handler.events = append(*handler.events, "request:"+request.Source.Adapter+":"+request.Source.ID)
	if handler.failuresRemaining > 0 {
		handler.failuresRemaining--
		return errors.New("durable handling failed")
	}
	return nil
}

func (handler *recordingEventHandler) Started(_ context.Context, source core.SourceReference, observedAt time.Time) error {
	*handler.events = append(*handler.events, "started:"+source.Adapter+":"+source.ID+":"+observedAt.Format(time.RFC3339))
	return handler.startedErr
}

func (handler *recordingEventHandler) Completed(_ context.Context, source core.SourceReference, result core.Result) error {
	*handler.events = append(
		*handler.events,
		"completed:"+source.Adapter+":"+source.ID+":"+string(result.Outcome)+":"+string(result.Origin),
	)
	return handler.completedErr
}

type recordingClient struct {
	messages     []*upstream.RunnerScaleSetMessage
	acknowledged []int
	events       []string
	receiveErr   error
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

func (client *recordingClient) DeleteMessage(_ context.Context, messageID int) error {
	if client.deleteErr != nil {
		return client.deleteErr
	}
	client.acknowledged = append(client.acknowledged, messageID)
	client.events = append(client.events, "ack:"+strconv.Itoa(messageID))
	return nil
}

func assertEvents(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("events = %#v, want %#v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("events = %#v, want %#v", got, want)
		}
	}
}

func assertSingleRequest(t *testing.T, requests []core.JobRequest, want core.JobRequest) {
	t.Helper()
	if len(requests) != 1 || requests[0] != want {
		t.Fatalf("source-neutral requests = %#v, want [%#v]", requests, want)
	}
}

func mustSourceID(t *testing.T, requestID int64) string {
	t.Helper()
	sourceID, err := githubsource.EncodeSourceID(githubsource.SourceIdentity{
		Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"), RequestID: requestID,
	})
	if err != nil {
		t.Fatalf("encode source identity: %v", err)
	}
	return sourceID
}

func assertReceivedWithCapacity(t *testing.T, adapter *githubsource.Adapter, client *recordingClient, capacity int) {
	t.Helper()
	received, err := adapter.PollOnce(context.Background(), capacity)
	if err != nil {
		t.Fatalf("poll available job: %v", err)
	}
	if !received || client.maxCapacity != capacity {
		t.Fatalf("poll result = (%t, capacity %d), want (true, capacity %d)",
			received, client.maxCapacity, capacity)
	}
}

func assertFailedAvailableBatch(
	t *testing.T,
	adapter *githubsource.Adapter,
	handler *recordingEventHandler,
	client *recordingClient,
) {
	t.Helper()
	if _, err := adapter.PollOnce(context.Background(), 0); err == nil {
		t.Fatal("PollOnce() error = nil, want unresolved available requests")
	}
	if len(handler.requests) != 2 {
		t.Fatalf("durably handled requests = %#v, want both available requests", handler.requests)
	}
	if len(client.acknowledged) != 0 {
		t.Fatalf("unresolved batch acknowledgements = %#v", client.acknowledged)
	}
}
