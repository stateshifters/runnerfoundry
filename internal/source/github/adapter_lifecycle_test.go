package github_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestRunnerNameRedeliveryDoesNotBlockFollowingDurableDemand(t *testing.T) {
	t.Parallel()
	fixture := newRuntimeCompositionFixture()
	fixture.pollSuccessfully(t, "lifecycle", 1)
	fixture.pollSuccessfully(t, "demand", 1)
	fixture.assertQueuedDemand(t, sourceReference(t, 42))
	fixture.assertJobProcessingWoken(t, "linux")
	if !reflect.DeepEqual(fixture.client.acknowledged, []int{1, 2}) {
		t.Fatalf("acknowledged messages = %#v, want [1 2]", fixture.client.acknowledged)
	}
}

type runtimeCompositionFixture struct {
	client  *recordingClient
	store   *runtimeCompositionStore
	woken   chan core.RunnerPool
	adapter *githubsource.Adapter
}

func newRuntimeCompositionFixture() runtimeCompositionFixture {
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{
		{MessageID: 1, JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: "legacy-runner"}}},
		availableMessage(2, 42),
	}, acquired: []int64{42}}
	store := &runtimeCompositionStore{}
	woken := make(chan core.RunnerPool, 1)
	handler := runtimeengine.NewQueueHandlerWithWake(store, func(pool core.RunnerPool) { woken <- pool })
	return runtimeCompositionFixture{client: client, store: store, woken: woken, adapter: newAdapter(client, handler)}
}

func (fixture runtimeCompositionFixture) pollSuccessfully(t *testing.T, label string, capacity int) {
	t.Helper()
	if result, err := fixture.adapter.Poll(context.Background(), capacity); err != nil || !result.Acknowledged {
		t.Fatalf("%s poll = %#v, %v", label, result, err)
	}
}

func (fixture runtimeCompositionFixture) assertQueuedDemand(t *testing.T, source core.SourceReference) {
	t.Helper()
	if len(fixture.store.queued) != 1 || fixture.store.queued[0].Source != source {
		t.Fatalf("durable queued jobs = %#v", fixture.store.queued)
	}
}

func (fixture runtimeCompositionFixture) assertJobProcessingWoken(t *testing.T, want core.RunnerPool) {
	t.Helper()
	select {
	case pool := <-fixture.woken:
		if pool != want {
			t.Fatalf("woken Runner Pool = %q", pool)
		}
	default:
		t.Fatal("durable demand did not wake queued-job processing")
	}
}

type runtimeCompositionStore struct{ queued []core.JobRequest }

func (store *runtimeCompositionStore) Queue(_ context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	store.queued = append(store.queued, request)
	return core.EnqueueResult{Job: core.Job{ID: core.JobID{1}, Source: request.Source, RunnerPool: request.RunnerPool},
		Outcome: core.QueueQueued}, nil
}

func (*runtimeCompositionStore) MarkRunnerStarted(
	context.Context,
	core.JobIdentity,
) (core.ObservationResult, error) {
	return core.ObservationResult{Outcome: core.ObservationUnmatched}, nil
}

func (*runtimeCompositionStore) Terminalize(
	context.Context,
	core.JobIdentity,
) (core.ObservationResult, error) {
	return core.ObservationResult{Outcome: core.ObservationUnmatched}, nil
}

func TestPollAcknowledgesRunnerNameOnlyStartedObservation(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 211, JobStartedMessages: []*upstream.JobStarted{{
		RunnerName: "runnerfoundry-018fbb38-3d4e-7a58-a608-f23e1b764ffe",
	}}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{}
	adapter := newAdapter(client, handler)
	result, err := adapter.Poll(context.Background(), 0)
	if err != nil || !result.Acknowledged {
		t.Fatalf("Poll() = (%#v, %v), want acknowledged success", result, err)
	}
	want := core.RunnerObservation{RunnerName: core.RunnerName(message.JobStartedMessages[0].RunnerName)}
	if !reflect.DeepEqual(handler.started, []core.RunnerObservation{want}) {
		t.Fatalf("started = %#v, want %#v", handler.started, []core.RunnerObservation{want})
	}
}

func TestPollPreservesBothStartedObservationIdentities(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 212, JobStartedMessages: []*upstream.JobStarted{{
		RunnerName:     "runnerfoundry-018fbb38-3d4e-7a58-a608-f23e1b764ffe",
		JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 63},
	}}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{}
	adapter := newAdapter(client, handler)
	if _, err := adapter.Poll(context.Background(), 0); err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	want := core.RunnerObservation{Source: sourceReference(t, 63), RunnerName: core.RunnerName(
		message.JobStartedMessages[0].RunnerName)}
	if !reflect.DeepEqual(handler.started, []core.RunnerObservation{want}) {
		t.Fatalf("started = %#v, want %#v", handler.started, []core.RunnerObservation{want})
	}
}

func TestPollAcknowledgesRunnerNameOnlyCompletedObservation(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 213, JobCompletedMessages: []*upstream.JobCompleted{{
		RunnerName: "runnerfoundry-018fbb38-3d4e-7a58-a608-f23e1b764ffe", Result: "succeeded",
	}}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{}
	adapter := newAdapter(client, handler)
	result, err := adapter.Poll(context.Background(), 0)
	if err != nil || !result.Acknowledged {
		t.Fatalf("Poll() = (%#v, %v), want acknowledged success", result, err)
	}
	want := core.RunnerObservation{RunnerName: core.RunnerName(message.JobCompletedMessages[0].RunnerName)}
	if !reflect.DeepEqual(handler.completed, []core.RunnerObservation{want}) {
		t.Fatalf("completed = %#v, want %#v", handler.completed, []core.RunnerObservation{want})
	}
}

func TestPollAcknowledgesAndReportsUnmatchedRunnerNameObservation(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 214, JobCompletedMessages: []*upstream.JobCompleted{{
		RunnerName: "legacy-runner-name", Result: "succeeded",
	}}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{completedOutcome: core.ObservationUnmatched}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(client, handler)
	adapter.Observer = githubsource.ManagerObserver{Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}
	result, err := adapter.Poll(context.Background(), 0)
	if err != nil || !result.Acknowledged {
		t.Fatalf("Poll() = (%#v, %v), want acknowledged success", result, err)
	}
	unmatched := findManagerActivity(activities, "terminalized", "unmatched")
	if unmatched.EventType != "JobCompleted" || unmatched.MessageID != 214 ||
		unmatched.RunnerName != "legacy-runner-name" || unmatched.Repository != "octo/repo" ||
		unmatched.RunnerPool != "linux" {
		t.Fatalf("unmatched activity = %#v", unmatched)
	}
}

func TestPollLeavesUnidentifiedLifecycleObservationWithCompleteGitHubContext(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 215, JobStartedMessages: []*upstream.JobStarted{{}}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(client, &recordingEventHandler{})
	adapter.Observer = githubsource.ManagerObserver{Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}
	result, err := adapter.Poll(context.Background(), 0)
	if err == nil || result != (githubsource.PollResult{Received: true}) {
		t.Fatalf("Poll() = (%#v, %v), want received unhandled failure", result, err)
	}
	if len(client.acknowledged) != 0 {
		t.Fatalf("acknowledgements = %#v, want none", client.acknowledged)
	}
	failure := findManagerActivity(activities, "validate_runner_started", "failed")
	if failure.EventType != "JobStarted" || failure.MessageID != 215 || failure.Repository != "octo/repo" ||
		failure.RunnerPool != "linux" {
		t.Fatalf("identity failure activity = %#v", failure)
	}
}

func TestPollPreservesBothLifecycleIdentitiesInFailure(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 216, JobStartedMessages: []*upstream.JobStarted{{
		RunnerName:     "runnerfoundry-018fbb38-3d4e-7a58-a608-f23e1b764ffe",
		JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 64},
	}}}
	adapter := newAdapter(&recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}},
		&recordingEventHandler{startedErr: errors.New("state failed")})
	_, err := adapter.Poll(context.Background(), 0)
	if err == nil || !strings.Contains(err.Error(), "request 64, runner") {
		t.Fatalf("Poll() error = %v", err)
	}
}

func TestPollRejectsNegativeLifecycleRequestID(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 217, JobCompletedMessages: []*upstream.JobCompleted{{
		JobMessageBase: upstream.JobMessageBase{RunnerRequestID: -1},
	}}}
	result, err := newAdapter(&recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}},
		&recordingEventHandler{}).Poll(context.Background(), 0)
	if err == nil || result != (githubsource.PollResult{Received: true}) {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
}

func TestPollReportsLifecycleSourceEncodingFailure(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 218, JobStartedMessages: []*upstream.JobStarted{{
		JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 65},
	}}}
	adapter := newAdapter(&recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}},
		&recordingEventHandler{})
	adapter.Repository = ""
	result, err := adapter.Poll(context.Background(), 0)
	if err == nil || result != (githubsource.PollResult{Received: true}) {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
}

type recordingEventHandler struct {
	events           *[]string
	requests         []core.JobRequest
	started          []core.RunnerObservation
	completed        []core.RunnerObservation
	requestErr       error
	startedErr       error
	completedErr     error
	startedOutcome   core.ObservationOutcome
	completedOutcome core.ObservationOutcome
}

func (handler *recordingEventHandler) Request(_ context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	handler.requests = append(handler.requests, request)
	if handler.events != nil {
		*handler.events = append(*handler.events, "request:"+string(request.Source.ID))
	}
	return core.EnqueueResult{Job: core.Job{ID: core.JobID{1}, Source: request.Source, RunnerPool: request.RunnerPool},
		Outcome: core.QueueQueued}, handler.requestErr
}

func (handler *recordingEventHandler) RunnerStarted(
	_ context.Context,
	observation core.RunnerObservation,
) (core.ObservationResult, error) {
	handler.started = append(handler.started, observation)
	if handler.events != nil {
		*handler.events = append(*handler.events, "started:"+string(observation.Source.ID))
	}
	outcome := handler.startedOutcome
	if outcome == "" {
		outcome = core.ObservationApplied
	}
	return core.ObservationResult{Outcome: outcome}, handler.startedErr
}

func (handler *recordingEventHandler) Terminalize(
	_ context.Context,
	observation core.RunnerObservation,
) (core.ObservationResult, error) {
	handler.completed = append(handler.completed, observation)
	if handler.events != nil {
		*handler.events = append(*handler.events, "completed:"+string(observation.Source.ID))
	}
	outcome := handler.completedOutcome
	if outcome == "" {
		outcome = core.ObservationApplied
	}
	return core.ObservationResult{Outcome: outcome}, handler.completedErr
}
