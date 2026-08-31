package github_test

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestRunnerNameRedeliveryDoesNotBlockFollowingDurableDemand(t *testing.T) {
	t.Parallel()
	fixture := newRuntimeCompositionFixture()
	fixture.pollSuccessfully(t, "lifecycle")
	fixture.pollSuccessfully(t, "demand")
	fixture.assertQueuedDemand(t, sourceReference(t, 42))
	fixture.assertJobProcessingWoken(t, "linux")
	if !reflect.DeepEqual(fixture.client.acknowledged, []int{1, 2}) {
		t.Fatalf("acknowledged messages = %#v, want [1 2]", fixture.client.acknowledged)
	}
}

func TestNonActionableDemandEvidenceDoesNotBlockFollowingDurableDemand(t *testing.T) {
	t.Parallel()
	fixture := newRuntimeCompositionFixtureWithMessages([]*upstream.RunnerScaleSetMessage{
		{MessageID: 3, JobAvailableMessages: []*upstream.JobAvailable{{}}},
		availableMessage(4, 43),
	})
	fixture.client.acquired = []int64{43}
	fixture.pollSuccessfully(t, "non-actionable evidence")
	fixture.pollSuccessfully(t, "demand")
	fixture.assertQueuedDemand(t, sourceReference(t, 43))
	fixture.assertJobProcessingWoken(t, "linux")
	if !reflect.DeepEqual(fixture.client.acknowledged, []int{3, 4}) {
		t.Fatalf("acknowledged messages = %#v, want [3 4]", fixture.client.acknowledged)
	}
}

func TestSourceNeutralHandlerFailuresPreventAcknowledgement(t *testing.T) {
	wantErr := errors.New("neutral handler failed")
	testCases := map[string]struct {
		message *upstream.RunnerScaleSetMessage
		handler core.SourceEventHandler
	}{
		"demand": {
			message: availableMessage(10, 42),
			handler: sourceEventHandlerFake{ingestErr: wantErr},
		},
		"acceptance": {
			message: &upstream.RunnerScaleSetMessage{MessageID: 11,
				JobStartedMessages: []*upstream.JobStarted{{RunnerName: "external-runner"}}},
			handler: sourceEventHandlerFake{acceptErr: wantErr},
		},
		"terminal": {
			message: &upstream.RunnerScaleSetMessage{MessageID: 12,
				JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: "external-runner"}}},
			handler: sourceEventHandlerFake{terminalErr: wantErr},
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{testCase.message}, acquired: []int64{42}}
			adapter := newAdapter(client, &recordingEventHandler{})
			adapter.SourceEvents = testCase.handler
			if result, err := adapter.Poll(context.Background(), 1); !errors.Is(err, wantErr) || result.Acknowledged {
				t.Fatalf("Poll() = (%#v, %v)", result, err)
			}
		})
	}
}

type sourceEventHandlerFake struct {
	ingestErr   error
	acceptErr   error
	terminalErr error
}

func (handler sourceEventHandlerFake) IngestDemand(
	context.Context,
	core.JobRequest,
) (core.EnqueueResult, error) {
	return core.EnqueueResult{}, handler.ingestErr
}

func (handler sourceEventHandlerFake) ObserveRunnerAcceptance(
	context.Context,
	core.RunnerObservation,
) (core.ObservationResult, error) {
	return core.ObservationResult{}, handler.acceptErr
}

func (handler sourceEventHandlerFake) ObserveTerminal(
	context.Context,
	core.RunnerObservation,
) (core.ObservationResult, error) {
	return core.ObservationResult{}, handler.terminalErr
}

type runtimeCompositionFixture struct {
	client  *recordingClient
	store   *runtimeCompositionStore
	woken   chan core.RunnerPool
	adapter *githubsource.Adapter
}

func newRuntimeCompositionFixture() runtimeCompositionFixture {
	return newRuntimeCompositionFixtureWithMessages([]*upstream.RunnerScaleSetMessage{
		{MessageID: 1,
			JobStartedMessages:   []*upstream.JobStarted{{RunnerName: "external-runner"}},
			JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: "external-runner"}}},
		availableMessage(2, 42),
	})
}

func newRuntimeCompositionFixtureWithMessages(messages []*upstream.RunnerScaleSetMessage) runtimeCompositionFixture {
	client := &recordingClient{messages: messages, acquired: []int64{42}}
	store := &runtimeCompositionStore{}
	woken := make(chan core.RunnerPool, 1)
	handler := runtimeQueueDemandHandler{QueueHandler: runtimeengine.NewQueueHandlerWithWake(
		store, func(pool core.RunnerPool) { woken <- pool },
	)}
	return runtimeCompositionFixture{client: client, store: store, woken: woken, adapter: newAdapter(client, handler)}
}

type runtimeQueueDemandHandler struct{ *runtimeengine.QueueHandler }

func (runtimeQueueDemandHandler) ReconcileAssignedDemand(
	context.Context,
	githubsource.AssignedDemand,
) (githubsource.AssignedDemandResult, error) {
	return githubsource.AssignedDemandResult{}, nil
}

func (fixture runtimeCompositionFixture) pollSuccessfully(t *testing.T, label string) {
	t.Helper()
	if result, err := fixture.adapter.Poll(context.Background(), 1); err != nil || !result.Acknowledged {
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

func (*runtimeCompositionStore) ObserveRunnerAcceptance(
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

func TestPollAcknowledgesRunnerNameOnlyAcceptanceObservation(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 211, JobStartedMessages: []*upstream.JobStarted{{
		RunnerName: "runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2A-AY-7OD1OelimCPI-G3ZP_w",
	}}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{}
	adapter := newAdapter(client, handler)
	result, err := adapter.Poll(context.Background(), 0)
	if err != nil || !result.Acknowledged {
		t.Fatalf("Poll() = (%#v, %v), want acknowledged success", result, err)
	}
	want := core.RunnerObservation{RunnerName: core.RunnerName(message.JobStartedMessages[0].RunnerName)}
	if !reflect.DeepEqual(handler.accepted, []core.RunnerObservation{want}) {
		t.Fatalf("accepted = %#v, want %#v", handler.accepted, []core.RunnerObservation{want})
	}
}

func TestPollPreservesBothAcceptanceObservationIdentities(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 212, JobStartedMessages: []*upstream.JobStarted{{
		RunnerName:     "runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2A-AY-7OD1OelimCPI-G3ZP_w",
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
	if !reflect.DeepEqual(handler.accepted, []core.RunnerObservation{want}) {
		t.Fatalf("accepted = %#v, want %#v", handler.accepted, []core.RunnerObservation{want})
	}
}

func TestPollAcknowledgesRunnerNameOnlyCompletedObservation(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 213, JobCompletedMessages: []*upstream.JobCompleted{{
		RunnerName: "runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2A-AY-7OD1OelimCPI-G3ZP_w", Result: "succeeded",
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

func TestPollAcknowledgesUnidentifiedLifecycleEvidenceWithCompleteGitHubContext(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 215, JobStartedMessages: []*upstream.JobStarted{{}}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(client, &recordingEventHandler{})
	adapter.Observer = githubsource.ManagerObserver{CurrentTime: adapter.Observer.CurrentTime, Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}
	result, err := adapter.Poll(context.Background(), 0)
	if err != nil || !result.Acknowledged {
		t.Fatalf("Poll() = (%#v, %v), want acknowledged success", result, err)
	}
	ignored := ignoredActivities(activities)
	if len(ignored) != 1 {
		t.Fatalf("ignored activities = %#v, want one", ignored)
	}
	evidence := ignored[0]
	if evidence.EventType != "JobStarted" || evidence.MessageID != 215 || evidence.Repository != "octo/repo" ||
		evidence.RunnerPool != "linux" || evidence.IdentityReason != "missing_identity" {
		t.Fatalf("ignored identity activity = %#v", evidence)
	}
}

func TestPollIgnoresNonActionableLifecycleEvidenceAndAppliesValidSibling(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 216, JobStartedMessages: []*upstream.JobStarted{
		nil,
		{RunnerName: "invalid-negative", RunnerID: 501, JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: -1, JobID: "negative-job", WorkflowRunID: 901,
		}},
		{RunnerID: 502, JobMessageBase: upstream.JobMessageBase{JobID: "missing-job", WorkflowRunID: 902}},
		{RunnerName: "runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2A-AY-7OD1OelimCPI-G3ZP_w", RunnerID: 503},
	}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(client, handler)
	adapter.Observer = githubsource.ManagerObserver{CurrentTime: adapter.Observer.CurrentTime, Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}

	result, err := adapter.Poll(context.Background(), 0)
	if err != nil || !result.Acknowledged {
		t.Fatalf("Poll() = (%#v, %v), want acknowledged success", result, err)
	}
	if len(handler.accepted) != 1 || handler.accepted[0].RunnerName !=
		"runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2A-AY-7OD1OelimCPI-G3ZP_w" {
		t.Fatalf("accepted observations = %#v", handler.accepted)
	}
	assertLifecycleEvidence(t, ignoredActivities(activities))
}

func assertLifecycleEvidence(t *testing.T, ignored []githubsource.ManagerActivity) {
	t.Helper()
	if len(ignored) != 3 || ignored[0].IdentityReason != "nil_event" ||
		ignored[1].IdentityReason != "negative_request_id" || ignored[1].GitHubRunnerID != 501 ||
		ignored[1].GitHubJobID != "negative-job" || ignored[1].WorkflowRunID != 901 ||
		ignored[2].IdentityReason != "missing_identity" || ignored[2].GitHubRunnerID != 502 ||
		ignored[2].EventType != "JobStarted" {
		t.Fatalf("ignored lifecycle activities = %#v", ignored)
	}
}

func TestPollPreservesBothLifecycleIdentitiesInFailure(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 216, JobStartedMessages: []*upstream.JobStarted{{
		RunnerName:     "runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2A-AY-7OD1OelimCPI-G3ZP_w",
		JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 64},
	}}}
	adapter := newAdapter(&recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}},
		&recordingEventHandler{acceptErr: errors.New("state failed")})
	_, err := adapter.Poll(context.Background(), 0)
	if err == nil || !strings.Contains(err.Error(), "request 64, runner") {
		t.Fatalf("Poll() error = %v", err)
	}
}

func TestPollAcknowledgesNegativeLifecycleRequestIDEvidence(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 217, JobCompletedMessages: []*upstream.JobCompleted{{
		JobMessageBase: upstream.JobMessageBase{RunnerRequestID: -1},
	}}}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(&recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}},
		&recordingEventHandler{})
	adapter.Observer = githubsource.ManagerObserver{CurrentTime: adapter.Observer.CurrentTime, Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}
	result, err := adapter.Poll(context.Background(), 0)
	if err != nil || !result.Acknowledged {
		t.Fatalf("Poll() = (%#v, %v), want acknowledged success", result, err)
	}
	ignored := ignoredActivities(activities)
	if len(ignored) != 1 || ignored[0].IdentityReason != "negative_request_id" ||
		ignored[0].SourceRequestID != -1 {
		t.Fatalf("ignored activities = %#v", ignored)
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
	accepted         []core.RunnerObservation
	completed        []core.RunnerObservation
	requestErr       error
	acceptErr        error
	completedErr     error
	acceptedOutcome  core.ObservationOutcome
	completedOutcome core.ObservationOutcome
	demands          []githubsource.AssignedDemand
	demandResult     githubsource.AssignedDemandResult
	demandErr        error
}

func (handler *recordingEventHandler) ReconcileAssignedDemand(
	_ context.Context,
	demand githubsource.AssignedDemand,
) (githubsource.AssignedDemandResult, error) {
	handler.demands = append(handler.demands, demand)
	if handler.events != nil && demand.DesiredJobs != 0 {
		*handler.events = append(*handler.events, "demand:"+strconv.Itoa(demand.DesiredJobs))
	}
	result := handler.demandResult
	if result == (githubsource.AssignedDemandResult{}) && demand.DesiredJobs > 0 {
		result.JobsAdded = demand.DesiredJobs
	}
	return result, handler.demandErr
}

func (handler *recordingEventHandler) IngestDemand(
	_ context.Context,
	request core.JobRequest,
) (core.EnqueueResult, error) {
	handler.requests = append(handler.requests, request)
	if handler.events != nil {
		*handler.events = append(*handler.events, "request:"+string(request.Source.ID))
	}
	return core.EnqueueResult{Job: core.Job{ID: core.JobID{1}, Source: request.Source, RunnerPool: request.RunnerPool},
		Outcome: core.QueueQueued}, handler.requestErr
}

func (handler *recordingEventHandler) ObserveRunnerAcceptance(
	_ context.Context,
	observation core.RunnerObservation,
) (core.ObservationResult, error) {
	handler.accepted = append(handler.accepted, observation)
	if handler.events != nil {
		*handler.events = append(*handler.events, "accepted:"+string(observation.Source.ID))
	}
	outcome := handler.acceptedOutcome
	if outcome == "" {
		outcome = core.ObservationApplied
	}
	return core.ObservationResult{Outcome: outcome}, handler.acceptErr
}

func (handler *recordingEventHandler) ObserveTerminal(
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
