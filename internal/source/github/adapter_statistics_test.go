package github_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestPollRejectsMessageWithoutStatisticsBeforeHandlingOrAcknowledgement(t *testing.T) {
	t.Parallel()

	client := &recordingClient{allowMissingStatistics: true, messages: []*upstream.RunnerScaleSetMessage{{
		MessageID: 41, JobAssignedMessages: []*upstream.JobAssigned{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 71},
		}},
	}}}
	handler := &recordingEventHandler{events: &client.events}
	adapter := newAdapter(client, handler)
	adapter.LastMessageID = 40

	result, err := adapter.Poll(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "missing statistics") {
		t.Fatalf("Poll() error = %v, want missing-statistics failure", err)
	}
	if result != (githubsource.PollResult{Received: true}) || adapter.LastMessageID != 40 ||
		len(handler.requests) != 0 || len(client.acknowledged) != 0 {
		t.Fatalf("Poll() = %#v, cursor %d, requests %d, acknowledgements %#v",
			result, adapter.LastMessageID, len(handler.requests), client.acknowledged)
	}
}

func TestPollRetriesMissingInitialSessionStatisticsBeforeReceive(t *testing.T) {
	t.Parallel()

	client := &recordingClient{}
	adapter := newAdapter(client, &recordingEventHandler{})
	adapter.InitialSessionID = uuid.MustParse("175423c3-2f36-429f-a853-f2ec45847c1e")
	adapter.InitialStatisticsPending = true

	for attempt := 0; attempt < 2; attempt++ {
		result, err := adapter.Poll(context.Background(), 1)
		if err == nil || !strings.Contains(err.Error(), "missing initial statistics") ||
			result != (githubsource.PollResult{}) || client.receives != 0 {
			t.Fatalf("Poll() attempt %d = (%#v, %v), receives %d; want retryable protocol failure",
				attempt+1, result, err, client.receives)
		}
	}
}

func TestPollRejectsInitialStatisticsWithoutSessionIdentityBeforeReceive(t *testing.T) {
	t.Parallel()

	client := &recordingClient{}
	adapter := newAdapter(client, &recordingEventHandler{})
	adapter.InitialStatistics = &upstream.RunnerScaleSetStatistic{}
	if result, err := adapter.Poll(context.Background(), 1); err == nil ||
		result != (githubsource.PollResult{}) || client.receives != 0 {
		t.Fatalf("Poll() = (%#v, %v), receives %d; want initial identity failure", result, err, client.receives)
	}
}

func TestPollRejectsStatisticsWithInvalidDemandContext(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 47, Statistics: &upstream.RunnerScaleSetStatistic{}}
	adapter := newAdapter(&recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}},
		&recordingEventHandler{})
	adapter.Repository = ""
	if result, err := adapter.Poll(context.Background(), 1); err == nil ||
		result != (githubsource.PollResult{Received: true}) {
		t.Fatalf("Poll() = (%#v, %v), want received invalid-demand failure", result, err)
	}
}

func TestPollUsesExplicitAssignedDemandHandlerForStatistics(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 48, Statistics: &upstream.RunnerScaleSetStatistic{}}
	assignedDemand := &recordingEventHandler{}
	adapter := newAdapter(&recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}, assignedDemand)
	adapter.SourceEvents = lifecycleOnlyEventHandler{}
	if result, err := adapter.Poll(context.Background(), 1); err != nil || !result.Acknowledged {
		t.Fatalf("Poll() = (%#v, %v), want acknowledged statistics", result, err)
	}
	if len(assignedDemand.demands) != 1 {
		t.Fatalf("assigned-demand observations = %d, want 1", len(assignedDemand.demands))
	}
}

type lifecycleOnlyEventHandler struct{}

func (lifecycleOnlyEventHandler) IngestDemand(context.Context, core.JobRequest) (core.EnqueueResult, error) {
	return core.EnqueueResult{}, nil
}

func (lifecycleOnlyEventHandler) ObserveRunnerAcceptance(
	context.Context,
	core.RunnerObservation,
) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}

func (lifecycleOnlyEventHandler) ObserveTerminal(
	context.Context,
	core.RunnerObservation,
) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}

func TestPollReconcilesMessageStatisticsAfterActionableEffectsBeforeAcknowledgement(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 42,
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 3},
		JobAssignedMessages: []*upstream.JobAssigned{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 72},
		}}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{events: &client.events}
	adapter := newAdapter(client, handler)

	result, err := adapter.Poll(context.Background(), 1)
	assertChangedAcknowledgement(t, result, err)
	wantEvents := []string{
		"demand:3",
		"ack:42",
	}
	if !reflect.DeepEqual(client.events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", client.events, wantEvents)
	}
	wantDemand := githubsource.AssignedDemand{Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"),
		ObservationID: "message:42", DesiredJobs: 3,
		ObservedAt: time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC)}
	if !reflect.DeepEqual(handler.demands, []githubsource.AssignedDemand{wantDemand}) {
		t.Fatalf("assigned demands = %#v, want %#v", handler.demands, wantDemand)
	}
}

func assertChangedAcknowledgement(t *testing.T, result githubsource.PollResult, err error) {
	t.Helper()
	want := githubsource.PollResult{Received: true, Handled: true, Acknowledged: true, DemandChanged: true}
	if err != nil || result != want {
		t.Fatalf("Poll() = (%#v, %v), want %#v", result, err, want)
	}
}

func TestPollReconcilesAssignedDemandDespiteUnusableLifecycleEntries(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 46,
		Statistics:         &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 75},
		JobStartedMessages: unusableStartedMessages(50)}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{events: &client.events}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(client, handler)
	adapter.Observer.Report = func(activity githubsource.ManagerActivity) { activities = append(activities, activity) }

	result, err := adapter.Poll(context.Background(), 1)
	assertChangedAcknowledgement(t, result, err)
	assertUnusableLifecycleReconciliation(t, handler, client.events, activities)
}

func unusableStartedMessages(count int) []*upstream.JobStarted {
	started := make([]*upstream.JobStarted, count)
	for index := range started {
		started[index] = &upstream.JobStarted{}
	}
	return started
}

func assertUnusableLifecycleReconciliation(
	t *testing.T,
	handler *recordingEventHandler,
	events []string,
	activities []githubsource.ManagerActivity,
) {
	t.Helper()
	if len(handler.accepted) != 0 || len(handler.completed) != 0 || len(handler.requests) != 0 {
		t.Fatalf("unusable lifecycle entries mutated state: requests=%d accepted=%d completed=%d",
			len(handler.requests), len(handler.accepted), len(handler.completed))
	}
	if len(handler.demands) != 1 || handler.demands[0].DesiredJobs != 75 {
		t.Fatalf("assigned demands = %#v, want desired 75", handler.demands)
	}
	if !reflect.DeepEqual(events, []string{"demand:75", "ack:46"}) {
		t.Fatalf("events = %#v, want reconciliation before acknowledgement", events)
	}
	if ignored := ignoredActivities(activities); len(ignored) != 50 {
		t.Fatalf("ignored lifecycle warnings = %d, want 50", len(ignored))
	}
}

func TestPollLeavesMessageUnHandledWhenStatisticsReconciliationFails(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("reconciliation failed")
	message := &upstream.RunnerScaleSetMessage{MessageID: 43,
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAssignedMessages: []*upstream.JobAssigned{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 73},
		}}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{events: &client.events, demandErr: wantErr}
	adapter := newAdapter(client, handler)
	adapter.LastMessageID = 42

	result, err := adapter.Poll(context.Background(), 1)
	if !errors.Is(err, wantErr) || result != (githubsource.PollResult{Received: true}) {
		t.Fatalf("Poll() = (%#v, %v), want received reconciliation failure", result, err)
	}
	if adapter.LastMessageID != 42 || len(client.acknowledged) != 0 || len(handler.requests) != 0 {
		t.Fatalf("cursor %d, acknowledgements %#v, durable requests %d",
			adapter.LastMessageID, client.acknowledged, len(handler.requests))
	}
}

func TestPollReportsAssignedDemandReconciliationMetadata(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 44,
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAvailableJobs: 7, TotalAcquiredJobs: 6,
			TotalAssignedJobs: 5, TotalRunningJobs: 4, TotalRegisteredRunners: 9,
			TotalBusyRunners: 3, TotalIdleRunners: 2}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{demandResult: githubsource.AssignedDemandResult{
		ActiveJobs: 5, QueuedJobs: 3, ProcessingJobs: 2, JobsAdded: 2, JobsRemoved: 1,
	}}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(client, handler)
	adapter.InstallationID, adapter.RunnerGroupID, adapter.ScaleSetID = 11, 12, 13
	adapter.Observer.Report = func(activity githubsource.ManagerActivity) { activities = append(activities, activity) }

	if _, err := adapter.Poll(context.Background(), 1); err != nil {
		t.Fatalf("Poll(): %v", err)
	}
	demandActivities := assignedDemandActivities(activities)
	if len(demandActivities) != 3 {
		t.Fatalf("assigned-demand activities = %#v, want debug start/completion and info completion", demandActivities)
	}
	want := expectedAssignedDemandActivityFacts()
	if got := assignedDemandFacts(demandActivities[2]); got != want {
		t.Fatalf("completed assigned-demand activity = %#v, want %#v", got, want)
	}
}

func assignedDemandActivities(activities []githubsource.ManagerActivity) []githubsource.ManagerActivity {
	demandActivities := make([]githubsource.ManagerActivity, 0)
	for _, activity := range activities {
		if activity.Operation == "assigned_demand" {
			demandActivities = append(demandActivities, activity)
		}
	}
	return demandActivities
}

func expectedAssignedDemandActivityFacts() assignedDemandActivityFacts {
	return assignedDemandActivityFacts{Visibility: githubsource.ActivityInfo, Stage: "reconcile", Outcome: "completed",
		Repository: "octo/repo", RunnerPool: "linux", InstallationID: 11, RunnerGroupID: 12, ScaleSetID: 13,
		MessageID: 44, DesiredJobs: 5, ActiveJobs: 5, AvailableJobs: 7, AcquiredJobs: 6, RunningJobs: 4,
		RegisteredRunners: 9, BusyRunners: 3, IdleRunners: 2, QueuedJobs: 3, ProcessingJobs: 2,
		JobsAdded: 2, JobsRemoved: 1}
}

type assignedDemandActivityFacts struct {
	Visibility                                               githubsource.ActivityVisibility
	Stage, Outcome, Repository                               string
	RunnerPool                                               core.RunnerPool
	InstallationID, RunnerGroupID, ScaleSetID, MessageID     int64
	DesiredJobs, ActiveJobs, AvailableJobs, AcquiredJobs     int
	RunningJobs, RegisteredRunners, BusyRunners, IdleRunners int
	QueuedJobs, ProcessingJobs, JobsAdded, JobsRemoved       int
}

func assignedDemandFacts(activity githubsource.ManagerActivity) assignedDemandActivityFacts {
	return assignedDemandActivityFacts{Visibility: activity.Visibility, Stage: activity.Stage, Outcome: activity.Outcome,
		Repository: activity.Repository, RunnerPool: activity.RunnerPool, InstallationID: activity.InstallationID,
		RunnerGroupID: activity.RunnerGroupID, ScaleSetID: activity.ScaleSetID, MessageID: activity.MessageID,
		DesiredJobs: activity.DesiredJobs, ActiveJobs: activity.ActiveJobs, AvailableJobs: activity.AvailableJobs,
		AcquiredJobs: activity.AcquiredJobs, RunningJobs: activity.RunningJobs,
		RegisteredRunners: activity.RegisteredRunners, BusyRunners: activity.BusyRunners,
		IdleRunners: activity.IdleRunners, QueuedJobs: activity.QueuedJobs,
		ProcessingJobs: activity.ProcessingJobs, JobsAdded: activity.JobsAdded, JobsRemoved: activity.JobsRemoved}
}

func TestPollReportsAssignedDemandStateFailureContext(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 45, Statistics: &upstream.RunnerScaleSetStatistic{}}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(&recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}},
		&recordingEventHandler{demandErr: adapterStateError{}})
	adapter.Observer.Report = func(activity githubsource.ManagerActivity) { activities = append(activities, activity) }
	if _, err := adapter.Poll(context.Background(), 1); err == nil {
		t.Fatal("Poll() succeeded")
	}
	failure := findManagerActivity(activities, "reconcile", "failed")
	if failure.Operation != "assigned_demand" || failure.StateBackend != "postgresql" ||
		failure.StateOperation != "enqueue_job" {
		t.Fatalf("assigned-demand failure activity = %#v", failure)
	}
}

func TestPollRetriesInitialAssignedDemandBeforeReceivingMessages(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("state unavailable")
	fixture := initialStatisticsFixture(wantErr)
	adapter, client, handler := fixture.adapter, fixture.client, fixture.handler

	result, err := adapter.Poll(context.Background(), 1)
	assertInitialReconciliationFailure(t, result, err, wantErr, client.receives)
	handler.demandErr = nil
	result, err = adapter.Poll(context.Background(), 1)
	assertInitialDemandChanged(t, result, err, client.receives)
	if _, err = adapter.Poll(context.Background(), 1); err != nil {
		t.Fatalf("subsequent Poll(): %v", err)
	}
	if client.receives != 1 {
		t.Fatalf("receives after unchanged poll = %d, want 1", client.receives)
	}
	wantDemands := repeatedInitialDemands(fixture.observedAt)
	if !reflect.DeepEqual(handler.demands, wantDemands) {
		t.Fatalf("assigned demands = %#v, want %#v", handler.demands, wantDemands)
	}
}

type initialDemandFixture struct {
	adapter    *githubsource.Adapter
	client     *recordingClient
	handler    *recordingEventHandler
	observedAt time.Time
}

func initialStatisticsFixture(wantErr error) initialDemandFixture {
	observedAt := time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC)
	client := &recordingClient{}
	handler := &recordingEventHandler{demandErr: wantErr}
	adapter := newAdapter(client, handler)
	adapter.Observer = githubsource.ManagerObserver{CurrentTime: func() time.Time { return observedAt }}
	adapter.InitialSessionID = uuid.MustParse("1d2da680-4390-4c6f-9f04-2f12c518ac34")
	adapter.InitialStatistics = &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2}
	return initialDemandFixture{adapter: adapter, client: client, handler: handler, observedAt: observedAt}
}

func repeatedInitialDemands(observedAt time.Time) []githubsource.AssignedDemand {
	demand := githubsource.AssignedDemand{Repository: "octo/repo", RunnerPool: core.RunnerPool("linux"),
		ObservationID: "session:1d2da680-4390-4c6f-9f04-2f12c518ac34", DesiredJobs: 2, ObservedAt: observedAt}
	return []githubsource.AssignedDemand{demand, demand}
}

func assertInitialReconciliationFailure(
	t *testing.T,
	result githubsource.PollResult,
	err error,
	wantErr error,
	receives int,
) {
	t.Helper()
	if !errors.Is(err, wantErr) || result != (githubsource.PollResult{}) || receives != 0 {
		t.Fatalf("first Poll() = (%#v, %v), receives %d; want reconciliation failure before receive",
			result, err, receives)
	}
}

func assertInitialDemandChanged(t *testing.T, result githubsource.PollResult, err error, receives int) {
	t.Helper()
	if err != nil || result != (githubsource.PollResult{DemandChanged: true}) || receives != 0 {
		t.Fatalf("retry Poll() = (%#v, %v), receives %d; want changed result before receive", result, err, receives)
	}
}
