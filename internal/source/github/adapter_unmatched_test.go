package github_test

import (
	"context"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestPollAcknowledgesAndReportsUnmatchedRunnerNameObservations(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 214,
		JobStartedMessages:   []*upstream.JobStarted{{RunnerName: "external-accepted-runner"}},
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: "external-completed-runner", Result: "succeeded"}},
	}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{
		acceptedOutcome: core.ObservationUnmatched, completedOutcome: core.ObservationUnmatched,
	}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(client, handler)
	adapter.Observer = githubsource.ManagerObserver{CurrentTime: adapter.Observer.CurrentTime, Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}
	result, err := adapter.Poll(context.Background(), 0)
	if err != nil || !result.Acknowledged {
		t.Fatalf("Poll() = (%#v, %v), want acknowledged success", result, err)
	}
	accepted := findManagerActivity(activities, "runner_accepted", "unmatched")
	completed := findManagerActivity(activities, "terminalized", "unmatched")
	assertUnmatchedActivity(t, accepted, "JobStarted", "external-accepted-runner")
	assertUnmatchedActivity(t, completed, "JobCompleted", "external-completed-runner")
}

func assertUnmatchedActivity(
	t *testing.T,
	activity githubsource.ManagerActivity,
	eventType string,
	runnerName core.RunnerName,
) {
	t.Helper()
	if activity.EventType != eventType || activity.RunnerName != runnerName || activity.MessageID != 214 ||
		activity.Repository != "octo/repo" || activity.RunnerPool != "linux" {
		t.Fatalf("unmatched activity = %#v", activity)
	}
}
