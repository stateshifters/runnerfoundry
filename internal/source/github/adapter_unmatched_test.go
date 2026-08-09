package github_test

import (
	"context"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestPollAcknowledgesAndReportsUnmatchedRunnerNameObservations(t *testing.T) {
	t.Parallel()
	message := &upstream.RunnerScaleSetMessage{MessageID: 214,
		JobStartedMessages:   []*upstream.JobStarted{{RunnerName: "legacy-started-runner"}},
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerName: "legacy-completed-runner", Result: "succeeded"}},
	}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	handler := &recordingEventHandler{
		startedOutcome: core.ObservationUnmatched, completedOutcome: core.ObservationUnmatched,
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
	started := findManagerActivity(activities, "runner_started", "unmatched")
	completed := findManagerActivity(activities, "terminalized", "unmatched")
	assertUnmatchedActivity(t, started, "JobStarted", "legacy-started-runner")
	assertUnmatchedActivity(t, completed, "JobCompleted", "legacy-completed-runner")
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
