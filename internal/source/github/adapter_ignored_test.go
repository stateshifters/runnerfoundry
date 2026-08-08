package github_test

import (
	"context"
	"errors"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestPollIgnoresNonActionableAvailableEvidenceAndQueuesValidSibling(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 181, JobAvailableMessages: []*upstream.JobAvailable{
		nil,
		{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 0, JobID: "ignored-job", WorkflowRunID: 801}},
		{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 51, JobID: "valid-job", WorkflowRunID: 802}},
	}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}, acquired: []int64{51}}
	handler := &recordingEventHandler{}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(client, handler)
	adapter.InstallationID, adapter.RunnerGroupID, adapter.ScaleSetID = 701, 702, 703
	adapter.Observer = githubsource.ManagerObserver{Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}

	result, err := adapter.Poll(context.Background(), 1)
	if err != nil || !result.Acknowledged {
		t.Fatalf("Poll() = (%#v, %v), want acknowledged success", result, err)
	}
	assertPersistedRequests(t, handler, []core.JobRequest{jobRequest(t, 51, time.Time{})})
	assertAvailableEvidence(t, ignoredActivities(activities))
}

func assertAvailableEvidence(t *testing.T, ignored []githubsource.ManagerActivity) {
	t.Helper()
	if len(ignored) != 2 || ignored[0].IdentityReason != "nil_event" || ignored[0].SourceRequestIDSet ||
		ignored[1].IdentityReason != "non_positive_request_id" || ignored[1].GitHubJobID != "ignored-job" ||
		ignored[1].WorkflowRunID != 801 || !ignored[1].SourceRequestIDSet || ignored[1].SourceRequestID != 0 ||
		ignored[1].EventType != "JobAvailable" {
		t.Fatalf("ignored activities = %#v", ignored)
	}
	if ignored[1].InstallationID != 701 || ignored[1].RunnerGroupID != 702 || ignored[1].ScaleSetID != 703 {
		t.Fatalf("ignored GitHub registration context = %#v", ignored[1])
	}
}

func ignoredActivities(activities []githubsource.ManagerActivity) []githubsource.ManagerActivity {
	ignored := make([]githubsource.ManagerActivity, 0)
	for _, activity := range activities {
		if activity.Outcome == "ignored" {
			ignored = append(ignored, activity)
		}
	}
	return ignored
}

func TestPollReportsIgnoredEvidenceForEachFailedHandlingAttempt(t *testing.T) {
	t.Parallel()

	message := &upstream.RunnerScaleSetMessage{MessageID: 182,
		JobAssignedMessages: []*upstream.JobAssigned{
			{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 52}},
			{},
		}}
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	activities := make([]githubsource.ManagerActivity, 0)
	adapter := newAdapter(client, &recordingEventHandler{requestErr: errors.New("state unavailable")})
	adapter.Observer = githubsource.ManagerObserver{Report: func(activity githubsource.ManagerActivity) {
		activities = append(activities, activity)
	}}

	result, err := adapter.Poll(context.Background(), 1)
	if err == nil || result != (githubsource.PollResult{Received: true}) {
		t.Fatalf("Poll() = (%#v, %v), want received durable failure", result, err)
	}
	ignored := ignoredActivities(activities)
	if len(ignored) != 1 || ignored[0].IdentityReason != "non_positive_request_id" ||
		!ignored[0].SourceRequestIDSet || ignored[0].SourceRequestID != 0 {
		t.Fatalf("ignored activities = %#v", ignored)
	}
	if len(client.acknowledged) != 0 {
		t.Fatalf("acknowledgements = %#v, want none", client.acknowledged)
	}
}

func TestAvailableEventWithInvalidSourceIdentityKeepsCursor(t *testing.T) {
	assertInvalidSourceIdentityKeepsCursor(t, &upstream.RunnerScaleSetMessage{MessageID: 191,
		JobAvailableMessages: []*upstream.JobAvailable{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 61},
		}}})
}

func TestAssignedEventWithInvalidSourceIdentityKeepsCursor(t *testing.T) {
	assertInvalidSourceIdentityKeepsCursor(t, &upstream.RunnerScaleSetMessage{MessageID: 192,
		JobAssignedMessages: []*upstream.JobAssigned{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 62},
		}}})
}

func TestStartedEventWithInvalidSourceIdentityKeepsCursor(t *testing.T) {
	assertInvalidSourceIdentityKeepsCursor(t, &upstream.RunnerScaleSetMessage{MessageID: 193,
		JobStartedMessages: []*upstream.JobStarted{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 63},
		}}})
}

func TestCompletedEventWithInvalidSourceIdentityKeepsCursor(t *testing.T) {
	assertInvalidSourceIdentityKeepsCursor(t, &upstream.RunnerScaleSetMessage{MessageID: 194,
		JobCompletedMessages: []*upstream.JobCompleted{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 64},
		}}})
}

func assertInvalidSourceIdentityKeepsCursor(t *testing.T, message *upstream.RunnerScaleSetMessage) {
	t.Helper()
	client := &recordingClient{messages: []*upstream.RunnerScaleSetMessage{message}}
	adapter := newAdapter(client, &recordingEventHandler{})
	adapter.Repository = ""

	result, err := adapter.Poll(context.Background(), 1)
	if err == nil || result != (githubsource.PollResult{Received: true}) {
		t.Fatalf("Poll() = (%#v, %v), want received source-identity failure", result, err)
	}
	if len(client.requested) != 0 || len(client.acknowledged) != 0 {
		t.Fatalf("invalid source identity produced GitHub effects: acquire=%#v ack=%#v",
			client.requested, client.acknowledged)
	}
}
