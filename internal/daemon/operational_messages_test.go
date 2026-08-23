package daemon

import (
	"errors"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

type operationalMessageCase struct {
	event operationalEvent
	want  string
}

func assertOperationalMessages(t *testing.T, cases []operationalMessageCase) {
	t.Helper()
	for _, testCase := range cases {
		message := operationalEventMessage(testCase.event)
		if !strings.Contains(message, testCase.want) {
			t.Errorf("operationalEventMessage(%#v) = %q, want %q", testCase.event, message, testCase.want)
		}
	}
}

func TestGitHubOperationalMessagesNameTheirSubjects(t *testing.T) {
	assertOperationalMessages(t, []operationalMessageCase{
		{operationalEvent{operation: "source_reconciliation", outcome: "failed", stage: "configure_runner_pool",
			repository: "repo", runnerPool: "linux"}, "runner scale set for Runner Pool linux in repo"},
		{operationalEvent{operation: "source_reconciliation", outcome: "failed", stage: "create_repository_client",
			repository: "repo"}, "GitHub App client for repo"},
		{operationalEvent{operation: "source_reconciliation", outcome: "failed", stage: "persist", repository: "repo"},
			"reconciliation failed for repo"},
		{operationalEvent{operation: "source_reconciliation", outcome: "failed", stage: "discovery"},
			"failed during discovery"},
		{operationalEvent{operation: "source_reconciliation", outcome: "completed", stage: "discovery"},
			"reconciliation completed"},
		{operationalEvent{operation: "source_sessions", outcome: "progress", current: 1, total: 2}, "sessions: 1/2"},
		{operationalEvent{operation: "source_sessions", outcome: "failed", repository: "repo"}, "sessions for repo"},
		{operationalEvent{operation: "source_sessions", outcome: "completed"}, "sessions completed"},
		{operationalEvent{operation: "assigned_demand", outcome: "completed", repository: "repo",
			runnerPool: "linux", jobsAdded: 12}, "Recovered 12 assigned GitHub jobs for repo in Runner Pool linux"},
		{operationalEvent{operation: "assigned_demand", outcome: "failed", repository: "repo",
			runnerPool: "linux"}, "Failed to reconcile assigned GitHub demand for repo in Runner Pool linux"},
		{operationalEvent{operation: "assigned_demand", outcome: "completed", repository: "repo",
			runnerPool: "linux", jobsRemoved: 3}, "Removed 3 excess assigned GitHub jobs for repo in Runner Pool linux"},
		{operationalEvent{operation: "assigned_demand", outcome: "completed", repository: "repo",
			runnerPool: "linux"}, "Reconciled assigned GitHub demand for repo in Runner Pool linux"},
	})
}

func TestGitHubPollMessagesNameRepositoryPoolAndMessage(t *testing.T) {
	assertOperationalMessages(t, []operationalMessageCase{
		{operationalEvent{operation: "source_poll", stage: "acknowledge_message", outcome: "failed",
			messageID: 9, repository: "repo", runnerPool: "linux"}, "Failed to acknowledge GitHub message 9"},
		{operationalEvent{operation: "source_poll", stage: "acknowledge_message", outcome: "completed",
			messageID: 9, repository: "repo", runnerPool: "linux"}, "Acknowledged GitHub message 9"},
		{operationalEvent{operation: "source_poll", outcome: "failed", repository: "repo", runnerPool: "linux"},
			"polling failed for repo"},
		{operationalEvent{operation: "source_poll", outcome: "completed", repository: "repo", runnerPool: "linux"},
			"polling completed for repo"},
	})
}

func TestGitHubIngestionMessagesNameRepositoryPoolAndRequest(t *testing.T) {
	assertOperationalMessages(t, []operationalMessageCase{
		{operationalEvent{operation: "job_ingestion", stage: "validate_available", outcome: "ignored",
			githubEventType: "JobAvailable", messageID: 7, repository: "repo", runnerPool: "linux"},
			"Cannot acquire GitHub JobAvailable event from message 7"},
		{operationalEvent{operation: "job_ingestion", stage: "validate_assigned", outcome: "ignored",
			githubEventType: "JobAssigned", messageID: 8, repository: "repo", runnerPool: "linux"},
			"Cannot recover GitHub JobAssigned event from message 8"},
		{operationalEvent{operation: "job_ingestion", outcome: "failed", repository: "repo", runnerPool: "linux",
			sourceRequestID: 42}, "Failed to persist GitHub runner request 42"},
		{operationalEvent{operation: "job_ingestion", outcome: string(core.QueueAlreadyQueued), repository: "repo",
			runnerPool: "linux", sourceRequestID: 42}, "is already queued"},
		{operationalEvent{operation: "job_ingestion", outcome: string(core.QueueQueued), repository: "repo",
			runnerPool: "linux", sourceRequestID: 42}, "Queued GitHub runner request 42"},
	})
}

func TestGitHubObservationMessagesNameRepositoryPoolAndIdentity(t *testing.T) {
	failed := errors.New("failed")
	assertOperationalMessages(t, []operationalMessageCase{
		{operationalEvent{operation: "job_observation", stage: "validate_runner_started", outcome: "ignored",
			githubEventType: "JobStarted", messageID: 9, repository: "repo", runnerPool: "linux"},
			"Cannot correlate GitHub JobStarted event from message 9"},
		{operationalEvent{operation: "job_observation", stage: "validate_terminal", outcome: "ignored",
			githubEventType: "JobCompleted", messageID: 10, repository: "repo", runnerPool: "linux"},
			"Cannot correlate GitHub JobCompleted event from message 10"},
		{operationalEvent{operation: "job_observation", outcome: "failed", stage: "persist_terminal",
			stateBackend: "sqlite", repository: "repo", runnerPool: "linux", sourceRequestID: 42, err: failed},
			"SQLite transaction failed while attempting to record terminal state for GitHub request 42"},
		{operationalEvent{operation: "job_observation", outcome: "failed", stage: "validate_terminal",
			repository: "repo", runnerPool: "linux", sourceRequestID: 42, err: failed},
			"Failed to record terminal state for GitHub request 42"},
		{operationalEvent{operation: "job_observation", outcome: "failed", stage: "persist_runner_started",
			repository: "repo", runnerPool: "linux", sourceRequestID: 42,
			runnerName:      "runnerfoundry-a73851b8-7df5-4d4c-bc8e-2381926746d8",
			githubEventType: "JobStarted", err: failed},
			"GitHub request 42 and runner runnerfoundry-a73851b8-7df5-4d4c-bc8e-2381926746d8"},
		{operationalEvent{operation: "job_observation", outcome: string(core.ObservationUnmatched),
			repository: "repo", runnerPool: "linux", runnerName: "legacy-runner", githubEventType: "JobCompleted"},
			"Acknowledged unmatched GitHub JobCompleted event for runner legacy-runner"},
	})
}

func TestIgnoredGitHubMessagesExplainEveryIdentityFailure(t *testing.T) {
	assertOperationalMessages(t, []operationalMessageCase{
		{operationalEvent{operation: "job_observation", outcome: "ignored", stage: "validate_terminal",
			githubEventType: "JobCompleted", messageID: 10, repository: "repo", runnerPool: "linux",
			identityReason: "nil_event"}, "because the event entry is empty"},
		{operationalEvent{operation: "job_ingestion", outcome: "ignored", stage: "validate_available",
			githubEventType: "JobAvailable", messageID: 11, repository: "repo", runnerPool: "linux",
			identityReason: "non_positive_request_id"}, "because it has no runner request ID"},
		{operationalEvent{operation: "job_observation", outcome: "ignored", stage: "validate_runner_started",
			githubEventType: "JobStarted", messageID: 12, repository: "repo", runnerPool: "linux",
			identityReason: "negative_request_id"}, "because its runner request ID is negative"},
		{operationalEvent{operation: "job_observation", outcome: "ignored", stage: "validate_terminal",
			githubEventType: "JobCompleted", messageID: 13, repository: "repo", runnerPool: "linux",
			identityReason: "missing_identity"}, "because it has neither a positive runner request ID nor a runner name"},
		{operationalEvent{operation: "job_observation", outcome: "ignored", stage: "validate_terminal",
			githubEventType: "JobCompleted", messageID: 14, repository: "repo", runnerPool: "linux"},
			"Runner Pool linux"},
		{operationalEvent{operation: "job_observation", outcome: "ignored", stage: "validate_terminal",
			githubEventType: "JobCompleted", messageID: 15, repository: "repo", runnerPool: "linux",
			identityReason: "unexpected"}, "because its lifecycle identity is invalid"},
	})
}

func TestCoreOperationalMessagesNameJobsPoolsAndRunners(t *testing.T) {
	jobID := core.JobID{1}
	failed := errors.New("failed")
	assertOperationalMessages(t, []operationalMessageCase{
		{operationalEvent{operation: "claim", outcome: "failed", runnerPool: "linux"}, "Failed to claim work"},
		{operationalEvent{operation: "claim", outcome: "failed", stateBackend: "sqlite", runnerPool: "linux"},
			"SQLite transaction failed while claiming"},
		{operationalEvent{operation: "claim", outcome: "failed", stateBackend: "custom", runnerPool: "linux"},
			"custom transaction failed while claiming"},
		{operationalEvent{operation: "runner_start", outcome: "started", jobID: jobID, runnerPool: "linux"},
			"runner for job"},
		{operationalEvent{operation: "runner_start", outcome: "started", sourceRequestID: 42,
			provider: "libvirt", runnerPool: "linux"}, "libvirt runner for GitHub request 42"},
		{operationalEvent{operation: "runner_retry", stage: "start_runner", outcome: "requeued", err: failed,
			jobID: jobID, runnerPool: "linux"}, "Failed to start execution-provider runner for job"},
		{operationalEvent{operation: "runner_retry", outcome: "requeued", jobID: jobID, runnerPool: "linux"},
			"Returned job"},
	})
}

func TestRuntimeOperationalMessagesNameTheirLifecycleStage(t *testing.T) {
	assertOperationalMessages(t, []operationalMessageCase{
		{operationalEvent{operation: "refresh_inventory", outcome: "failed"}, "Runner inventory refresh failed"},
		{operationalEvent{operation: "capacity_publication", outcome: "published", runnerPool: "linux", capacity: 2},
			"Published GitHub capacity 2"},
		{operationalEvent{operation: "leadership", outcome: "acquired", leadership: leadershipLeader}, "leadership is leader"},
		{operationalEvent{operation: "job_processing", outcome: "failed", runnerPool: "linux"}, "processing failed"},
		{operationalEvent{operation: "reload", outcome: "applied"}, "Runtime reload applied"},
		{operationalEvent{operation: "generation", outcome: "activated"}, "Runtime generation activated"},
		{operationalEvent{operation: "maintenance", outcome: "failed"}, "Maintenance cycle failed unexpectedly"},
		{operationalEvent{operation: "heartbeat", outcome: "failed"}, "membership heartbeat failed"},
		{operationalEvent{operation: "refresh_member_routes", outcome: "failed"}, "member route refresh failed"},
		{operationalEvent{operation: "leader_work", outcome: "failed"}, "leader work failed"},
		{operationalEvent{operation: "recover_foreign", outcome: "failed", runnerPool: "linux"}, "recovery failed"},
		{operationalEvent{operation: "reconcile_runner_pool", outcome: "failed", runnerPool: "linux"},
			"Runner Pool linux reconciliation failed"},
		{operationalEvent{operation: "unknown_operation", outcome: "completed"}, "unknown operation completed"},
		{operationalEvent{operation: "maintenance_cycle", outcome: "started"}, "Runtime maintenance cycle started"},
	})
}

func TestOperationalComponentHonorsExplicitOwnership(t *testing.T) {
	event := operationalEvent{component: "sqlite", operation: "unknown"}
	if got := operationalEventComponent(event); got != "sqlite" {
		t.Fatalf("component = %q", got)
	}
}

func TestProviderOperationalMessagesNameResourcesAndEndpoints(t *testing.T) {
	jobID := core.JobID{1}
	assertOperationalMessages(t, []operationalMessageCase{
		{operationalEvent{operation: "provider_destroy", outcome: "failed", provider: "docker",
			runnerID: "runner-1", composeService: "runner", runnerPool: "linux"}, "service runner"},
		{operationalEvent{operation: "provider_destroy", outcome: "failed", provider: "libvirt",
			runnerID: "domain-1", runnerPool: "linux"}, "libvirt runner domain-1"},
		{operationalEvent{operation: "provider_start", outcome: "failed", provider: "custom", jobID: jobID,
			runnerPool: "linux"}, "custom runner start failed for job"},
		{operationalEvent{operation: "provider_inspect", outcome: "failed", endpoint: "endpoint-1"},
			"execution-provider inspect failed at endpoint endpoint-1"},
		{operationalEvent{operation: "provider_refresh", outcome: "completed", provider: "docker",
			endpoint: "endpoint-1"}, "Docker Compose refresh completed at endpoint endpoint-1"},
	})
}
