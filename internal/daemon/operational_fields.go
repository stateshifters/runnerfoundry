package daemon

import (
	"context"
	"errors"
	"log/slog"
	"runtime"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func (reporter *productionRuntimeReporter) writeEvent(
	level slog.Level,
	eventType string,
	event operationalEvent,
	extra []slog.Attr,
) {
	attributes := make([]slog.Attr, 0, len(extra)+24)
	attributes = append(attributes,
		slog.String("type", eventType),
		slog.String("component", operationalEventComponent(event)),
		slog.String("operation", event.operation),
		slog.String("member", string(reporter.member)),
	)
	attributes = append(attributes, eventStageAttributes(event)...)
	attributes = append(attributes, eventIdentityAttributes(event)...)
	attributes = append(attributes, eventCountAttributes(event)...)
	attributes = append(attributes, eventContextAttributes(event)...)
	attributes = append(attributes, eventResultAttributes(event)...)
	attributes = append(attributes, reporter.sourceAttributes(event)...)
	attributes = append(attributes, extra...)
	reporter.logging.Logger().LogAttrs(context.Background(), level, operationalEventMessage(event), attributes...)
}

func (reporter *productionRuntimeReporter) sourceAttributes(event operationalEvent) []slog.Attr {
	if event.sourcePC == 0 || !reporter.logging.Logger().Enabled(context.Background(), slog.LevelDebug) {
		return nil
	}
	frames := runtime.CallersFrames([]uintptr{event.sourcePC})
	frame, _ := frames.Next()
	return []slog.Attr{slog.Group("source",
		slog.String("file", frame.File),
		slog.Int("line", frame.Line),
		slog.String("function", frame.Function),
	)}
}

func eventStageAttributes(event operationalEvent) []slog.Attr {
	return []slog.Attr{
		slog.String("stage", operationalEventStage(event)),
		slog.String("outcome", event.outcome),
	}
}

func operationalEventStage(event operationalEvent) string {
	if event.stage != "" {
		return event.stage
	}
	if stage := operationalDefaultStages[event.operation]; stage != "" {
		return stage
	}
	return "operation"
}

var operationalDefaultStages = map[string]string{
	"capacity_publication": "runner_pool", "claim": "atomic_claim", "runner_start": "provisioning",
	"runner_retry": "retry", "cleanup": "resource_cleanup", "leadership": "election",
	"job_processing": "queue_processor", "reload": "activation", "generation": "lifecycle", "maintenance": "cycle",
}

func eventIdentityAttributes(event operationalEvent) []slog.Attr {
	attributes := coreIdentityAttributes(event)
	return append(attributes, githubIdentityAttributes(event)...)
}

func coreIdentityAttributes(event operationalEvent) []slog.Attr {
	attributes := make([]slog.Attr, 0, 4)
	if event.runnerPool != "" {
		attributes = append(attributes, slog.String("runner_pool", string(event.runnerPool)))
	}
	if event.jobID != (core.JobID{}) {
		attributes = append(attributes, slog.String("job_id", event.jobID.String()))
	}
	if event.runnerID != "" {
		attributes = append(attributes, slog.String("runner_id", event.runnerID))
	}
	if event.runnerName != "" {
		attributes = append(attributes, slog.String("runner_name", string(event.runnerName)))
	}
	return attributes
}

func githubIdentityAttributes(event operationalEvent) []slog.Attr {
	attributes := make([]slog.Attr, 0, 8)
	if event.githubAppID > 0 {
		attributes = append(attributes, slog.Int64("github_app_id", int64(event.githubAppID)))
	}
	if event.installationID != 0 {
		attributes = append(attributes, slog.Int64("installation_id", event.installationID))
	}
	if event.runnerGroupID != 0 {
		attributes = append(attributes, slog.Int64("runner_group_id", event.runnerGroupID))
	}
	if event.scaleSetID != 0 {
		attributes = append(attributes, slog.Int64("scale_set_id", event.scaleSetID))
	}
	if event.messageID != 0 {
		attributes = append(attributes, slog.Int64("message_id", event.messageID))
	}
	if event.githubEventType != "" {
		attributes = append(attributes, slog.String("github_event_type", event.githubEventType))
	}
	if event.githubJobID != "" {
		attributes = append(attributes, slog.String("github_job_id", event.githubJobID))
	}
	if event.workflowRunID != 0 {
		attributes = append(attributes, slog.Int64("workflow_run_id", event.workflowRunID))
	}
	if event.githubRunnerID != 0 {
		attributes = append(attributes, slog.Int64("github_runner_id", event.githubRunnerID))
	}
	return attributes
}

func eventCountAttributes(event operationalEvent) []slog.Attr {
	attributes := generalCountAttributes(event)
	if event.operation == "assigned_demand" {
		attributes = append(attributes, assignedDemandAttributes(event)...)
	}
	return attributes
}

func generalCountAttributes(event operationalEvent) []slog.Attr {
	attributes := make([]slog.Attr, 0, 5)
	if event.count != 0 {
		attributes = append(attributes, slog.Int("count", event.count))
	}
	if event.repositoryCount != 0 {
		attributes = append(attributes, slog.Int("repository_count", event.repositoryCount))
	}
	if event.installationCount != 0 {
		attributes = append(attributes, slog.Int("installation_count", event.installationCount))
	}
	if event.current != 0 {
		attributes = append(attributes, slog.Int("current", event.current))
	}
	if event.total != 0 {
		attributes = append(attributes, slog.Int("total", event.total))
	}
	return attributes
}

func assignedDemandAttributes(event operationalEvent) []slog.Attr {
	return []slog.Attr{
		slog.Int("desired_jobs", event.desiredJobs),
		slog.Int("available_jobs", event.availableJobs),
		slog.Int("acquired_jobs", event.acquiredJobs),
		slog.Int("running_jobs", event.runningJobs),
		slog.Int("registered_runners", event.registeredRunners),
		slog.Int("busy_runners", event.busyRunners),
		slog.Int("idle_runners", event.idleRunners),
		slog.Int("active_jobs", event.activeJobs),
		slog.Int("queued_jobs", event.queuedJobs),
		slog.Int("processing_jobs", event.processingJobs),
		slog.Int("jobs_added", event.jobsAdded),
		slog.Int("jobs_removed", event.jobsRemoved),
	}
}

func eventContextAttributes(event operationalEvent) []slog.Attr {
	attributes := make([]slog.Attr, 0, 4)
	if event.repository != "" {
		attributes = append(attributes, slog.String("repository", event.repository))
	}
	if event.endpoint != "" {
		attributes = append(attributes, slog.String("endpoint", event.endpoint))
	}
	if event.diagnostic != "" {
		attributes = append(attributes, slog.String("error", event.diagnostic))
	}
	if event.sourceRequestID != 0 || event.sourceRequestIDSet {
		attributes = append(attributes, slog.Int64("source_request_id", event.sourceRequestID))
	}
	if event.identityReason != "" {
		attributes = append(attributes, slog.String("identity_reason", event.identityReason))
	}
	return attributes
}

func eventResultAttributes(event operationalEvent) []slog.Attr {
	attributes := make([]slog.Attr, 0, 8)
	if event.capacity != 0 {
		attributes = append(attributes, slog.Int("capacity", event.capacity))
	}
	if event.duration != 0 {
		attributes = append(attributes, slog.Int64("duration_ms", event.duration.Milliseconds()))
	}
	if event.leadership != "" {
		attributes = append(attributes, slog.String("leadership", string(event.leadership)))
	}
	if event.provider != "" {
		attributes = append(attributes, slog.String("provider", event.provider))
	}
	if event.composeService != "" {
		attributes = append(attributes, slog.String("compose_service", event.composeService))
	}
	if event.stateBackend != "" {
		attributes = append(attributes, slog.String("state_backend", event.stateBackend))
	}
	if event.stateOperation != "" {
		attributes = append(attributes, slog.String("state_operation", event.stateOperation))
	}
	if event.err != nil {
		attributes = append(attributes, slog.String("error_category", classifyOperationalError(event)))
	}
	return attributes
}

func classifyOperationalError(event operationalEvent) string {
	if event.errorCategory != "" {
		return event.errorCategory
	}
	if errors.Is(event.err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(event.err, context.DeadlineExceeded) {
		return "deadline"
	}
	return "operation"
}
