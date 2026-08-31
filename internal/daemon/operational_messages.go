package daemon

import (
	"fmt"
	"strings"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func operationalEventComponent(event operationalEvent) string {
	if event.component != "" {
		return event.component
	}
	if component := operationalComponents[event.operation]; component != "" {
		return component
	}
	if strings.HasPrefix(event.operation, "provider_") {
		return "execution_provider"
	}
	return "runtime"
}

var operationalComponents = map[string]string{
	"source_reconciliation": "github_registration", "source_sessions": "github_sessions",
	"source_poll": "github_messages", "job_ingestion": "github_messages", "job_observation": "github_messages",
	"assigned_demand": "github_messages",
	"source_routes":   "github_routes", "capacity_publication": "github_capacity",
	"claim": "job_queue", "job_processing": "job_queue", "runner_retry": "job_queue",
	"runner_start": "runner_provisioning", "provisioning_failure": "runner_provisioning",
	"leadership": "github_leadership",
	"heartbeat":  "cluster_membership", "refresh_member_routes": "github_control", "leader_work": "github_control",
	"refresh_inventory": "runner_inventory", "recover_foreign": "runner_recovery",
	"reconcile_runner_pool": "runner_reconciliation", "generation": "runtime", "maintenance": "runtime",
	"maintenance_cycle": "runtime", "reload": "runtime",
}

func operationalEventMessage(event operationalEvent) string {
	if message := operationalMessageBuilders[event.operation]; message != nil {
		return message(event)
	}
	if strings.HasPrefix(event.operation, "provider_") {
		return providerEventMessage(event)
	}
	return fmt.Sprintf("%s %s", titleWords(event.operation), event.outcome)
}

var operationalMessageBuilders = map[string]func(operationalEvent) string{
	"source_reconciliation": githubReconciliationMessage,
	"source_sessions":       githubSessionMessage,
	"job_ingestion":         githubJobIngestionMessage,
	"job_observation":       githubJobObservationMessage,
	"source_poll":           githubPollMessage,
	"assigned_demand":       githubAssignedDemandMessage,
	"capacity_publication": func(event operationalEvent) string {
		return fmt.Sprintf("Published GitHub capacity %d for Runner Pool %s", event.capacity, event.runnerPool)
	},
	"claim":                claimMessage,
	"runner_start":         runnerStartMessage,
	"runner_retry":         runnerRetryMessage,
	"provisioning_failure": provisioningFailureMessage,
	"leadership":           func(event operationalEvent) string { return fmt.Sprintf("GitHub leadership is %s", event.leadership) },
	"job_processing": func(event operationalEvent) string {
		return fmt.Sprintf("Queued-job processing %s for Runner Pool %s", event.outcome, event.runnerPool)
	},
	"reload":     func(event operationalEvent) string { return fmt.Sprintf("Runtime reload %s", event.outcome) },
	"generation": func(event operationalEvent) string { return fmt.Sprintf("Runtime generation %s", event.outcome) },
	"maintenance_cycle": func(event operationalEvent) string {
		return fmt.Sprintf("%s maintenance cycle %s", titleWord(event.stage), event.outcome)
	},
	"maintenance": func(operationalEvent) string { return "Maintenance cycle failed unexpectedly" },
	"heartbeat": func(event operationalEvent) string {
		return fmt.Sprintf("Cluster membership heartbeat %s", event.outcome)
	},
	"refresh_member_routes": func(event operationalEvent) string {
		return fmt.Sprintf("GitHub member route refresh %s", event.outcome)
	},
	"leader_work":       func(event operationalEvent) string { return fmt.Sprintf("GitHub leader work %s", event.outcome) },
	"refresh_inventory": inventoryRefreshMessage,
	"recover_foreign": func(event operationalEvent) string {
		return fmt.Sprintf("Foreign runner recovery %s for Runner Pool %s", event.outcome, event.runnerPool)
	},
	"reconcile_runner_pool": func(event operationalEvent) string {
		return fmt.Sprintf("Runner Pool %s reconciliation %s", event.runnerPool, event.outcome)
	},
}

func claimMessage(event operationalEvent) string {
	if event.outcome != "failed" {
		return fmt.Sprintf("Claimed job %s from Runner Pool %s", event.jobID, event.runnerPool)
	}
	if event.stateBackend != "" {
		return fmt.Sprintf("%s transaction failed while claiming work from Runner Pool %s",
			stateBackendDisplayName(event.stateBackend), event.runnerPool)
	}
	return fmt.Sprintf("Failed to claim work from Runner Pool %s", event.runnerPool)
}

func inventoryRefreshMessage(event operationalEvent) string {
	if event.runnerPool != "" {
		return fmt.Sprintf("Runner inventory refresh %s for Runner Pool %s", event.outcome, event.runnerPool)
	}
	return fmt.Sprintf("Runner inventory refresh %s", event.outcome)
}

func githubJobObservationMessage(event operationalEvent) string {
	if event.outcome == "ignored" {
		return githubIgnoredLifecycleMessage(event)
	}
	if event.outcome == "failed" {
		return githubJobObservationFailureMessage(event)
	}
	return fmt.Sprintf("Acknowledged unmatched %s for runner %s in %s and Runner Pool %s",
		githubEventDisplayName(event.githubEventType), event.runnerName, event.repository, event.runnerPool)
}

func githubJobObservationFailureMessage(event operationalEvent) string {
	action := "record runner acceptance for"
	if event.stage == "persist_terminal" || event.stage == "validate_terminal" {
		action = "record terminal state for"
	}
	subject := githubObservationSubject(event)
	if event.stateBackend != "" {
		return fmt.Sprintf("%s transaction failed while attempting to %s %s for %s in Runner Pool %s",
			stateBackendDisplayName(event.stateBackend), action, subject, event.repository, event.runnerPool)
	}
	return fmt.Sprintf("Failed to %s %s for %s in Runner Pool %s",
		action, subject, event.repository, event.runnerPool)
}

func githubIgnoredLifecycleMessage(event operationalEvent) string {
	return fmt.Sprintf("%s GitHub %s event from message %d for %s in Runner Pool %s%s",
		githubIgnoredLifecycleAction(event.stage), event.githubEventType, event.messageID,
		event.repository, event.runnerPool, githubIgnoredIdentityReason(event.identityReason))
}

func githubIgnoredLifecycleAction(stage string) string {
	switch stage {
	case "validate_available":
		return "Cannot acquire"
	case "validate_assigned":
		return "Cannot recover"
	default:
		return "Cannot correlate"
	}
}

func githubIgnoredIdentityReason(reason string) string {
	switch reason {
	case "nil_event":
		return " because the event entry is empty"
	case "non_positive_request_id":
		return " because it has no runner request ID"
	case "negative_request_id":
		return " because its runner request ID is negative"
	case "missing_identity":
		return " because it has neither a positive runner request ID nor a runner name"
	case "":
		return ""
	default:
		return " because its lifecycle identity is invalid"
	}
}

func githubObservationSubject(event operationalEvent) string {
	if event.sourceRequestID != 0 && event.runnerName != "" {
		return fmt.Sprintf("GitHub request %d and runner %s", event.sourceRequestID, event.runnerName)
	}
	if event.sourceRequestID != 0 {
		return fmt.Sprintf("GitHub request %d", event.sourceRequestID)
	}
	if event.runnerName != "" {
		return fmt.Sprintf("GitHub runner %s", event.runnerName)
	}
	return ""
}

func githubEventDisplayName(eventType string) string {
	return fmt.Sprintf("GitHub %s event", eventType)
}

func githubJobIngestionMessage(event operationalEvent) string {
	if event.outcome == "ignored" {
		return githubIgnoredLifecycleMessage(event)
	}
	if event.outcome == "failed" {
		if event.stateBackend != "" {
			return fmt.Sprintf("%s transaction failed while enqueuing GitHub runner request %d for %s in Runner Pool %s",
				stateBackendDisplayName(event.stateBackend), event.sourceRequestID, event.repository, event.runnerPool)
		}
		return fmt.Sprintf("Failed to persist GitHub runner request %d for %s in Runner Pool %s",
			event.sourceRequestID, event.repository, event.runnerPool)
	}
	if event.outcome == string(core.QueueQueued) {
		return fmt.Sprintf("Queued GitHub runner request %d for %s in Runner Pool %s",
			event.sourceRequestID, event.repository, event.runnerPool)
	}
	return fmt.Sprintf("GitHub runner request %d for %s is %s",
		event.sourceRequestID, event.repository, titleWords(event.outcome))
}

func stateBackendDisplayName(backend string) string {
	switch backend {
	case "postgresql":
		return "PostgreSQL"
	case "sqlite":
		return "SQLite"
	default:
		return backend
	}
}

func githubPollMessage(event operationalEvent) string {
	if event.stage == "acknowledge_message" {
		if event.outcome == "failed" {
			return fmt.Sprintf("Failed to acknowledge GitHub message %d for %s and Runner Pool %s",
				event.messageID, event.repository, event.runnerPool)
		}
		return fmt.Sprintf("Acknowledged GitHub message %d for %s and Runner Pool %s",
			event.messageID, event.repository, event.runnerPool)
	}
	if event.outcome == "failed" {
		return fmt.Sprintf("GitHub message polling failed for %s and Runner Pool %s",
			event.repository, event.runnerPool)
	}
	return fmt.Sprintf("GitHub message polling %s for %s and Runner Pool %s",
		event.outcome, event.repository, event.runnerPool)
}

func runnerStartMessage(event operationalEvent) string {
	provider := providerDisplayName(event.provider)
	if event.sourceRequestID != 0 {
		return fmt.Sprintf("Started %s runner for GitHub request %d in Runner Pool %s",
			provider, event.sourceRequestID, event.runnerPool)
	}
	return fmt.Sprintf("Started %s runner for job %s in Runner Pool %s", provider, event.jobID, event.runnerPool)
}

func runnerRetryMessage(event operationalEvent) string {
	if event.outcome == string(core.RetryFailureReportingPending) {
		return pendingProvisioningFailureMessage(event)
	}
	provider := providerDisplayName(event.provider)
	if event.err != nil && event.stage == "start_runner" {
		if event.sourceRequestID != 0 {
			return fmt.Sprintf("Failed to start %s runner for GitHub request %d from %s in Runner Pool %s",
				provider, event.sourceRequestID, event.repository, event.runnerPool)
		}
		return fmt.Sprintf("Failed to start %s runner for job %s in Runner Pool %s",
			provider, event.jobID, event.runnerPool)
	}
	return fmt.Sprintf("Returned job %s to Runner Pool %s after provisioning failure", event.jobID, event.runnerPool)
}

func provisioningFailureMessage(event operationalEvent) string {
	if event.outcome == string(core.RetryFailureReportingPending) {
		return pendingProvisioningFailureMessage(event)
	}
	if event.count != 0 {
		return fmt.Sprintf("Reported %d replayed provisioning failures in Runner Pool %s", event.count, event.runnerPool)
	}
	return fmt.Sprintf("Reported provisioning failure for job %s in Runner Pool %s", event.jobID, event.runnerPool)
}

func pendingProvisioningFailureMessage(event operationalEvent) string {
	return fmt.Sprintf("Retained job %s in Runner Pool %s while provisioning failure reporting remains pending",
		event.jobID, event.runnerPool)
}

func providerDisplayName(provider string) string {
	switch provider {
	case "docker":
		return "Docker Compose"
	case "libvirt":
		return "libvirt"
	case "":
		return "execution-provider"
	default:
		return provider
	}
}

func githubReconciliationMessage(event operationalEvent) string {
	if event.outcome == "progress" && event.current != 0 && event.total != 0 {
		return fmt.Sprintf("Reconciling GitHub repositories: %d/%d", event.current, event.total)
	}
	if event.outcome == "failed" {
		return githubReconciliationFailureMessage(event)
	}
	return fmt.Sprintf("GitHub repository reconciliation %s: %s", event.outcome, titleWords(event.stage))
}

func githubReconciliationFailureMessage(event operationalEvent) string {
	switch {
	case event.stage == "configure_runner_pool":
		return fmt.Sprintf("Failed to configure GitHub runner scale set for Runner Pool %s in %s",
			event.runnerPool, event.repository)
	case event.stage == "create_repository_client":
		return fmt.Sprintf("Failed to create GitHub App client for %s", event.repository)
	case event.repository != "":
		return fmt.Sprintf("GitHub repository reconciliation failed for %s", event.repository)
	default:
		return fmt.Sprintf("GitHub repository reconciliation failed during %s", titleWords(event.stage))
	}
}

func githubSessionMessage(event operationalEvent) string {
	if event.outcome == "progress" && event.current != 0 && event.total != 0 {
		return fmt.Sprintf("Opening GitHub repository sessions: %d/%d", event.current, event.total)
	}
	if event.outcome == "failed" && event.repository != "" {
		return fmt.Sprintf("Failed to open GitHub message sessions for %s", event.repository)
	}
	return fmt.Sprintf("GitHub repository sessions %s", event.outcome)
}

func providerEventMessage(event operationalEvent) string {
	operation := strings.TrimPrefix(event.operation, "provider_")
	provider := providerDisplayName(event.provider)
	if event.outcome == "failed" {
		return providerFailureMessage(event, operation, provider)
	}
	return fmt.Sprintf("%s %s %s at endpoint %s", provider, operation, event.outcome, event.endpoint)
}

func providerFailureMessage(event operationalEvent, operation string, provider string) string {
	if operation == "destroy" && event.runnerID != "" {
		return providerDestroyFailureMessage(event, provider)
	}
	if event.jobID != (core.JobID{}) {
		return fmt.Sprintf("%s runner %s failed for job %s in Runner Pool %s",
			provider, operation, event.jobID, event.runnerPool)
	}
	return fmt.Sprintf("%s %s failed at endpoint %s", provider, operation, event.endpoint)
}

func providerDestroyFailureMessage(event operationalEvent, provider string) string {
	if event.composeService != "" {
		return fmt.Sprintf("Failed to destroy Docker Compose runner %s from service %s in Runner Pool %s",
			event.runnerID, event.composeService, event.runnerPool)
	}
	return fmt.Sprintf("Failed to destroy %s runner %s in Runner Pool %s",
		provider, event.runnerID, event.runnerPool)
}

func titleWords(value string) string {
	return strings.ReplaceAll(value, "_", " ")
}

func titleWord(value string) string {
	if value == "" {
		return "Runtime"
	}
	return strings.ToUpper(value[:1]) + value[1:]
}
