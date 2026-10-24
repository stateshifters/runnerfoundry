package state

import "github.com/stateshifters/runnerfoundry/internal/core"

func sameProcessingClaim(actual, expected core.ProcessingJob) bool {
	return actual.Job.ID == expected.Job.ID && actual.ClaimedAt.Equal(expected.ClaimedAt) &&
		actual.InventoryEndpointID == expected.InventoryEndpointID && actual.RunnerName == expected.RunnerName
}

func ClassifyProcessingRetry(actual, expected core.ProcessingJob) core.RetryOutcome {
	if !sameProcessingClaim(actual, expected) {
		return core.RetryClaimChanged
	}
	if actual.SourceTerminal {
		return core.RetrySourceTerminal
	}
	if actual.RunnerAccepted {
		return core.RetryRunnerAccepted
	}
	if actual.ProvisioningFailure == core.ProvisioningFailureReported {
		return core.RetryFailureFinalizationPending
	}
	if actual.ProvisioningFailure == core.ProvisioningFailureReporting {
		return core.RetryFailureReportingPending
	}
	return ""
}
