package runtime

import (
	"context"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func (orchestrator *Orchestrator) reconcileProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
) (reconciliationChange, error) {
	if processing.ProvisioningFailure == core.ProvisioningFailureReported {
		return orchestrator.finalizeReconciledProvisioningFailure(ctx, processing)
	}
	return orchestrator.resumeProvisioningFailureReport(ctx, processing)
}

func (orchestrator *Orchestrator) finalizeReconciledProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
) (reconciliationChange, error) {
	resolution, err := orchestrator.removeReportedProvisioningFailure(ctx, processing, false)
	if err != nil {
		return reconciliationChange{}, err
	}
	change := provisioningFailureTransition(resolution, processing.Job)
	change.finalizedTerminalJobs = []core.Job{processing.Job}
	return change, nil
}

func (orchestrator *Orchestrator) resumeProvisioningFailureReport(
	ctx context.Context,
	processing core.ProcessingJob,
) (reconciliationChange, error) {
	operations, err := orchestrator.sourceOperationsFor(processing.Job)
	if err != nil {
		return reconciliationChange{}, err
	}
	reporter, supported := operations.RunnerBootstrapProvider.(core.ProvisioningFailureReporter)
	if !supported {
		return reconciliationChange{}, fmt.Errorf(
			"resume provisioning failure for job %s: source does not support failure reporting", processing.Job.ID,
		)
	}
	if err = reporter.ReportProvisioningFailure(ctx, processing.Job, processingRunnerName(processing)); err != nil {
		return reconciliationChange{}, fmt.Errorf("report provisioning failure for job %s: %w", processing.Job.ID, err)
	}
	return orchestrator.recordReconciledProvisioningFailure(ctx, processing)
}

func (orchestrator *Orchestrator) recordReconciledProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
) (reconciliationChange, error) {
	recorded, err := orchestrator.failures.RecordProvisioningFailureReported(ctx, processing)
	if err != nil {
		return reconciliationChange{}, fmt.Errorf("record reported provisioning failure for job %s: %w", processing.Job.ID, err)
	}
	if recorded.Outcome == core.RetryMissing {
		return reconciliationChange{removedProcessing: 1, noLongerProcessing: 1}, nil
	}
	if recorded.Outcome != core.RetryFailureFinalizationPending && recorded.Outcome != core.RetrySourceTerminal {
		return reconciliationChange{}, fmt.Errorf(
			"record reported provisioning failure for job %s: outcome %q", processing.Job.ID, recorded.Outcome,
		)
	}
	resolution, err := orchestrator.removeReportedProvisioningFailure(ctx, processing, true)
	if err != nil {
		return reconciliationChange{}, err
	}
	return provisioningFailureTransition(resolution, processing.Job), nil
}
