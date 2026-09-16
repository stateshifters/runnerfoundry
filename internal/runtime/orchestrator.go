package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

// Orchestrator owns member-local processing from queue pickup through provider
// cleanup. Workflow outcomes remain between the provisioned runner and source.
type Orchestrator struct {
	store       state.ProcessingJobStore
	transitions state.ProcessingTransitions
	failures    state.ProvisioningFailureTransitions
	sources     core.SourceOperationsResolver
	launcher    execution.RunnerLauncher
	inventory   execution.RunnerInventory
	member      core.MemberID
	currentTime func() time.Time
}

type ProcessOutcome string

const (
	ProcessNoWork  ProcessOutcome = "no_work"
	ProcessClaimed ProcessOutcome = "claimed"
	// ProcessRunnerStarted means provider startup returned successfully. Source
	// acceptance is a later observation recorded through SourceEventHandler.
	ProcessRunnerStarted ProcessOutcome = "runner_started"
	// ProcessProvisioningFailureReported means the source accepted a terminal
	// provisioning result and the internal processing record was removed.
	ProcessProvisioningFailureReported ProcessOutcome = "provisioning_failure_reported"
	ProcessRetry                       ProcessOutcome = "retry"
)

// ProcessResult reports whether a cycle found no demand, claimed demand,
// started a runner, or retained/requeued work for retry. Errors describe the
// failed boundary and do not weaken the durable outcome.
type ProcessResult struct {
	Outcome      ProcessOutcome
	RetryOutcome core.RetryOutcome
	JobID        core.JobID
	Job          core.Job
	FailedStage  string
}

// NewOrchestratorWithSourceOperations constructs the complete processing and
// reconciliation service. The resolver must reject unknown job namespaces and
// preserve optional capabilities on the selected source instance.
func NewOrchestratorWithSourceOperations(
	store state.ProcessingJobStore,
	transitions state.ProcessingTransitions,
	failures state.ProvisioningFailureTransitions,
	sources core.SourceOperationsResolver,
	launcher execution.RunnerLauncher,
	inventory execution.RunnerInventory,
	member core.MemberID,
	currentTime func() time.Time,
) *Orchestrator {
	return &Orchestrator{
		store: store, transitions: transitions, failures: failures, sources: sources, launcher: launcher, inventory: inventory, member: member,
		currentTime: currentTime,
	}
}

// ProcessOne moves at most one queued job into processing and starts its
// ephemeral runner. Provisioning failures return unstarted work to the queue
// only after provider absence is known or partial cleanup succeeds.
func (orchestrator *Orchestrator) ProcessOne(ctx context.Context, runnerPool core.RunnerPool) (bool, error) {
	result, err := orchestrator.Process(ctx, runnerPool)
	return result.Outcome != "" && result.Outcome != ProcessNoWork, err
}

func (orchestrator *Orchestrator) Process(ctx context.Context, runnerPool core.RunnerPool) (ProcessResult, error) {
	processing, empty, err := orchestrator.claimOldest(ctx, runnerPool)
	if err != nil {
		return ProcessResult{}, err
	}
	if empty {
		return ProcessResult{Outcome: ProcessNoWork}, nil
	}
	return orchestrator.provisionClaimed(ctx, runnerPool, processing)
}

func (orchestrator *Orchestrator) claimOldest(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (core.ProcessingJob, bool, error) {
	take, err := orchestrator.store.ClaimOldest(ctx, runnerPool, core.ProcessingClaim{
		MemberID: orchestrator.member, ClaimedAt: orchestrator.currentTime(),
		AttemptID:           core.NewAttemptID(),
		InventoryEndpointID: orchestrator.launcher.EndpointID(),
	})
	if err != nil {
		return core.ProcessingJob{}, false, fmt.Errorf("take queued work for Runner Pool %q: %w", runnerPool, err)
	}
	if take.Outcome == core.ClaimEmpty {
		return core.ProcessingJob{}, true, nil
	}
	return take.Processing, false, nil
}

func (orchestrator *Orchestrator) provisionClaimed(
	ctx context.Context,
	runnerPool core.RunnerPool,
	processing core.ProcessingJob,
) (ProcessResult, error) {
	operations, err := orchestrator.sourceOperationsFor(processing.Job)
	if err != nil {
		return ProcessResult{Outcome: ProcessClaimed, JobID: processing.Job.ID, Job: processing.Job,
			FailedStage: "resolve_source_operations"}, err
	}
	return orchestrator.generateBootstrapAndStart(ctx, runnerPool, processing, operations.RunnerBootstrapProvider)
}

func (orchestrator *Orchestrator) generateBootstrapAndStart(
	ctx context.Context,
	runnerPool core.RunnerPool,
	processing core.ProcessingJob,
	operations core.RunnerBootstrapProvider,
) (ProcessResult, error) {
	runnerName := processingRunnerName(processing)
	bootstrap, err := operations.GenerateBootstrap(ctx, processing.Job, runnerName)
	if err != nil {
		bootstrapErr := fmt.Errorf("generate bootstrap for job %s: %w", processing.Job.ID, err)
		if cleanupErr := orchestrator.cleanupBootstrapWith(ctx, processing, operations); cleanupErr != nil {
			return retryProcessResult(processing.Job, core.RetryCleanupPending, "generate_bootstrap"),
				errors.Join(bootstrapErr, cleanupErr)
		}
		return orchestrator.completeProvisioningFailure(ctx, processing, operations, "generate_bootstrap", bootstrapErr)
	}
	return orchestrator.startClaimedRunner(ctx, runnerPool, processing, operations, bootstrap)
}

func (orchestrator *Orchestrator) startClaimedRunner(
	ctx context.Context,
	runnerPool core.RunnerPool,
	processing core.ProcessingJob,
	operations core.RunnerBootstrapProvider,
	bootstrap core.Bootstrap,
) (ProcessResult, error) {
	runnerName := processingRunnerName(processing)
	if err := orchestrator.launcher.Start(ctx, execution.StartRequest{
		JobID: processing.Job.ID, RunnerPool: runnerPool, EndpointID: orchestrator.launcher.EndpointID(),
		RunnerName: runnerName, Bootstrap: bootstrap,
	}); err != nil {
		startErr := fmt.Errorf("start runner for job %s: %w", processing.Job.ID, err)
		result, cleanupErr := orchestrator.cleanupFailedStart(
			ctx, processing, operations,
		)
		return result, errors.Join(startErr, cleanupErr)
	}
	return ProcessResult{Outcome: ProcessRunnerStarted, JobID: processing.Job.ID, Job: processing.Job}, nil
}

func (orchestrator *Orchestrator) completeProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
	operations core.RunnerBootstrapProvider,
	failedStage string,
	cause error,
) (ProcessResult, error) {
	resolution, completionErr := orchestrator.finalizeProvisioningFailure(
		ctx,
		processing,
		operations,
		func() (core.RetryResult, error) { return orchestrator.transitions.RetryUnstarted(ctx, processing) },
	)
	if resolution.reported && completionErr == nil {
		return ProcessResult{Outcome: ProcessProvisioningFailureReported, RetryOutcome: resolution.outcome,
			JobID: processing.Job.ID, Job: processing.Job, FailedStage: failedStage}, cause
	}
	return retryProcessResult(processing.Job, resolution.outcome, failedStage), errors.Join(cause, completionErr)
}

func retryProcessResult(job core.Job, outcome core.RetryOutcome, failedStage string) ProcessResult {
	return ProcessResult{Outcome: ProcessRetry, RetryOutcome: outcome, JobID: job.ID,
		Job: job, FailedStage: failedStage}
}

func (orchestrator *Orchestrator) cleanupFailedStart(
	ctx context.Context,
	processing core.ProcessingJob,
	operations core.RunnerBootstrapProvider,
) (ProcessResult, error) {
	instances, err := orchestrator.inventory.InspectEndpoint(ctx, processing.InventoryEndpointID)
	if err != nil {
		return retryProcessResult(processing.Job, core.RetryCleanupPending, "start_runner"),
			fmt.Errorf("inspect partial runner for job %s: %w", processing.Job.ID, err)
	}
	name := processingRunnerName(processing)
	for _, instance := range instances {
		if instance.RunnerName != name {
			continue
		}
		if err = orchestrator.inventory.Destroy(ctx, instance); err != nil {
			return retryProcessResult(processing.Job, core.RetryCleanupPending, "start_runner"),
				fmt.Errorf("destroy partial runner %q: %w", instance.ID, err)
		}
		if err = orchestrator.cleanupBootstrapWith(ctx, processing, operations); err != nil {
			return retryProcessResult(processing.Job, core.RetryCleanupPending, "start_runner"), err
		}
		return orchestrator.completeProvisioningFailure(ctx, processing, operations, "start_runner", nil)
	}
	if err = orchestrator.cleanupBootstrapWith(ctx, processing, operations); err != nil {
		return retryProcessResult(processing.Job, core.RetryCleanupPending, "start_runner"), err
	}
	return orchestrator.completeProvisioningFailure(ctx, processing, operations, "start_runner", nil)
}

func (orchestrator *Orchestrator) cleanupBootstrap(ctx context.Context, processing core.ProcessingJob) error {
	operations, err := orchestrator.sourceOperationsFor(processing.Job)
	if err != nil {
		return err
	}
	return orchestrator.cleanupBootstrapWith(ctx, processing, operations.RunnerBootstrapProvider)
}

func (orchestrator *Orchestrator) cleanupBootstrapWith(
	ctx context.Context,
	processing core.ProcessingJob,
	operations core.RunnerBootstrapProvider,
) error {
	if err := operations.CleanupBootstrap(ctx, processing.Job, processingRunnerName(processing)); err != nil {
		return fmt.Errorf("cleanup bootstrap for job %s: %w", processing.Job.ID, err)
	}
	return nil
}

func (orchestrator *Orchestrator) sourceOperationsFor(job core.Job) (core.SourceOperations, error) {
	if orchestrator.sources == nil {
		return core.SourceOperations{}, errors.New("source operations resolver is required")
	}
	operations, err := orchestrator.sources.ResolveSourceOperations(job.Source.Adapter)
	if err != nil {
		return core.SourceOperations{}, fmt.Errorf("resolve source operations for adapter %q: %w", job.Source.Adapter, err)
	}
	if !operations.IsValid() {
		return core.SourceOperations{}, fmt.Errorf(
			"resolve source operations for adapter %q: no operations returned", job.Source.Adapter,
		)
	}
	return operations, nil
}

func (orchestrator *Orchestrator) finalizeProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
	operations core.RunnerBootstrapProvider,
	retry func() (core.RetryResult, error),
) (provisioningFailureResolution, error) {
	reporter, supported := operations.(core.ProvisioningFailureReporter)
	if !supported {
		return retryProvisioningFailure(processing.Job.ID, retry)
	}
	return orchestrator.reserveAndReportProvisioningFailure(ctx, processing, reporter)
}

func (orchestrator *Orchestrator) reserveAndReportProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
	reporter core.ProvisioningFailureReporter,
) (provisioningFailureResolution, error) {
	reservation, err := orchestrator.failures.ReserveProvisioningFailure(ctx, processing)
	if err != nil {
		return pendingProvisioningFailure(),
			fmt.Errorf("reserve provisioning failure for job %s: %w", processing.Job.ID, err)
	}
	switch reservation.Outcome {
	case core.RetryRunnerAccepted, core.RetrySourceTerminal, core.RetryMissing, core.RetryClaimChanged:
		return provisioningFailureResolution{outcome: reservation.Outcome}, nil
	case core.RetryFailureFinalizationPending:
		return orchestrator.removeReportedProvisioningFailure(ctx, processing, false)
	case core.RetryFailureReportingPending:
	case core.RetryRequeued, core.RetryCleanupPending:
		return pendingProvisioningFailure(), fmt.Errorf(
			"reserve provisioning failure for job %s: outcome %q", processing.Job.ID, reservation.Outcome,
		)
	}
	return orchestrator.reportReservedProvisioningFailure(ctx, processing, reporter)
}

func (orchestrator *Orchestrator) reportReservedProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
	reporter core.ProvisioningFailureReporter,
) (provisioningFailureResolution, error) {
	if reportErr := reporter.ReportProvisioningFailure(
		ctx, processing.Job, processingRunnerName(processing),
	); reportErr != nil {
		return pendingProvisioningFailure(),
			fmt.Errorf("report provisioning failure for job %s: %w", processing.Job.ID, reportErr)
	}
	recorded, err := orchestrator.failures.RecordProvisioningFailureReported(ctx, processing)
	if err != nil {
		return pendingProvisioningFailureFinalization(),
			fmt.Errorf("record reported provisioning failure for job %s: %w", processing.Job.ID, err)
	}
	if recorded.Outcome == core.RetryMissing {
		return provisioningFailureResolution{
			outcome: core.RetryMissing, reported: true, finalized: true,
		}, nil
	}
	if recorded.Outcome != core.RetryFailureFinalizationPending && recorded.Outcome != core.RetrySourceTerminal {
		return pendingProvisioningFailureFinalization(), fmt.Errorf(
			"record reported provisioning failure for job %s: outcome %q", processing.Job.ID, recorded.Outcome,
		)
	}
	return orchestrator.removeReportedProvisioningFailure(ctx, processing, true)
}

type provisioningFailureResolution struct {
	outcome   core.RetryOutcome
	reported  bool
	finalized bool
}

func retryProvisioningFailure(
	jobID core.JobID,
	retry func() (core.RetryResult, error),
) (provisioningFailureResolution, error) {
	result, err := retry()
	if err != nil {
		return provisioningFailureResolution{outcome: result.Outcome},
			fmt.Errorf("retry unstarted processing job %s: %w", jobID, err)
	}
	return provisioningFailureResolution{outcome: result.Outcome}, nil
}

func (orchestrator *Orchestrator) removeReportedProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
	reported bool,
) (provisioningFailureResolution, error) {
	removal, err := orchestrator.failures.FinalizeProvisioningFailure(ctx, processing)
	if err != nil {
		return pendingProvisioningFailureFinalization(),
			fmt.Errorf("finalize reported provisioning failure for job %s: %w", processing.Job.ID, err)
	}
	if removal.Outcome != core.TerminalRemoved && removal.Outcome != core.TerminalMissing {
		return pendingProvisioningFailureFinalization(),
			fmt.Errorf("finalize reported provisioning failure for job %s: outcome %q",
				processing.Job.ID, removal.Outcome)
	}
	return provisioningFailureResolution{outcome: core.RetrySourceTerminal, reported: reported, finalized: true}, nil
}

func pendingProvisioningFailure() provisioningFailureResolution {
	return provisioningFailureResolution{outcome: core.RetryFailureReportingPending}
}

func pendingProvisioningFailureFinalization() provisioningFailureResolution {
	return provisioningFailureResolution{outcome: core.RetryFailureFinalizationPending, reported: true}
}
