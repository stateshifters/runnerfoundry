package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

type BootstrapGenerator interface {
	GenerateBootstrap(ctx context.Context, job core.Job, runnerName core.RunnerName) (execution.Bootstrap, error)
	CleanupBootstrap(ctx context.Context, job core.Job, runnerName core.RunnerName) error
}

// Orchestrator owns member-local processing from queue pickup through provider
// cleanup. Workflow outcomes remain between the provisioned runner and source.
type Orchestrator struct {
	store        core.ProcessingJobStore
	bootstrapper BootstrapGenerator
	launcher     execution.RunnerLauncher
	inventory    execution.RunnerInventory
	member       core.MemberID
	currentTime  func() time.Time
}

type ProcessOutcome string

const (
	ProcessNoWork        ProcessOutcome = "no_work"
	ProcessClaimed       ProcessOutcome = "claimed"
	ProcessRunnerStarted ProcessOutcome = "runner_started"
	ProcessRetry         ProcessOutcome = "retry"
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

func NewOrchestrator(
	store core.ProcessingJobStore,
	bootstrapper BootstrapGenerator,
	launcher execution.RunnerLauncher,
	inventory execution.RunnerInventory,
	member core.MemberID,
	currentTime func() time.Time,
) *Orchestrator {
	return &Orchestrator{
		store: store, bootstrapper: bootstrapper, launcher: launcher, inventory: inventory, member: member,
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
		AttemptID:           uuid.New(),
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
	runnerName := processingRunnerName(processing)
	bootstrap, err := orchestrator.bootstrapper.GenerateBootstrap(ctx, processing.Job, runnerName)
	if err != nil {
		bootstrapErr := fmt.Errorf("generate bootstrap for job %s: %w", processing.Job.ID, err)
		if cleanupErr := orchestrator.cleanupBootstrap(ctx, processing); cleanupErr != nil {
			return retryProcessResult(processing.Job, core.RetryCleanupPending, "generate_bootstrap"),
				errors.Join(bootstrapErr, cleanupErr)
		}
		return orchestrator.retryClaim(ctx, processing, "generate_bootstrap", bootstrapErr)
	}
	if err = orchestrator.launcher.Start(ctx, execution.StartRequest{
		JobID: processing.Job.ID, RunnerPool: runnerPool, EndpointID: orchestrator.launcher.EndpointID(),
		RunnerName: runnerName, Bootstrap: bootstrap,
	}); err != nil {
		startErr := fmt.Errorf("start runner for job %s: %w", processing.Job.ID, err)
		retryOutcome, cleanupErr := orchestrator.cleanupFailedStart(ctx, processing)
		return retryProcessResult(processing.Job, retryOutcome, "start_runner"), errors.Join(startErr, cleanupErr)
	}
	return ProcessResult{Outcome: ProcessRunnerStarted, JobID: processing.Job.ID, Job: processing.Job}, nil
}

func (orchestrator *Orchestrator) retryClaim(
	ctx context.Context,
	processing core.ProcessingJob,
	failedStage string,
	cause error,
) (ProcessResult, error) {
	retryOutcome, retryErr := orchestrator.retryUnstartedProcessing(ctx, processing.Job.ID)
	return retryProcessResult(processing.Job, retryOutcome, failedStage), errors.Join(cause, retryErr)
}

func retryProcessResult(job core.Job, outcome core.RetryOutcome, failedStage string) ProcessResult {
	return ProcessResult{Outcome: ProcessRetry, RetryOutcome: outcome, JobID: job.ID,
		Job: job, FailedStage: failedStage}
}

func (orchestrator *Orchestrator) cleanupFailedStart(ctx context.Context, processing core.ProcessingJob) (core.RetryOutcome, error) {
	instances, err := orchestrator.inventory.InspectEndpoint(ctx, processing.InventoryEndpointID)
	if err != nil {
		return "", fmt.Errorf("inspect partial runner for job %s: %w", processing.Job.ID, err)
	}
	name := processingRunnerName(processing)
	for _, instance := range instances {
		if instance.RunnerName != name {
			continue
		}
		if err = orchestrator.inventory.Destroy(ctx, instance); err != nil {
			return "", fmt.Errorf("destroy partial runner %q: %w", instance.ID, err)
		}
		if err = orchestrator.cleanupBootstrap(ctx, processing); err != nil {
			return core.RetryCleanupPending, err
		}
		return orchestrator.retryUnstartedProcessing(ctx, processing.Job.ID)
	}
	if err = orchestrator.cleanupBootstrap(ctx, processing); err != nil {
		return core.RetryCleanupPending, err
	}
	return orchestrator.retryUnstartedProcessing(ctx, processing.Job.ID)
}

func (orchestrator *Orchestrator) cleanupBootstrap(ctx context.Context, processing core.ProcessingJob) error {
	if err := orchestrator.bootstrapper.CleanupBootstrap(ctx, processing.Job, processingRunnerName(processing)); err != nil {
		return fmt.Errorf("cleanup bootstrap for job %s: %w", processing.Job.ID, err)
	}
	return nil
}

func (orchestrator *Orchestrator) retryUnstartedProcessing(ctx context.Context, jobID core.JobID) (core.RetryOutcome, error) {
	result, err := orchestrator.store.RetryUnstarted(ctx, jobID)
	if err != nil {
		return "", fmt.Errorf("retry unstarted processing job %s: %w", jobID, err)
	}
	return result.Outcome, nil
}
