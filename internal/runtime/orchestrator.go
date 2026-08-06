package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
)

type BootstrapGenerator interface {
	GenerateBootstrap(ctx context.Context, job core.Job, runnerName core.RunnerName) (execution.Bootstrap, error)
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
	take, err := orchestrator.store.ClaimOldest(ctx, runnerPool, core.ProcessingClaim{
		MemberID: orchestrator.member, ClaimedAt: orchestrator.currentTime(),
		InventoryEndpointID: orchestrator.launcher.EndpointID(),
	})
	if err != nil {
		return ProcessResult{}, fmt.Errorf("take queued work for Runner Pool %q: %w", runnerPool, err)
	}
	if take.Outcome == core.ClaimEmpty {
		return ProcessResult{Outcome: ProcessNoWork}, nil
	}
	processing := take.Processing
	runnerName := runnerName(processing.Job.ID)
	bootstrap, err := orchestrator.bootstrapper.GenerateBootstrap(ctx, processing.Job, runnerName)
	if err != nil {
		retryOutcome, retryErr := orchestrator.retryUnstartedProcessing(ctx, processing.Job.ID)
		return ProcessResult{Outcome: ProcessRetry, RetryOutcome: retryOutcome}, errors.Join(
			fmt.Errorf("generate bootstrap for job %s: %w", processing.Job.ID, err),
			retryErr,
		)
	}
	if err = orchestrator.launcher.Start(ctx, execution.StartRequest{
		RunnerPool: runnerPool, EndpointID: orchestrator.launcher.EndpointID(), RunnerName: runnerName, Bootstrap: bootstrap,
	}); err != nil {
		startErr := fmt.Errorf("start runner for job %s: %w", processing.Job.ID, err)
		retryOutcome, cleanupErr := orchestrator.cleanupFailedStart(ctx, processing)
		return ProcessResult{Outcome: ProcessRetry, RetryOutcome: retryOutcome}, errors.Join(startErr, cleanupErr)
	}
	return ProcessResult{Outcome: ProcessRunnerStarted}, nil
}

func (orchestrator *Orchestrator) cleanupFailedStart(ctx context.Context, processing core.ProcessingJob) (core.RetryOutcome, error) {
	instances, err := orchestrator.inventory.InspectEndpoint(ctx, processing.InventoryEndpointID)
	if err != nil {
		return "", fmt.Errorf("inspect partial runner for job %s: %w", processing.Job.ID, err)
	}
	name := runnerName(processing.Job.ID)
	for _, instance := range instances {
		if instance.RunnerName != name {
			continue
		}
		if err = orchestrator.inventory.Destroy(ctx, instance); err != nil {
			return "", fmt.Errorf("destroy partial runner %q: %w", instance.ID, err)
		}
		return orchestrator.retryUnstartedProcessing(ctx, processing.Job.ID)
	}
	return orchestrator.retryUnstartedProcessing(ctx, processing.Job.ID)
}

func (orchestrator *Orchestrator) retryUnstartedProcessing(ctx context.Context, jobID core.JobID) (core.RetryOutcome, error) {
	result, err := orchestrator.store.RetryUnstarted(ctx, jobID)
	if err != nil {
		return "", fmt.Errorf("retry unstarted processing job %s: %w", jobID, err)
	}
	return result.Outcome, nil
}
