package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
)

type BootstrapGenerator interface {
	GenerateBootstrap(ctx context.Context, job core.Job, runnerName string) (execution.Bootstrap, error)
}

// Orchestrator owns member-local processing from queue pickup through provider
// cleanup. Workflow outcomes remain between the provisioned runner and source.
type Orchestrator struct {
	store        core.JobStore
	bootstrapper BootstrapGenerator
	provider     execution.ExecutionProvider
	member       core.MemberID
}

func NewOrchestrator(
	store core.JobStore,
	bootstrapper BootstrapGenerator,
	provider execution.ExecutionProvider,
	member core.MemberID,
) *Orchestrator {
	return &Orchestrator{store: store, bootstrapper: bootstrapper, provider: provider, member: member}
}

// ProcessOne moves at most one queued job into processing and starts its
// ephemeral runner. It never requeues failed work.
func (orchestrator *Orchestrator) ProcessOne(ctx context.Context, runnerPool core.RunnerPool) (bool, error) {
	processing, found, err := orchestrator.store.TakeOldest(ctx, runnerPool, orchestrator.member)
	if err != nil {
		return false, fmt.Errorf("take queued work for Runner Pool %q: %w", runnerPool, err)
	}
	if !found {
		return false, nil
	}
	runnerName := runnerName(processing.Job.ID)
	bootstrap, err := orchestrator.bootstrapper.GenerateBootstrap(ctx, processing.Job, runnerName)
	if err != nil {
		return true, errors.Join(
			fmt.Errorf("generate bootstrap for job %s: %w", processing.Job.ID, err),
			orchestrator.removeProcessing(ctx, processing.Job.ID),
		)
	}
	if err = orchestrator.provider.Start(ctx, execution.StartRequest{
		RunnerPool: string(runnerPool), RunnerName: runnerName, Bootstrap: bootstrap,
	}); err != nil {
		startErr := fmt.Errorf("start runner for job %s: %w", processing.Job.ID, err)
		return true, errors.Join(startErr, orchestrator.cleanupFailedStart(ctx, processing))
	}
	return true, nil
}

func (orchestrator *Orchestrator) cleanupFailedStart(ctx context.Context, processing core.ProcessingJob) error {
	instances, err := orchestrator.provider.Inspect(ctx, string(processing.Job.RunnerPool))
	if err != nil {
		return fmt.Errorf("inspect partial runner for job %s: %w", processing.Job.ID, err)
	}
	name := runnerName(processing.Job.ID)
	for _, instance := range instances {
		if instance.RunnerName != name {
			continue
		}
		if err = orchestrator.provider.Destroy(ctx, instance); err != nil {
			return fmt.Errorf("destroy partial runner %q: %w", instance.ID, err)
		}
		return orchestrator.removeProcessing(ctx, processing.Job.ID)
	}
	return orchestrator.removeProcessing(ctx, processing.Job.ID)
}

func (orchestrator *Orchestrator) removeProcessing(ctx context.Context, jobID core.JobID) error {
	if err := orchestrator.store.RemoveProcessing(ctx, jobID); err != nil {
		return fmt.Errorf("remove processing job %s: %w", jobID, err)
	}
	return nil
}
