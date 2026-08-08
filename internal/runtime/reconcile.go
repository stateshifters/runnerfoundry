package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
)

const unstartedProcessingLease = 5 * time.Minute

type PoolReconciliation struct {
	RemovedProcessing   int
	RemovedRunners      int
	RemainingProcessing int
}

type reconciliationPurpose uint8

type reconciliationChange struct {
	removedRunners     int
	removedProcessing  int
	noLongerProcessing int
}

const (
	preserveRunning reconciliationPurpose = iota
	terminateRunning
)

func (orchestrator *Orchestrator) OccupiedSlots(ctx context.Context, runnerPool core.RunnerPool) (int, error) {
	processing, err := orchestrator.store.ProcessingByMember(ctx, orchestrator.member, runnerPool)
	if err != nil {
		return 0, fmt.Errorf("list processing work for Runner Pool %q: %w", runnerPool, err)
	}
	return len(processing), nil
}

// ReconcilePool repairs member-owned processing after provisioning failures or
// restarts. Running resources are preserved; source-terminal work is cleaned
// up; and only unstarted, non-terminal work can return to the queue.
func (orchestrator *Orchestrator) ReconcilePool(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (PoolReconciliation, error) {
	ownedProcessing, err := orchestrator.store.ProcessingByMember(ctx, orchestrator.member, runnerPool)
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("list member processing work for Runner Pool %q: %w", runnerPool, err)
	}
	poolProcessing, err := orchestrator.store.ProcessingByPool(ctx, runnerPool)
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("list pool processing work for Runner Pool %q: %w", runnerPool, err)
	}
	instances, err := orchestrator.inventory.Inspect(ctx)
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("inspect Runner Pool %q: %w", runnerPool, err)
	}
	poolInstances := instancesForPool(instances, runnerPool)
	instancesByName := indexInstancesByName(poolInstances)
	result, processingErr := orchestrator.reconcileOwnedProcessing(ctx, ownedProcessing, instancesByName, preserveRunning)
	removedOrphans, orphanErr := orchestrator.cleanupOrphanedRunners(ctx, poolInstances, poolProcessing)
	result.RemovedRunners += removedOrphans
	return result, errors.Join(processingErr, orphanErr)
}

// TerminatePool destroys member-owned resources for an operator-requested
// shutdown or provider transition. Unstarted jobs return to the queue after
// cleanup, while started jobs retain processing until the source terminalizes
// them. Cleanup failures always retain processing for a later retry.
func (orchestrator *Orchestrator) TerminatePool(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (PoolReconciliation, error) {
	processing, err := orchestrator.store.ProcessingByMember(ctx, orchestrator.member, runnerPool)
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("list member processing work for Runner Pool %q: %w", runnerPool, err)
	}
	instances, err := orchestrator.inventory.Inspect(ctx)
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("inspect Runner Pool %q: %w", runnerPool, err)
	}
	return orchestrator.reconcileOwnedProcessing(
		ctx, processing, indexInstancesByName(instancesForPool(instances, runnerPool)), terminateRunning,
	)
}

// RecoverForeignPool performs leader-authorized cleanup and recovery for
// processing owned by other members. Callers must establish cluster leadership
// before invoking it; endpoints that cannot be inspected from this member are
// retained without mutation.
func (orchestrator *Orchestrator) RecoverForeignPool(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (PoolReconciliation, error) {
	processing, err := orchestrator.store.ProcessingByPool(ctx, runnerPool)
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("list foreign processing work for Runner Pool %q: %w", runnerPool, err)
	}
	instances, err := orchestrator.inventory.Inspect(ctx)
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("inspect foreign Runner Pool %q: %w", runnerPool, err)
	}
	return orchestrator.reconcileForeignProcessing(ctx, processing, instancesForPool(instances, runnerPool))
}

func (orchestrator *Orchestrator) reconcileForeignProcessing(
	ctx context.Context,
	processing []core.ProcessingJob,
	instances []execution.RunnerInstance,
) (PoolReconciliation, error) {
	instancesByName := indexInstancesByName(instances)
	result := PoolReconciliation{}
	failures := make([]error, 0)
	for _, job := range processing {
		if job.MemberID == orchestrator.member {
			continue
		}
		result.RemainingProcessing++
		if !orchestrator.inventory.CanInspectEndpointForMember(job.InventoryEndpointID, job.MemberID) {
			continue
		}
		instance, present := instancesByName[runnerName(job.Job.ID)]
		var instanceReference *execution.RunnerInstance
		if present && (instance.EndpointID == "" || instance.EndpointID == job.InventoryEndpointID) {
			instanceReference = &instance
		}
		change, err := orchestrator.reconcileProcessing(ctx, job, instanceReference, preserveRunning)
		result.RemovedRunners += change.removedRunners
		result.RemovedProcessing += change.removedProcessing
		result.RemainingProcessing -= change.noLongerProcessing
		failures = appendFailure(failures, err)
	}
	return result, errors.Join(failures...)
}

func indexInstancesByName(instances []execution.RunnerInstance) map[core.RunnerName]execution.RunnerInstance {
	instancesByName := make(map[core.RunnerName]execution.RunnerInstance, len(instances))
	for _, instance := range instances {
		instancesByName[instance.RunnerName] = instance
	}
	return instancesByName
}

func (orchestrator *Orchestrator) reconcileOwnedProcessing(
	ctx context.Context,
	processing []core.ProcessingJob,
	instancesByName map[core.RunnerName]execution.RunnerInstance,
	purpose reconciliationPurpose,
) (PoolReconciliation, error) {
	result := PoolReconciliation{RemainingProcessing: len(processing)}
	failures := make([]error, 0)
	for _, job := range processing {
		name := runnerName(job.Job.ID)
		instance, present := instancesByName[name]
		var instanceReference *execution.RunnerInstance
		if present {
			instanceReference = &instance
		}
		change, err := orchestrator.reconcileProcessing(ctx, job, instanceReference, purpose)
		result.RemovedRunners += change.removedRunners
		result.RemovedProcessing += change.removedProcessing
		result.RemainingProcessing -= change.noLongerProcessing
		failures = appendFailure(failures, err)
	}
	return result, errors.Join(failures...)
}

func (orchestrator *Orchestrator) cleanupOrphanedRunners(
	ctx context.Context,
	instances []execution.RunnerInstance,
	processing []core.ProcessingJob,
) (int, error) {
	processingByName := make(map[core.RunnerName]struct{}, len(processing))
	for _, job := range processing {
		processingByName[runnerName(job.Job.ID)] = struct{}{}
	}
	removed := 0
	failures := make([]error, 0)
	for _, instance := range instances {
		if _, owned := processingByName[instance.RunnerName]; owned || !isRunnerFoundryName(instance.RunnerName) {
			continue
		}
		if err := orchestrator.inventory.Destroy(ctx, instance); err != nil {
			failures = append(failures, fmt.Errorf("destroy orphaned runner %q: %w", instance.ID, err))
			continue
		}
		removed++
	}
	return removed, errors.Join(failures...)
}

func (orchestrator *Orchestrator) reconcileProcessing(
	ctx context.Context,
	processing core.ProcessingJob,
	instance *execution.RunnerInstance,
	purpose reconciliationPurpose,
) (reconciliationChange, error) {
	if processing.InventoryEndpointID != "" && !orchestrator.inventory.KnowsEndpoint(processing.InventoryEndpointID) {
		return reconciliationChange{}, nil
	}
	if processing.SourceTerminal {
		return orchestrator.cleanupTerminalProcessing(ctx, processing, instance)
	}
	if processing.InventoryEndpointID != "" && !processing.RunnerStarted {
		return orchestrator.recoverExpiredProcessing(ctx, processing, instance)
	}
	if instance == nil {
		removed, gone, retryErr := orchestrator.retryIfUnstarted(ctx, processing)
		return reconciliationChange{removedProcessing: removed, noLongerProcessing: gone}, retryErr
	}
	if purpose == preserveRunning && instance.State == execution.RunnerStateRunning {
		return reconciliationChange{}, nil
	}
	if err := orchestrator.inventory.Destroy(ctx, *instance); err != nil {
		return reconciliationChange{}, fmt.Errorf("destroy runner %q: %w", instance.ID, err)
	}
	removed, gone, retryErr := orchestrator.retryIfUnstarted(ctx, processing)
	return reconciliationChange{removedRunners: 1, removedProcessing: removed, noLongerProcessing: gone}, retryErr
}

func (orchestrator *Orchestrator) recoverExpiredProcessing(
	ctx context.Context,
	processing core.ProcessingJob,
	instance *execution.RunnerInstance,
) (reconciliationChange, error) {
	if orchestrator.currentTime().Before(processing.ClaimedAt.Add(unstartedProcessingLease)) {
		return reconciliationChange{}, nil
	}
	change := reconciliationChange{}
	if instance != nil {
		if err := orchestrator.inventory.Destroy(ctx, *instance); err != nil {
			return reconciliationChange{}, fmt.Errorf("destroy expired unstarted runner %q: %w", instance.ID, err)
		}
		change.removedRunners = 1
	}
	processing.Job.EnqueuedAt = orchestrator.currentTime()
	transition, err := orchestrator.store.RecoverUnstartedProcessing(ctx, processing)
	if err != nil {
		return change, fmt.Errorf("recover expired processing job %s: %w", processing.Job.ID, err)
	}
	if transition.Outcome == core.RetryRequeued {
		change.removedProcessing = 1
	}
	if transition.Outcome == core.RetryRequeued || transition.Outcome == core.RetryMissing {
		change.noLongerProcessing = 1
	}
	return change, nil
}

func (orchestrator *Orchestrator) cleanupTerminalProcessing(
	ctx context.Context,
	processing core.ProcessingJob,
	instance *execution.RunnerInstance,
) (reconciliationChange, error) {
	change := reconciliationChange{}
	if instance != nil {
		if err := orchestrator.inventory.Destroy(ctx, *instance); err != nil {
			return reconciliationChange{}, fmt.Errorf("destroy terminal runner %q: %w", instance.ID, err)
		}
		change.removedRunners = 1
	}
	transition, err := orchestrator.store.RemoveTerminal(ctx, processing.Job.ID)
	if err != nil {
		return change, fmt.Errorf("remove terminal processing job %s: %w", processing.Job.ID, err)
	}
	if transition.Outcome == core.TerminalRemoved {
		change.removedProcessing = 1
	}
	if transition.Outcome == core.TerminalRemoved || transition.Outcome == core.TerminalMissing {
		change.noLongerProcessing = 1
	}
	return change, nil
}

func (orchestrator *Orchestrator) retryIfUnstarted(ctx context.Context, processing core.ProcessingJob) (removed, gone int, resultErr error) {
	if processing.RunnerStarted {
		return 0, 0, nil
	}
	transition, err := orchestrator.store.RetryUnstarted(ctx, processing.Job.ID)
	if err != nil {
		return 0, 0, fmt.Errorf("retry unstarted processing job %s: %w", processing.Job.ID, err)
	}
	if transition.Outcome == core.RetryRequeued {
		return 1, 1, nil
	}
	if transition.Outcome == core.RetryMissing {
		return 0, 1, nil
	}
	return 0, 0, nil
}

func appendFailure(failures []error, err error) []error {
	if err == nil {
		return failures
	}
	return append(failures, err)
}

func instancesForPool(instances []execution.RunnerInstance, runnerPool core.RunnerPool) []execution.RunnerInstance {
	selected := make([]execution.RunnerInstance, 0, len(instances))
	for _, instance := range instances {
		if instance.RunnerPool == "" || instance.RunnerPool == runnerPool {
			selected = append(selected, instance)
		}
	}
	return selected
}
