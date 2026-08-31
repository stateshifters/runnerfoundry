package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

const unstartedProcessingLease = 5 * time.Minute

// PoolReconciliation distinguishes newly reported source failures from
// internal terminal finalization completed during a later reconciliation.
// Daemon health tracking uses both lists to clear the corresponding degraded
// operation only after its durable step succeeds.
type PoolReconciliation struct {
	RemovedProcessing            int
	RemovedRunners               int
	RemainingProcessing          int
	ReportedProvisioningFailures []core.Job
	FinalizedTerminalJobs        []core.Job
}

type reconciliationPurpose uint8

type reconciliationChange struct {
	removedRunners               int
	removedProcessing            int
	noLongerProcessing           int
	reportedProvisioningFailures []core.Job
	finalizedTerminalJobs        []core.Job
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
	stateOnly, providerDependent := partitionProvisioningFailures(ownedProcessing)
	result, stateErr := orchestrator.reconcileOwnedProcessing(ctx, stateOnly, nil, preserveRunning)
	instances, err := orchestrator.inventory.Inspect(ctx)
	if err != nil {
		result.RemainingProcessing += len(providerDependent)
		return result, errors.Join(stateErr, fmt.Errorf("inspect Runner Pool %q: %w", runnerPool, err))
	}
	poolInstances := instancesForPool(instances, runnerPool)
	instancesByName := indexInstancesByName(poolInstances)
	providerResult, processingErr := orchestrator.reconcileOwnedProcessing(
		ctx, providerDependent, instancesByName, preserveRunning,
	)
	result = mergePoolReconciliation(result, providerResult)
	removedOrphans, orphanErr := orchestrator.cleanupOrphanedRunners(ctx, poolInstances, poolProcessing)
	result.RemovedRunners += removedOrphans
	return result, errors.Join(stateErr, processingErr, orphanErr)
}

// TerminatePool destroys member-owned resources for an operator-requested
// shutdown or provider transition. Unstarted jobs return to the queue after
// cleanup, while source-accepted jobs retain processing until the source terminalizes
// them. Cleanup failures always retain processing for a later retry.
func (orchestrator *Orchestrator) TerminatePool(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (PoolReconciliation, error) {
	processing, err := orchestrator.store.ProcessingByMember(ctx, orchestrator.member, runnerPool)
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("list member processing work for Runner Pool %q: %w", runnerPool, err)
	}
	stateOnly, providerDependent := partitionProvisioningFailures(processing)
	result, stateErr := orchestrator.reconcileOwnedProcessing(ctx, stateOnly, nil, terminateRunning)
	instances, err := orchestrator.inventory.Inspect(ctx)
	if err != nil {
		result.RemainingProcessing += len(providerDependent)
		return result, errors.Join(stateErr, fmt.Errorf("inspect Runner Pool %q: %w", runnerPool, err))
	}
	providerResult, providerErr := orchestrator.reconcileOwnedProcessing(
		ctx, providerDependent, indexInstancesByName(instancesForPool(instances, runnerPool)), terminateRunning,
	)
	return mergePoolReconciliation(result, providerResult), errors.Join(stateErr, providerErr)
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
	stateOnly, providerDependent := partitionProvisioningFailures(processing)
	result, stateErr := orchestrator.reconcileForeignProcessing(ctx, stateOnly, nil)
	instances, err := orchestrator.inventory.Inspect(ctx)
	if err != nil {
		result.RemainingProcessing += countForeignProcessing(providerDependent, orchestrator.member)
		return result, errors.Join(stateErr, fmt.Errorf("inspect foreign Runner Pool %q: %w", runnerPool, err))
	}
	providerResult, providerErr := orchestrator.reconcileForeignProcessing(
		ctx, providerDependent, instancesForPool(instances, runnerPool),
	)
	return mergePoolReconciliation(result, providerResult), errors.Join(stateErr, providerErr)
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
		name := processingRunnerName(job)
		instance, present := instancesByName[name]
		var instanceReference *execution.RunnerInstance
		if present {
			instanceReference = &instance
		}
		change, err := orchestrator.reconcileProcessing(ctx, job, instanceReference, purpose)
		applyReconciliationChange(&result, change)
		failures = appendFailure(failures, err)
	}
	return result, errors.Join(failures...)
}

func applyReconciliationChange(result *PoolReconciliation, change reconciliationChange) {
	result.RemovedRunners += change.removedRunners
	result.RemovedProcessing += change.removedProcessing
	result.RemainingProcessing -= change.noLongerProcessing
	result.ReportedProvisioningFailures = append(
		result.ReportedProvisioningFailures, change.reportedProvisioningFailures...,
	)
	result.FinalizedTerminalJobs = append(result.FinalizedTerminalJobs, change.finalizedTerminalJobs...)
}

func (orchestrator *Orchestrator) cleanupOrphanedRunners(
	ctx context.Context,
	instances []execution.RunnerInstance,
	processing []core.ProcessingJob,
) (int, error) {
	processingByName := make(map[core.RunnerName]struct{}, len(processing))
	for _, job := range processing {
		processingByName[processingRunnerName(job)] = struct{}{}
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
	if processing.ProvisioningFailure != core.ProvisioningFailureOpen {
		return orchestrator.reconcileProvisioningFailure(ctx, processing)
	}
	if processing.InventoryEndpointID != "" && !orchestrator.inventory.KnowsEndpoint(processing.InventoryEndpointID) {
		return reconciliationChange{}, nil
	}
	if processing.SourceTerminal {
		return orchestrator.cleanupTerminalProcessing(ctx, processing, instance)
	}
	if processing.InventoryEndpointID != "" && !processing.RunnerAccepted {
		return orchestrator.recoverExpiredProcessing(ctx, processing, instance)
	}
	if instance == nil {
		return orchestrator.retryIfUnstarted(ctx, processing)
	}
	if purpose == preserveRunning && instance.State == execution.RunnerStateRunning {
		return reconciliationChange{}, nil
	}
	if err := orchestrator.inventory.Destroy(ctx, *instance); err != nil {
		return reconciliationChange{}, fmt.Errorf("destroy runner %q: %w", instance.ID, err)
	}
	change, retryErr := orchestrator.retryIfUnstarted(ctx, processing)
	change.removedRunners = 1
	return change, retryErr
}

func (orchestrator *Orchestrator) recoverExpiredProcessing(
	ctx context.Context,
	processing core.ProcessingJob,
	instance *execution.RunnerInstance,
) (reconciliationChange, error) {
	if orchestrator.currentTime().Before(processing.ClaimedAt.Add(unstartedProcessingLease)) {
		return reconciliationChange{}, nil
	}
	removedRunners, err := orchestrator.destroyExpiredRunner(ctx, instance)
	if err != nil {
		return reconciliationChange{}, err
	}
	return orchestrator.recoverExpiredClaim(ctx, processing, removedRunners)
}

func (orchestrator *Orchestrator) destroyExpiredRunner(
	ctx context.Context,
	instance *execution.RunnerInstance,
) (int, error) {
	if instance == nil {
		return 0, nil
	}
	if err := orchestrator.inventory.Destroy(ctx, *instance); err != nil {
		return 0, fmt.Errorf("destroy expired unstarted runner %q: %w", instance.ID, err)
	}
	return 1, nil
}

func (orchestrator *Orchestrator) recoverExpiredClaim(
	ctx context.Context,
	processing core.ProcessingJob,
	removedRunners int,
) (reconciliationChange, error) {
	change := reconciliationChange{removedRunners: removedRunners}
	operations, err := orchestrator.sourceOperationsFor(processing.Job)
	if err != nil {
		return change, err
	}
	if cleanupErr := orchestrator.cleanupBootstrapWith(
		ctx, processing, operations.RunnerBootstrapProvider,
	); cleanupErr != nil {
		return change, cleanupErr
	}
	processing.Job.EnqueuedAt = orchestrator.currentTime()
	resolution, err := orchestrator.finalizeProvisioningFailure(
		ctx,
		processing,
		operations.RunnerBootstrapProvider,
		func() (core.RetryResult, error) {
			return orchestrator.store.RecoverUnstartedProcessing(ctx, processing)
		},
	)
	if err != nil {
		return change, fmt.Errorf("recover expired processing job %s: %w", processing.Job.ID, err)
	}
	transition := provisioningFailureTransition(resolution, processing.Job)
	change.removedProcessing = transition.removedProcessing
	change.noLongerProcessing = transition.noLongerProcessing
	change.reportedProvisioningFailures = transition.reportedProvisioningFailures
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
	if !processing.RunnerAccepted {
		if err := orchestrator.cleanupBootstrap(ctx, processing); err != nil {
			return change, err
		}
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
		change.finalizedTerminalJobs = []core.Job{processing.Job}
	}
	return change, nil
}

func (orchestrator *Orchestrator) retryIfUnstarted(
	ctx context.Context,
	processing core.ProcessingJob,
) (reconciliationChange, error) {
	if processing.RunnerAccepted {
		return reconciliationChange{}, nil
	}
	operations, err := orchestrator.sourceOperationsFor(processing.Job)
	if err != nil {
		return reconciliationChange{}, err
	}
	if cleanupErr := orchestrator.cleanupBootstrapWith(
		ctx, processing, operations.RunnerBootstrapProvider,
	); cleanupErr != nil {
		return reconciliationChange{}, cleanupErr
	}
	resolution, err := orchestrator.finalizeProvisioningFailure(
		ctx,
		processing,
		operations.RunnerBootstrapProvider,
		func() (core.RetryResult, error) {
			return orchestrator.store.RetryUnstarted(ctx, processing)
		},
	)
	if err != nil {
		return reconciliationChange{}, fmt.Errorf("retry unstarted processing job %s: %w", processing.Job.ID, err)
	}
	return provisioningFailureTransition(resolution, processing.Job), nil
}

func provisioningFailureTransition(
	resolution provisioningFailureResolution,
	job core.Job,
) reconciliationChange {
	if resolution.finalized {
		change := reconciliationChange{removedProcessing: 1, noLongerProcessing: 1}
		if resolution.reported {
			change.reportedProvisioningFailures = []core.Job{job}
		}
		return change
	}
	if resolution.outcome == core.RetryRequeued {
		return reconciliationChange{removedProcessing: 1, noLongerProcessing: 1}
	}
	if resolution.outcome == core.RetryMissing {
		return reconciliationChange{noLongerProcessing: 1}
	}
	return reconciliationChange{}
}

func appendFailure(failures []error, err error) []error {
	if err == nil {
		return failures
	}
	return append(failures, err)
}

func partitionProvisioningFailures(
	processing []core.ProcessingJob,
) (stateOnly, providerDependent []core.ProcessingJob) {
	for _, job := range processing {
		if job.ProvisioningFailure != core.ProvisioningFailureOpen {
			stateOnly = append(stateOnly, job)
			continue
		}
		providerDependent = append(providerDependent, job)
	}
	return stateOnly, providerDependent
}

func mergePoolReconciliation(first, second PoolReconciliation) PoolReconciliation {
	first.RemovedProcessing += second.RemovedProcessing
	first.RemovedRunners += second.RemovedRunners
	first.RemainingProcessing += second.RemainingProcessing
	first.ReportedProvisioningFailures = append(
		first.ReportedProvisioningFailures, second.ReportedProvisioningFailures...,
	)
	first.FinalizedTerminalJobs = append(first.FinalizedTerminalJobs, second.FinalizedTerminalJobs...)
	return first
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
