package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
)

type PoolReconciliation struct {
	RemovedProcessing int
	RemovedRunners    int
}

func (orchestrator *Orchestrator) OccupiedSlots(ctx context.Context, runnerPool core.RunnerPool) (int, error) {
	processing, err := orchestrator.store.ProcessingByMember(ctx, orchestrator.member, runnerPool)
	if err != nil {
		return 0, fmt.Errorf("list processing work for Runner Pool %q: %w", runnerPool, err)
	}
	instances, err := orchestrator.provider.Inspect(ctx, string(runnerPool))
	if err != nil {
		return 0, fmt.Errorf("inspect occupied runners for Runner Pool %q: %w", runnerPool, err)
	}
	occupied := make(map[string]struct{}, len(processing)+len(instances))
	for _, job := range processing {
		occupied[runnerName(job.Job.ID)] = struct{}{}
	}
	for _, instance := range instances {
		key := instance.RunnerName
		if key == "" {
			key = "provider:" + instance.ID
		}
		occupied[key] = struct{}{}
	}
	return len(occupied), nil
}

func (orchestrator *Orchestrator) ReconcilePool(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (PoolReconciliation, error) {
	processing, err := orchestrator.store.ProcessingByMember(ctx, orchestrator.member, runnerPool)
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("list processing work for Runner Pool %q: %w", runnerPool, err)
	}
	instances, err := orchestrator.provider.Inspect(ctx, string(runnerPool))
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("inspect Runner Pool %q: %w", runnerPool, err)
	}
	return orchestrator.reconcileInventory(ctx, processing, instances)
}

// CleanupPool destroys every member-owned RunnerFoundry resource, including a
// running runner, before removing its processing row. It is reserved for final
// process shutdown; reload uses ReconcilePool and preserves running work.
func (orchestrator *Orchestrator) CleanupPool(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (PoolReconciliation, error) {
	processing, err := orchestrator.store.ProcessingByMember(ctx, orchestrator.member, runnerPool)
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("list shutdown work for Runner Pool %q: %w", runnerPool, err)
	}
	instances, err := orchestrator.provider.Inspect(ctx, string(runnerPool))
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("inspect shutdown Runner Pool %q: %w", runnerPool, err)
	}
	return orchestrator.cleanupInventory(ctx, processing, instances)
}

func (orchestrator *Orchestrator) cleanupInventory(
	ctx context.Context,
	processing []core.ProcessingJob,
	instances []execution.RunnerInstance,
) (PoolReconciliation, error) {
	processingByName := make(map[string]core.ProcessingJob, len(processing))
	instancesByName := make(map[string]struct{}, len(instances))
	for _, job := range processing {
		processingByName[runnerName(job.Job.ID)] = job
	}
	result := PoolReconciliation{}
	failures := make([]error, 0)
	for _, instance := range instances {
		instancesByName[instance.RunnerName] = struct{}{}
		removedRunners, removedProcessing, err := orchestrator.cleanupInstance(ctx, instance, processingByName)
		result.RemovedRunners += removedRunners
		result.RemovedProcessing += removedProcessing
		failures = appendFailure(failures, err)
	}
	removedProcessing, missingFailures := orchestrator.removeMissingProcessing(ctx, processingByName, func(name string) bool {
		_, found := instancesByName[name]
		return found
	})
	result.RemovedProcessing += removedProcessing
	failures = append(failures, missingFailures...)
	return result, errors.Join(failures...)
}

func (orchestrator *Orchestrator) cleanupInstance(
	ctx context.Context,
	instance execution.RunnerInstance,
	processingByName map[string]core.ProcessingJob,
) (removedRunners, removedProcessing int, resultErr error) {
	if !isRunnerFoundryName(instance.RunnerName) {
		return 0, 0, nil
	}
	if err := orchestrator.provider.Destroy(ctx, instance); err != nil {
		return 0, 0, fmt.Errorf("destroy shutdown runner %q: %w", instance.ID, err)
	}
	job, owned := processingByName[instance.RunnerName]
	if !owned {
		return 1, 0, nil
	}
	if err := orchestrator.removeProcessing(ctx, job.Job.ID); err != nil {
		return 1, 0, err
	}
	return 1, 1, nil
}

func (orchestrator *Orchestrator) reconcileInventory(
	ctx context.Context,
	processing []core.ProcessingJob,
	instances []execution.RunnerInstance,
) (PoolReconciliation, error) {
	processingByName := make(map[string]core.ProcessingJob, len(processing))
	instancesByName := make(map[string]execution.RunnerInstance, len(instances))
	for _, job := range processing {
		processingByName[runnerName(job.Job.ID)] = job
	}
	for _, instance := range instances {
		instancesByName[instance.RunnerName] = instance
	}
	result := PoolReconciliation{}
	failures := make([]error, 0)
	for _, instance := range instances {
		removedRunner, removedProcessing, err := orchestrator.reconcileInstance(ctx, instance, processingByName)
		failures = appendFailure(failures, err)
		result.RemovedRunners += removedRunner
		result.RemovedProcessing += removedProcessing
	}
	removedProcessing, missingFailures := orchestrator.removeMissingProcessing(ctx, processingByName, func(name string) bool {
		_, found := instancesByName[name]
		return found
	})
	result.RemovedProcessing += removedProcessing
	failures = append(failures, missingFailures...)
	return result, errors.Join(failures...)
}

func (orchestrator *Orchestrator) removeMissingProcessing(
	ctx context.Context,
	processingByName map[string]core.ProcessingJob,
	instancePresent func(string) bool,
) (int, []error) {
	removed := 0
	failures := make([]error, 0)
	for name, job := range processingByName {
		if instancePresent(name) {
			continue
		}
		if err := orchestrator.removeProcessing(ctx, job.Job.ID); err != nil {
			failures = append(failures, err)
			continue
		}
		removed++
	}
	return removed, failures
}

func appendFailure(failures []error, err error) []error {
	if err == nil {
		return failures
	}
	return append(failures, err)
}

func (orchestrator *Orchestrator) reconcileInstance(
	ctx context.Context,
	instance execution.RunnerInstance,
	processingByName map[string]core.ProcessingJob,
) (removedRunners, removedProcessing int, resultErr error) {
	processing, owned := processingByName[instance.RunnerName]
	runnerFoundryOwned := isRunnerFoundryName(instance.RunnerName)
	if !runnerFoundryOwned || (owned && instance.State == execution.RunnerStateRunning) {
		return 0, 0, nil
	}
	if err := orchestrator.provider.Destroy(ctx, instance); err != nil {
		return 0, 0, fmt.Errorf("destroy runner %q: %w", instance.ID, err)
	}
	if !owned {
		return 1, 0, nil
	}
	if err := orchestrator.removeProcessing(ctx, processing.Job.ID); err != nil {
		return 1, 0, err
	}
	return 1, 1, nil
}

func runnerName(jobID core.JobID) string { return "runnerfoundry-" + jobID.String() }

func isRunnerFoundryName(stableRunnerName string) bool {
	identifier, found := strings.CutPrefix(stableRunnerName, "runnerfoundry-")
	if !found {
		return false
	}
	jobID, err := uuid.Parse(identifier)
	return err == nil && jobID != uuid.Nil
}
