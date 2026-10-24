package runtime

import (
	"context"
	"errors"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

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
		change, err := orchestrator.reconcileForeignJob(ctx, job, instancesByName)
		applyReconciliationChange(&result, change)
		failures = appendFailure(failures, err)
	}
	return result, errors.Join(failures...)
}

func (orchestrator *Orchestrator) reconcileForeignJob(
	ctx context.Context,
	job core.ProcessingJob,
	instancesByName map[core.RunnerName]execution.RunnerInstance,
) (reconciliationChange, error) {
	if job.ProvisioningFailure == core.ProvisioningFailureOpen &&
		!orchestrator.inventory.CanInspectEndpointForMember(job.InventoryEndpointID, job.MemberID) {
		return reconciliationChange{}, nil
	}
	instance, present := instancesByName[processingRunnerName(job)]
	if !present || (instance.EndpointID != "" && instance.EndpointID != job.InventoryEndpointID) {
		return orchestrator.reconcileProcessing(ctx, job, nil, preserveRunning)
	}
	return orchestrator.reconcileProcessing(ctx, job, &instance, preserveRunning)
}

func countForeignProcessing(processing []core.ProcessingJob, member core.MemberID) int {
	count := 0
	for _, job := range processing {
		if job.MemberID != member {
			count++
		}
	}
	return count
}
