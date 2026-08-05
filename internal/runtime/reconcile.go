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
	FailedAttempts int
	RemovedRunners int
}

// OccupiedSlots returns the union of durable member attempts and provider
// instances so claimed work consumes capacity before a runner exists.
func (orchestrator *Orchestrator) OccupiedSlots(ctx context.Context, runnerPool core.RunnerPool) (int, error) {
	attempts, err := orchestrator.store.AttemptsByMember(ctx, orchestrator.member, runnerPool)
	if err != nil {
		return 0, fmt.Errorf("list occupied attempts for Runner Pool %q: %w", runnerPool, err)
	}
	instances, err := orchestrator.provider.Inspect(ctx, string(runnerPool))
	if err != nil {
		return 0, fmt.Errorf("inspect occupied runners for Runner Pool %q: %w", runnerPool, err)
	}

	occupied := make(map[string]struct{}, len(attempts)+len(instances))
	for _, attempt := range attempts {
		occupied[runnerName(attempt.Job.ID)] = struct{}{}
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

// ReconcilePool aligns this member's durable attempts with its provider
// inventory. Providers may explicitly identify a lost runner as unknown;
// absence alone is left to the start timeout or source completion because an
// inventory observation can race a source event. Cleanup errors do not change
// job state and are retried on the next reconciliation.
func (orchestrator *Orchestrator) ReconcilePool(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (PoolReconciliation, error) {
	instances, err := orchestrator.provider.Inspect(ctx, string(runnerPool))
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("inspect Runner Pool %q: %w", runnerPool, err)
	}
	attempts, err := orchestrator.store.AttemptsByMember(ctx, orchestrator.member, runnerPool)
	if err != nil {
		return PoolReconciliation{}, fmt.Errorf("list member attempts for Runner Pool %q: %w", runnerPool, err)
	}

	ownedNames := make(map[string]core.JobRecord, len(attempts))
	providerStates := make(map[string]execution.RunnerState, len(instances))
	for _, attempt := range attempts {
		ownedNames[runnerName(attempt.Job.ID)] = attempt
	}
	for _, instance := range instances {
		providerStates[instance.RunnerName] = instance.State
	}

	result, attemptErrors := orchestrator.failUnknownAttempts(ctx, ownedNames, providerStates)
	removed, cleanupErrors := orchestrator.removeTerminalAndOrphanInstances(ctx, instances, ownedNames)
	result.RemovedRunners = removed

	return result, errors.Join(attemptErrors, cleanupErrors)
}

func (orchestrator *Orchestrator) failUnknownAttempts(
	ctx context.Context,
	attempts map[string]core.JobRecord,
	providerStates map[string]execution.RunnerState,
) (PoolReconciliation, error) {
	result := PoolReconciliation{}
	failures := make([]error, 0)
	for name, attempt := range attempts {
		if providerStates[name] != execution.RunnerStateUnknown {
			continue
		}
		attemptErr := fmt.Errorf("runner %q disappeared during the job attempt", name)
		if err := orchestrator.completeAttempt(ctx, attempt.Job, attemptErr); err != nil {
			failures = append(failures, err)
		}
		result.FailedAttempts++
	}

	return result, errors.Join(failures...)
}

func (orchestrator *Orchestrator) removeTerminalAndOrphanInstances(
	ctx context.Context,
	instances []execution.RunnerInstance,
	ownedNames map[string]core.JobRecord,
) (int, error) {
	removed := 0
	failures := make([]error, 0)
	for _, instance := range instances {
		_, owned := ownedNames[instance.RunnerName]
		_, runnerFoundryOwned := runnerJobID(instance.RunnerName)
		if instance.State == execution.RunnerStateRunning && (owned || !runnerFoundryOwned) {
			continue
		}
		if err := orchestrator.provider.Destroy(ctx, instance); err != nil {
			failures = append(failures, fmt.Errorf("destroy runner %q: %w", instance.ID, err))
			continue
		}
		removed++
	}

	return removed, errors.Join(failures...)
}

func runnerName(jobID core.JobID) string {
	return "runnerfoundry-" + jobID.String()
}

func runnerJobID(stableRunnerName string) (core.JobID, bool) {
	identifier, found := strings.CutPrefix(stableRunnerName, "runnerfoundry-")
	if !found {
		return uuid.Nil, false
	}
	jobID, err := uuid.Parse(identifier)
	if err != nil || jobID == uuid.Nil {
		return uuid.Nil, false
	}

	return jobID, true
}
