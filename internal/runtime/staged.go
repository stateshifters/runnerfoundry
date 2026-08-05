package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
)

func (orchestrator *Orchestrator) acquireClaimedJob(ctx context.Context, record core.JobRecord) error {
	begun, err := orchestrator.store.BeginAcquisition(ctx, record.Job.ID, orchestrator.currentTime())
	if err != nil {
		return fmt.Errorf("begin acquisition for job %s: %w", record.Job.ID, err)
	}
	if err = orchestrator.source.Acquire(ctx, begun.Job); err != nil {
		acquisitionErr := fmt.Errorf("acquire source job %s: %w", begun.Job.ID, err)
		return orchestrator.completeAttempt(ctx, begun.Job, acquisitionErr)
	}
	if _, err = orchestrator.store.ConfirmAcquisition(ctx, begun.Job.ID, orchestrator.currentTime()); err != nil {
		return fmt.Errorf("confirm acquisition for job %s: %w", begun.Job.ID, err)
	}

	return nil
}

func (orchestrator *Orchestrator) terminalizeInterruptedAcquisition(ctx context.Context, record core.JobRecord) error {
	return orchestrator.completeAttempt(
		ctx,
		record.Job,
		errors.New("source acquisition was interrupted before confirmation"),
	)
}

// RecoverInterruptedAcquisitions terminalizes begun but unconfirmed source
// calls. Their ambiguity is resolved as failure; they are never reacquired.
func (orchestrator *Orchestrator) RecoverInterruptedAcquisitions(ctx context.Context, limit int) (int, error) {
	records, err := orchestrator.store.PendingAcquisitions(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("list interrupted acquisitions: %w", err)
	}

	result := core.Result{
		Outcome: core.OutcomeFailure,
		Origin:  core.ResultOriginService,
		Reason:  "source acquisition was interrupted before confirmation",
	}
	pending := make([]core.JobRecord, 0, len(records))
	transitionErrors := make([]error, 0)
	for _, record := range records {
		updated, transitionErr := orchestrator.store.SetResultPending(ctx, record.Job.ID, result)
		if transitionErr != nil {
			transitionErrors = append(transitionErrors, fmt.Errorf("terminalize interrupted acquisition for job %s: %w", record.Job.ID, transitionErr))
			continue
		}
		pending = append(pending, updated)
	}

	return len(pending), errors.Join(errors.Join(transitionErrors...), orchestrator.deliverResults(ctx, pending, result))
}

// StartAcquired performs the member-owned bootstrap and provider-start stage
// for acquired attempts that do not already have a provider instance.
func (orchestrator *Orchestrator) StartAcquired(ctx context.Context, runnerPool core.RunnerPool) (int, error) {
	attempts, err := orchestrator.store.AttemptsByMember(ctx, orchestrator.member, runnerPool)
	if err != nil {
		return 0, fmt.Errorf("list acquired attempts for Runner Pool %q: %w", runnerPool, err)
	}
	instances, err := orchestrator.provider.Inspect(ctx, string(runnerPool))
	if err != nil {
		return 0, fmt.Errorf("inspect acquired runners for Runner Pool %q: %w", runnerPool, err)
	}
	existing := make(map[string]struct{}, len(instances))
	for _, instance := range instances {
		existing[instance.RunnerName] = struct{}{}
	}

	started := 0
	startErrors := make([]error, 0)
	for _, attempt := range attempts {
		attemptStarted, startErr := orchestrator.startAcquiredAttempt(ctx, runnerPool, attempt, existing)
		if startErr != nil {
			startErrors = append(startErrors, startErr)
		}
		if attemptStarted {
			started++
		}
	}

	return started, errors.Join(startErrors...)
}

func (orchestrator *Orchestrator) startAcquiredAttempt(
	ctx context.Context,
	runnerPool core.RunnerPool,
	attempt core.JobRecord,
	existing map[string]struct{},
) (bool, error) {
	stableRunnerName := runnerName(attempt.Job.ID)
	if attempt.AcquiredAt.IsZero() {
		return false, nil
	}
	if _, exists := existing[stableRunnerName]; exists {
		return false, nil
	}
	bootstrap, err := orchestrator.source.GenerateBootstrap(ctx, attempt.Job, stableRunnerName)
	if err != nil {
		stageErr := fmt.Errorf("generate bootstrap for job %s: %w", attempt.Job.ID, err)
		return false, errors.Join(stageErr, orchestrator.completeAttempt(ctx, attempt.Job, stageErr))
	}
	err = orchestrator.provider.Start(ctx, execution.StartRequest{
		RunnerPool: string(runnerPool), RunnerName: stableRunnerName, Bootstrap: bootstrap,
	})
	if err != nil {
		stageErr := fmt.Errorf("start runner for job %s: %w", attempt.Job.ID, err)
		return false, errors.Join(stageErr, orchestrator.completeAttempt(ctx, attempt.Job, stageErr))
	}

	return true, nil
}
