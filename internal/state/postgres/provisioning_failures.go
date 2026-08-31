package postgres

import (
	"context"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

const reserveProvisioningFailureSQL = `UPDATE public.runnerfoundry_processing_jobs AS processing
	SET provisioning_failure_reserved = true
	WHERE job_id = $1 AND source_name = $2 AND source_reference = $3 AND runner_name = $4
	AND NOT runner_started AND NOT provisioning_failure_reserved
	AND NOT EXISTS (SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)`

const insertProvisioningFailureSQL = `INSERT INTO public.runnerfoundry_provisioning_failures(job_id, runner_name)
	SELECT job_id, runner_name FROM public.runnerfoundry_processing_jobs
	WHERE job_id = $1 AND source_name = $2 AND source_reference = $3 AND runner_name = $4
	AND provisioning_failure_reserved
	ON CONFLICT(job_id) DO NOTHING`

const recordProvisioningFailureSQL = `UPDATE public.runnerfoundry_provisioning_failures failures SET is_reported = true
	FROM public.runnerfoundry_processing_jobs processing
	WHERE failures.job_id = $1 AND failures.runner_name = $2
	AND processing.job_id = failures.job_id AND processing.source_name = $3
	AND processing.source_reference = $4 AND processing.runner_name = failures.runner_name
	AND processing.provisioning_failure_reserved`

const tombstoneReportedProvisioningFailureSQL = `INSERT INTO public.runnerfoundry_source_tombstones(source_name, source_reference)
	SELECT source_name, source_reference FROM public.runnerfoundry_processing_jobs
	WHERE job_id = $1 AND source_name = $2 AND source_reference = $3 AND runner_name = $4
	AND provisioning_failure_reserved
	ON CONFLICT(source_name, source_reference) DO NOTHING`

const deleteFinalizableProvisioningFailureSQL = `DELETE FROM public.runnerfoundry_provisioning_failures failures
	USING public.runnerfoundry_processing_jobs processing
	WHERE failures.job_id = $1 AND failures.runner_name = $2
	AND processing.job_id = failures.job_id AND processing.source_name = $3
	AND processing.source_reference = $4 AND processing.runner_name = failures.runner_name
	AND processing.provisioning_failure_reserved
	AND (failures.is_reported OR EXISTS (
		SELECT 1 FROM public.runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference))`

const deleteFinalizedProcessingSQL = `DELETE FROM public.runnerfoundry_processing_jobs
	WHERE job_id = $1 AND source_name = $2 AND source_reference = $3 AND runner_name = $4
	AND provisioning_failure_reserved
	AND NOT EXISTS (SELECT 1 FROM public.runnerfoundry_provisioning_failures failures
		WHERE failures.job_id = runnerfoundry_processing_jobs.job_id)`

func (store *Store) ReserveProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
) (core.RetryResult, error) {
	return store.applyProvisioningFailureTransition(ctx, "reserve provisioning failure", processing,
		func(ctx context.Context, transaction Transaction) (core.RetryOutcome, error) {
			identity := processingFailureArguments(processing)
			if _, err := transaction.Execute(ctx, reserveProvisioningFailureSQL, identity...); err != nil {
				return "", fmt.Errorf("reserve provisioning failure: %w", err)
			}
			if _, err := transaction.Execute(ctx, insertProvisioningFailureSQL, identity...); err != nil {
				return "", fmt.Errorf("record provisioning failure reservation: %w", err)
			}
			return classifyProvisioningFailure(ctx, transaction, processing)
		})
}

func (store *Store) RecordProvisioningFailureReported(
	ctx context.Context,
	processing core.ProcessingJob,
) (core.RetryResult, error) {
	return store.applyProvisioningFailureTransition(ctx, "record reported provisioning failure", processing,
		func(ctx context.Context, transaction Transaction) (core.RetryOutcome, error) {
			arguments := []any{processing.Job.ID, processing.RunnerName,
				string(processing.Job.Source.Adapter), string(processing.Job.Source.ID)}
			if _, err := transaction.Execute(ctx, recordProvisioningFailureSQL, arguments...); err != nil {
				return "", fmt.Errorf("record reported provisioning failure: %w", err)
			}
			if _, err := transaction.Execute(ctx, tombstoneReportedProvisioningFailureSQL,
				processingFailureArguments(processing)...); err != nil {
				return "", fmt.Errorf("terminalize reported provisioning failure: %w", err)
			}
			return classifyProvisioningFailure(ctx, transaction, processing)
		})
}

func (store *Store) FinalizeProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
) (_ core.TerminalRemovalResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.TerminalRemovalResult{}, fmt.Errorf("begin finalize provisioning failure: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if err = serializeProcessingRetry(ctx, transaction, processing.Job.Source); err != nil {
		return core.TerminalRemovalResult{}, err
	}
	outcome, err := finalizeProvisioningFailureTransaction(ctx, transaction, processing)
	if err != nil {
		return core.TerminalRemovalResult{}, err
	}
	if err = transaction.Commit(ctx); err != nil {
		return core.TerminalRemovalResult{}, fmt.Errorf("commit finalize provisioning failure: %w", err)
	}
	open = false
	return core.TerminalRemovalResult{Outcome: outcome}, nil
}

func finalizeProvisioningFailureTransaction(
	ctx context.Context,
	transaction Transaction,
	processing core.ProcessingJob,
) (core.TerminalRemovalOutcome, error) {
	arguments := []any{processing.Job.ID, processing.RunnerName,
		string(processing.Job.Source.Adapter), string(processing.Job.Source.ID)}
	if _, err := transaction.Execute(ctx, deleteFinalizableProvisioningFailureSQL, arguments...); err != nil {
		return "", fmt.Errorf("release provisioning failure reservation: %w", err)
	}
	result, err := transaction.Execute(ctx, deleteFinalizedProcessingSQL, processingFailureArguments(processing)...)
	if err != nil {
		return "", fmt.Errorf("remove finalized provisioning failure: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("read finalized provisioning failure count: %w", err)
	}
	if removed > 0 {
		return core.TerminalRemoved, nil
	}
	return classifyTerminalRemoval(ctx, transaction, processing.Job.ID)
}

func (store *Store) applyProvisioningFailureTransition(
	ctx context.Context,
	description string,
	processing core.ProcessingJob,
	transition func(context.Context, Transaction) (core.RetryOutcome, error),
) (_ core.RetryResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.RetryResult{}, fmt.Errorf("begin %s: %w", description, err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if err = serializeProcessingRetry(ctx, transaction, processing.Job.Source); err != nil {
		return core.RetryResult{}, err
	}
	outcome, err := transition(ctx, transaction)
	if err != nil {
		return core.RetryResult{}, err
	}
	if err = transaction.Commit(ctx); err != nil {
		return core.RetryResult{}, fmt.Errorf("commit %s: %w", description, err)
	}
	open = false
	return core.RetryResult{Outcome: outcome}, nil
}

func classifyProvisioningFailure(
	ctx context.Context,
	transaction Transaction,
	expected core.ProcessingJob,
) (core.RetryOutcome, error) {
	actual, found, err := loadProcessingRetryCandidate(ctx, transaction, expected.Job.ID)
	if err != nil || !found {
		return core.RetryMissing, err
	}
	if actual.Job.Source != expected.Job.Source || actual.RunnerName != expected.RunnerName {
		return core.RetryClaimChanged, nil
	}
	if actual.ProvisioningFailure == core.ProvisioningFailureReported {
		return core.RetryFailureFinalizationPending, nil
	}
	if actual.SourceTerminal {
		return core.RetrySourceTerminal, nil
	}
	if actual.RunnerAccepted {
		return core.RetryRunnerAccepted, nil
	}
	if actual.ProvisioningFailure == core.ProvisioningFailureReporting {
		return core.RetryFailureReportingPending, nil
	}
	return core.RetryClaimChanged, nil
}

func processingFailureArguments(processing core.ProcessingJob) []any {
	return []any{processing.Job.ID, string(processing.Job.Source.Adapter),
		string(processing.Job.Source.ID), processing.RunnerName}
}
