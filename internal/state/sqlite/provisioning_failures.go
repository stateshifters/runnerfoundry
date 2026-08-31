package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

const reserveProvisioningFailureSQL = `UPDATE runnerfoundry_processing_jobs AS processing
	SET provisioning_failure_reserved = 1
	WHERE job_id = ? AND source_name = ? AND source_reference = ? AND runner_name = ?
	AND runner_started = 0 AND provisioning_failure_reserved = 0
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones tombstones
		WHERE tombstones.source_name = processing.source_name
		AND tombstones.source_reference = processing.source_reference)`

const insertProvisioningFailureSQL = `INSERT INTO runnerfoundry_provisioning_failures(job_id, runner_name)
	SELECT job_id, runner_name FROM runnerfoundry_processing_jobs
	WHERE job_id = ? AND source_name = ? AND source_reference = ? AND runner_name = ?
	AND provisioning_failure_reserved = 1
	ON CONFLICT(job_id) DO NOTHING`

const recordProvisioningFailureSQL = `UPDATE runnerfoundry_provisioning_failures SET is_reported = 1
	WHERE job_id = ? AND runner_name = ? AND EXISTS (
		SELECT 1 FROM runnerfoundry_processing_jobs processing
		WHERE processing.job_id = runnerfoundry_provisioning_failures.job_id
		AND processing.source_name = ? AND processing.source_reference = ?
		AND processing.runner_name = runnerfoundry_provisioning_failures.runner_name
		AND processing.provisioning_failure_reserved = 1)`

const tombstoneReportedProvisioningFailureSQL = `INSERT INTO runnerfoundry_source_tombstones(source_name, source_reference)
	SELECT source_name, source_reference FROM runnerfoundry_processing_jobs
	WHERE job_id = ? AND source_name = ? AND source_reference = ? AND runner_name = ?
	AND provisioning_failure_reserved = 1
	ON CONFLICT(source_name, source_reference) DO NOTHING`

const deleteFinalizableProvisioningFailureSQL = `DELETE FROM runnerfoundry_provisioning_failures
	WHERE job_id = ? AND runner_name = ? AND EXISTS (
		SELECT 1 FROM runnerfoundry_processing_jobs processing
		WHERE processing.job_id = runnerfoundry_provisioning_failures.job_id
		AND processing.source_name = ? AND processing.source_reference = ?
		AND processing.runner_name = runnerfoundry_provisioning_failures.runner_name
		AND processing.provisioning_failure_reserved = 1
		AND (runnerfoundry_provisioning_failures.is_reported = 1 OR EXISTS (
			SELECT 1 FROM runnerfoundry_source_tombstones tombstones
			WHERE tombstones.source_name = processing.source_name
			AND tombstones.source_reference = processing.source_reference)))`

const deleteFinalizedProcessingSQL = `DELETE FROM runnerfoundry_processing_jobs
	WHERE job_id = ? AND source_name = ? AND source_reference = ? AND runner_name = ?
	AND provisioning_failure_reserved = 1
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_provisioning_failures failures
		WHERE failures.job_id = runnerfoundry_processing_jobs.job_id)`

func (store *Store) ReserveProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
) (core.RetryResult, error) {
	return store.executeRetryTransition(ctx, "reserve provisioning failure",
		func(transaction Transaction) (core.RetryOutcome, error) {
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
	return store.executeRetryTransition(ctx, "record reported provisioning failure",
		func(transaction Transaction) (core.RetryOutcome, error) {
			arguments := []any{processing.Job.ID.String(), string(processing.RunnerName),
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
	arguments := []any{processing.Job.ID.String(), string(processing.RunnerName),
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
	return classifyTerminalRemoval(ctx, transaction, processing.Job.ID.String())
}

func classifyProvisioningFailure(
	ctx context.Context,
	transaction Transaction,
	expected core.ProcessingJob,
) (core.RetryOutcome, error) {
	actual, err := scanProcessingJob(transaction.QueryRow(ctx, processingByIDSQL, expected.Job.ID.String()))
	if err != nil {
		return classifyMissingProcessing(err)
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

func classifyMissingProcessing(err error) (core.RetryOutcome, error) {
	if errors.Is(err, sql.ErrNoRows) {
		return core.RetryMissing, nil
	}
	return "", fmt.Errorf("classify provisioning failure: %w", err)
}

func processingFailureArguments(processing core.ProcessingJob) []any {
	return []any{processing.Job.ID.String(), string(processing.Job.Source.Adapter),
		string(processing.Job.Source.ID), string(processing.RunnerName)}
}
