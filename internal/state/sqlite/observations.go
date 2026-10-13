package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
)

const sourceByJobIDSQL = `SELECT source_name, source_reference FROM (
	SELECT source_name, source_reference FROM runnerfoundry_queued_jobs WHERE job_id = ?
	UNION ALL
	SELECT source_name, source_reference FROM runnerfoundry_processing_jobs WHERE job_id = ?
) LIMIT 1`
const sourceTombstonedSQL = `SELECT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones
	WHERE source_name = ? AND source_reference = ?)`
const processingAttemptExistsSQL = `SELECT EXISTS (SELECT 1 FROM runnerfoundry_processing_jobs
	WHERE source_name = ? AND source_reference = ? AND runner_name = ?)`
const insertConditionalTombstoneSQL = `INSERT INTO runnerfoundry_source_tombstones(source_name, source_reference)
	SELECT ?, ? WHERE EXISTS (
		SELECT 1 FROM runnerfoundry_queued_jobs
		WHERE job_id = ? AND source_name = ? AND source_reference = ?
		UNION ALL
		SELECT 1 FROM runnerfoundry_processing_jobs
		WHERE job_id = ? AND source_name = ? AND source_reference = ?
			AND runner_started = 0 AND provisioning_failure_reserved = 0)
	ON CONFLICT(source_name, source_reference) DO NOTHING`
const deleteConditionalQueuedJobSQL = `DELETE FROM runnerfoundry_queued_jobs
	WHERE job_id = ? AND source_name = ? AND source_reference = ?`
const classifyConditionalTerminalSQL = `SELECT CASE
	WHEN EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones
		WHERE source_name = ? AND source_reference = ?) THEN 'already_applied'
	WHEN EXISTS (SELECT 1 FROM runnerfoundry_processing_jobs
		WHERE job_id = ? AND source_name = ? AND source_reference = ?
		AND (runner_started = 1 OR provisioning_failure_reserved = 1)) THEN 'protected'
	ELSE 'missing' END`

func (store *Store) Terminalize(
	ctx context.Context,
	identity core.JobIdentity,
) (core.ObservationResult, error) {
	return store.applyObservation(ctx, identity, sqliteObservationOperation{
		name: "source terminalization", apply: terminalizeSource,
	})
}

func (store *Store) ObserveTerminalIfUnaccepted(
	ctx context.Context,
	identity core.JobIdentity,
) (core.ConditionalTerminalResult, error) {
	outcome := core.ConditionalTerminalMissing
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		var err error
		outcome, err = applyConditionalTerminal(ctx, transaction, identity)
		return err
	})
	if err != nil {
		return core.ConditionalTerminalResult{}, fmt.Errorf("conditionally terminalize job: %w", err)
	}
	return core.ConditionalTerminalResult{Outcome: outcome}, nil
}

func applyConditionalTerminal(
	ctx context.Context,
	transaction Transaction,
	identity core.JobIdentity,
) (core.ConditionalTerminalOutcome, error) {
	source, matched, err := resolveJobIdentity(ctx, transaction, identity)
	if err != nil || !matched {
		return core.ConditionalTerminalMissing, err
	}
	arguments := []any{source.Adapter, source.ID, identity.JobID.String(), source.Adapter, source.ID,
		identity.JobID.String(), source.Adapter, source.ID}
	inserted, err := transaction.Execute(ctx, insertConditionalTombstoneSQL, arguments...)
	if err != nil {
		return "", fmt.Errorf("tombstone unaccepted job: %w", err)
	}
	if _, err = transaction.Execute(ctx, deleteConditionalQueuedJobSQL,
		identity.JobID.String(), source.Adapter, source.ID); err != nil {
		return "", fmt.Errorf("remove conditionally terminal queued job: %w", err)
	}
	if inserted > 0 {
		return core.ConditionalTerminalApplied, nil
	}
	var classified string
	if err = transaction.ReadRow(ctx, classifyConditionalTerminalSQL, func(row Scanner) error {
		return row.Scan(&classified)
	}, source.Adapter, source.ID, identity.JobID.String(), source.Adapter, source.ID); err != nil {
		return "", fmt.Errorf("classify conditional terminal observation: %w", err)
	}
	return core.ConditionalTerminalOutcome(classified), nil
}

func (store *Store) ObserveRunnerAcceptance(
	ctx context.Context,
	identity core.JobIdentity,
) (core.ObservationResult, error) {
	return store.applyObservation(ctx, identity, sqliteObservationOperation{
		name: "runner acceptance", apply: applyRunnerAcceptedObservation,
	})
}

type sqliteObservationOperation struct {
	name  string
	apply func(context.Context, Transaction, core.SourceReference, core.JobIdentity) (core.ObservationOutcome, error)
}

func (store *Store) applyObservation(ctx context.Context, identity core.JobIdentity, operation sqliteObservationOperation) (core.ObservationResult, error) {
	outcome := core.ObservationUnmatched
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		source, matched, err := resolveJobIdentity(ctx, transaction, identity)
		if err != nil || !matched {
			return err
		}
		outcome, err = operation.apply(ctx, transaction, source, identity)
		return err
	})
	if err != nil {
		return core.ObservationResult{}, fmt.Errorf("%s: %w", operation.name, err)
	}
	return core.ObservationResult{Outcome: outcome}, nil
}

func terminalizeSource(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
	identity core.JobIdentity,
) (core.ObservationOutcome, error) {
	if identity.AttemptID.IsValid() {
		matched, outcome, err := classifyProcessingAttempt(ctx, transaction, source, identity)
		if err != nil || !matched {
			return outcome, err
		}
	}
	inserted, err := transaction.Execute(ctx, insertTombstoneSQL, source.Adapter, source.ID)
	if err != nil {
		return "", fmt.Errorf("tombstone terminal source: %w", err)
	}
	if _, err = transaction.Execute(ctx, cancelQueuedJobSQL, source.Adapter, source.ID); err != nil {
		return "", fmt.Errorf("remove terminal queued job: %w", err)
	}
	if inserted > 0 {
		return core.ObservationApplied, nil
	}
	return core.ObservationAlreadyApplied, nil
}

func applyRunnerAcceptedObservation(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
	identity core.JobIdentity,
) (core.ObservationOutcome, error) {
	accepted, err := countAcceptedProcessing(ctx, transaction, source, identity)
	if err != nil {
		return "", err
	}
	if identity.AttemptID.IsValid() {
		if accepted > 0 {
			return core.ObservationApplied, nil
		}
		return classifyUnchangedRunnerAcceptance(ctx, transaction, source, identity)
	}
	tombstoned, err := countDelayedAcceptanceTombstone(ctx, transaction, source)
	if err != nil {
		return "", err
	}
	removed, err := countRemovedDelayedAcceptance(ctx, transaction, source)
	if err != nil {
		return "", err
	}
	if accepted > 0 || tombstoned > 0 || removed > 0 {
		return core.ObservationApplied, nil
	}
	return classifyUnchangedRunnerAcceptance(ctx, transaction, source, identity)
}

func countAcceptedProcessing(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
	identity core.JobIdentity,
) (int64, error) {
	statement := markProcessingAcceptedSQL
	arguments := []any{source.Adapter, source.ID, source.Adapter, source.ID}
	if identity.AttemptID.IsValid() {
		statement = markProcessingAttemptAcceptedSQL
		arguments = []any{source.Adapter, source.ID,
			core.AttemptRunnerName(identity.JobID, identity.AttemptID), source.Adapter, source.ID}
	}
	count, err := transaction.Execute(ctx, statement, arguments...)
	if err != nil {
		return 0, fmt.Errorf("mark processing runner accepted: %w", err)
	}
	return count, nil
}

func countDelayedAcceptanceTombstone(ctx context.Context, transaction Transaction, source core.SourceReference) (int64, error) {
	count, err := transaction.Execute(ctx, tombstoneDelayedAcceptanceSQL,
		source.Adapter, source.ID, source.Adapter, source.ID)
	if err != nil {
		return 0, fmt.Errorf("tombstone delayed runner acceptance: %w", err)
	}
	return count, nil
}

func countRemovedDelayedAcceptance(ctx context.Context, transaction Transaction, source core.SourceReference) (int64, error) {
	count, err := transaction.Execute(ctx, cancelQueuedJobSQL, source.Adapter, source.ID)
	if err != nil {
		return 0, fmt.Errorf("remove delayed-acceptance queued job: %w", err)
	}
	return count, nil
}

func classifyUnchangedRunnerAcceptance(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
	identity core.JobIdentity,
) (core.ObservationOutcome, error) {
	var tombstoned bool
	err := transaction.ReadRow(ctx, sourceTombstonedSQL, func(row Scanner) error { return row.Scan(&tombstoned) }, source.Adapter, source.ID)
	if err != nil {
		return "", fmt.Errorf("classify runner-acceptance observation: %w", err)
	}
	if tombstoned {
		return core.ObservationAlreadyApplied, nil
	}
	statement := sourceAcceptedProcessingSQL
	arguments := []any{source.Adapter, source.ID}
	if identity.AttemptID.IsValid() {
		statement = attemptAcceptedProcessingSQL
		arguments = append(arguments, core.AttemptRunnerName(identity.JobID, identity.AttemptID))
	}
	var accepted bool
	if err = transaction.ReadRow(ctx, statement, func(row Scanner) error { return row.Scan(&accepted) }, arguments...); err != nil {
		return "", fmt.Errorf("classify runner-acceptance observation: %w", err)
	}
	if accepted {
		return core.ObservationAlreadyApplied, nil
	}
	return core.ObservationUnmatched, nil
}

func classifyProcessingAttempt(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
	identity core.JobIdentity,
) (bool, core.ObservationOutcome, error) {
	runnerName := core.AttemptRunnerName(identity.JobID, identity.AttemptID)
	var matched bool
	if err := transaction.ReadRow(ctx, processingAttemptExistsSQL, func(row Scanner) error { return row.Scan(&matched) },
		source.Adapter, source.ID, runnerName); err != nil {
		return false, "", fmt.Errorf("match lifecycle processing attempt: %w", err)
	}
	if matched {
		return true, "", nil
	}
	var tombstoned bool
	if err := transaction.ReadRow(ctx, sourceTombstonedSQL, func(row Scanner) error { return row.Scan(&tombstoned) },
		source.Adapter, source.ID); err != nil {
		return false, "", fmt.Errorf("classify lifecycle processing attempt: %w", err)
	}
	if tombstoned {
		return false, core.ObservationAlreadyApplied, nil
	}
	return false, core.ObservationUnmatched, nil
}

func resolveJobIdentity(
	ctx context.Context,
	transaction Transaction,
	identity core.JobIdentity,
) (core.SourceReference, bool, error) {
	if err := identity.Validate(); err != nil {
		return core.SourceReference{}, false, fmt.Errorf("validate lifecycle job identity: %w", err)
	}
	if identity.JobID == uuid.Nil {
		return identity.Source, true, nil
	}
	durableSource, matched, err := findSourceByJobID(ctx, transaction, identity.JobID)
	if err != nil {
		return core.SourceReference{}, false, err
	}
	if !matched && identity.AttemptID.IsValid() && identity.Source.IsValid() {
		return identity.Source, true, nil
	}
	if !matched {
		return core.SourceReference{}, matched, err
	}
	if identity.Source.IsValid() && identity.Source != durableSource {
		return core.SourceReference{}, false, fmt.Errorf("validate lifecycle job identity: %w", core.ErrJobIdentityMismatch)
	}
	return durableSource, true, nil
}

func findSourceByJobID(
	ctx context.Context,
	transaction Transaction,
	jobID core.JobID,
) (core.SourceReference, bool, error) {
	var adapter, sourceID string
	err := transaction.ReadRow(ctx, sourceByJobIDSQL, func(row Scanner) error { return row.Scan(&adapter, &sourceID) }, jobID.String(), jobID.String())
	if errors.Is(err, sql.ErrNoRows) {
		return core.SourceReference{}, false, nil
	}
	if err != nil {
		return core.SourceReference{}, false, fmt.Errorf("resolve lifecycle job identity: %w", err)
	}
	return core.SourceReference{Adapter: core.SourceAdapter(adapter), ID: core.SourceIdentifier(sourceID)}, true, nil
}
