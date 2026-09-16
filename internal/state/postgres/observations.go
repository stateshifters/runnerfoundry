package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
)

func (store *Store) Terminalize(
	ctx context.Context,
	identity core.JobIdentity,
) (core.ObservationResult, error) {
	return store.applyObservation(ctx, identity, "terminalize job", terminalizeResolvedSource)
}

type postgresObservationOperation func(
	context.Context,
	Transaction,
	core.SourceReference,
	core.JobIdentity,
) (core.ObservationOutcome, error)

func (store *Store) applyObservation(
	ctx context.Context,
	identity core.JobIdentity,
	begin string,
	operation postgresObservationOperation,
) (core.ObservationResult, error) {
	outcome := core.ObservationUnmatched
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		source, matched, err := resolveAndSerializeObservation(ctx, transaction, identity)
		if err != nil || !matched {
			return err
		}
		outcome, err = operation(ctx, transaction, source, identity)
		return err
	})
	if err != nil {
		return core.ObservationResult{}, fmt.Errorf("%s: %w", begin, err)
	}
	return core.ObservationResult{Outcome: outcome}, nil
}

func terminalizeResolvedSource(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
	identity core.JobIdentity,
) (core.ObservationOutcome, error) {
	if identity.AttemptID.IsValid() {
		matched, outcome, matchErr := classifyProcessingAttempt(ctx, transaction, source, identity)
		if matchErr != nil || !matched {
			return outcome, matchErr
		}
	}
	inserted, err := executeAffected(ctx, transaction, insertTombstoneSQL, source.Adapter, source.ID)
	if err != nil {
		return "", fmt.Errorf("tombstone terminal job: %w", err)
	}
	if _, err = transaction.Execute(ctx, deleteQueuedJobBySourceSQL, source.Adapter, source.ID); err != nil {
		return "", fmt.Errorf("remove terminal queued job: %w", err)
	}
	outcome := core.ObservationAlreadyApplied
	if inserted > 0 {
		outcome = core.ObservationApplied
	}
	return outcome, nil
}

func (store *Store) ObserveRunnerAcceptance(
	ctx context.Context,
	identity core.JobIdentity,
) (core.ObservationResult, error) {
	return store.applyObservation(ctx, identity, "mark runner accepted", applyRunnerAcceptanceInTransaction)
}

func applyRunnerAcceptanceInTransaction(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
	identity core.JobIdentity,
) (core.ObservationOutcome, error) {
	statement, arguments := runnerAcceptanceUpdate(identity, source)
	accepted, err := executeAffected(ctx, transaction, statement, arguments...)
	if err != nil {
		return "", fmt.Errorf("mark processing runner accepted: %w", err)
	}
	if identity.AttemptID.IsValid() {
		return classifyRunnerAcceptance(ctx, transaction, source, identity, accepted)
	}
	delayed, err := executeAffected(ctx, transaction, tombstoneDelayedAcceptanceSQL, source.Adapter, source.ID)
	if err != nil {
		return "", fmt.Errorf("tombstone delayed runner acceptance: %w", err)
	}
	if _, err = transaction.Execute(ctx, deleteQueuedJobBySourceSQL, source.Adapter, source.ID); err != nil {
		return "", fmt.Errorf("remove delayed-acceptance queued job: %w", err)
	}
	return classifyRunnerAcceptance(ctx, transaction, source, identity, accepted+delayed)
}

func runnerAcceptanceUpdate(
	identity core.JobIdentity,
	source core.SourceReference,
) (string, []any) {
	arguments := []any{source.Adapter, source.ID}
	if identity.AttemptID.IsValid() {
		return markProcessingAttemptAcceptedSQL,
			append(arguments, core.AttemptRunnerName(identity.JobID, identity.AttemptID))
	}
	return markProcessingAcceptedSQL, arguments
}

func resolveAndSerializeObservation(
	ctx context.Context,
	transaction Transaction,
	identity core.JobIdentity,
) (core.SourceReference, bool, error) {
	if err := validateJobIdentity(identity); err != nil {
		return core.SourceReference{}, false, err
	}
	source, matched, err := resolveObservationSource(ctx, transaction, identity)
	if err != nil || !matched {
		return core.SourceReference{}, matched, err
	}
	if _, err = transaction.Execute(ctx, serializeSourceSQL, advisoryLockIdentity(
		string(source.Adapter), string(source.ID))); err != nil {
		return core.SourceReference{}, false, fmt.Errorf("serialize job observation: %w", err)
	}
	if identity.JobID == uuid.Nil {
		return source, true, nil
	}
	return resolveSerializedObservation(ctx, transaction, identity, source)
}

func resolveSerializedObservation(
	ctx context.Context,
	transaction Transaction,
	identity core.JobIdentity,
	source core.SourceReference,
) (core.SourceReference, bool, error) {
	lockedSource, found, err := lookupSourceByJobID(ctx, transaction, identity.JobID)
	if err != nil {
		return core.SourceReference{}, false, err
	}
	if !found && identity.AttemptID.IsValid() && identity.Source.IsValid() {
		return source, true, nil
	}
	if !found {
		return core.SourceReference{}, found, err
	}
	if lockedSource != source {
		return core.SourceReference{}, false, fmt.Errorf("%w: job %s changed source", core.ErrJobIdentityMismatch, identity.JobID)
	}
	return source, true, nil
}

func resolveObservationSource(
	ctx context.Context,
	transaction Transaction,
	identity core.JobIdentity,
) (core.SourceReference, bool, error) {
	if identity.JobID == uuid.Nil {
		return identity.Source, true, nil
	}
	resolved, found, err := lookupSourceByJobID(ctx, transaction, identity.JobID)
	if err != nil {
		return core.SourceReference{}, false, err
	}
	if !found && identity.AttemptID.IsValid() && identity.Source.IsValid() {
		return identity.Source, true, nil
	}
	if !found {
		return core.SourceReference{}, found, err
	}
	if identity.Source != (core.SourceReference{}) && identity.Source != resolved {
		return core.SourceReference{}, false, fmt.Errorf("%w: job %s", core.ErrJobIdentityMismatch, identity.JobID)
	}
	return resolved, true, nil
}

func lookupSourceByJobID(
	ctx context.Context,
	transaction Transaction,
	jobID core.JobID,
) (core.SourceReference, bool, error) {
	var adapter string
	var identifier string
	err := transaction.ReadRow(ctx, sourceByJobIDSQL, func(row Scanner) error {
		return row.Scan(&adapter, &identifier)
	}, jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return core.SourceReference{}, false, nil
	}
	if err != nil {
		return core.SourceReference{}, false, fmt.Errorf("resolve job source: %w", err)
	}
	return core.SourceReference{Adapter: core.SourceAdapter(adapter), ID: core.SourceIdentifier(identifier)}, true, nil
}

func validateJobIdentity(identity core.JobIdentity) error {
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("validate job observation identity: %w", err)
	}
	return nil
}

func classifyRunnerAcceptance(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
	identity core.JobIdentity,
	affected int64,
) (core.ObservationOutcome, error) {
	if affected > 0 {
		return core.ObservationApplied, nil
	}
	statement := runnerAcceptanceAlreadyAppliedSQL
	arguments := []any{source.Adapter, source.ID}
	if identity.AttemptID.IsValid() {
		statement = attemptAcceptanceAlreadyAppliedSQL
		arguments = append(arguments, core.AttemptRunnerName(identity.JobID, identity.AttemptID))
	}
	var accepted bool
	if err := transaction.ReadRow(ctx, statement, func(row Scanner) error {
		return row.Scan(&accepted)
	}, arguments...); err != nil {
		return "", fmt.Errorf("classify runner acceptance observation: %w", err)
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
	var matched bool
	if err := transaction.ReadRow(ctx, processingAttemptExistsSQL, func(row Scanner) error {
		return row.Scan(&matched)
	}, source.Adapter, source.ID, core.AttemptRunnerName(identity.JobID, identity.AttemptID)); err != nil {
		return false, "", fmt.Errorf("match lifecycle processing attempt: %w", err)
	}
	if matched {
		return true, "", nil
	}
	var tombstoned bool
	if err := transaction.ReadRow(ctx, sourceTombstonedSQL, func(row Scanner) error {
		return row.Scan(&tombstoned)
	}, source.Adapter, source.ID); err != nil {
		return false, "", fmt.Errorf("classify lifecycle processing attempt: %w", err)
	}
	if tombstoned {
		return false, core.ObservationAlreadyApplied, nil
	}
	return false, core.ObservationUnmatched, nil
}

func executeAffected(
	ctx context.Context,
	transaction Transaction,
	statement string,
	arguments ...any,
) (int64, error) {
	affected, err := transaction.Execute(ctx, statement, arguments...)
	if err != nil {
		return 0, fmt.Errorf("execute observation statement: %w", err)
	}
	return affected, nil
}
