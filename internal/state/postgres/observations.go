package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
)

func (store *Store) Terminalize(
	ctx context.Context,
	identity core.JobIdentity,
) (_ core.ObservationResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.ObservationResult{}, fmt.Errorf("begin terminalize job: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	source, matched, err := resolveAndSerializeObservation(ctx, transaction, identity)
	if err != nil || !matched {
		return commitObservation(ctx, transaction, &open, core.ObservationUnmatched, err)
	}
	inserted, err := executeAffected(ctx, transaction, insertTombstoneSQL, source.Adapter, source.ID)
	if err != nil {
		return core.ObservationResult{}, fmt.Errorf("tombstone terminal job: %w", err)
	}
	if _, err = transaction.Execute(ctx, deleteQueuedJobBySourceSQL, source.Adapter, source.ID); err != nil {
		return core.ObservationResult{}, fmt.Errorf("remove terminal queued job: %w", err)
	}
	outcome := core.ObservationAlreadyApplied
	if inserted > 0 {
		outcome = core.ObservationApplied
	}
	return commitObservation(ctx, transaction, &open, outcome, nil)
}

func (store *Store) MarkRunnerStarted(
	ctx context.Context,
	identity core.JobIdentity,
) (_ core.ObservationResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.ObservationResult{}, fmt.Errorf("begin mark runner started: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	source, matched, err := resolveAndSerializeObservation(ctx, transaction, identity)
	if err != nil || !matched {
		return commitObservation(ctx, transaction, &open, core.ObservationUnmatched, err)
	}
	started, err := executeAffected(ctx, transaction, markProcessingStartedSQL, source.Adapter, source.ID)
	if err != nil {
		return core.ObservationResult{}, fmt.Errorf("mark processing runner started: %w", err)
	}
	delayed, err := executeAffected(ctx, transaction, tombstoneDelayedStartSQL, source.Adapter, source.ID)
	if err != nil {
		return core.ObservationResult{}, fmt.Errorf("tombstone delayed runner start: %w", err)
	}
	if _, err = transaction.Execute(ctx, deleteQueuedJobBySourceSQL, source.Adapter, source.ID); err != nil {
		return core.ObservationResult{}, fmt.Errorf("remove delayed-start queued job: %w", err)
	}
	outcome, err := classifyRunnerStart(ctx, transaction, source, started+delayed)
	if err != nil {
		return core.ObservationResult{}, err
	}
	return commitObservation(ctx, transaction, &open, outcome, nil)
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
	lockedSource, found, err := lookupSourceByJobID(ctx, transaction, identity.JobID)
	if err != nil || !found {
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
	if err != nil || !found {
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
	err := transaction.QueryRow(ctx, sourceByJobIDSQL, jobID).Scan(&adapter, &identifier)
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

func classifyRunnerStart(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
	affected int64,
) (core.ObservationOutcome, error) {
	if affected > 0 {
		return core.ObservationApplied, nil
	}
	var terminal bool
	if err := transaction.QueryRow(ctx, runnerStartAlreadyAppliedSQL, source.Adapter, source.ID).Scan(&terminal); err != nil {
		return "", fmt.Errorf("classify runner start observation: %w", err)
	}
	if terminal {
		return core.ObservationAlreadyApplied, nil
	}
	return core.ObservationUnmatched, nil
}

func executeAffected(
	ctx context.Context,
	transaction Transaction,
	statement string,
	arguments ...any,
) (int64, error) {
	result, err := transaction.Execute(ctx, statement, arguments...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func commitObservation(
	ctx context.Context,
	transaction Transaction,
	open *bool,
	outcome core.ObservationOutcome,
	observationErr error,
) (core.ObservationResult, error) {
	if observationErr != nil {
		return core.ObservationResult{}, observationErr
	}
	if err := transaction.Commit(ctx); err != nil {
		return core.ObservationResult{}, fmt.Errorf("commit job observation: %w", err)
	}
	*open = false
	return core.ObservationResult{Outcome: outcome}, nil
}
