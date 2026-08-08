package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
)

const sourceByJobIDSQL = `SELECT source_name, source_reference FROM (
	SELECT source_name, source_reference FROM runnerfoundry_queued_jobs WHERE job_id = ?
	UNION ALL
	SELECT source_name, source_reference FROM runnerfoundry_processing_jobs WHERE job_id = ?
) LIMIT 1`
const sourceTombstonedSQL = `SELECT EXISTS (SELECT 1 FROM runnerfoundry_source_tombstones
	WHERE source_name = ? AND source_reference = ?)`

func (store *Store) Terminalize(
	ctx context.Context,
	identity core.JobIdentity,
) (core.ObservationResult, error) {
	return store.applyObservation(ctx, identity, sqliteObservationOperation{
		begin: "terminalize source", unmatchedCommit: "unmatched terminal observation",
		commit: "source terminalization", apply: terminalizeSource,
	})
}

func (store *Store) MarkRunnerStarted(
	ctx context.Context,
	identity core.JobIdentity,
) (core.ObservationResult, error) {
	return store.applyObservation(ctx, identity, sqliteObservationOperation{
		begin: "mark runner started", unmatchedCommit: "unmatched runner-start observation",
		commit: "runner start", apply: applyRunnerStartedObservation,
	})
}

type sqliteObservationOperation struct {
	begin           string
	unmatchedCommit string
	commit          string
	apply           func(context.Context, Transaction, core.SourceReference) (core.ObservationOutcome, error)
}

func (store *Store) applyObservation(
	ctx context.Context,
	identity core.JobIdentity,
	operation sqliteObservationOperation,
) (_ core.ObservationResult, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return core.ObservationResult{}, fmt.Errorf("begin %s: %w", operation.begin, err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	source, matched, err := resolveJobIdentity(ctx, transaction, identity)
	if err != nil {
		return core.ObservationResult{}, err
	}
	if !matched {
		return commitObservation(ctx, transaction, &open, core.ObservationUnmatched, operation.unmatchedCommit)
	}
	outcome, err := operation.apply(ctx, transaction, source)
	if err != nil {
		return core.ObservationResult{}, err
	}
	return commitObservation(ctx, transaction, &open, outcome, operation.commit)
}

func terminalizeSource(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
) (core.ObservationOutcome, error) {
	tombstone, err := transaction.Execute(ctx, insertTombstoneSQL, source.Adapter, source.ID)
	if err != nil {
		return "", fmt.Errorf("tombstone terminal source: %w", err)
	}
	inserted, err := tombstone.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("read terminal tombstone count: %w", err)
	}
	if _, err = transaction.Execute(ctx, cancelQueuedJobSQL, source.Adapter, source.ID); err != nil {
		return "", fmt.Errorf("remove terminal queued job: %w", err)
	}
	if inserted > 0 {
		return core.ObservationApplied, nil
	}
	return core.ObservationAlreadyApplied, nil
}

func applyRunnerStartedObservation(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
) (core.ObservationOutcome, error) {
	started, err := countStartedProcessing(ctx, transaction, source)
	if err != nil {
		return "", err
	}
	tombstoned, err := countDelayedStartTombstone(ctx, transaction, source)
	if err != nil {
		return "", err
	}
	removed, err := countRemovedDelayedStart(ctx, transaction, source)
	if err != nil {
		return "", err
	}
	if started > 0 || tombstoned > 0 || removed > 0 {
		return core.ObservationApplied, nil
	}
	return classifyUnchangedRunnerStart(ctx, transaction, source)
}

func countStartedProcessing(ctx context.Context, transaction Transaction, source core.SourceReference) (int64, error) {
	result, err := transaction.Execute(ctx, markProcessingStartedSQL,
		source.Adapter, source.ID, source.Adapter, source.ID)
	if err != nil {
		return 0, fmt.Errorf("mark processing runner started: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read started processing count: %w", err)
	}
	return count, nil
}

func countDelayedStartTombstone(ctx context.Context, transaction Transaction, source core.SourceReference) (int64, error) {
	result, err := transaction.Execute(ctx, tombstoneDelayedStartSQL,
		source.Adapter, source.ID, source.Adapter, source.ID)
	if err != nil {
		return 0, fmt.Errorf("tombstone delayed runner start: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read delayed-start tombstone count: %w", err)
	}
	return count, nil
}

func countRemovedDelayedStart(ctx context.Context, transaction Transaction, source core.SourceReference) (int64, error) {
	result, err := transaction.Execute(ctx, cancelQueuedJobSQL, source.Adapter, source.ID)
	if err != nil {
		return 0, fmt.Errorf("remove delayed-start queued job: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read delayed-start removal count: %w", err)
	}
	return count, nil
}

func classifyUnchangedRunnerStart(
	ctx context.Context,
	transaction Transaction,
	source core.SourceReference,
) (core.ObservationOutcome, error) {
	var tombstoned bool
	err := transaction.QueryRow(ctx, sourceTombstonedSQL, source.Adapter, source.ID).Scan(&tombstoned)
	if err != nil {
		return "", fmt.Errorf("classify runner-start observation: %w", err)
	}
	if tombstoned {
		return core.ObservationAlreadyApplied, nil
	}
	return core.ObservationUnmatched, nil
}

func commitObservation(
	ctx context.Context,
	transaction Transaction,
	open *bool,
	outcome core.ObservationOutcome,
	operation string,
) (core.ObservationResult, error) {
	if err := transaction.Commit(ctx); err != nil {
		return core.ObservationResult{}, fmt.Errorf("commit %s: %w", operation, err)
	}
	*open = false
	return core.ObservationResult{Outcome: outcome}, nil
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
	if err != nil || !matched {
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
	err := transaction.QueryRow(ctx, sourceByJobIDSQL, jobID.String(), jobID.String()).Scan(&adapter, &sourceID)
	if errors.Is(err, sql.ErrNoRows) {
		return core.SourceReference{}, false, nil
	}
	if err != nil {
		return core.SourceReference{}, false, fmt.Errorf("resolve lifecycle job identity: %w", err)
	}
	return core.SourceReference{Adapter: core.SourceAdapter(adapter), ID: core.SourceIdentifier(sourceID)}, true, nil
}
