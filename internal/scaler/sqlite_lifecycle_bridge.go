package scaler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func reconcileSQLiteBulkReapMirrors(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository, reason string,
) error {
	rows, err := transaction.QueryContext(ctx, `SELECT repository, provider, runner_name, request_id, phase
		FROM runner_lifecycle WHERE scale_set = ? AND (? = '' OR repository = ?)
		AND phase = 'reap_pending' AND reap_reason = ?`,
		scaleSet, repository, repository, reason)
	if err != nil {
		return fmt.Errorf("load bulk authoritative runner lifecycles: %w", err)
	}
	runners := make([]RunnerLifecycle, 0)
	for rows.Next() {
		runner := RunnerLifecycle{ScaleSet: scaleSet}
		if scanErr := rows.Scan(
			&runner.Repository, &runner.Provider, &runner.RunnerName, &runner.RequestID, &runner.Phase,
		); scanErr != nil {
			return errors.Join(fmt.Errorf("scan bulk authoritative runner lifecycle: %w", scanErr), rows.Close())
		}
		runners = append(runners, runner)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("finish bulk authoritative runner lifecycles: %w", err)
	}
	for _, runner := range runners {
		if err = reconcileSQLiteMirrorForRunner(ctx, transaction, runner); err != nil {
			return err
		}
	}
	return nil
}

func reconcileSQLiteMirrorForIdentity(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	identity RunnerIdentity,
) error {
	runner, err := loadSQLiteAuthoritativeRunner(ctx, transaction, scaleSet, repository, identity)
	if err != nil {
		return err
	}
	return reconcileSQLiteMirrorForRunner(ctx, transaction, runner)
}

func loadSQLiteAuthoritativeRunner(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	identity RunnerIdentity,
) (RunnerLifecycle, error) {
	statement, identityArguments, err := sqliteIdentityStatement(identity,
		`SELECT provider, runner_name, request_id, phase FROM runner_lifecycle
		 WHERE scale_set = ? AND repository = ? AND request_id = ?`,
		`SELECT provider, runner_name, request_id, phase FROM runner_lifecycle
		 WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		`SELECT provider, runner_name, request_id, phase FROM runner_lifecycle
		 WHERE scale_set = ? AND repository = ? AND request_id = ? AND runner_name = ?`)
	if err != nil {
		return RunnerLifecycle{}, err
	}
	runner := RunnerLifecycle{ScaleSet: scaleSet, Repository: repository}
	err = transaction.QueryRowContext(ctx, statement,
		append([]any{scaleSet, repository}, identityArguments...)...).Scan(
		&runner.Provider, &runner.RunnerName, &runner.RequestID, &runner.Phase)
	if err != nil {
		return RunnerLifecycle{}, fmt.Errorf("load authoritative runner lifecycle: %w", err)
	}
	return runner, nil
}

func reconcileSQLiteMirrorForRunner(
	ctx context.Context,
	transaction *sql.Tx,
	runner RunnerLifecycle,
) error {
	if err := validateSQLiteMirrorRequestOwnership(ctx, transaction, runner); err != nil {
		return err
	}
	mirrorRequestID, mirrorExists, err := inspectSQLiteMirrorRequest(ctx, transaction, runner)
	if err != nil {
		return err
	}
	if runner.Provider != RunnerProviderLibvirt {
		if mirrorExists {
			return sqliteLifecycleConsistencyError(runner, "legacy lifecycle conflicts with authoritative provider")
		}
		return nil
	}
	if mirrorExists && mirrorRequestID > 0 && mirrorRequestID != runner.RequestID {
		return sqliteLifecycleConsistencyError(runner, "legacy request identity conflicts with authoritative lifecycle")
	}
	return upsertSQLiteLifecycleMirror(ctx, transaction, runner)
}

func inspectSQLiteMirrorRequest(
	ctx context.Context,
	transaction *sql.Tx,
	runner RunnerLifecycle,
) (requestID int64, exists bool, resultErr error) {
	err := transaction.QueryRowContext(ctx, `SELECT request_id FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, false, fmt.Errorf("inspect mirrored runner lifecycle transition: %w", err)
	}
	return requestID, true, nil
}

func upsertSQLiteLifecycleMirror(ctx context.Context, transaction *sql.Tx, runner RunnerLifecycle) error {
	_, err := transaction.ExecContext(ctx, `INSERT INTO libvirt_runner_lifecycle
		(scale_set, repository, request_id, runner_name, reap_requested) VALUES(?, ?, ?, ?, ?)
		ON CONFLICT(scale_set, repository, runner_name) DO UPDATE SET
		request_id = excluded.request_id, reap_requested = excluded.reap_requested`,
		runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName, runner.Phase == RunnerReapPending)
	if err != nil {
		return fmt.Errorf("reconcile mirrored runner lifecycle transition: %w", err)
	}
	return nil
}

func validateSQLiteCompatibleMirror(
	ctx context.Context,
	transaction *sql.Tx,
	runner RunnerLifecycle,
) error {
	if err := validateSQLiteMirrorRequestOwnership(ctx, transaction, runner); err != nil {
		return err
	}
	var mirrorRequestID int64
	err := transaction.QueryRowContext(ctx, `SELECT request_id FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&mirrorRequestID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect terminal mirrored runner lifecycle: %w", err)
	}
	if runner.Provider != RunnerProviderLibvirt {
		return sqliteLifecycleConsistencyError(runner, "legacy lifecycle conflicts with authoritative provider")
	}
	if mirrorRequestID > 0 && mirrorRequestID != runner.RequestID {
		return sqliteLifecycleConsistencyError(runner, "legacy request identity conflicts with terminal reap")
	}
	return nil
}

func validateSQLiteMirrorRequestOwnership(
	ctx context.Context,
	transaction *sql.Tx,
	runner RunnerLifecycle,
) error {
	if runner.RequestID <= 0 {
		return nil
	}
	var conflictingOwners int
	if err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND request_id = ? AND runner_name <> ?`,
		runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName).Scan(&conflictingOwners); err != nil {
		return fmt.Errorf("inspect mirrored request ownership: %w", err)
	}
	if conflictingOwners > 0 {
		return sqliteLifecycleConsistencyError(runner, "legacy request identity is owned by another runner name")
	}
	return nil
}

func deleteSQLiteMirror(ctx context.Context, transaction *sql.Tx, runner RunnerLifecycle) error {
	if _, err := transaction.ExecContext(ctx, `DELETE FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName); err != nil {
		return fmt.Errorf("delete terminal mirrored runner lifecycle: %w", err)
	}
	return nil
}
