package scaler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type sqliteLifecycleRegistrationState struct {
	genericExists                  bool
	provider                       RunnerProvider
	requestID                      int64
	assignmentID                   int64
	phase                          RunnerLifecyclePhase
	mirrorExists                   bool
	mirrorRequest                  int64
	mirrorReap                     bool
	genericRequestOwnedByOtherName bool
	mirrorRequestOwnedByOtherName  bool
}

func (store *SQLiteDemandStore) RegisterRunner(ctx context.Context, runner RunnerLifecycle) error {
	registration := normalizeSQLiteRunnerRegistration(runner)
	return store.runSQLiteTransaction(ctx, "runner lifecycle registration", func(transaction *sql.Tx) error {
		return registerSQLiteRunner(ctx, transaction, registration)
	})
}

func registerSQLiteRunner(ctx context.Context, transaction *sql.Tx, runner RunnerLifecycle) error {
	state, err := loadSQLiteLifecycleRegistrationState(ctx, transaction, runner)
	if err != nil {
		return err
	}
	if err = validateSQLiteLifecycleRegistrationState(runner, state); err != nil {
		return err
	}
	if !state.genericExists {
		result, insertErr := insertSQLiteRunnerRegistration(ctx, transaction, runner)
		if insertErr != nil {
			return fmt.Errorf("register runner lifecycle: %w", insertErr)
		}
		if changedErr := requireLifecycleChange(result, "count registered runner lifecycle"); changedErr != nil {
			return changedErr
		}
	}
	if runner.Provider != RunnerProviderLibvirt {
		return nil
	}
	return reconcileSQLiteLibvirtRegistration(ctx, transaction, runner)
}

func loadSQLiteLifecycleRegistrationState(ctx context.Context, transaction *sql.Tx, runner RunnerLifecycle) (sqliteLifecycleRegistrationState, error) {
	var state sqliteLifecycleRegistrationState
	var provider, phase string
	err := transaction.QueryRowContext(ctx, inspectSQLiteRunnerLifecycleSQL, runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&provider, &state.requestID, &state.assignmentID, &phase)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return state, fmt.Errorf("inspect registered runner lifecycle: %w", err)
	}
	if err == nil {
		state.genericExists, state.provider, state.phase = true, RunnerProvider(provider), RunnerLifecyclePhase(phase)
	}
	err = transaction.QueryRowContext(ctx, `SELECT request_id, reap_requested FROM libvirt_runner_lifecycle WHERE scale_set = ? AND repository = ? AND runner_name = ?`, runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&state.mirrorRequest, &state.mirrorReap)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return state, fmt.Errorf("inspect mirrored runner lifecycle: %w", err)
	}
	state.mirrorExists = err == nil
	if err = loadSQLiteRegistrationRequestOwners(ctx, transaction, runner, &state); err != nil {
		return state, err
	}
	return state, nil
}

func loadSQLiteRegistrationRequestOwners(
	ctx context.Context,
	transaction *sql.Tx,
	runner RunnerLifecycle,
	state *sqliteLifecycleRegistrationState,
) error {
	if runner.RequestID <= 0 {
		return nil
	}
	var mirrorOwners int
	if err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM libvirt_runner_lifecycle WHERE scale_set = ? AND repository = ? AND request_id = ? AND runner_name <> ?`, runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName).Scan(&mirrorOwners); err != nil {
		return fmt.Errorf("inspect mirrored request ownership: %w", err)
	}
	state.mirrorRequestOwnedByOtherName = mirrorOwners > 0
	var genericOwners int
	if err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_lifecycle WHERE scale_set = ? AND repository = ? AND request_id = ? AND runner_name <> ?`, runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName).Scan(&genericOwners); err != nil {
		return fmt.Errorf("inspect authoritative request ownership: %w", err)
	}
	state.genericRequestOwnedByOtherName = genericOwners > 0
	return nil
}

func validateSQLiteLifecycleRegistrationState(runner RunnerLifecycle, state sqliteLifecycleRegistrationState) error {
	if err := validateSQLiteGenericRegistrationState(runner, state); err != nil {
		return err
	}
	return validateSQLiteMirrorRegistrationState(runner, state)
}

func validateSQLiteGenericRegistrationState(runner RunnerLifecycle, state sqliteLifecycleRegistrationState) error {
	if state.genericRequestOwnedByOtherName {
		return sqliteLifecycleConsistencyError(runner, "authoritative request identity is owned by another runner name")
	}
	if state.genericExists && state.provider != runner.Provider {
		return sqliteLifecycleConsistencyError(runner, "authoritative provider conflicts with registration")
	}
	if state.genericExists && (state.requestID != runner.RequestID || state.assignmentID != runner.AssignmentID) {
		return sqliteLifecycleConsistencyError(runner, "authoritative identity conflicts with registration")
	}
	if state.genericExists && state.phase == RunnerReapPending && runner.Phase != RunnerReapPending {
		return sqliteLifecycleConsistencyError(runner, "authoritative lifecycle is pending reap")
	}
	return nil
}

func validateSQLiteMirrorRegistrationState(runner RunnerLifecycle, state sqliteLifecycleRegistrationState) error {
	if state.mirrorRequestOwnedByOtherName {
		return sqliteLifecycleConsistencyError(runner, "legacy request identity is owned by another runner name")
	}
	if state.mirrorExists && runner.Provider != RunnerProviderLibvirt {
		return sqliteLifecycleConsistencyError(runner, "legacy libvirt lifecycle conflicts with authoritative provider")
	}
	if state.mirrorExists && state.mirrorRequest > 0 && state.mirrorRequest != runner.RequestID {
		return sqliteLifecycleConsistencyError(runner, "legacy request identity conflicts with authoritative lifecycle")
	}
	return nil
}

func sqliteLifecycleConsistencyError(runner RunnerLifecycle, reason string) error {
	return &RunnerLifecycleConsistencyError{ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerName: runner.RunnerName, Reason: reason}
}

func reconcileSQLiteLibvirtRegistration(ctx context.Context, transaction *sql.Tx, runner RunnerLifecycle) error {
	_, err := transaction.ExecContext(ctx, `INSERT INTO libvirt_runner_lifecycle (scale_set, repository, request_id, runner_name, reap_requested) VALUES(?, ?, ?, ?, ?) ON CONFLICT(scale_set, repository, runner_name) DO UPDATE SET request_id = excluded.request_id, reap_requested = excluded.reap_requested`, runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName, runner.Phase == RunnerReapPending)
	if err != nil {
		return fmt.Errorf("reconcile mirrored runner lifecycle: %w", err)
	}
	return nil
}

func normalizeSQLiteRunnerRegistration(runner RunnerLifecycle) RunnerLifecycle {
	if runner.CreatedAt.IsZero() {
		runner.CreatedAt = time.Now().UTC()
	}
	if runner.Phase == "" {
		runner.Phase = RunnerProvisioning
	}
	if runner.WaitingSince.IsZero() && (runner.Phase == RunnerWaitingForJob || runner.Phase == RunnerBusy) {
		runner.WaitingSince = runner.CreatedAt
	}
	if runner.ReapReason == "" && runner.Phase == RunnerReapPending {
		runner.ReapReason = "lifecycle_reap_requested"
	}
	return runner
}

type sqliteRegistrationExecutor interface {
	ExecContext(ctx context.Context, statement string, arguments ...any) (sql.Result, error)
}

func insertSQLiteRunnerRegistration(
	ctx context.Context,
	executor sqliteRegistrationExecutor,
	runner RunnerLifecycle,
) (sql.Result, error) {
	createdAt := runner.CreatedAt.UnixNano()
	waitingSince := unixNanoOrZero(runner.WaitingSince)
	var result sql.Result
	var err error
	if runner.AssignmentID > 0 {
		result, err = executor.ExecContext(ctx, registerSQLiteRunnerLifecycleSQL,
			runner.NodeID, runner.Provider, runner.RunnerName, runner.InstallationID,
			runner.GitHubScaleSetID, runner.GitHubRunnerID, runner.Phase, createdAt,
			waitingSince, createdAt, runner.ReapReason, runner.AssignmentID)
	} else {
		result, err = executor.ExecContext(ctx, registerUnassignedSQLiteRunnerLifecycleSQL,
			runner.NodeID, runner.ScaleSet, runner.Repository, runner.Provider, runner.RunnerName,
			runner.RequestID, runner.InstallationID, runner.GitHubScaleSetID, runner.GitHubRunnerID,
			runner.Phase, createdAt, waitingSince, createdAt, runner.ReapReason)
	}
	if err != nil {
		return nil, fmt.Errorf("insert runner registration: %w", err)
	}
	return result, nil
}
