package scaler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type LibvirtRunnerLifecycle struct {
	ScaleSet   string
	Repository string
	RequestID  int64
	RunnerName string
}

// Identity returns the persisted request/name pair used for exact mutations.
func (runner LibvirtRunnerLifecycle) Identity() RunnerIdentity {
	return RunnerIdentity{RequestID: runner.RequestID, RunnerName: runner.RunnerName}
}

type JobCompletion struct {
	ScaleSet   string
	Repository string
	RunnerIdentity
}

// LibvirtLifecycleStore applies the RunnerIdentity selection and error contract
// documented by RunnerLifecycleTransitions to completion and reap confirmation.
// Completion never creates a missing lifecycle, and confirmation only removes
// the exact pending lifecycle supplied by the caller.
type LibvirtLifecycleStore interface {
	RegisterLibvirtRunner(ctx context.Context, runner LibvirtRunnerLifecycle) error
	CompleteJob(ctx context.Context, completion JobCompletion) error
	PendingLibvirtReaps(ctx context.Context, scaleSet string, repository string) ([]LibvirtRunnerLifecycle, error)
	ConfirmLibvirtReaped(ctx context.Context, runner LibvirtRunnerLifecycle) error
}

func (store *SQLiteDemandStore) RegisterLibvirtRunner(ctx context.Context, runner LibvirtRunnerLifecycle) error {
	result, err := store.database.ExecContext(ctx, `INSERT INTO libvirt_runner_lifecycle
		(scale_set, repository, request_id, runner_name, reap_requested)
		VALUES(?, ?, ?, ?, 0)
		ON CONFLICT(scale_set, repository, runner_name) DO NOTHING`,
		runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName)
	if err != nil {
		return fmt.Errorf("register libvirt runner lifecycle: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count registered libvirt runner lifecycle: %w", err)
	}
	if inserted == 1 {
		return nil
	}
	var existingRequestID int64
	var reapRequested bool
	if err = store.database.QueryRowContext(ctx, `SELECT request_id, reap_requested
		FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&existingRequestID, &reapRequested); err != nil {
		return fmt.Errorf("inspect registered libvirt runner lifecycle: %w", err)
	}
	if existingRequestID != runner.RequestID || reapRequested {
		return errors.New("libvirt runner identity remains reserved by another lifecycle")
	}
	return nil
}

func (store *SQLiteDemandStore) CompleteJob(ctx context.Context, completion JobCompletion) error {
	return store.runSQLiteTransaction(ctx, "job completion", func(transaction *sql.Tx) error {
		return completeSQLiteLibvirtJob(ctx, transaction, completion)
	})
}

func completeSQLiteLibvirtJob(ctx context.Context, transaction *sql.Tx, completion JobCompletion) error {
	identity := completion.RunnerIdentity
	if err := resolveSQLiteLibvirtIdentity(ctx, transaction, completion.ScaleSet, completion.Repository,
		anySQLiteRunner, identity); err != nil {
		return err
	}
	query, identityArguments := sqliteValidatedIdentityStatement(identity,
		`UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE scale_set = ? AND repository = ? AND request_id = ?`,
		`UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		`UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE scale_set = ? AND repository = ? AND request_id = ? AND runner_name = ?`)
	result, err := transaction.ExecContext(ctx, query,
		append([]any{completion.ScaleSet, completion.Repository}, identityArguments...)...)
	if err != nil {
		return fmt.Errorf("request libvirt runner reap: %w", err)
	}
	if err = requireSingleSQLiteIdentityChange(result, identity, "count requested libvirt runner reap"); err != nil {
		return err
	}
	if _, err = transaction.ExecContext(ctx, `DELETE FROM demand
		WHERE scale_set = ? AND repository = ? AND request_id = ?`,
		completion.ScaleSet, completion.Repository, completion.RequestID); err != nil {
		return fmt.Errorf("remove completed demand: %w", err)
	}
	return nil
}

func (store *SQLiteDemandStore) PendingLibvirtReaps(
	ctx context.Context,
	scaleSet string,
	repository string,
) (runners []LibvirtRunnerLifecycle, resultErr error) {
	rows, err := store.database.QueryContext(ctx, `SELECT scale_set, repository, request_id, runner_name
		FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND (? = '' OR repository = ?) AND reap_requested = 1
		ORDER BY repository, runner_name`, scaleSet, repository, repository)
	if err != nil {
		return nil, fmt.Errorf("load pending libvirt reaps: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		var runner LibvirtRunnerLifecycle
		if err = rows.Scan(&runner.ScaleSet, &runner.Repository, &runner.RequestID, &runner.RunnerName); err != nil {
			return nil, fmt.Errorf("scan pending libvirt reap: %w", err)
		}
		runners = append(runners, runner)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending libvirt reaps: %w", err)
	}
	return runners, nil
}

func (store *SQLiteDemandStore) ConfirmLibvirtReaped(
	ctx context.Context, runner LibvirtRunnerLifecycle,
) error {
	return store.runSQLiteTransaction(ctx, "confirm libvirt runner reaped", func(transaction *sql.Tx) error {
		return confirmSQLiteLibvirtReaped(ctx, transaction, runner)
	})
}

func confirmSQLiteLibvirtReaped(ctx context.Context, transaction *sql.Tx, runner LibvirtRunnerLifecycle) error {
	identity := runner.Identity()
	if err := resolveSQLiteLibvirtIdentity(ctx, transaction, runner.ScaleSet, runner.Repository,
		pendingSQLiteRunner, identity); err != nil {
		return err
	}
	query, identityArguments := sqliteValidatedIdentityStatement(identity,
		`DELETE FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND reap_requested = 1 AND request_id = ?`,
		`DELETE FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND reap_requested = 1 AND runner_name = ?`,
		`DELETE FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND reap_requested = 1
		AND request_id = ? AND runner_name = ?`)
	result, err := transaction.ExecContext(ctx, query,
		append([]any{runner.ScaleSet, runner.Repository}, identityArguments...)...)
	if err != nil {
		return fmt.Errorf("confirm libvirt runner reaped: %w", err)
	}
	if err = requireSingleSQLiteIdentityChange(result, identity, "count confirmed libvirt runner reap"); err != nil {
		return err
	}
	return nil
}
