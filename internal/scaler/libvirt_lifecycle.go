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

type JobCompletion struct {
	ScaleSet   string
	Repository string
	RequestID  int64
	RunnerName string
}

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

func (store *SQLiteDemandStore) CompleteJob(ctx context.Context, completion JobCompletion) (resultErr error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin job completion: %w", err)
	}
	transactionOpen := true
	defer finishSQLiteTransaction(transaction, &transactionOpen, "job completion", &resultErr)
	if completion.RunnerName != "" {
		_, err = transaction.ExecContext(ctx, `INSERT INTO libvirt_runner_lifecycle
			(scale_set, repository, request_id, runner_name, reap_requested)
			VALUES(?, ?, ?, ?, 1)
			ON CONFLICT(scale_set, repository, runner_name) DO UPDATE SET reap_requested = 1`,
			completion.ScaleSet, completion.Repository, completion.RequestID, completion.RunnerName)
		if err != nil {
			return fmt.Errorf("record completed libvirt runner: %w", err)
		}
	}
	if _, err = transaction.ExecContext(ctx, `UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE scale_set = ? AND repository = ? AND request_id = ?`,
		completion.ScaleSet, completion.Repository, completion.RequestID); err != nil {
		return fmt.Errorf("request libvirt runner reap: %w", err)
	}
	if _, err = transaction.ExecContext(ctx, `DELETE FROM demand
		WHERE scale_set = ? AND repository = ? AND request_id = ?`,
		completion.ScaleSet, completion.Repository, completion.RequestID); err != nil {
		return fmt.Errorf("remove completed demand: %w", err)
	}
	transactionOpen = false
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("commit job completion: %w", err)
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
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close pending libvirt reaps: %w", closeErr))
		}
	}()
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

func (store *SQLiteDemandStore) ConfirmLibvirtReaped(ctx context.Context, runner LibvirtRunnerLifecycle) error {
	result, err := store.database.ExecContext(ctx, `DELETE FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ? AND reap_requested = 1`,
		runner.ScaleSet, runner.Repository, runner.RunnerName)
	if err != nil {
		return fmt.Errorf("confirm libvirt runner reaped: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count confirmed libvirt runner reap: %w", err)
	}
	if removed != 1 {
		return sql.ErrNoRows
	}
	return nil
}
