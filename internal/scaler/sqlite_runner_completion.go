package scaler

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (store *SQLiteDemandStore) CompleteRunnerLifecycleJob(
	ctx context.Context,
	completion JobCompletion,
	completedAt time.Time,
) error {
	return store.runSQLiteTransaction(ctx, "runner job completion", func(transaction *sql.Tx) error {
		return completeSQLiteRunnerJob(ctx, transaction, completion, completedAt)
	})
}

func completeSQLiteRunnerJob(ctx context.Context, transaction *sql.Tx, completion JobCompletion, completedAt time.Time) error {
	identity := completion.RunnerIdentity
	if err := resolveSQLiteRunnerIdentity(
		ctx, transaction, completion.ScaleSet, completion.Repository, anySQLiteRunner, identity,
	); err != nil {
		return err
	}
	if err := requestSQLiteCompletedRunnerReap(ctx, transaction, completion, completedAt); err != nil {
		return err
	}
	if err := mirrorSQLiteCompletedRunner(ctx, transaction, completion); err != nil {
		return err
	}
	if err := removeSQLiteCompletedRunnerDemand(ctx, transaction, completion); err != nil {
		return err
	}
	return removeSQLiteCompletedRequestDemand(ctx, transaction, completion)
}

func requestSQLiteCompletedRunnerReap(
	ctx context.Context,
	transaction *sql.Tx,
	completion JobCompletion,
	completedAt time.Time,
) error {
	identity := completion.RunnerIdentity
	result, err := executeSQLiteIdentityStatement(ctx, transaction, identity,
		[]any{completedAt.UnixNano(), completion.ScaleSet, completion.Repository},
		`UPDATE runner_lifecycle SET phase = 'reap_pending', reap_reason = 'job_completed', updated_at = ?
		WHERE scale_set = ? AND repository = ? AND request_id = ?`,
		`UPDATE runner_lifecycle SET phase = 'reap_pending', reap_reason = 'job_completed', updated_at = ?
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		`UPDATE runner_lifecycle SET phase = 'reap_pending', reap_reason = 'job_completed', updated_at = ?
		WHERE scale_set = ? AND repository = ? AND request_id = ? AND runner_name = ?`,
		"request completed runner reap")
	if err != nil {
		return err
	}
	return requireSingleSQLiteIdentityChange(result, identity, "count completed runner reap")
}

func removeSQLiteCompletedRequestDemand(ctx context.Context, transaction *sql.Tx, completion JobCompletion) error {
	if completion.RequestID <= 0 {
		return nil
	}
	if _, err := transaction.ExecContext(ctx, `DELETE FROM demand
		WHERE scale_set = ? AND repository = ? AND request_id = ?`,
		completion.ScaleSet, completion.Repository, completion.RequestID); err != nil {
		return fmt.Errorf("remove completed demand: %w", err)
	}
	return nil
}

func mirrorSQLiteCompletedRunner(ctx context.Context, transaction *sql.Tx, completion JobCompletion) error {
	result, err := executeSQLiteIdentityStatement(ctx, transaction, completion.RunnerIdentity,
		[]any{completion.ScaleSet, completion.Repository},
		`UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE scale_set = ? AND repository = ? AND request_id = ?`,
		`UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		`UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE scale_set = ? AND repository = ? AND request_id = ? AND runner_name = ?`,
		"mirror completed libvirt runner reap")
	if err != nil {
		return err
	}
	return requireAtMostOneSQLiteIdentityChange(result, completion.RunnerIdentity,
		"count mirrored completed runner reap")
}

func removeSQLiteCompletedRunnerDemand(ctx context.Context, transaction *sql.Tx, completion JobCompletion) error {
	_, err := executeSQLiteIdentityStatement(ctx, transaction, completion.RunnerIdentity,
		[]any{completion.ScaleSet, completion.Repository},
		`DELETE FROM demand WHERE source = 'assigned_count' AND demand_id IN
		(SELECT assignment_id FROM runner_lifecycle WHERE scale_set = ? AND repository = ? AND request_id = ?)`,
		`DELETE FROM demand WHERE source = 'assigned_count' AND demand_id IN
		(SELECT assignment_id FROM runner_lifecycle WHERE scale_set = ? AND repository = ? AND runner_name = ?)`,
		`DELETE FROM demand WHERE source = 'assigned_count' AND demand_id IN
		(SELECT assignment_id FROM runner_lifecycle WHERE scale_set = ? AND repository = ?
		AND request_id = ? AND runner_name = ?)`,
		"complete assigned capacity obligation")
	return err
}
