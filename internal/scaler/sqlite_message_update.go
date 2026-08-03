package scaler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ApplyMessageUpdate persists one complete GitHub message under the current
// standalone controller epoch.
func (store *SQLiteDemandStore) ApplyMessageUpdate(
	ctx context.Context,
	update MessageUpdate,
) (result MessageUpdateResult, resultErr error) {
	if !ValidMessageUpdate(update) {
		return result, ErrInvalidMessageUpdate
	}
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin scale-set message update: %w", err)
	}
	transactionOpen := true
	defer finishSQLiteTransaction(transaction, &transactionOpen, "scale-set message update", &resultErr)
	if err = validateSQLiteMessageEpoch(ctx, transaction, update); err != nil {
		return result, err
	}
	if err = applySQLiteMessageEffects(ctx, transaction, update, &result); err != nil {
		return result, err
	}
	transactionOpen = false
	if err = transaction.Commit(); err != nil {
		return result, fmt.Errorf("commit scale-set message update: %w", err)
	}
	return result, nil
}

func validateSQLiteMessageEpoch(ctx context.Context, transaction *sql.Tx, update MessageUpdate) error {
	var current int
	err := transaction.QueryRowContext(ctx, `SELECT 1 FROM repository_controls
		WHERE app_id = ? AND controller_epoch = ?`, update.AppID, update.ControllerEpoch).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotLeader
	}
	if err != nil {
		return fmt.Errorf("validate message controller epoch: %w", err)
	}
	return nil
}

func applySQLiteMessageEffects(
	ctx context.Context,
	transaction *sql.Tx,
	update MessageUpdate,
	result *MessageUpdateResult,
) error {
	removals := startedDemandRemovals(update)
	if err := applyDemandChanges(ctx, transaction, update.ScaleSet, update.Available, removals); err != nil {
		return err
	}
	for _, started := range update.Started {
		if err := markSQLiteMessageRunnerBusy(ctx, transaction, update, started); err != nil {
			return err
		}
	}
	for _, completed := range update.Completed {
		if err := completeSQLiteRunnerJob(ctx, transaction, JobCompletion{
			ScaleSet: update.ScaleSet, Repository: update.Repository,
			RequestID: completed.RequestID, RunnerName: completed.RunnerName,
		}, update.ObservedAt); err != nil {
			return err
		}
	}
	activeCapacity, err := sqliteMessageActiveCapacity(ctx, transaction, update.ScaleSet, update.Repository)
	if err != nil {
		return err
	}
	decision, err := reconcileAssignedCapacityObservation(ctx, transaction, AssignedCapacityObservation{
		ScaleSet: update.ScaleSet, Repository: update.Repository, ControllerEpoch: update.ControllerEpoch,
		AssignedJobs: update.AssignedJobs, ObservedAt: update.ObservedAt,
	}, activeCapacity)
	result.AssignedObligations = decision
	return err
}

func sqliteMessageActiveCapacity(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet string,
	repository string,
) (int, error) {
	var active int
	if err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND phase <> 'reap_pending'`, scaleSet, repository).Scan(&active); err != nil {
		return 0, fmt.Errorf("count post-message runner capacity: %w", err)
	}
	return active, nil
}

func startedDemandRemovals(update MessageUpdate) []DemandRemoval {
	removals := make([]DemandRemoval, 0, len(update.Started))
	for _, started := range update.Started {
		if started.RequestID > 0 {
			removals = append(removals, DemandRemoval{Repository: update.Repository, RequestID: started.RequestID})
		}
	}
	return removals
}

func markSQLiteMessageRunnerBusy(
	ctx context.Context,
	transaction *sql.Tx,
	update MessageUpdate,
	started RunnerMessageIdentity,
) error {
	_, err := transaction.ExecContext(ctx, `UPDATE runner_lifecycle
		SET phase = 'busy', request_id = CASE WHEN request_id = 0 AND ? > 0 THEN ? ELSE request_id END,
		github_runner_id = CASE WHEN ? > 0 THEN ? ELSE github_runner_id END, updated_at = ?
		WHERE scale_set = ? AND repository = ? AND phase <> 'reap_pending'
		AND ((? <> '' AND runner_name = ?) OR (? > 0 AND request_id = ?))`,
		started.RequestID, started.RequestID, started.GitHubRunnerID, started.GitHubRunnerID,
		update.ObservedAt.UnixNano(), update.ScaleSet, update.Repository,
		started.RunnerName, started.RunnerName, started.RequestID, started.RequestID)
	if err != nil {
		return fmt.Errorf("mark message runner busy: %w", err)
	}
	return nil
}
