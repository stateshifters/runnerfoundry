package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

// ApplyMessageUpdate persists every durable effect from one GitHub message in
// a single epoch-fenced PostgreSQL transaction.
func (store *Store) ApplyMessageUpdate(
	ctx context.Context,
	update scaler.MessageUpdate,
) (result scaler.MessageUpdateResult, resultErr error) {
	if !scaler.ValidMessageUpdate(update) {
		return result, scaler.ErrInvalidMessageUpdate
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return result, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin clustered message update: %w", err)
	}
	transactionOpen := true
	defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
	if err = validateClusterMessageEpoch(ctx, transaction, update); err != nil {
		return result, err
	}
	if err = applyClusterMessageEffects(ctx, transaction, update); err != nil {
		return result, err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit clustered message update: %w", err)
	}
	return result, nil
}

func validateClusterMessageEpoch(
	ctx context.Context,
	transaction *databaseTransaction,
	update scaler.MessageUpdate,
) error {
	var currentEpoch int64
	err := transaction.QueryRow(ctx, `SELECT controller_epoch
		FROM scaler_repository_controls WHERE app_id = $1
		FOR SHARE`, update.AppID).Scan(&currentEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotLeader
	}
	if err != nil {
		return fmt.Errorf("validate clustered message controller epoch: %w", err)
	}
	if currentEpoch != update.ControllerEpoch {
		return ErrNotLeader
	}
	return nil
}

func applyClusterMessageEffects(
	ctx context.Context,
	transaction *databaseTransaction,
	update scaler.MessageUpdate,
) error {
	if err := applyClusterMessageDemand(ctx, transaction, update); err != nil {
		return err
	}
	if err := applyClusterMessageStarts(ctx, transaction, update); err != nil {
		return err
	}
	if err := applyClusterMessageCompletions(ctx, transaction, update); err != nil {
		return err
	}
	return applyClusterMessageStatistics(ctx, transaction, update)
}

func applyClusterMessageDemand(ctx context.Context, transaction *databaseTransaction, update scaler.MessageUpdate) error {
	for _, demand := range update.Available {
		if _, err := transaction.Exec(ctx, `INSERT INTO scaler_demand
			(app_id, scale_set, repository, request_id, queue_time, source,
			 controller_epoch, attempt, next_attempt_at)
			VALUES ($1, $2, $3, $4, $5, 'job_available', 0, 0, $5)
			ON CONFLICT (app_id, scale_set, repository, request_id)
			WHERE source = 'job_available' DO NOTHING`,
			update.AppID, update.ScaleSet, update.Repository, demand.RequestID, demand.QueueTime); err != nil {
			return fmt.Errorf("record clustered message demand: %w", err)
		}
	}
	return nil
}

func applyClusterMessageStarts(ctx context.Context, transaction *databaseTransaction, update scaler.MessageUpdate) error {
	for _, started := range update.Started {
		if err := applyClusterMessageStart(ctx, transaction, update, started); err != nil {
			return err
		}
	}
	return nil
}

func applyClusterMessageCompletions(ctx context.Context, transaction *databaseTransaction, update scaler.MessageUpdate) error {
	for _, completed := range update.Completed {
		if err := completeJobInTransaction(ctx, transaction, JobCompletion{
			ScaleSet: update.ScaleSet, Repository: update.Repository,
			RequestID: completed.RequestID, RunnerName: completed.RunnerName,
		}); err != nil {
			return err
		}
	}
	return nil
}

func applyClusterMessageStatistics(ctx context.Context, transaction *databaseTransaction, update scaler.MessageUpdate) error {
	capacity := scaler.AssignedJobCapacity{}
	return applyAssignedJobObservation(ctx, transaction, AssignedJobObservation{
		AppID: update.AppID, Repository: update.Repository, ScaleSet: update.ScaleSet,
		ControllerEpoch: update.ControllerEpoch, AssignedJobs: update.AssignedJobs, ObservedAt: update.ObservedAt,
	}, &capacity)
}

func applyClusterMessageStart(
	ctx context.Context,
	transaction *databaseTransaction,
	update scaler.MessageUpdate,
	started scaler.RunnerMessageIdentity,
) error {
	if _, err := transaction.Exec(ctx, `UPDATE scaler_runner_lifecycle
		SET phase = 'busy', github_runner_id = CASE WHEN $1 > 0 THEN $1 ELSE github_runner_id END,
		request_id = CASE WHEN request_id = 0 AND $4 > 0 AND $3 <> '' THEN $4 ELSE request_id END,
		updated_at = $2
		WHERE scale_set = $5 AND repository = $6 AND phase <> 'reap_pending'
		AND (($3 <> '' AND runner_name = $3) OR ($4 > 0 AND request_id = $4))`,
		started.GitHubRunnerID, update.ObservedAt, started.RunnerName, started.RequestID,
		update.ScaleSet, update.Repository); err != nil {
		return fmt.Errorf("mark clustered message runner busy: %w", err)
	}
	if _, err := transaction.Exec(ctx, `UPDATE scaler_libvirt_runner_lifecycle
		SET request_id = $1 WHERE scale_set = $2 AND repository = $3 AND runner_name = $4
		AND request_id = 0 AND $1 > 0 AND $4 <> ''`,
		started.RequestID, update.ScaleSet, update.Repository, started.RunnerName); err != nil {
		return fmt.Errorf("bind clustered message libvirt identity: %w", err)
	}
	return nil
}
