package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
	result, err = applyMessageUpdateInTransaction(ctx, transaction, update)
	if err != nil {
		return result, err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit clustered message update: %w", err)
	}
	return result, nil
}

func applyMessageUpdateInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	update scaler.MessageUpdate,
) (scaler.MessageUpdateResult, error) {
	result := scaler.MessageUpdateResult{}
	if err := validateClusterMessageEpoch(ctx, transaction, update); err != nil {
		return result, err
	}
	newlyQueued, decision, err := applyClusterMessageEffects(ctx, transaction, update)
	if err != nil {
		return result, err
	}
	result.AssignedObligations = decision
	result.QueueSnapshot, err = loadCapacityObligationQueueSnapshot(ctx, transaction, update)
	if err != nil {
		return scaler.MessageUpdateResult{}, err
	}
	result.QueueSnapshot.NewlyQueued = newlyQueued + decision.Create
	result.QueueSnapshot.Canceled = decision.Cancel
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
) (int, scaler.AssignedObligationDecision, error) {
	newlyQueued, err := applyClusterMessageDemand(ctx, transaction, update)
	if err != nil {
		return 0, scaler.AssignedObligationDecision{}, err
	}
	if startErr := applyClusterMessageStarts(ctx, transaction, update); startErr != nil {
		return 0, scaler.AssignedObligationDecision{}, startErr
	}
	if completionErr := applyClusterMessageCompletions(ctx, transaction, update); completionErr != nil {
		return 0, scaler.AssignedObligationDecision{}, completionErr
	}
	decision, err := applyClusterMessageStatistics(ctx, transaction, update)
	return newlyQueued, decision, err
}

func applyClusterMessageDemand(
	ctx context.Context,
	transaction *databaseTransaction,
	update scaler.MessageUpdate,
) (int, error) {
	newlyQueued := 0
	for _, demand := range update.Available {
		commandTag, err := transaction.Exec(ctx, `INSERT INTO scaler_demand
			(app_id, scale_set, repository, request_id, queue_time, source,
			 controller_epoch, attempt, next_attempt_at)
			VALUES ($1, $2, $3, $4, $5, 'job_available', 0, 0, $5)
			ON CONFLICT (app_id, scale_set, repository, request_id)
			WHERE source = 'job_available' DO NOTHING`,
			update.AppID, update.ScaleSet, update.Repository, demand.RequestID, demand.QueueTime)
		if err != nil {
			return 0, fmt.Errorf("record clustered message demand: %w", err)
		}
		newlyQueued += int(commandTag.RowsAffected())
	}
	return newlyQueued, nil
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
		if err := applyClusterMessageCompletion(ctx, transaction, update, completed); err != nil {
			return err
		}
	}
	return nil
}

func applyClusterMessageCompletion(
	ctx context.Context,
	transaction *databaseTransaction,
	update scaler.MessageUpdate,
	completed scaler.RunnerMessageIdentity,
) error {
	completion := scaler.JobCompletion{
		ScaleSet: update.ScaleSet, Repository: update.Repository,
		RunnerIdentity: scaler.RunnerIdentity{RequestID: completed.RequestID, RunnerName: completed.RunnerName},
	}
	err := completeRunnerLifecycleJobInTransaction(ctx, transaction, completion)
	if !errors.Is(err, scaler.ErrRunnerIdentityNotFound) {
		return err
	}
	if completion.RequestID <= 0 {
		return nil
	}
	return completeClusterRequestDemand(ctx, transaction, completion)
}

func applyClusterMessageStatistics(
	ctx context.Context,
	transaction *databaseTransaction,
	update scaler.MessageUpdate,
) (scaler.AssignedObligationDecision, error) {
	capacity := scaler.AssignedJobCapacity{}
	return applyAssignedJobObservation(ctx, transaction, AssignedJobObservation{
		AppID: update.AppID, Repository: update.Repository, ScaleSet: update.ScaleSet,
		ControllerEpoch: update.ControllerEpoch, AssignedJobs: update.AssignedJobs, ObservedAt: update.ObservedAt,
	}, &capacity)
}

func loadCapacityObligationQueueSnapshot(
	ctx context.Context,
	transaction *databaseTransaction,
	update scaler.MessageUpdate,
) (scaler.CapacityObligationQueueSnapshot, error) {
	var snapshot scaler.CapacityObligationQueueSnapshot
	err := transaction.QueryRow(ctx, `SELECT
		COUNT(*) FILTER (WHERE demand.source = 'job_available'),
		COUNT(*) FILTER (WHERE demand.source = 'assigned_count'),
		MIN(demand.queue_time) FILTER (WHERE demand.next_attempt_at <= $5
		  AND (demand.source = 'job_available' OR (
		      assigned.controller_epoch = demand.controller_epoch
		      AND control.controller_epoch = demand.controller_epoch
		  ))
		  AND NOT EXISTS (SELECT 1 FROM scaler_assignments assignment
		      WHERE assignment.demand_id = demand.demand_id AND assignment.state <> 'complete')
		  AND NOT EXISTS (SELECT 1 FROM scaler_runner_lifecycle lifecycle
		      JOIN scaler_assignments lifecycle_assignment
		        ON lifecycle_assignment.assignment_id = lifecycle.assignment_id
		      WHERE lifecycle_assignment.demand_id = demand.demand_id)
		  )
		FROM scaler_demand demand
		LEFT JOIN scaler_assigned_job_observations assigned
		  ON assigned.assigned_observation_id = demand.assigned_observation_id
		LEFT JOIN scaler_repository_controls control ON control.app_id = demand.app_id
		WHERE demand.app_id = $1 AND demand.repository = $2 AND demand.scale_set = $3
		  AND control.controller_epoch = $4`,
		update.AppID, update.Repository, update.ScaleSet, update.ControllerEpoch, update.ObservedAt,
	).Scan(&snapshot.JobAvailable, &snapshot.AssignedCount, &snapshot.OldestRunnableAt)
	if err != nil {
		return scaler.CapacityObligationQueueSnapshot{}, fmt.Errorf("load clustered capacity-obligation queue snapshot: %w", err)
	}
	return snapshot, nil
}

func applyClusterMessageStart(
	ctx context.Context,
	transaction *databaseTransaction,
	update scaler.MessageUpdate,
	started scaler.RunnerMessageIdentity,
) error {
	identity := scaler.RunnerIdentity{RequestID: started.RequestID, RunnerName: started.RunnerName}
	var err error
	if identity.RequestID > 0 && strings.TrimSpace(identity.RunnerName) != "" {
		err = bindClusterRunnerIdentityInTransaction(ctx, transaction, update.ScaleSet, update.Repository,
			identity, started.GitHubRunnerID, update.ObservedAt)
	} else {
		err = markClusterRunnerBusyInTransaction(ctx, transaction, update.ScaleSet, update.Repository,
			identity, started.GitHubRunnerID, update.ObservedAt)
	}
	if errors.Is(err, scaler.ErrRunnerIdentityNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("mark clustered message runner busy: %w", err)
	}
	return nil
}
