package cluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

const cancelExcessReadyAssignedJobsSQL = `WITH ranked_ready AS (
	SELECT assignment.assignment_id,
	       ROW_NUMBER() OVER (ORDER BY assignment.assignment_id) AS position,
	       GREATEST($5 - (SELECT COUNT(*)
	           FROM scaler_assignments active
	           JOIN scaler_demand active_demand ON active_demand.demand_id = active.demand_id
	           WHERE active_demand.scale_set = observation.scale_set
	             AND active_demand.repository = observation.repository_url
	             AND active.state <> 'complete'
	             AND NOT (active_demand.assigned_observation_id = observation.assigned_observation_id
	                      AND active.state = 'ready')), 0) AS retain
	FROM scaler_assignments assignment
	JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
	JOIN scaler_assigned_job_observations observation
	  ON observation.assigned_observation_id = demand.assigned_observation_id
	WHERE observation.app_id = $1 AND observation.repository_url = $2
	  AND observation.scale_set = $3 AND observation.controller_epoch = $4
	  AND assignment.state = 'ready'
), excess AS (
	SELECT assignment_id FROM ranked_ready WHERE position > retain
)
UPDATE scaler_assignments assignment SET state = 'complete'
FROM excess WHERE assignment.assignment_id = excess.assignment_id
  AND assignment.state = 'ready'`

const requestExcessAssignedRunnerReapsSQL = `WITH assigned_busy AS (
	SELECT COUNT(*) AS runners FROM scaler_runner_lifecycle
	WHERE scale_set = $2 AND repository = $3 AND phase = 'busy'
), ranked_waiting AS (
	SELECT lifecycle.node_id, lifecycle.scale_set, lifecycle.repository, lifecycle.runner_name,
	       ROW_NUMBER() OVER (ORDER BY lifecycle.waiting_since, lifecycle.runner_name) AS position
	FROM scaler_runner_lifecycle lifecycle
	WHERE lifecycle.scale_set = $2 AND lifecycle.repository = $3
	  AND lifecycle.request_id = 0 AND lifecycle.phase = 'waiting_for_job'
), excess AS (
	SELECT ranked_waiting.node_id, ranked_waiting.scale_set,
	       ranked_waiting.repository, ranked_waiting.runner_name
	FROM ranked_waiting, assigned_busy
	WHERE ranked_waiting.position > GREATEST($1 - assigned_busy.runners, 0)
)
UPDATE scaler_runner_lifecycle lifecycle
SET phase = 'reap_pending', reap_reason = $5, updated_at = $4
FROM excess
WHERE lifecycle.node_id = excess.node_id AND lifecycle.scale_set = excess.scale_set
  AND lifecycle.repository = excess.repository AND lifecycle.runner_name = excess.runner_name
  AND lifecycle.phase = 'waiting_for_job'`

const completeExcessAssignedAssignmentsSQL = `UPDATE scaler_assignments assignment SET state = 'complete'
	WHERE assignment.assignment_id IN (
		SELECT assignment_id FROM scaler_runner_lifecycle
		WHERE scale_set = $1 AND repository = $2 AND request_id = 0
		  AND phase = 'reap_pending' AND assignment_id IS NOT NULL
	) AND assignment.state <> 'complete'`

// ObserveAssignedJobs durably publishes an authoritative aggregate count and
// returns the repository and cluster capacity visible to that observation.
func (store *Store) ObserveAssignedJobs(
	ctx context.Context,
	observation AssignedJobObservation,
) (result AssignedJobObservationResult, resultErr error) {
	if err := validateAssignedJobObservation(observation); err != nil {
		return result, err
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return result, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin assigned-job observation: %w", err)
	}
	transactionOpen := true
	defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
	result, err = observeAssignedJobsInTransaction(ctx, transaction, observation)
	if err != nil {
		return result, err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit assigned-job observation: %w", err)
	}
	return result, nil
}

func observeAssignedJobsInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
) (AssignedJobObservationResult, error) {
	result := AssignedJobObservationResult{}
	decision, err := applyAssignedJobObservation(ctx, transaction, observation, &result.Capacity)
	if err != nil {
		return result, err
	}
	result.QueueUpdate.AssignedObligations = decision
	result.QueueUpdate.QueueSnapshot, err = loadCapacityObligationQueueSnapshot(ctx, transaction, scaler.MessageUpdate{
		AppID: observation.AppID, Repository: observation.Repository, ScaleSet: observation.ScaleSet,
		ControllerEpoch: observation.ControllerEpoch, ObservedAt: observation.ObservedAt,
	})
	if err != nil {
		return AssignedJobObservationResult{}, err
	}
	result.QueueUpdate.QueueSnapshot.NewlyQueued = decision.Create
	result.QueueUpdate.QueueSnapshot.Canceled = decision.Cancel
	return result, nil
}

func applyAssignedJobObservation(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
	capacity *scaler.AssignedJobCapacity,
) (scaler.AssignedObligationDecision, error) {
	operations := []func(context.Context, *databaseTransaction, AssignedJobObservation) error{
		upsertAssignedJobObservation,
		adoptLifecycleBackedAssignedAssignments,
		fenceObsoleteAssignedAssignments,
		adoptQueuedAssignedObligations,
		reconcileExcessAssignedRunners,
	}
	for _, operation := range operations {
		if err := operation(ctx, transaction, observation); err != nil {
			return scaler.AssignedObligationDecision{}, err
		}
	}
	if err := loadAssignedJobCapacity(ctx, transaction, observation, capacity); err != nil {
		return scaler.AssignedObligationDecision{}, err
	}
	return reconcileQueuedAssignedObligations(ctx, transaction, observation, capacity.ActiveRunners)
}

func adoptQueuedAssignedObligations(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
) error {
	if _, err := transaction.Exec(ctx, `UPDATE scaler_demand demand
		SET controller_epoch = $4
		FROM scaler_assigned_job_observations assigned
		WHERE assigned.assigned_observation_id = demand.assigned_observation_id
		  AND demand.app_id = $1 AND demand.repository = $2 AND demand.scale_set = $3
		  AND assigned.app_id = $1 AND assigned.repository_url = $2 AND assigned.scale_set = $3
		  AND assigned.controller_epoch = $4 AND demand.controller_epoch <> $4
		  AND demand.source = 'assigned_count'
		  AND NOT EXISTS (SELECT 1 FROM scaler_assignments assignment
		      WHERE assignment.demand_id = demand.demand_id AND assignment.state <> 'complete')`,
		observation.AppID, observation.Repository, observation.ScaleSet, observation.ControllerEpoch); err != nil {
		return fmt.Errorf("adopt queued assigned obligations: %w", err)
	}
	return nil
}

func reconcileQueuedAssignedObligations(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
	activeCapacity int,
) (scaler.AssignedObligationDecision, error) {
	var queuedObligations, blockedObligations int
	err := transaction.QueryRow(ctx, `SELECT
		COUNT(*) FILTER (WHERE demand.blocked_reason IS NULL),
		COUNT(*) FILTER (WHERE demand.blocked_reason IS NOT NULL)
		FROM scaler_demand demand
		JOIN scaler_assigned_job_observations assigned
		  ON assigned.assigned_observation_id = demand.assigned_observation_id
		WHERE assigned.app_id = $1 AND assigned.repository_url = $2
		  AND assigned.scale_set = $3 AND demand.source = 'assigned_count'
		  AND NOT EXISTS (SELECT 1 FROM scaler_assignments assignment
		      WHERE assignment.demand_id = demand.demand_id AND assignment.state <> 'complete')`,
		observation.AppID, observation.Repository, observation.ScaleSet).
		Scan(&queuedObligations, &blockedObligations)
	if err != nil {
		return scaler.AssignedObligationDecision{}, fmt.Errorf("count queued assigned obligations: %w", err)
	}
	classification := scaler.ClassifyAssignedCapacity(scaler.AssignedCapacityStatistics{
		AssignedJobs: observation.AssignedJobs, RunningJobs: observation.RunningJobs,
		RegisteredRunners: observation.RegisteredRunners, BusyRunners: observation.BusyRunners,
	})
	requested := scaler.DecideAssignedObligations(scaler.AssignedObligationSnapshot{
		AssignedJobs: observation.AssignedJobs, StrandedAssigned: classification.StrandedAssigned,
		ActiveCapacity: activeCapacity, QueuedObligations: queuedObligations,
		BlockedObligations: blockedObligations,
	})
	return applyAssignedObligationDecision(ctx, transaction, observation, requested)
}

func applyAssignedObligationDecision(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
	requested scaler.AssignedObligationDecision,
) (scaler.AssignedObligationDecision, error) {
	actual := scaler.AssignedObligationDecision{CancelBlocked: requested.CancelBlocked}
	for _, operation := range assignedObligationOperations(ctx, transaction, observation, requested, &actual) {
		if err := operation.apply(); err != nil {
			return scaler.AssignedObligationDecision{}, err
		}
	}
	return actual, nil
}

type assignedObligationOperation struct {
	count       int
	changed     *int
	applyChange func(int) (int, error)
}

func (operation assignedObligationOperation) apply() error {
	if operation.count <= 0 {
		return nil
	}
	changed, err := operation.applyChange(operation.count)
	if err != nil {
		return err
	}
	*operation.changed = changed
	return nil
}

func assignedObligationOperations(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
	requested scaler.AssignedObligationDecision,
	actual *scaler.AssignedObligationDecision,
) []assignedObligationOperation {
	return []assignedObligationOperation{
		{count: requested.Cancel, changed: &actual.Cancel, applyChange: func(count int) (int, error) {
			return cancelQueuedAssignedObligations(ctx, transaction, observation, count)
		}},
		{count: requested.Create, changed: &actual.Create, applyChange: func(count int) (int, error) {
			return createQueuedAssignedObligations(ctx, transaction, observation, count)
		}},
		{count: requested.Block, changed: &actual.Block, applyChange: func(count int) (int, error) {
			return blockQueuedAssignedObligations(ctx, transaction, observation, count)
		}},
		{count: requested.Unblock, changed: &actual.Unblock, applyChange: func(count int) (int, error) {
			return unblockQueuedAssignedObligations(ctx, transaction, observation, count)
		}},
	}
}

func cancelQueuedAssignedObligations(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
	count int,
) (int, error) {
	commandTag, err := transaction.Exec(ctx, `WITH canceled AS (
		SELECT demand.demand_id
		FROM scaler_demand demand
		JOIN scaler_assigned_job_observations assigned
		  ON assigned.assigned_observation_id = demand.assigned_observation_id
		WHERE assigned.app_id = $1 AND assigned.repository_url = $2
		  AND assigned.scale_set = $3 AND demand.source = 'assigned_count'
		  AND NOT EXISTS (SELECT 1 FROM scaler_assignments assignment
		      WHERE assignment.demand_id = demand.demand_id AND assignment.state <> 'complete')
		ORDER BY (demand.blocked_reason IS NOT NULL) DESC,
		         demand.queue_time DESC, demand.observation_id DESC
		LIMIT $4
	)
	DELETE FROM scaler_demand demand USING canceled
	WHERE demand.demand_id = canceled.demand_id`, observation.AppID, observation.Repository,
		observation.ScaleSet, count)
	if err != nil {
		return 0, fmt.Errorf("cancel queued assigned obligations: %w", err)
	}
	return int(commandTag.RowsAffected()), nil
}

func blockQueuedAssignedObligations(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
	count int,
) (int, error) {
	commandTag, err := transaction.Exec(ctx, `WITH blocked AS (
		SELECT demand.demand_id
		FROM scaler_demand demand
		JOIN scaler_assigned_job_observations assigned
		  ON assigned.assigned_observation_id = demand.assigned_observation_id
		WHERE assigned.app_id = $1 AND assigned.repository_url = $2
		  AND assigned.scale_set = $3 AND demand.source = 'assigned_count'
		  AND demand.blocked_reason IS NULL
		  AND NOT EXISTS (SELECT 1 FROM scaler_assignments assignment
		      WHERE assignment.demand_id = demand.demand_id AND assignment.state <> 'complete')
		ORDER BY demand.queue_time DESC, demand.observation_id DESC
		LIMIT $4
	)
	UPDATE scaler_demand demand SET blocked_reason = 'stranded_upstream_assignment'
	FROM blocked WHERE demand.demand_id = blocked.demand_id`, observation.AppID,
		observation.Repository, observation.ScaleSet, count)
	if err != nil {
		return 0, fmt.Errorf("block stranded assigned obligations: %w", err)
	}
	return int(commandTag.RowsAffected()), nil
}

func unblockQueuedAssignedObligations(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
	count int,
) (int, error) {
	commandTag, err := transaction.Exec(ctx, `WITH unblocked AS (
		SELECT demand.demand_id
		FROM scaler_demand demand
		JOIN scaler_assigned_job_observations assigned
		  ON assigned.assigned_observation_id = demand.assigned_observation_id
		WHERE assigned.app_id = $1 AND assigned.repository_url = $2
		  AND assigned.scale_set = $3 AND demand.source = 'assigned_count'
		  AND demand.blocked_reason IS NOT NULL
		  AND NOT EXISTS (SELECT 1 FROM scaler_assignments assignment
		      WHERE assignment.demand_id = demand.demand_id AND assignment.state <> 'complete')
		ORDER BY demand.queue_time, demand.observation_id
		LIMIT $4
	)
	UPDATE scaler_demand demand SET blocked_reason = NULL
	FROM unblocked WHERE demand.demand_id = unblocked.demand_id`, observation.AppID,
		observation.Repository, observation.ScaleSet, count)
	if err != nil {
		return 0, fmt.Errorf("unblock recovered assigned obligations: %w", err)
	}
	return int(commandTag.RowsAffected()), nil
}

func createQueuedAssignedObligations(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
	count int,
) (int, error) {
	commandTag, err := transaction.Exec(ctx, `INSERT INTO scaler_demand
		(app_id, scale_set, repository, request_id, queue_time, source,
		 assigned_observation_id, controller_epoch, attempt, next_attempt_at)
	SELECT assigned.app_id, assigned.scale_set, assigned.repository_url, NULL,
	       assigned.observed_at, 'assigned_count', assigned.assigned_observation_id,
	       assigned.controller_epoch, 0, assigned.observed_at
	FROM scaler_assigned_job_observations assigned
	CROSS JOIN generate_series(1, $5)
	WHERE assigned.app_id = $1 AND assigned.repository_url = $2
	  AND assigned.scale_set = $3 AND assigned.controller_epoch = $4`,
		observation.AppID, observation.Repository, observation.ScaleSet,
		observation.ControllerEpoch, count)
	if err != nil {
		return 0, fmt.Errorf("queue assigned obligations: %w", err)
	}
	return int(commandTag.RowsAffected()), nil
}

func adoptLifecycleBackedAssignedAssignments(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
) error {
	if _, err := transaction.Exec(ctx, `UPDATE scaler_demand demand
		SET controller_epoch = $4
		FROM scaler_assigned_job_observations assigned
		WHERE assigned.assigned_observation_id = demand.assigned_observation_id
		  AND assigned.app_id = $1 AND assigned.repository_url = $2 AND assigned.scale_set = $3
		  AND assigned.controller_epoch = $4 AND demand.controller_epoch <> $4
		  AND EXISTS (SELECT 1 FROM scaler_assignments assignment
		      JOIN scaler_runner_lifecycle lifecycle ON lifecycle.assignment_id = assignment.assignment_id
		      WHERE assignment.demand_id = demand.demand_id
		        AND assignment.state IN ('starting', 'running'))`, observation.AppID,
		observation.Repository, observation.ScaleSet, observation.ControllerEpoch); err != nil {
		return fmt.Errorf("adopt lifecycle-backed assigned assignments: %w", err)
	}
	return nil
}

func fenceObsoleteAssignedAssignments(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
) error {
	if _, err := transaction.Exec(ctx, `UPDATE scaler_assignments assignment SET state = 'complete'
		FROM scaler_demand demand,
		     scaler_assigned_job_observations assigned
		WHERE demand.demand_id = assignment.demand_id
		  AND assigned.assigned_observation_id = demand.assigned_observation_id
		  AND assigned.app_id = $1 AND assigned.repository_url = $2 AND assigned.scale_set = $3
		  AND assigned.controller_epoch = $4 AND demand.controller_epoch <> $4
		  AND (assignment.state = 'ready' OR (
		      assignment.state = 'starting' AND NOT EXISTS (
		          SELECT 1 FROM scaler_runner_lifecycle lifecycle
		          WHERE lifecycle.assignment_id = assignment.assignment_id
		      )
		  ))`, observation.AppID, observation.Repository, observation.ScaleSet,
		observation.ControllerEpoch); err != nil {
		return fmt.Errorf("fence obsolete assigned assignments: %w", err)
	}
	return nil
}

func validateAssignedJobObservation(observation AssignedJobObservation) error {
	if observation.AppID <= 0 || strings.TrimSpace(observation.Repository) == "" ||
		strings.TrimSpace(observation.ScaleSet) == "" || observation.ControllerEpoch <= 0 ||
		observation.AssignedJobs < 0 || observation.RunningJobs < 0 ||
		observation.RegisteredRunners < 0 || observation.BusyRunners < 0 || observation.ObservedAt.IsZero() {
		return ErrInvalidDemand
	}
	return nil
}

func upsertAssignedJobObservation(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
) error {
	commandTag, err := transaction.Exec(ctx, `INSERT INTO scaler_assigned_job_observations
		(app_id, repository_url, scale_set, assigned_jobs, running_jobs,
		 registered_runners, busy_runners, controller_epoch, observed_at)
		SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9
		WHERE EXISTS (SELECT 1 FROM scaler_repository_controls
			WHERE app_id = $1 AND controller_epoch = $8)
		ON CONFLICT (app_id, repository_url, scale_set) DO UPDATE SET
			assigned_jobs = EXCLUDED.assigned_jobs,
			running_jobs = EXCLUDED.running_jobs,
			registered_runners = EXCLUDED.registered_runners,
			busy_runners = EXCLUDED.busy_runners,
			controller_epoch = EXCLUDED.controller_epoch,
			observed_at = EXCLUDED.observed_at
		WHERE scaler_assigned_job_observations.controller_epoch <= EXCLUDED.controller_epoch`,
		observation.AppID, observation.Repository, observation.ScaleSet, observation.AssignedJobs,
		observation.RunningJobs, observation.RegisteredRunners, observation.BusyRunners,
		observation.ControllerEpoch, observation.ObservedAt)
	if err != nil {
		return fmt.Errorf("publish assigned-job observation: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrNotLeader
	}
	return nil
}

func reconcileExcessAssignedRunners(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
) error {
	classification := scaler.ClassifyAssignedCapacity(scaler.AssignedCapacityStatistics{
		AssignedJobs: observation.AssignedJobs, RunningJobs: observation.RunningJobs,
		RegisteredRunners: observation.RegisteredRunners, BusyRunners: observation.BusyRunners,
	})
	reapReason := "assigned_jobs_decreased"
	if classification.StrandedAssigned > 0 {
		reapReason = "stranded_upstream_assignment"
	}
	if _, err := transaction.Exec(ctx, cancelExcessReadyAssignedJobsSQL, observation.AppID, observation.Repository,
		observation.ScaleSet, observation.ControllerEpoch, classification.RunnableAssigned); err != nil {
		return fmt.Errorf("cancel excess ready assigned jobs: %w", err)
	}
	if _, err := transaction.Exec(ctx, requestExcessAssignedRunnerReapsSQL, classification.RunnableAssigned, observation.ScaleSet,
		observation.Repository, observation.ObservedAt, reapReason); err != nil {
		return fmt.Errorf("request excess assigned runner reaps: %w", err)
	}
	if _, err := transaction.Exec(ctx, completeExcessAssignedAssignmentsSQL,
		observation.ScaleSet, observation.Repository); err != nil {
		return fmt.Errorf("complete excess assigned assignments: %w", err)
	}
	return nil
}

func loadAssignedJobCapacity(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
	capacity *scaler.AssignedJobCapacity,
) error {
	err := transaction.QueryRow(ctx, contributorEligibilitySQL+`, repository_assignments AS (
		SELECT COUNT(*) AS runners FROM scaler_assignments assignment
		JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
		WHERE demand.scale_set = $2
		  AND demand.repository = $3
		  AND assignment.state <> 'complete'
	), repository_unassigned_lifecycle AS (
		SELECT COUNT(*) AS runners FROM scaler_runner_lifecycle lifecycle
		WHERE lifecycle.scale_set = $2 AND lifecycle.repository = $3
		  AND lifecycle.phase <> 'reap_pending'
		  AND (lifecycle.assignment_id IS NULL OR NOT EXISTS (
		      SELECT 1 FROM scaler_assignments assignment
		      WHERE assignment.assignment_id = lifecycle.assignment_id AND assignment.state <> 'complete'
		  ))
	), effective_capacity AS (
		SELECT COALESCE(SUM(contribution.max_runners), 0) AS maximum
		FROM eligible_contributions contribution
		WHERE contribution.scale_set = $2
	), occupied AS (
		SELECT COUNT(*) AS runners FROM scaler_assignments assignment
		JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
		WHERE demand.scale_set = $2 AND assignment.state <> 'complete'
	), pending_reaps AS (
		SELECT COUNT(*) AS runners FROM scaler_runner_lifecycle
		WHERE scale_set = $2 AND phase = 'reap_pending'
	)
	SELECT repository_assignments.runners + repository_unassigned_lifecycle.runners,
	       GREATEST(effective_capacity.maximum - occupied.runners - pending_reaps.runners, 0)
	FROM repository_assignments, repository_unassigned_lifecycle, effective_capacity, occupied, pending_reaps`,
		observation.ObservedAt.Add(-NodeLifetime), observation.ScaleSet, observation.Repository).Scan(
		&capacity.ActiveRunners, &capacity.AvailableCapacity)
	if err != nil {
		return fmt.Errorf("load assigned-job capacity: %w", err)
	}
	return nil
}
