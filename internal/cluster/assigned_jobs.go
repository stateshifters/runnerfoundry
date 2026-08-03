package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

const cancelExcessReadyAssignedJobsSQL = `WITH ranked_ready AS (
	SELECT assignment.assignment_id,
	       ROW_NUMBER() OVER (ORDER BY assignment.assignment_id) AS position,
	       GREATEST(observation.assigned_jobs - (SELECT COUNT(*)
	           FROM scaler_assignments active
	           LEFT JOIN scaler_demand active_demand ON active_demand.demand_id = active.demand_id
	           LEFT JOIN scaler_assigned_job_observations active_observation
	             ON active_observation.assigned_observation_id = active.assigned_observation_id
	           WHERE COALESCE(active_demand.scale_set, active_observation.scale_set) = observation.scale_set
	             AND COALESCE(active_demand.repository, active_observation.repository_url) = observation.repository_url
	             AND active.state <> 'complete'
	             AND NOT (active.assigned_observation_id = observation.assigned_observation_id
	                      AND active.state = 'ready')), 0) AS retain
	FROM scaler_assignments assignment
	JOIN scaler_assigned_job_observations observation
	  ON observation.assigned_observation_id = assignment.assigned_observation_id
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
SET phase = 'reap_pending', reap_reason = 'assigned_jobs_decreased', updated_at = $4
FROM excess
WHERE lifecycle.node_id = excess.node_id AND lifecycle.scale_set = excess.scale_set
  AND lifecycle.repository = excess.repository AND lifecycle.runner_name = excess.runner_name
  AND lifecycle.phase = 'waiting_for_job'`

const mirrorExcessAssignedRunnerReapsSQL = `UPDATE scaler_libvirt_runner_lifecycle legacy
	SET reap_requested = true
	WHERE legacy.scale_set = $1 AND legacy.repository = $2
	  AND EXISTS (SELECT 1 FROM scaler_runner_lifecycle lifecycle
		WHERE lifecycle.node_id = legacy.node_id AND lifecycle.scale_set = legacy.scale_set
		  AND lifecycle.repository = legacy.repository AND lifecycle.runner_name = legacy.runner_name
		  AND lifecycle.phase = 'reap_pending' AND lifecycle.reap_reason = 'assigned_jobs_decreased')`

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
) (capacity scaler.AssignedJobCapacity, resultErr error) {
	if err := validateAssignedJobObservation(observation); err != nil {
		return capacity, err
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return capacity, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return capacity, fmt.Errorf("begin assigned-job observation: %w", err)
	}
	transactionOpen := true
	defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
	if err = applyAssignedJobObservation(ctx, transaction, observation, &capacity); err != nil {
		return capacity, err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return capacity, fmt.Errorf("commit assigned-job observation: %w", err)
	}
	return capacity, nil
}

func applyAssignedJobObservation(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
	capacity *scaler.AssignedJobCapacity,
) error {
	operations := []func(context.Context, *databaseTransaction, AssignedJobObservation) error{
		upsertAssignedJobObservation,
		adoptLifecycleBackedAssignedAssignments,
		fenceObsoleteAssignedAssignments,
		reconcileExcessAssignedRunners,
	}
	for _, operation := range operations {
		if err := operation(ctx, transaction, observation); err != nil {
			return err
		}
	}
	return loadAssignedJobCapacity(ctx, transaction, observation, capacity)
}

func adoptLifecycleBackedAssignedAssignments(
	ctx context.Context,
	transaction *databaseTransaction,
	observation AssignedJobObservation,
) error {
	if _, err := transaction.Exec(ctx, `UPDATE scaler_assignments assignment
		SET controller_epoch = $4
		FROM scaler_assigned_job_observations assigned
		WHERE assigned.assigned_observation_id = assignment.assigned_observation_id
		  AND assigned.app_id = $1 AND assigned.repository_url = $2 AND assigned.scale_set = $3
		  AND assigned.controller_epoch = $4 AND assignment.controller_epoch <> $4
		  AND assignment.state IN ('starting', 'running')
		  AND EXISTS (SELECT 1 FROM scaler_runner_lifecycle lifecycle
		      WHERE lifecycle.assignment_id = assignment.assignment_id)`, observation.AppID,
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
		FROM scaler_assigned_job_observations assigned
		WHERE assigned.assigned_observation_id = assignment.assigned_observation_id
		  AND assigned.app_id = $1 AND assigned.repository_url = $2 AND assigned.scale_set = $3
		  AND assigned.controller_epoch = $4 AND assignment.controller_epoch <> $4
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
		observation.AssignedJobs < 0 || observation.ObservedAt.IsZero() {
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
		(app_id, repository_url, scale_set, assigned_jobs, controller_epoch, observed_at)
		SELECT $1, $2, $3, $4, $5, $6
		WHERE EXISTS (SELECT 1 FROM scaler_repository_controls
			WHERE app_id = $1 AND controller_epoch = $5)
		ON CONFLICT (app_id, repository_url, scale_set) DO UPDATE SET
			assigned_jobs = EXCLUDED.assigned_jobs,
			controller_epoch = EXCLUDED.controller_epoch,
			observed_at = EXCLUDED.observed_at
		WHERE scaler_assigned_job_observations.controller_epoch <= EXCLUDED.controller_epoch`,
		observation.AppID, observation.Repository, observation.ScaleSet, observation.AssignedJobs,
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
	if _, err := transaction.Exec(ctx, cancelExcessReadyAssignedJobsSQL, observation.AppID, observation.Repository,
		observation.ScaleSet, observation.ControllerEpoch); err != nil {
		return fmt.Errorf("cancel excess ready assigned jobs: %w", err)
	}
	if _, err := transaction.Exec(ctx, requestExcessAssignedRunnerReapsSQL, observation.AssignedJobs, observation.ScaleSet,
		observation.Repository, observation.ObservedAt); err != nil {
		return fmt.Errorf("request excess assigned runner reaps: %w", err)
	}
	if _, err := transaction.Exec(ctx, mirrorExcessAssignedRunnerReapsSQL,
		observation.ScaleSet, observation.Repository); err != nil {
		return fmt.Errorf("mirror excess assigned runner reaps: %w", err)
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
		LEFT JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
		LEFT JOIN scaler_assigned_job_observations assigned
		  ON assigned.assigned_observation_id = assignment.assigned_observation_id
		WHERE COALESCE(demand.scale_set, assigned.scale_set) = $2
		  AND COALESCE(demand.repository, assigned.repository_url) = $3
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
		LEFT JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
		LEFT JOIN scaler_assigned_job_observations assigned
		  ON assigned.assigned_observation_id = assignment.assigned_observation_id
		WHERE COALESCE(demand.scale_set, assigned.scale_set) = $2 AND assignment.state <> 'complete'
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

// AssignAssignedJob atomically reserves one identity-free assigned job on an
// eligible contributor when the observation still has a deficit and capacity.
func (store *Store) AssignAssignedJob(
	ctx context.Context,
	observation AssignedJobObservation,
	now time.Time,
	liveFor time.Duration,
) (assignment *Assignment, resultErr error) {
	if err := validateAssignedJobObservation(observation); err != nil || now.IsZero() || liveFor <= 0 {
		return nil, ErrInvalidDemand
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	var assigned Assignment
	err = connection.QueryRow(ctx, assignAssignedJobSQL, now.Add(-liveFor), observation.AppID,
		observation.Repository, observation.ScaleSet, observation.ControllerEpoch).Scan(
		&assigned.ID, &assigned.NodeID, &assigned.ScaleSet, &assigned.Repository, &assigned.QueueTime,
		&assigned.InstallationID, &assigned.GitHubScaleSetID, &assigned.ControllerEpoch, &assigned.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoAssignment
	}
	if err != nil {
		return nil, fmt.Errorf("assign observed assigned job: %w", err)
	}
	assigned.IdentityFree = true
	return &assigned, nil
}

const assignAssignedJobSQL = contributorEligibilitySQL + `, observation AS (
	SELECT assigned.assigned_observation_id, assigned.repository_url, assigned.scale_set,
	       assigned.assigned_jobs, assigned.controller_epoch, assigned.observed_at,
	       repository.installation_id, registration.github_scale_set_id
	FROM scaler_assigned_job_observations assigned
	JOIN scaler_repository_controls control ON control.app_id = assigned.app_id
	JOIN scaler_repositories repository
	  ON repository.app_id = assigned.app_id AND repository.repository_url = assigned.repository_url
	JOIN scaler_registrations registration
	  ON registration.app_id = assigned.app_id AND registration.repository_url = assigned.repository_url
	 AND registration.scale_set_name = assigned.scale_set
	WHERE assigned.app_id = $2 AND assigned.repository_url = $3 AND assigned.scale_set = $4
	  AND assigned.controller_epoch = $5 AND control.controller_epoch = $5
	FOR UPDATE OF assigned
), contributor AS (
	SELECT contribution.node_id
	FROM scaler_contributions contribution
	JOIN eligible_contributions eligible
	  ON eligible.node_id = contribution.node_id AND eligible.scale_set = contribution.scale_set
	CROSS JOIN observation
	WHERE contribution.scale_set = observation.scale_set
	  AND contribution.max_runners > (
		SELECT COUNT(*) FROM scaler_assignments assignment
		LEFT JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
		LEFT JOIN scaler_assigned_job_observations assigned
		  ON assigned.assigned_observation_id = assignment.assigned_observation_id
		WHERE assignment.node_id = contribution.node_id
		  AND COALESCE(demand.scale_set, assigned.scale_set) = contribution.scale_set
		  AND assignment.state <> 'complete'
	  ) + (SELECT COUNT(*) FROM scaler_runner_lifecycle lifecycle
		WHERE lifecycle.node_id = contribution.node_id AND lifecycle.scale_set = contribution.scale_set
		  AND lifecycle.phase = 'reap_pending')
	  AND observation.assigned_jobs > (SELECT COUNT(*) FROM scaler_assignments assignment
		LEFT JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
		LEFT JOIN scaler_assigned_job_observations assigned
		  ON assigned.assigned_observation_id = assignment.assigned_observation_id
		WHERE COALESCE(demand.scale_set, assigned.scale_set) = observation.scale_set
		  AND COALESCE(demand.repository, assigned.repository_url) = observation.repository_url
		  AND assignment.state <> 'complete')
	ORDER BY contribution.node_id LIMIT 1 FOR UPDATE OF contribution
), inserted AS (
	INSERT INTO scaler_assignments
		(demand_id, assigned_observation_id, node_id, installation_id, github_scale_set_id, controller_epoch, state)
	SELECT NULL, observation.assigned_observation_id, contributor.node_id, observation.installation_id,
	       observation.github_scale_set_id, observation.controller_epoch, 'ready'
	FROM observation, contributor
	RETURNING assignment_id, node_id, installation_id, github_scale_set_id, controller_epoch, state
)
SELECT inserted.assignment_id, inserted.node_id, observation.scale_set, observation.repository_url,
	observation.observed_at, inserted.installation_id, inserted.github_scale_set_id,
	inserted.controller_epoch, inserted.state
FROM inserted, observation`
