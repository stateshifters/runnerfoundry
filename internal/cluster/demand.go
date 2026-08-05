package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Enqueue records demand unless the same scale set, repository, and request ID
// is already queued. Duplicate observations are successful no-ops and preserve
// the first observation's queue time. It returns ErrInvalidDemand for a blank
// scale set or repository, a non-positive request ID, or a zero queue time;
// callers should correct or discard that input rather than retry it unchanged.
func (store *Store) Enqueue(ctx context.Context, demand Demand) (resultErr error) {
	if demand.AppID <= 0 || strings.TrimSpace(demand.ScaleSet) == "" || strings.TrimSpace(demand.Repository) == "" || demand.RequestID <= 0 || demand.QueueTime.IsZero() {
		return ErrInvalidDemand
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	if _, err = connection.Exec(ctx, `
		INSERT INTO scaler_demand
			(app_id, scale_set, repository, request_id, queue_time, source,
			 controller_epoch, attempt, next_attempt_at)
		VALUES ($1, $2, $3, $4, $5, 'job_available', 0, 0, $5)
		ON CONFLICT (app_id, scale_set, repository, request_id)
		WHERE source = 'job_available' DO NOTHING`,
		demand.AppID, demand.ScaleSet, demand.Repository, demand.RequestID, demand.QueueTime,
	); err != nil {
		return fmt.Errorf("enqueue scale-set demand: %w", err)
	}
	return nil
}

// AssignOldest atomically reserves the oldest unassigned demand for scaleSet,
// ordered by queue time and then observation order. It selects only a live,
// healthy, compatible contributor with unused local capacity and, in the same
// transaction, completes active assignments owned by expired nodes.
//
// AssignOldest returns ErrInvalidDemand for a blank scale set. It returns
// ErrNoAssignment when no demand or eligible contributor is available; callers
// should use errors.Is to distinguish either sentinel from store failures.
func (store *Store) AssignOldest(
	ctx context.Context,
	scaleSet string,
	now time.Time,
	liveFor time.Duration,
) (assignment *Assignment, resultErr error) {
	if strings.TrimSpace(scaleSet) == "" {
		return nil, ErrInvalidDemand
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin FIFO assignment: %w", err)
	}
	transactionOpen := true
	defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
	assignment, err = assignOldestInTransaction(ctx, transaction, scaleSet, now, liveFor)
	if err != nil {
		return nil, err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit FIFO assignment: %w", err)
	}
	return assignment, nil
}

func assignOldestInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet string,
	now time.Time,
	liveFor time.Duration,
) (*Assignment, error) {
	if err := releaseUnavailableAssignments(ctx, transaction, now.Add(-liveFor)); err != nil {
		return nil, err
	}
	obligation, err := loadOldestDemand(ctx, transaction, scaleSet, now)
	if err != nil {
		return nil, err
	}
	assignment := obligation.assignment
	if err = selectContributor(ctx, transaction, scaleSet, now.Add(-liveFor), assignment); err != nil {
		return nil, err
	}
	assignment.ScaleSet = scaleSet
	if err = persistAssignment(ctx, transaction, obligation); err != nil {
		return nil, err
	}
	return assignment, nil
}

func persistAssignment(ctx context.Context, transaction *databaseTransaction, obligation oldestDemand) error {
	assignment := obligation.assignment
	insertStatement := `INSERT INTO scaler_assignments (demand_id, node_id, state, source)
		VALUES ($1, $2, 'reserved', 'job_available') RETURNING assignment_id`
	insertArguments := []any{obligation.demandID, assignment.NodeID}
	assignment.State = AssignmentReserved
	if assignment.IdentityFree {
		insertStatement = `INSERT INTO scaler_assignments
			(demand_id, node_id, installation_id, github_scale_set_id, state, source)
			VALUES ($1, $2, $3, $4, 'ready', 'assigned_count') RETURNING assignment_id`
		insertArguments = []any{obligation.demandID, assignment.NodeID,
			assignment.InstallationID, assignment.GitHubScaleSetID}
		assignment.State = AssignmentReady
	}
	if err := transaction.QueryRow(ctx, insertStatement, insertArguments...).Scan(&assignment.ID); err != nil {
		return fmt.Errorf("persist FIFO assignment: %w", err)
	}
	return nil
}

func releaseUnavailableAssignments(ctx context.Context, transaction *databaseTransaction, expiredBefore time.Time) error {
	if _, err := transaction.Exec(ctx, `
		UPDATE scaler_assignments assignment
		SET state = 'complete'
		FROM scaler_nodes node
		WHERE assignment.node_id = node.node_id
		  AND assignment.state <> 'complete'
		  AND node.heartbeat_at < $1`, expiredBefore); err != nil {
		return fmt.Errorf("release assignments from unavailable nodes: %w", err)
	}
	return nil
}

type oldestDemand struct {
	demandID              int64
	assignedObservationID int64
	assignment            *Assignment
}

const loadOldestDemandSQL = `
	SELECT demand.demand_id, demand.repository, COALESCE(demand.request_id, 0),
	       demand.queue_time, demand.source, COALESCE(demand.assigned_observation_id, 0),
	       COALESCE(repository.installation_id, 0),
	       COALESCE(registration.github_scale_set_id, 0), demand.controller_epoch
	FROM scaler_demand demand
	LEFT JOIN scaler_assigned_job_observations assigned
	  ON assigned.assigned_observation_id = demand.assigned_observation_id
	LEFT JOIN scaler_repository_controls control ON control.app_id = assigned.app_id
	LEFT JOIN scaler_repositories repository
	  ON repository.app_id = assigned.app_id AND repository.repository_url = demand.repository
	LEFT JOIN scaler_registrations registration
	  ON registration.app_id = assigned.app_id AND registration.repository_url = demand.repository
	 AND registration.scale_set_name = demand.scale_set
	WHERE demand.scale_set = $1 AND demand.blocked_reason IS NULL AND demand.next_attempt_at <= $2
	  AND (demand.source = 'job_available' OR (
	      assigned.controller_epoch = demand.controller_epoch
	      AND control.controller_epoch = demand.controller_epoch
	  ))
	  AND NOT EXISTS (SELECT 1 FROM scaler_assignments WHERE scaler_assignments.demand_id = demand.demand_id AND state <> 'complete')
	  AND NOT EXISTS (
	      SELECT 1 FROM scaler_runner_lifecycle lifecycle
	      JOIN scaler_assignments lifecycle_assignment
	        ON lifecycle_assignment.assignment_id = lifecycle.assignment_id
	      WHERE lifecycle_assignment.demand_id = demand.demand_id
	  )
	ORDER BY demand.queue_time, demand.observation_id
	LIMIT 1
	FOR UPDATE OF demand`

func loadOldestDemand(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet string,
	now time.Time,
) (oldestDemand, error) {
	var obligation oldestDemand
	var source string
	obligation.assignment = &Assignment{}
	err := transaction.QueryRow(ctx, loadOldestDemandSQL, scaleSet, now).Scan(
		&obligation.demandID, &obligation.assignment.Repository, &obligation.assignment.RequestID,
		&obligation.assignment.QueueTime, &source, &obligation.assignedObservationID,
		&obligation.assignment.InstallationID, &obligation.assignment.GitHubScaleSetID,
		&obligation.assignment.ControllerEpoch,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return oldestDemand{}, ErrNoAssignment
	}
	if err != nil {
		return oldestDemand{}, fmt.Errorf("load oldest scale-set demand: %w", err)
	}
	obligation.assignment.IdentityFree = source == "assigned_count"
	return obligation, nil
}

func selectContributor(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet string,
	liveAfter time.Time,
	assignment *Assignment,
) error {
	err := transaction.QueryRow(ctx, contributorEligibilitySQL+`
		SELECT contribution.node_id
		FROM scaler_contributions contribution
		JOIN eligible_contributions eligible_contribution
		  ON eligible_contribution.node_id = contribution.node_id
		 AND eligible_contribution.scale_set = contribution.scale_set
		WHERE contribution.scale_set = $2
		  AND contribution.max_runners > (
		      SELECT COUNT(*)
		      FROM scaler_assignments assignment
		      LEFT JOIN scaler_demand assignment_demand ON assignment_demand.demand_id = assignment.demand_id
		      WHERE assignment.node_id = contribution.node_id
		        AND assignment_demand.scale_set = contribution.scale_set
		        AND assignment.state <> 'complete'
		  ) + (
		      SELECT COUNT(*)
		      FROM scaler_runner_lifecycle lifecycle
		      WHERE lifecycle.node_id = contribution.node_id
		        AND lifecycle.scale_set = contribution.scale_set
		        AND lifecycle.phase = 'reap_pending'
		  )
		ORDER BY contribution.node_id
		LIMIT 1
		FOR UPDATE OF contribution`, liveAfter, scaleSet).Scan(&assignment.NodeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoAssignment
	}
	if err != nil {
		return fmt.Errorf("select scale-set contributor: %w", err)
	}
	return nil
}
