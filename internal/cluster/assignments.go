package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/githubapp"
)

const claimReadyAssignmentSQL = `
	UPDATE scaler_assignments assignment
	SET state = 'starting'
	FROM scaler_assignments source
	LEFT JOIN scaler_demand demand ON demand.demand_id = source.demand_id
	LEFT JOIN scaler_assigned_job_observations observation
	  ON observation.assigned_observation_id = source.assigned_observation_id
	WHERE assignment.assignment_id = (
	    SELECT candidate.assignment_id
	    FROM scaler_assignments candidate
	    WHERE candidate.node_id = $1 AND candidate.state = 'ready'
	      AND (candidate.assigned_observation_id IS NULL OR EXISTS (
	          SELECT 1 FROM scaler_assigned_job_observations current_observation
	          JOIN scaler_repository_controls control ON control.app_id = current_observation.app_id
	          WHERE current_observation.assigned_observation_id = candidate.assigned_observation_id
	            AND current_observation.controller_epoch = candidate.controller_epoch
	            AND control.controller_epoch = candidate.controller_epoch
	      ))
	    ORDER BY candidate.assignment_id
	    LIMIT 1
	    FOR UPDATE SKIP LOCKED
	)
	AND source.assignment_id = assignment.assignment_id
	RETURNING assignment.assignment_id, assignment.node_id,
	          COALESCE(demand.scale_set, observation.scale_set),
	          COALESCE(demand.repository, observation.repository_url),
	          COALESCE(demand.request_id, 0), COALESCE(demand.queue_time, observation.observed_at),
	          assignment.installation_id, assignment.github_scale_set_id,
	          assignment.controller_epoch, assignment.assigned_observation_id IS NOT NULL, assignment.state`

const loadAssignmentsSQLPrefix = `
	SELECT assignment.assignment_id, assignment.node_id,
	       COALESCE(demand.scale_set, observation.scale_set),
	       COALESCE(demand.repository, observation.repository_url),
	       COALESCE(demand.request_id, 0), COALESCE(demand.queue_time, observation.observed_at),
	       COALESCE(assignment.installation_id, 0), COALESCE(assignment.github_scale_set_id, 0),
	       assignment.controller_epoch, assignment.assigned_observation_id IS NOT NULL, assignment.state
	FROM scaler_assignments assignment
	LEFT JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
	LEFT JOIN scaler_assigned_job_observations observation
	  ON observation.assigned_observation_id = assignment.assigned_observation_id
	WHERE (assignment.assigned_observation_id IS NULL OR EXISTS (
	      SELECT 1 FROM scaler_repository_controls control
	      WHERE control.app_id = observation.app_id
	        AND control.controller_epoch = assignment.controller_epoch
	        AND observation.controller_epoch = assignment.controller_epoch
	  ))`

const loadPendingAcquisitionsSQL = loadAssignmentsSQLPrefix +
	" AND assignment.state IN ('reserved', 'acquiring')"

const loadStartingAssignmentsSQL = loadAssignmentsSQLPrefix +
	" AND assignment.state = 'starting'"

// BeginAcquisition records the GitHub identifiers before acquisition begins.
// The assignment must still be reserved; stale or repeated transitions return
// ErrAssignmentConflict.
func (store *Store) BeginAcquisition(
	ctx context.Context,
	assignmentID AssignmentID,
	installationID githubapp.InstallationID,
	githubScaleSetID int,
) error {
	return store.transitionAssignment(ctx, assignmentID, AssignmentReserved, `
		UPDATE scaler_assignments
		SET state = 'acquiring', installation_id = $2, github_scale_set_id = $3
		WHERE assignment_id = $1 AND state = 'reserved'`, installationID, githubScaleSetID)
}

// ConfirmAcquisition records that GitHub accepted the reserved request.
func (store *Store) ConfirmAcquisition(ctx context.Context, assignmentID AssignmentID) error {
	return store.transitionAssignment(ctx, assignmentID, AssignmentAcquiring, `
		UPDATE scaler_assignments SET state = 'ready'
		WHERE assignment_id = $1 AND state = 'acquiring'`)
}

// ClaimForStart atomically claims the oldest ready assignment owned by nodeID.
func (store *Store) ClaimForStart(ctx context.Context, nodeID NodeID) (assignment *Assignment, resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	var claimed Assignment
	err = connection.QueryRow(ctx, claimReadyAssignmentSQL, nodeID).Scan(
		&claimed.ID, &claimed.NodeID, &claimed.ScaleSet, &claimed.Repository,
		&claimed.RequestID, &claimed.QueueTime, &claimed.InstallationID, &claimed.GitHubScaleSetID,
		&claimed.ControllerEpoch, &claimed.IdentityFree, &claimed.State,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoReadyAssignment
	}
	if err != nil {
		return nil, fmt.Errorf("claim local runner assignment: %w", err)
	}
	return &claimed, nil
}

// PendingAcquisitions returns assignments whose GitHub acquisition has not
// been confirmed. Reserved rows have zero-valued GitHub identifiers.
func (store *Store) PendingAcquisitions(ctx context.Context) ([]Assignment, error) {
	return store.loadAssignments(ctx, loadPendingAcquisitionsSQL, "", "load pending acquisitions")
}

// Starting returns assignments owned by nodeID whose provider start must be
// completed or repaired.
func (store *Store) Starting(ctx context.Context, nodeID NodeID) ([]Assignment, error) {
	return store.loadAssignments(ctx, loadStartingAssignmentsSQL, nodeID, "load starting assignments")
}

func (store *Store) loadAssignments(
	ctx context.Context,
	statement string,
	nodeID NodeID,
	operation string,
) (assignments []Assignment, resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	arguments := []any{}
	if nodeID != NodeID("") {
		statement += " AND assignment.node_id = $1"
		arguments = append(arguments, nodeID)
	}
	statement += " ORDER BY assignment.assignment_id"
	rows, err := connection.Query(ctx, statement, arguments...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	defer rows.Close()
	return scanAssignments(rows)
}

func scanAssignments(rows *databaseRows) ([]Assignment, error) {
	assignments := make([]Assignment, 0)
	for rows.Next() {
		var assignment Assignment
		if err := rows.Scan(
			&assignment.ID, &assignment.NodeID, &assignment.ScaleSet, &assignment.Repository,
			&assignment.RequestID, &assignment.QueueTime, &assignment.InstallationID,
			&assignment.GitHubScaleSetID, &assignment.ControllerEpoch, &assignment.IdentityFree, &assignment.State,
		); err != nil {
			return nil, fmt.Errorf("scan assignment recovery state: %w", err)
		}
		assignments = append(assignments, assignment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read assignment recovery state: %w", err)
	}
	return assignments, nil
}

// ConfirmRunning records that the owning node has observed its named runner.
func (store *Store) ConfirmRunning(ctx context.Context, assignmentID AssignmentID, nodeID NodeID) error {
	return store.transitionAssignment(ctx, assignmentID, AssignmentStarting, `
		UPDATE scaler_assignments assignment SET state = 'running'
		WHERE assignment.assignment_id = $1 AND assignment.node_id = $2 AND assignment.state = 'starting'
		  AND (assignment.assigned_observation_id IS NULL OR EXISTS (
		      SELECT 1 FROM scaler_assigned_job_observations observation
		      JOIN scaler_repository_controls control ON control.app_id = observation.app_id
		      WHERE observation.assigned_observation_id = assignment.assigned_observation_id
		        AND observation.controller_epoch = assignment.controller_epoch
		        AND control.controller_epoch = assignment.controller_epoch
		  ))`, nodeID)
}

// CompleteAssignment releases an assignment only when its owner and current
// state match the caller's observation.
func (store *Store) CompleteAssignment(
	ctx context.Context,
	assignmentID AssignmentID,
	nodeID NodeID,
	expectedState AssignmentState,
) error {
	return store.transitionAssignment(ctx, assignmentID, expectedState, `
		UPDATE scaler_assignments SET state = 'complete'
		WHERE assignment_id = $1 AND node_id = $2 AND state = $3`, nodeID, expectedState)
}

func (store *Store) transitionAssignment(
	ctx context.Context,
	assignmentID AssignmentID,
	expectedState AssignmentState,
	statement string,
	arguments ...any,
) (resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	arguments = append([]any{assignmentID}, arguments...)
	commandTag, err := connection.Exec(ctx, statement, arguments...)
	if err != nil {
		return fmt.Errorf("update runner assignment: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return &AssignmentTransitionError{AssignmentID: assignmentID, Expected: expectedState}
	}
	return nil
}

// CompleteDemand transactionally completes active assignments and removes the
// corresponding demand. Repeated completion is a successful no-op.
func (store *Store) CompleteDemand(ctx context.Context, scaleSet string, repository string, requestID int64) (resultErr error) {
	if strings.TrimSpace(scaleSet) == "" || strings.TrimSpace(repository) == "" || requestID <= 0 {
		return ErrInvalidDemand
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin demand completion: %w", err)
	}
	transactionOpen := true
	defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
	if err = completeDemandInTransaction(ctx, transaction, scaleSet, repository, requestID); err != nil {
		return err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit demand completion: %w", err)
	}
	return nil
}

func completeDemandInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet string,
	repository string,
	requestID int64,
) error {
	if _, err := transaction.Exec(ctx, `
		UPDATE scaler_assignments assignment SET state = 'complete'
		FROM scaler_demand demand
		WHERE demand.demand_id = assignment.demand_id
		  AND demand.scale_set = $1 AND demand.repository = $2 AND demand.request_id = $3
		  AND assignment.state <> 'complete'`, scaleSet, repository, requestID); err != nil {
		return fmt.Errorf("complete demand assignments: %w", err)
	}
	if _, err := transaction.Exec(ctx, `
		DELETE FROM scaler_demand
		WHERE scale_set = $1 AND repository = $2 AND request_id = $3`, scaleSet, repository, requestID); err != nil {
		return fmt.Errorf("remove completed scale-set demand: %w", err)
	}
	return nil
}
