package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// BeginAcquisition records the GitHub identifiers before acquisition begins.
// The assignment must still be reserved; stale or repeated transitions return
// ErrAssignmentConflict.
func (store *Store) BeginAcquisition(
	ctx context.Context,
	assignmentID int64,
	installationID int64,
	githubScaleSetID int,
) error {
	return store.transitionAssignment(ctx, assignmentID, AssignmentReserved, `
		UPDATE scaler_assignments
		SET state = 'acquiring', installation_id = $2, github_scale_set_id = $3
		WHERE assignment_id = $1 AND state = 'reserved'`, installationID, githubScaleSetID)
}

// ConfirmAcquisition records that GitHub accepted the reserved request.
func (store *Store) ConfirmAcquisition(ctx context.Context, assignmentID int64) error {
	return store.transitionAssignment(ctx, assignmentID, AssignmentAcquiring, `
		UPDATE scaler_assignments SET state = 'ready'
		WHERE assignment_id = $1 AND state = 'acquiring'`)
}

// ClaimForStart atomically claims the oldest ready assignment owned by nodeID.
func (store *Store) ClaimForStart(ctx context.Context, nodeID string) (assignment *Assignment, resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	var claimed Assignment
	err = connection.QueryRow(ctx, `
		UPDATE scaler_assignments assignment
		SET state = 'starting'
		FROM scaler_demand demand
		WHERE assignment.assignment_id = (
		    SELECT candidate.assignment_id
		    FROM scaler_assignments candidate
		    WHERE candidate.node_id = $1 AND candidate.state = 'ready'
		    ORDER BY candidate.assignment_id
		    LIMIT 1
		    FOR UPDATE SKIP LOCKED
		)
		AND demand.demand_id = assignment.demand_id
		RETURNING assignment.assignment_id, assignment.node_id, demand.scale_set,
		          demand.repository, demand.request_id, demand.queue_time,
		          assignment.installation_id, assignment.github_scale_set_id, assignment.state`, nodeID).Scan(
		&claimed.ID, &claimed.NodeID, &claimed.ScaleSet, &claimed.Repository,
		&claimed.RequestID, &claimed.QueueTime, &claimed.InstallationID, &claimed.GitHubScaleSetID,
		&claimed.State,
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
	return store.loadAssignments(ctx, "assignment.state IN ('reserved', 'acquiring')", "", "load pending acquisitions")
}

// Starting returns assignments owned by nodeID whose provider start must be
// completed or repaired.
func (store *Store) Starting(ctx context.Context, nodeID string) ([]Assignment, error) {
	return store.loadAssignments(ctx, "assignment.state = 'starting'", nodeID, "load starting assignments")
}

func (store *Store) loadAssignments(
	ctx context.Context,
	statePredicate string,
	nodeID string,
	operation string,
) (assignments []Assignment, resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	statement := `
		SELECT assignment.assignment_id, assignment.node_id, demand.scale_set,
		       demand.repository, demand.request_id, demand.queue_time,
		       COALESCE(assignment.installation_id, 0), COALESCE(assignment.github_scale_set_id, 0),
		       assignment.state
		FROM scaler_assignments assignment
		JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
		WHERE ` + statePredicate
	arguments := []any{}
	if nodeID != "" {
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
			&assignment.GitHubScaleSetID, &assignment.State,
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
func (store *Store) ConfirmRunning(ctx context.Context, assignmentID int64, nodeID string) error {
	return store.transitionAssignment(ctx, assignmentID, AssignmentStarting, `
		UPDATE scaler_assignments SET state = 'running'
		WHERE assignment_id = $1 AND node_id = $2 AND state = 'starting'`, nodeID)
}

// CompleteAssignment releases an assignment only when its owner and current
// state match the caller's observation.
func (store *Store) CompleteAssignment(
	ctx context.Context,
	assignmentID int64,
	nodeID string,
	expectedState AssignmentState,
) error {
	return store.transitionAssignment(ctx, assignmentID, expectedState, `
		UPDATE scaler_assignments SET state = 'complete'
		WHERE assignment_id = $1 AND node_id = $2 AND state = $3`, nodeID, expectedState)
}

func (store *Store) transitionAssignment(
	ctx context.Context,
	assignmentID int64,
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
