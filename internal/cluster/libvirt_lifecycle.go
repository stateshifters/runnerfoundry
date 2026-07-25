package cluster

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (store *Store) RegisterLibvirtRunner(
	ctx context.Context,
	assignmentID AssignmentID,
	runner LibvirtRunnerLifecycle,
) (resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	inserted, err := insertLibvirtRunnerRegistration(ctx, connection, assignmentID, runner)
	if err != nil {
		return err
	}
	if inserted {
		return nil
	}
	matches, err := registeredLibvirtRunnerMatches(ctx, connection, runner)
	if err != nil {
		return err
	}
	if !matches {
		return &AssignmentTransitionError{AssignmentID: assignmentID, Expected: AssignmentStarting}
	}
	return nil
}

func insertLibvirtRunnerRegistration(
	ctx context.Context,
	connection *databaseConnection,
	assignmentID AssignmentID,
	runner LibvirtRunnerLifecycle,
) (bool, error) {
	commandTag, err := connection.Exec(ctx, `
		INSERT INTO scaler_libvirt_runner_lifecycle
		    (node_id, scale_set, repository, request_id, runner_name, reap_requested)
		SELECT assignment.node_id, demand.scale_set, demand.repository, demand.request_id, $3, false
		FROM scaler_assignments assignment
		JOIN scaler_demand demand ON demand.demand_id = assignment.demand_id
		WHERE assignment.assignment_id = $1 AND assignment.node_id = $2 AND assignment.state <> 'complete'
		ON CONFLICT (node_id, scale_set, repository, runner_name) DO NOTHING`,
		assignmentID, runner.NodeID, runner.RunnerName)
	if err != nil {
		return false, fmt.Errorf("register clustered libvirt runner: %w", err)
	}
	return commandTag.RowsAffected() == 1, nil
}

func registeredLibvirtRunnerMatches(
	ctx context.Context,
	connection *databaseConnection,
	runner LibvirtRunnerLifecycle,
) (bool, error) {
	var exists bool
	if err := connection.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM scaler_libvirt_runner_lifecycle
		WHERE node_id = $1 AND scale_set = $2 AND repository = $3 AND request_id = $4 AND runner_name = $5
		  AND NOT reap_requested
	)`, runner.NodeID, runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName).Scan(&exists); err != nil {
		return false, fmt.Errorf("verify clustered libvirt runner registration: %w", err)
	}
	return exists, nil
}

func (store *Store) CompleteJob(ctx context.Context, completion JobCompletion) (resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin clustered job completion: %w", err)
	}
	transactionOpen := true
	defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
	if _, err = transaction.Exec(ctx, `UPDATE scaler_libvirt_runner_lifecycle SET reap_requested = true
		WHERE scale_set = $1 AND repository = $2 AND request_id = $3`,
		completion.ScaleSet, completion.Repository, completion.RequestID); err != nil {
		return fmt.Errorf("request clustered libvirt runner reap: %w", err)
	}
	if err = completeDemandInTransaction(ctx, transaction, completion.ScaleSet, completion.Repository, completion.RequestID); err != nil {
		return err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit clustered job completion: %w", err)
	}
	return nil
}

func (store *Store) PendingLibvirtReaps(
	ctx context.Context,
	nodeID NodeID,
) (runners []LibvirtRunnerLifecycle, resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	rows, err := connection.Query(ctx, `SELECT node_id, scale_set, repository, request_id, runner_name
		FROM scaler_libvirt_runner_lifecycle
		WHERE node_id = $1 AND reap_requested
		ORDER BY scale_set, repository, runner_name`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("load clustered libvirt reaps: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var runner LibvirtRunnerLifecycle
		if err = rows.Scan(&runner.NodeID, &runner.ScaleSet, &runner.Repository, &runner.RequestID, &runner.RunnerName); err != nil {
			return nil, fmt.Errorf("scan clustered libvirt reap: %w", err)
		}
		runners = append(runners, runner)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read clustered libvirt reaps: %w", err)
	}
	return runners, nil
}

func (store *Store) ConfirmLibvirtReaped(ctx context.Context, runner LibvirtRunnerLifecycle) (resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	commandTag, err := connection.Exec(ctx, `DELETE FROM scaler_libvirt_runner_lifecycle
		WHERE node_id = $1 AND scale_set = $2 AND repository = $3 AND runner_name = $4 AND reap_requested`,
		runner.NodeID, runner.ScaleSet, runner.Repository, runner.RunnerName)
	if err != nil {
		return fmt.Errorf("confirm clustered libvirt runner reaped: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return pgx.ErrNoRows
	}
	return nil
}
