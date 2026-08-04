package cluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/meza/RunnerFoundry/internal/scaler"
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
	if err := completion.Validate(); err != nil {
		return fmt.Errorf("validate clustered job completion identity: %w", err)
	}
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
	if err = completeJobInTransaction(ctx, transaction, completion); err != nil {
		return err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit clustered job completion: %w", err)
	}
	return nil
}

func completeJobInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	completion JobCompletion,
) error {
	predicate, err := newClusterRunnerIdentityPredicate(completion.RunnerIdentity, 3)
	if err != nil {
		return err
	}
	arguments := append([]any{completion.ScaleSet, completion.Repository}, predicate.arguments...)
	commandTag, err := transaction.Exec(ctx, fmt.Sprintf(`UPDATE scaler_runner_lifecycle
		SET phase = 'reap_pending', reap_reason = 'job_completed', updated_at = clock_timestamp()
		WHERE scale_set = $1 AND repository = $2 AND %s`, predicate.condition), arguments...)
	if err != nil {
		return fmt.Errorf("request clustered runner reap: %w", err)
	}
	scope := clusterRunnerIdentityScope{
		table: "scaler_runner_lifecycle", condition: "scale_set = $1 AND repository = $2",
		arguments: []any{completion.ScaleSet, completion.Repository},
	}
	if err = requireClusterRunnerIdentityMatch(
		ctx, transaction, scope, completion.RunnerIdentity, commandTag.RowsAffected(),
	); err != nil {
		return err
	}
	if err = applyClusterJobCompletionAssociations(ctx, transaction, completion, predicate, arguments); err != nil {
		return err
	}
	if completion.RequestID <= 0 {
		return nil
	}
	return completeClusterRequestDemand(ctx, transaction, completion)
}

func applyClusterJobCompletionAssociations(
	ctx context.Context,
	transaction *databaseTransaction,
	completion JobCompletion,
	predicate clusterRunnerIdentityPredicate,
	arguments []any,
) error {
	operations := clusterJobCompletionAssociationOperations(predicate)
	for _, operation := range operations {
		if err := applyClusterJobCompletionAssociation(
			ctx, transaction, completion.RunnerIdentity, operation.statement, operation.failure, arguments,
		); err != nil {
			return err
		}
	}
	return nil
}

type clusterJobCompletionAssociationOperation struct {
	statement string
	failure   string
}

func clusterJobCompletionAssociationOperations(
	predicate clusterRunnerIdentityPredicate,
) []clusterJobCompletionAssociationOperation {
	return []clusterJobCompletionAssociationOperation{
		clusterLibvirtReapOperation(predicate),
		clusterAssignmentCompletionOperation(predicate),
		clusterDemandCompletionOperation(predicate),
	}
}

func clusterLibvirtReapOperation(predicate clusterRunnerIdentityPredicate) clusterJobCompletionAssociationOperation {
	return clusterJobCompletionAssociationOperation{
		statement: fmt.Sprintf(`UPDATE scaler_libvirt_runner_lifecycle SET reap_requested = true
			WHERE scale_set = $1 AND repository = $2 AND %s`, predicate.condition),
		failure: "request clustered libvirt runner reap",
	}
}

func clusterAssignmentCompletionOperation(
	predicate clusterRunnerIdentityPredicate,
) clusterJobCompletionAssociationOperation {
	return clusterJobCompletionAssociationOperation{
		statement: fmt.Sprintf(`UPDATE scaler_assignments assignment SET state = 'complete'
			WHERE assignment.assignment_id IN (
				SELECT assignment_id FROM scaler_runner_lifecycle
				WHERE scale_set = $1 AND repository = $2 AND assignment_id IS NOT NULL AND %s
			) AND assignment.assigned_observation_id IS NOT NULL AND assignment.state <> 'complete'`, predicate.condition),
		failure: "complete observed assignment",
	}
}

func clusterDemandCompletionOperation(predicate clusterRunnerIdentityPredicate) clusterJobCompletionAssociationOperation {
	return clusterJobCompletionAssociationOperation{
		statement: fmt.Sprintf(`DELETE FROM scaler_demand demand
			USING scaler_assignments assignment, scaler_runner_lifecycle lifecycle
			WHERE assignment.demand_id = demand.demand_id
			  AND lifecycle.assignment_id = assignment.assignment_id
			  AND demand.source = 'assigned_count'
			  AND lifecycle.scale_set = $1 AND lifecycle.repository = $2
			  AND %s`, qualifiedClusterRunnerPredicate(predicate, "lifecycle.")),
		failure: "complete assigned capacity obligation",
	}
}

func applyClusterJobCompletionAssociation(
	ctx context.Context,
	transaction *databaseTransaction,
	identity scaler.RunnerIdentity,
	statement, failure string,
	arguments []any,
) error {
	commandTag, err := transaction.Exec(ctx, statement, arguments...)
	if err != nil {
		return fmt.Errorf("%s: %w", failure, err)
	}
	return requireAtMostOneClusterRunnerIdentityMatch(identity, commandTag.RowsAffected())
}

func qualifiedClusterRunnerPredicate(predicate clusterRunnerIdentityPredicate, qualifier string) string {
	return strings.ReplaceAll(strings.ReplaceAll(predicate.condition, "request_id", qualifier+"request_id"),
		"runner_name", qualifier+"runner_name")
}

func completeClusterRequestDemand(
	ctx context.Context,
	transaction *databaseTransaction,
	completion JobCompletion,
) error {
	commandTag, err := transaction.Exec(ctx, `UPDATE scaler_assignments assignment SET state = 'complete'
		FROM scaler_demand demand
		WHERE demand.demand_id = assignment.demand_id
		  AND demand.scale_set = $1 AND demand.repository = $2 AND demand.request_id = $3
		  AND assignment.state <> 'complete'`, completion.ScaleSet, completion.Repository, completion.RequestID)
	if err != nil {
		return fmt.Errorf("complete demand assignments: %w", err)
	}
	if err = requireAtMostOneClusterRunnerIdentityMatch(completion.RunnerIdentity, commandTag.RowsAffected()); err != nil {
		return err
	}
	commandTag, err = transaction.Exec(ctx, `DELETE FROM scaler_demand
		WHERE scale_set = $1 AND repository = $2 AND request_id = $3`,
		completion.ScaleSet, completion.Repository, completion.RequestID)
	if err != nil {
		return fmt.Errorf("remove completed scale-set demand: %w", err)
	}
	return requireAtMostOneClusterRunnerIdentityMatch(completion.RunnerIdentity, commandTag.RowsAffected())
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

func (store *Store) ConfirmLibvirtReaped(ctx context.Context, runner LibvirtRunnerLifecycle) error {
	identity := runner.Identity()
	predicate, err := newClusterRunnerIdentityPredicate(identity, 4)
	if err != nil {
		return err
	}
	return store.runClusterRunnerIdentityTransaction(ctx, "clustered libvirt runner reap confirmation",
		func(transaction *databaseTransaction) error {
			statement := fmt.Sprintf(`DELETE FROM scaler_libvirt_runner_lifecycle
				WHERE node_id = $1 AND scale_set = $2 AND repository = $3 AND reap_requested AND %s`,
				predicate.condition)
			arguments := append([]any{runner.NodeID, runner.ScaleSet, runner.Repository}, predicate.arguments...)
			commandTag, execErr := transaction.Exec(ctx, statement, arguments...)
			if execErr != nil {
				return fmt.Errorf("confirm clustered libvirt runner reaped: %w", execErr)
			}
			scope := clusterRunnerIdentityScope{
				table:     "scaler_libvirt_runner_lifecycle",
				condition: "node_id = $1 AND scale_set = $2 AND repository = $3 AND reap_requested",
				arguments: []any{runner.NodeID, runner.ScaleSet, runner.Repository},
			}
			return requireClusterRunnerIdentityMatch(ctx, transaction, scope, identity, commandTag.RowsAffected())
		})
}
