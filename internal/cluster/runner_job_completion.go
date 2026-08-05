package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

func (store *Store) CompleteRunnerLifecycleJob(
	ctx context.Context,
	completion scaler.JobCompletion,
	_ time.Time,
) (resultErr error) {
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
	if err = completeRunnerLifecycleJobInTransaction(ctx, transaction, completion); err != nil {
		return err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit clustered job completion: %w", err)
	}
	return nil
}

func completeRunnerLifecycleJobInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	completion scaler.JobCompletion,
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
	completion scaler.JobCompletion,
	predicate clusterRunnerIdentityPredicate,
	arguments []any,
) error {
	for _, operation := range clusterJobCompletionAssociationOperations(predicate) {
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
		clusterAssignmentCompletionOperation(predicate),
		clusterDemandCompletionOperation(predicate),
	}
}

func clusterAssignmentCompletionOperation(
	predicate clusterRunnerIdentityPredicate,
) clusterJobCompletionAssociationOperation {
	return clusterJobCompletionAssociationOperation{
		statement: fmt.Sprintf(`UPDATE scaler_assignments assignment SET state = 'complete'
			FROM scaler_demand demand
			WHERE demand.demand_id = assignment.demand_id
			  AND assignment.assignment_id IN (
				SELECT assignment_id FROM scaler_runner_lifecycle
				WHERE scale_set = $1 AND repository = $2 AND assignment_id IS NOT NULL AND %s
			) AND demand.source = 'assigned_count' AND assignment.state <> 'complete'`, predicate.condition),
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
	completion scaler.JobCompletion,
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
