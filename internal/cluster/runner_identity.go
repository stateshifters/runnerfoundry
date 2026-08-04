package cluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

type clusterRunnerIdentityPredicate struct {
	condition string
	arguments []any
}

type clusterRunnerIdentityScope struct {
	table     string
	qualifier string
	condition string
	arguments []any
}

func newClusterRunnerIdentityPredicate(
	identity scaler.RunnerIdentity,
	firstParameter int,
) (clusterRunnerIdentityPredicate, error) {
	if err := identity.Validate(); err != nil {
		return clusterRunnerIdentityPredicate{}, fmt.Errorf("validate clustered runner identity: %w", err)
	}
	return clusterRunnerIdentityPredicateFor(identity, firstParameter), nil
}

func clusterRunnerIdentityPredicateFor(
	identity scaler.RunnerIdentity,
	firstParameter int,
) clusterRunnerIdentityPredicate {
	requestColumn := "request_id"
	runnerColumn := "runner_name"
	switch {
	case identity.RequestID > 0 && strings.TrimSpace(identity.RunnerName) != "":
		return clusterRunnerIdentityPredicate{
			condition: fmt.Sprintf("%s = $%d AND %s = $%d", requestColumn, firstParameter,
				runnerColumn, firstParameter+1),
			arguments: []any{identity.RequestID, identity.RunnerName},
		}
	case identity.RequestID > 0:
		return clusterRunnerIdentityPredicate{
			condition: fmt.Sprintf("%s = $%d", requestColumn, firstParameter),
			arguments: []any{identity.RequestID},
		}
	default:
		return clusterRunnerIdentityPredicate{
			condition: fmt.Sprintf("%s = $%d", runnerColumn, firstParameter),
			arguments: []any{identity.RunnerName},
		}
	}
}

func requireClusterRunnerIdentityMatch(
	ctx context.Context,
	transaction *databaseTransaction,
	scope clusterRunnerIdentityScope,
	identity scaler.RunnerIdentity,
	matches int64,
) error {
	if matches > 1 {
		return &scaler.RunnerIdentityInvariantError{Identity: identity, Matches: matches}
	}
	if matches == 1 {
		return nil
	}
	if identity.RequestID <= 0 || strings.TrimSpace(identity.RunnerName) == "" {
		return &scaler.RunnerIdentityNotFoundError{Identity: identity}
	}
	requestMatches, err := countClusterRunnerIdentityComponent(
		ctx, transaction, scope, "request_id", identity.RequestID,
	)
	if err != nil {
		return err
	}
	runnerMatches, err := countClusterRunnerIdentityComponent(
		ctx, transaction, scope, "runner_name", identity.RunnerName,
	)
	if err != nil {
		return err
	}
	if requestMatches > 0 || runnerMatches > 0 {
		return &scaler.RunnerIdentityMismatchError{Identity: identity}
	}
	return &scaler.RunnerIdentityNotFoundError{Identity: identity}
}

func countClusterRunnerIdentityComponent(
	ctx context.Context,
	transaction *databaseTransaction,
	scope clusterRunnerIdentityScope,
	column string,
	value any,
) (int64, error) {
	parameter := len(scope.arguments) + 1
	statement := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s AND %s%s = $%d",
		scope.table, scope.condition, scope.qualifier, column, parameter)
	arguments := append(append([]any(nil), scope.arguments...), value)
	var matches int64
	if err := transaction.QueryRow(ctx, statement, arguments...).Scan(&matches); err != nil {
		return 0, fmt.Errorf("inspect clustered runner identity component: %w", err)
	}
	return matches, nil
}

func requireAtMostOneClusterRunnerIdentityMatch(identity scaler.RunnerIdentity, matches int64) error {
	if matches > 1 {
		return &scaler.RunnerIdentityInvariantError{Identity: identity, Matches: matches}
	}
	return nil
}

func (store *Store) runClusterRunnerIdentityTransaction(
	ctx context.Context,
	operation string,
	apply func(*databaseTransaction) error,
) (resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s: %w", operation, err)
	}
	transactionOpen := true
	defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
	if err = apply(transaction); err != nil {
		return err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", operation, err)
	}
	return nil
}
