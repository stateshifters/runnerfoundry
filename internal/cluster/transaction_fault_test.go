package cluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func storeWithConnection(connection *fakeDatabaseConnection) *Store {
	store := NewStore(postgresconfig.Settings{})
	store.connector = func(context.Context) (*databaseConnection, error) { return connection.databaseConnection(), nil }
	store.directConnector = func(context.Context) (leaderConnection, error) { return connection, nil }
	return store
}

func (connection *fakeDatabaseConnection) Query(context.Context, string, ...any) (*databaseRows, error) {
	queryErr := connection.queryErr
	if len(connection.queryErrors) > 0 {
		queryErr = nextScriptedError(&connection.queryErrors)
	}
	if queryErr != nil {
		return nil, queryErr
	}
	if len(connection.rowsResults) > 0 {
		rows := connection.rowsResults[0]
		connection.rowsResults = connection.rowsResults[1:]
		return newDatabaseRows(rows), nil
	}
	if connection.rowsResult == nil {
		connection.rowsResult = &fakeDatabaseRows{}
	}
	return newDatabaseRows(connection.rowsResult), nil
}

type fakeDatabaseTransaction struct {
	execHook      func(string, ...any) (pgconn.CommandTag, error)
	queryRowHook  func(string, ...any) databaseRow
	commitHook    func() error
	rollbackHook  func() error
	execErrors    []error
	execTags      []pgconn.CommandTag
	executions    []fakeDatabaseExecution
	queries       []fakeDatabaseExecution
	queryResults  []rowIterator
	queryErrors   []error
	rowResults    []fakeRow
	commitErr     error
	rollbackErr   error
	rollbackCalls int
}

func (transaction *fakeDatabaseTransaction) databaseTransaction() *databaseTransaction {
	return &databaseTransaction{
		commit: transaction.Commit, exec: transaction.Exec, query: transaction.Query,
		queryRow: transaction.QueryRow, rollback: transaction.Rollback,
	}
}

func (transaction *fakeDatabaseTransaction) Query(context.Context, string, ...any) (*databaseRows, error) {
	if err := nextScriptedError(&transaction.queryErrors); err != nil {
		return nil, err
	}
	if len(transaction.queryResults) == 0 {
		return newDatabaseRows(&fakeDatabaseRows{}), nil
	}
	rows := transaction.queryResults[0]
	transaction.queryResults = transaction.queryResults[1:]
	return newDatabaseRows(rows), nil
}

func (transaction *fakeDatabaseTransaction) Commit(context.Context) error {
	if transaction.commitHook != nil {
		return transaction.commitHook()
	}
	return transaction.commitErr
}

func (transaction *fakeDatabaseTransaction) Exec(
	_ context.Context,
	statement string,
	arguments ...any,
) (pgconn.CommandTag, error) {
	transaction.executions = append(transaction.executions, fakeDatabaseExecution{
		statement: statement, arguments: append([]any(nil), arguments...),
	})
	if transaction.execHook != nil {
		return transaction.execHook(statement, arguments...)
	}
	if err := nextScriptedError(&transaction.execErrors); err != nil {
		return pgconn.CommandTag{}, err
	}
	if len(transaction.execTags) > 0 {
		tag := transaction.execTags[0]
		transaction.execTags = transaction.execTags[1:]
		return tag, nil
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

type fakeDatabaseExecution struct {
	statement string
	arguments []any
}

func (transaction *fakeDatabaseTransaction) QueryRow(_ context.Context, statement string, arguments ...any) databaseRow {
	if transaction.queryRowHook != nil {
		return transaction.queryRowHook(statement, arguments...)
	}
	if strings.Contains(statement, "cluster_bulk_lifecycle_consistency") {
		return databaseRow{scanner: fakeRow{scan: func(destinations ...any) {
			setFakeDestination(destinations[0], false)
		}}}
	}
	if strings.Contains(statement, "cluster_terminal_lifecycle_authority") {
		return databaseRow{scanner: terminalAuthorityRow(fakeTerminalRunner(arguments), 0, 0)}
	}
	transaction.queries = append(transaction.queries, fakeDatabaseExecution{
		statement: statement, arguments: append([]any(nil), arguments...),
	})
	return databaseRow{scanner: nextScriptedRow(&transaction.rowResults)}
}

func fakeTerminalRunner(arguments []any) scaler.RunnerLifecycle {
	runner := scaler.RunnerLifecycle{Provider: scaler.RunnerProviderLibvirt}
	if len(arguments) >= 3 {
		runner.NodeID = fmt.Sprint(arguments[0])
		runner.ScaleSet = fmt.Sprint(arguments[1])
		runner.Repository = fmt.Sprint(arguments[2])
	}
	for _, argument := range arguments[3:] {
		switch value := argument.(type) {
		case int64:
			runner.RequestID = value
		case string:
			runner.RunnerName = value
		}
	}
	return runner
}

func terminalAuthorityRow(runner scaler.RunnerLifecycle, nameConflicts, requestOwners int64) fakeRow {
	return fakeRow{scan: func(destinations ...any) {
		setFakeDestination(destinations[0], runner.Provider)
		setFakeDestination(destinations[1], runner.RunnerName)
		setFakeDestination(destinations[2], runner.RequestID)
		setFakeDestination(destinations[3], nameConflicts)
		setFakeDestination(destinations[4], requestOwners)
	}}
}

func (transaction *fakeDatabaseTransaction) Rollback(context.Context) error {
	transaction.rollbackCalls++
	if transaction.rollbackHook != nil {
		return transaction.rollbackHook()
	}
	return transaction.rollbackErr
}
