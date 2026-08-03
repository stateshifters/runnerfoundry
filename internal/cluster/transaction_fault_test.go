package cluster

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
)

type fakeDatabaseTransaction struct {
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
	transaction.queries = append(transaction.queries, fakeDatabaseExecution{
		statement: statement, arguments: append([]any(nil), arguments...),
	})
	return databaseRow{scanner: nextScriptedRow(&transaction.rowResults)}
}

func (transaction *fakeDatabaseTransaction) Rollback(context.Context) error {
	transaction.rollbackCalls++
	return transaction.rollbackErr
}
