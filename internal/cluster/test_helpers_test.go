package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeSessionOpener struct {
	connection *fakeDatabaseConnection
	session    *databaseSession
	err        error
}

func (opener fakeSessionOpener) openSession(ctx context.Context) (*databaseSession, error) {
	if opener.session != nil {
		return opener.session, opener.err
	}
	if opener.connection == nil {
		return nil, opener.err
	}
	return opener.connection.session(), opener.err
}

type fakeDatabaseExecution struct {
	statement string
	arguments []any
}

type fakeDatabaseConnection struct {
	execErrors       []error
	executions       []fakeDatabaseExecution
	queries          []fakeDatabaseExecution
	rowResults       []fakeRow
	closeErr         error
	closeContextErr  error
	closeDeadline    time.Time
	closeHasDeadline bool
	pingErr          error
	closed           bool
	executor         *migrationExecutor
	executorErr      error
}

func (connection *fakeDatabaseConnection) Close(ctx context.Context) error {
	connection.closed = true
	connection.closeContextErr = ctx.Err()
	connection.closeDeadline, connection.closeHasDeadline = ctx.Deadline()
	return connection.closeErr
}

func (connection *fakeDatabaseConnection) Exec(
	_ context.Context,
	statement string,
	arguments ...any,
) (pgconn.CommandTag, error) {
	connection.executions = append(connection.executions, fakeDatabaseExecution{
		statement: statement, arguments: append([]any(nil), arguments...),
	})
	if len(connection.execErrors) > 0 {
		err := connection.execErrors[0]
		connection.execErrors = connection.execErrors[1:]
		if err != nil {
			return pgconn.CommandTag{}, err
		}
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (connection *fakeDatabaseConnection) IsClosed() bool             { return connection.closed }
func (connection *fakeDatabaseConnection) Ping(context.Context) error { return connection.pingErr }

func (connection *fakeDatabaseConnection) QueryRow(
	_ context.Context,
	statement string,
	arguments ...any,
) databaseRow {
	connection.queries = append(connection.queries, fakeDatabaseExecution{
		statement: statement, arguments: append([]any(nil), arguments...),
	})
	return databaseRow{scanner: nextScriptedRow(&connection.rowResults)}
}

func (connection *fakeDatabaseConnection) newMigrationExecutor(
	context.Context,
	*Catalog,
) (*migrationExecutor, error) {
	return connection.executor, connection.executorErr
}

func (connection *fakeDatabaseConnection) session() *databaseSession {
	return &databaseSession{
		close: connection.Close, exec: connection.Exec, isClosed: connection.IsClosed,
		ping: connection.Ping, queryRow: connection.QueryRow, newExecutor: connection.newMigrationExecutor,
	}
}

type fakeRow struct {
	err  error
	scan func(...any)
}

func (row fakeRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if row.scan != nil {
		row.scan(destinations...)
	}
	return nil
}

func nextScriptedRow(rows *[]fakeRow) fakeRow {
	if len(*rows) == 0 {
		return fakeRow{err: pgx.ErrNoRows}
	}
	row := (*rows)[0]
	*rows = (*rows)[1:]
	return row
}

func destination[valueType any](t *testing.T, value any) *valueType {
	t.Helper()
	destination, ok := value.(*valueType)
	if !ok {
		t.Fatalf("unexpected scan destination type %T", value)
	}
	return destination
}
