package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestConnectionStoreDelegatesOwnedProtocol(t *testing.T) {
	t.Parallel()
	connection := Connection{begin: transactionBeginning(Transaction{execute: successfulExecute,
		commit: successfulCompletion, rollback: successfulCompletion}), queryRow: func(context.Context, string, ...any) Row {
		return rowFromValues([]any{4})
	}}
	store := NewConnectionStore(connection)
	if err := store.PublishMemberState(context.Background(), testMemberIDA, fixedTime(), nil); err != nil {
		t.Fatalf("heartbeat through connection: %v", err)
	}
	if count, err := store.CountLiveMembers(context.Background(), fixedTime()); err != nil || count != 4 {
		t.Fatalf("count through connection = %d, %v", count, err)
	}
}

func TestPGXConnectionAdaptsBeginAndQueryRow(t *testing.T) {
	t.Parallel()
	connection := connectionFromPGXFunctions(
		func(context.Context) (pgx.Tx, error) { return inertPGXTransaction{}, nil },
		func(context.Context, string, ...any) pgx.Row { return fakePGXRow{values: []any{7}} },
	)
	if _, err := connection.begin(context.Background()); err != nil {
		t.Fatalf("begin: %v", err)
	}
	var value int
	if err := connection.queryRow(context.Background(), "select").Scan(&value); err != nil || value != 7 {
		t.Fatalf("query row = %d, %v", value, err)
	}
	_ = NewPGXConnection(nil)
}

func TestPGXConnectionReportsBeginFailure(t *testing.T) {
	t.Parallel()
	connection := connectionFromPGXFunctions(func(context.Context) (pgx.Tx, error) {
		return nil, errors.New("protocol")
	}, func(context.Context, string, ...any) pgx.Row { return fakePGXRow{} })
	if _, err := connection.begin(context.Background()); err == nil {
		t.Fatal("expected begin error")
	}
}

func TestPGXTransactionDelegatesOwnedProtocol(t *testing.T) {
	t.Parallel()
	rowsProtocol := &fakePGXRows{}
	transaction := newPGXTransaction(
		func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
		func(context.Context, string, ...any) (pgx.Rows, error) { return rowsProtocol, nil },
		func(context.Context, string, ...any) pgx.Row {
			return fakePGXRow{values: fullPostgreSQLJobValues(fixedTime())}
		},
		func(context.Context) error { return nil }, func(context.Context) error { return nil },
	)
	verifyPGXTransactionSuccess(t, transaction)
	_ = adaptPGXTransaction(inertPGXTransaction{})
}

func verifyPGXTransactionSuccess(t *testing.T, transaction Transaction) {
	t.Helper()
	result, err := transaction.Execute(context.Background(), "update")
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil || affected != 1 {
		t.Fatalf("rows affected = %d, %v", affected, rowsErr)
	}
	verifyPGXTransactionRows(t, transaction)
	verifyPGXTransactionCompletion(t, transaction)
}

func verifyPGXTransactionRows(t *testing.T, transaction Transaction) {
	t.Helper()
	rows, err := transaction.Query(context.Background(), "select")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	_ = rows.Next()
	if err := rows.Scan(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := transaction.QueryRow(context.Background(), "select").Scan(makeJobDestinations()...); err != nil {
		t.Fatalf("query row: %v", err)
	}
}

func verifyPGXTransactionCompletion(t *testing.T, transaction Transaction) {
	t.Helper()
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := transaction.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}

func TestPGXTransactionReportsProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := newPGXTransaction(
		func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, protocolErr
		},
		func(context.Context, string, ...any) (pgx.Rows, error) { return nil, protocolErr },
		func(context.Context, string, ...any) pgx.Row { return fakePGXRow{err: protocolErr} },
		func(context.Context) error { return protocolErr }, func(context.Context) error { return protocolErr },
	)
	if _, err := transaction.Execute(context.Background(), "update"); err == nil {
		t.Fatal("expected execute error")
	}
	if _, err := transaction.Query(context.Background(), "select"); err == nil {
		t.Fatal("expected query error")
	}
	if err := transaction.Commit(context.Background()); err == nil {
		t.Fatal("expected commit error")
	}
	if err := transaction.Rollback(context.Background()); err == nil {
		t.Fatal("expected rollback error")
	}
}

func TestRollbackHelperToleratesRollbackFailureAndClosedTransaction(t *testing.T) {
	t.Parallel()
	open := true
	rollback(context.Background(), Transaction{rollback: failingCompletion(errors.New("protocol"))}, &open)
	open = false
	rollback(context.Background(), Transaction{}, &open)
}

type inertPGXTransaction struct{ pgx.Tx }

type fakePGXRow struct {
	values []any
	err    error
}

func (row fakePGXRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	return assignDestinations(destinations, row.values)
}

type fakePGXRows struct{ pgx.Rows }

func (rows *fakePGXRows) Close()                                       {}
func (rows *fakePGXRows) Err() error                                   { return nil }
func (rows *fakePGXRows) Next() bool                                   { return false }
func (rows *fakePGXRows) Scan(...any) error                            { return nil }
func (rows *fakePGXRows) Values() ([]any, error)                       { return nil, nil }
func (rows *fakePGXRows) RawValues() [][]byte                          { return nil }
func (rows *fakePGXRows) Conn() *pgx.Conn                              { return nil }
func (rows *fakePGXRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (rows *fakePGXRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func makeJobDestinations() []any {
	var jobID uuid.UUID
	var adapter, sourceID, runnerPool, state string
	var enqueuedAt time.Time
	var memberID uuid.NullUUID
	var outcome, origin, reason sql.NullString
	var claimedAt, startedAt, acquisitionStartedAt, acquiredAt sql.NullTime
	return []any{&jobID, &adapter, &sourceID, &runnerPool, &state, &enqueuedAt, &memberID, &claimedAt,
		&startedAt, &acquisitionStartedAt, &acquiredAt, &outcome, &origin, &reason}
}
