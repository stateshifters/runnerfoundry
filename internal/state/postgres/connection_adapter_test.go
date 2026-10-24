package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConnectionStoreDelegatesOwnedProtocol(t *testing.T) {
	t.Parallel()
	connection := &Connection{executor: executorStub{
		begin: transactionBeginning(transactionStub{execute: successfulExecute,
			commit: successfulCompletion, rollback: successfulCompletion}),
		queryRow: func(context.Context, string, ...any) rowStub { return rowFromValues([]any{4}) },
	}}
	store := NewConnectionStore(connection)
	if err := store.PublishMemberState(context.Background(), testMemberIDA, fixedTime(), nil); err != nil {
		t.Fatalf("heartbeat through connection: %v", err)
	}
	if count, err := store.CountLiveMembers(context.Background(), fixedTime()); err != nil || count != 4 {
		t.Fatalf("count through connection = %d, %v", count, err)
	}
}

func TestConnectionAndStoreExposeTransactionExtension(t *testing.T) {
	t.Parallel()
	transaction := transactionStub{execute: successfulExecute, commit: successfulCompletion, rollback: successfulCompletion}
	executor := executorStub{begin: transactionBeginning(transaction)}
	connection := &Connection{executor: executor}
	for name, extension := range map[string]*AdapterExtension{
		"connection": connection.AdapterExtension(), "store": newStore(executor).AdapterExtension(),
	} {
		if err := extension.WithinTransaction(context.Background(), func(tx Transaction) error {
			_, err := tx.Execute(context.Background(), "update")
			if err != nil {
				return fmt.Errorf("execute extension statement: %w", err)
			}
			return nil
		}); err != nil {
			t.Fatalf("%s adapter extension: %v", name, err)
		}
	}
}

func TestPGXConnectionConstruction(t *testing.T) {
	t.Parallel()
	connection := connectionFromPGXFunctions(
		func(context.Context) (pgx.Tx, error) { return &pgxTransactionFake{}, nil },
		func(context.Context, string, ...any) pgx.Row { return fakePGXRow{values: []any{7}} },
	)
	var value int
	if err := connection.executor.ReadRow(context.Background(), "select", func(row Scanner) error {
		return row.Scan(&value)
	}); err != nil || value != 7 {
		t.Fatalf("read row = %d, %v", value, err)
	}
	if NewPGXConnection(&pgxpool.Pool{}) == nil {
		t.Fatal("expected connection")
	}
}

func TestPGXConnectionReportsReadFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("read")
	connection := connectionFromPGXFunctions(nil, func(context.Context, string, ...any) pgx.Row {
		return fakePGXRow{err: protocolErr}
	})
	err := connection.executor.ReadRow(context.Background(), "select", func(row Scanner) error {
		return row.Scan()
	})
	if !errors.Is(err, protocolErr) {
		t.Fatalf("read error = %v", err)
	}
}

func TestAdapterExtensionReportsTransactionFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("transaction")
	extension := (&Connection{executor: executorStub{begin: func(context.Context) (transactionStub, error) {
		return transactionStub{}, protocolErr
	}}}).AdapterExtension()
	err := extension.WithinTransaction(context.Background(), func(Transaction) error { return nil })
	if !errors.Is(err, protocolErr) {
		t.Fatalf("transaction error = %v", err)
	}
}

func TestPGXTransactionAdaptsDriverOperations(t *testing.T) {
	t.Parallel()
	rows := &fakePGXRows{remaining: 1}
	transaction := pgxTransaction{
		execute: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
		query:    func(context.Context, string, ...any) (pgx.Rows, error) { return rows, nil },
		queryRow: func(context.Context, string, ...any) pgx.Row { return fakePGXRow{values: []any{7}} },
	}
	affected, err := transaction.Execute(context.Background(), "update")
	if err != nil || affected != 1 {
		t.Fatalf("execute = %d, %v", affected, err)
	}
	var value int
	if err = transaction.ReadRow(context.Background(), "select", func(row Scanner) error {
		return row.Scan(&value)
	}); err != nil || value != 7 {
		t.Fatalf("read row = %d, %v", value, err)
	}
	visits := 0
	if err = transaction.VisitRows(context.Background(), "select", func(Scanner) error {
		visits++
		return nil
	}); err != nil || visits != 1 || !rows.closed {
		t.Fatalf("visit rows = %d, closed %t, %v", visits, rows.closed, err)
	}
}

func TestPGXTransactionReportsExecuteAndReadFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := pgxTransaction{
		execute: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, protocolErr
		},
		queryRow: func(context.Context, string, ...any) pgx.Row { return fakePGXRow{err: protocolErr} },
	}
	if _, err := transaction.Execute(context.Background(), "update"); !errors.Is(err, protocolErr) {
		t.Fatalf("execute error = %v", err)
	}
	if err := transaction.ReadRow(context.Background(), "select", func(row Scanner) error {
		return row.Scan()
	}); !errors.Is(err, protocolErr) {
		t.Fatalf("read error = %v", err)
	}
}

func TestPGXTransactionReportsQueryAndIterationFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := pgxTransaction{
		query: func(context.Context, string, ...any) (pgx.Rows, error) { return nil, protocolErr },
	}
	if err := transaction.VisitRows(context.Background(), "select", func(Scanner) error { return nil }); !errors.Is(err, protocolErr) {
		t.Fatalf("query error = %v", err)
	}
	rows := &fakePGXRows{remaining: 1, iterationErr: protocolErr}
	transaction.query = func(context.Context, string, ...any) (pgx.Rows, error) { return rows, nil }
	if err := transaction.VisitRows(context.Background(), "select", func(Scanner) error { return nil }); !errors.Is(err, protocolErr) {
		t.Fatalf("iteration error = %v", err)
	}
}

func TestPGXTransactionReportsVisitorFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	rows := &fakePGXRows{remaining: 1}
	transaction := pgxTransaction{
		query: func(context.Context, string, ...any) (pgx.Rows, error) { return rows, nil },
	}
	if err := transaction.VisitRows(context.Background(), "select", func(Scanner) error {
		return protocolErr
	}); !errors.Is(err, protocolErr) {
		t.Fatalf("visit error = %v", err)
	}
}

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

type fakePGXRows struct {
	pgx.Rows
	remaining    int
	iterationErr error
	closed       bool
}

func (rows *fakePGXRows) Close()                                       { rows.closed = true }
func (rows *fakePGXRows) Err() error                                   { return rows.iterationErr }
func (rows *fakePGXRows) Next() bool                                   { rows.remaining--; return rows.remaining >= 0 }
func (rows *fakePGXRows) Scan(...any) error                            { return nil }
func (rows *fakePGXRows) Values() ([]any, error)                       { return nil, nil }
func (rows *fakePGXRows) RawValues() [][]byte                          { return nil }
func (rows *fakePGXRows) Conn() *pgx.Conn                              { return nil }
func (rows *fakePGXRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (rows *fakePGXRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
