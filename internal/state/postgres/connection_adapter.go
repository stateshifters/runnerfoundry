package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type executeProtocol func(context.Context, string, ...any) (Result, error)
type queryProtocol func(context.Context, string, ...any) (Rows, error)
type queryRowProtocol func(context.Context, string, ...any) Row

// Connection is the state store's PostgreSQL protocol. Construct one with
// NewPGXConnection and pass it to NewConnectionStore during application startup.
type Connection struct {
	begin    func(context.Context) (Transaction, error)
	queryRow func(context.Context, string, ...any) Row
}

// NewConnectionStore builds a state store from an adapted PostgreSQL connection.
// The returned store is safe to use with the concurrency guarantees of the
// underlying pool and does not own or close that pool.
func NewConnectionStore(connection Connection) *Store {
	return NewStore(Executor(connection))
}

// NewPGXConnection adapts a pgx pool to the state store's narrow database
// protocol. The caller retains ownership of the pool and must close it after
// all store operations have stopped.
func NewPGXConnection(pool *pgxpool.Pool) Connection {
	return connectionFromPGXFunctions(pool.Begin, pool.QueryRow)
}

func connectionFromPGXFunctions(begin func(context.Context) (pgx.Tx, error),
	queryRow func(context.Context, string, ...any) pgx.Row) Connection {
	return Connection{
		begin: func(ctx context.Context) (Transaction, error) {
			transaction, err := begin(ctx)
			if err != nil {
				return Transaction{}, fmt.Errorf("begin PostgreSQL transaction: %w", err)
			}
			return adaptPGXTransaction(transaction), nil
		},
		queryRow: func(ctx context.Context, statement string, arguments ...any) Row {
			return Row{scan: queryRow(ctx, statement, arguments...).Scan}
		},
	}
}

func adaptPGXTransaction(transaction pgx.Tx) Transaction {
	return newPGXTransaction(transaction.Exec, transaction.Query, transaction.QueryRow,
		transaction.Commit, transaction.Rollback)
}

func newPGXTransaction(
	execute func(context.Context, string, ...any) (pgconn.CommandTag, error),
	query func(context.Context, string, ...any) (pgx.Rows, error),
	queryRow func(context.Context, string, ...any) pgx.Row,
	commit func(context.Context) error,
	rollback func(context.Context) error,
) Transaction {
	return Transaction{
		execute: pgxExecutor(execute), query: pgxQuery(query), queryRow: pgxQueryRow(queryRow),
		commit: pgxCompletion("commit", commit), rollback: pgxCompletion("rollback", rollback),
	}
}

func pgxExecutor(
	execute func(context.Context, string, ...any) (pgconn.CommandTag, error),
) executeProtocol {
	return func(ctx context.Context, statement string, arguments ...any) (Result, error) {
		commandTag, err := execute(ctx, statement, arguments...)
		if err != nil {
			return Result{}, fmt.Errorf("execute PostgreSQL statement: %w", err)
		}
		return Result{rowsAffected: func() (int64, error) { return commandTag.RowsAffected(), nil }}, nil
	}
}

func pgxQuery(
	query func(context.Context, string, ...any) (pgx.Rows, error),
) queryProtocol {
	return func(ctx context.Context, statement string, arguments ...any) (Rows, error) {
		rows, err := query(ctx, statement, arguments...)
		if err != nil {
			return Rows{}, fmt.Errorf("query PostgreSQL rows: %w", err)
		}
		return Rows{close: func() error { rows.Close(); return nil }, err: rows.Err,
			next: rows.Next, scan: rows.Scan}, nil
	}
}

func pgxQueryRow(
	queryRow func(context.Context, string, ...any) pgx.Row,
) queryRowProtocol {
	return func(ctx context.Context, statement string, arguments ...any) Row {
		return Row{scan: queryRow(ctx, statement, arguments...).Scan}
	}
}

func pgxCompletion(operation string, complete func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := complete(ctx); err != nil {
			return fmt.Errorf("%s PostgreSQL transaction: %w", operation, err)
		}
		return nil
	}
}
