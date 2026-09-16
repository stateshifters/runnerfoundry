package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Connection struct{ executor executor }

func NewConnectionStore(connection *Connection) *Store { return newStore(connection.executor) }

func NewPGXConnection(pool *pgxpool.Pool) *Connection {
	return connectionFromPGXFunctions(pool.Begin, pool.QueryRow)
}

func connectionFromPGXFunctions(
	begin func(context.Context) (pgx.Tx, error),
	queryRow func(context.Context, string, ...any) pgx.Row,
) *Connection {
	return &Connection{executor: pgxExecutor{begin: begin, queryRow: queryRow}}
}

type pgxExecutor struct {
	begin    func(context.Context) (pgx.Tx, error)
	queryRow func(context.Context, string, ...any) pgx.Row
}

func (executor pgxExecutor) WithinTransaction(
	ctx context.Context,
	operation func(Transaction) error,
) (resultErr error) {
	transaction, err := executor.begin(ctx)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			if err := transaction.Rollback(context.WithoutCancel(ctx)); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("rollback PostgreSQL transaction: %w", err))
			}
		}
	}()
	if err := operation(adaptPGXTransaction(transaction)); err != nil {
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit PostgreSQL transaction: %w", err)
	}
	committed = true
	return nil
}

func (executor pgxExecutor) ReadRow(
	ctx context.Context,
	statement string,
	read func(Scanner) error,
	arguments ...any,
) error {
	if err := read(executor.queryRow(ctx, statement, arguments...)); err != nil {
		return fmt.Errorf("read PostgreSQL row: %w", err)
	}
	return nil
}

type pgxTransaction struct {
	execute  func(context.Context, string, ...any) (pgconn.CommandTag, error)
	query    func(context.Context, string, ...any) (pgx.Rows, error)
	queryRow func(context.Context, string, ...any) pgx.Row
}

func adaptPGXTransaction(transaction pgx.Tx) pgxTransaction {
	return pgxTransaction{
		execute:  transaction.Exec,
		query:    transaction.Query,
		queryRow: transaction.QueryRow,
	}
}

func (adapter pgxTransaction) Execute(ctx context.Context, statement string, arguments ...any) (int64, error) {
	result, err := adapter.execute(ctx, statement, arguments...)
	if err != nil {
		return 0, fmt.Errorf("execute PostgreSQL statement: %w", err)
	}
	return result.RowsAffected(), nil
}

func (adapter pgxTransaction) ReadRow(
	ctx context.Context,
	statement string,
	read func(Scanner) error,
	arguments ...any,
) error {
	if err := read(adapter.queryRow(ctx, statement, arguments...)); err != nil {
		return fmt.Errorf("read PostgreSQL transaction row: %w", err)
	}
	return nil
}

func (adapter pgxTransaction) VisitRows(
	ctx context.Context,
	statement string,
	visit func(Scanner) error,
	arguments ...any,
) error {
	rows, err := adapter.query(ctx, statement, arguments...)
	if err != nil {
		return fmt.Errorf("query PostgreSQL rows: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := visit(rows); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate PostgreSQL rows: %w", err)
	}
	return nil
}
