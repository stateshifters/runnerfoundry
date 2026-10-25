package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type databaseExecutor struct{ database *sql.DB }

func (executor databaseExecutor) WithinTransaction(ctx context.Context, operation func(Transaction) error) (resultErr error) {
	transaction, err := executor.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			if err := transaction.Rollback(); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("rollback SQLite transaction: %w", err))
			}
		}
		resultErr = canceledTransactionResult(ctx, resultErr)
	}()
	if err := operation(transactionAdapter{transaction: transaction}); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit SQLite transaction: %w", err)
	}
	committed = true
	return nil
}

func canceledTransactionResult(ctx context.Context, err error) error {
	if err == nil || !errors.Is(ctx.Err(), context.Canceled) || !onlyCanceledTransactionCauses(err) {
		return err
	}
	return fmt.Errorf("SQLite transaction canceled: %w", ctx.Err())
}

func onlyCanceledTransactionCauses(err error) bool {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		return allCanceledTransactionCauses(joined.Unwrap())
	}
	if cause := errors.Unwrap(err); cause != nil {
		return onlyCanceledTransactionCauses(cause)
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, sql.ErrTxDone)
}

func allCanceledTransactionCauses(causes []error) bool {
	for _, cause := range causes {
		if !onlyCanceledTransactionCauses(cause) {
			return false
		}
	}
	return len(causes) > 0
}
func (executor databaseExecutor) ReadRow(ctx context.Context, statement string, read func(Scanner) error, arguments ...any) error {
	if err := read(executor.database.QueryRowContext(ctx, statement, arguments...)); err != nil {
		return fmt.Errorf("read SQLite row: %w", err)
	}
	return nil
}

type transactionAdapter struct{ transaction *sql.Tx }

func (adapter transactionAdapter) Execute(ctx context.Context, statement string, arguments ...any) (int64, error) {
	result, err := adapter.transaction.ExecContext(ctx, statement, arguments...)
	if err != nil {
		return 0, fmt.Errorf("execute SQLite statement: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read SQLite affected rows: %w", err)
	}
	return affected, nil
}
func (adapter transactionAdapter) ReadRow(ctx context.Context, statement string, read func(Scanner) error, arguments ...any) error {
	if err := read(adapter.transaction.QueryRowContext(ctx, statement, arguments...)); err != nil {
		return fmt.Errorf("read SQLite transaction row: %w", err)
	}
	return nil
}
func (adapter transactionAdapter) VisitRows(ctx context.Context, statement string, visit func(Scanner) error, arguments ...any) (resultErr error) {
	rows, err := adapter.transaction.QueryContext(ctx, statement, arguments...)
	if err != nil {
		return fmt.Errorf("query SQLite rows: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		if err := visit(rows); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate SQLite rows: %w", err)
	}
	return nil
}
