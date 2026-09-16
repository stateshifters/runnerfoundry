package postgres

import (
	"context"
	"errors"
)

type resultStub struct{ rowsAffected func() (int64, error) }

func (result resultStub) RowsAffected() (int64, error) { return result.rowsAffected() }

type rowStub struct{ scan func(...any) error }

func (row rowStub) Scan(destinations ...any) error { return row.scan(destinations...) }

type rowsStub struct {
	close func() error
	err   func() error
	next  func() bool
	scan  func(...any) error
}

func (rows rowsStub) Close() error                   { return rows.close() }
func (rows rowsStub) Err() error                     { return rows.err() }
func (rows rowsStub) Next() bool                     { return rows.next() }
func (rows rowsStub) Scan(destinations ...any) error { return rows.scan(destinations...) }

type transactionStub struct {
	execute  func(context.Context, string, ...any) (resultStub, error)
	query    func(context.Context, string, ...any) (rowsStub, error)
	queryRow func(context.Context, string, ...any) rowStub
	commit   func(context.Context) error
	rollback func(context.Context) error
}

func (transaction transactionStub) Execute(ctx context.Context, statement string, arguments ...any) (int64, error) {
	result, err := transaction.execute(ctx, statement, arguments...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (transaction transactionStub) ReadRow(
	ctx context.Context,
	statement string,
	read func(Scanner) error,
	arguments ...any,
) error {
	return read(transaction.queryRow(ctx, statement, arguments...))
}

func (transaction transactionStub) VisitRows(
	ctx context.Context,
	statement string,
	visit func(Scanner) error,
	arguments ...any,
) (resultErr error) {
	rows, err := transaction.query(ctx, statement, arguments...)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		if err := visit(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

type executorStub struct {
	begin    func(context.Context) (transactionStub, error)
	queryRow func(context.Context, string, ...any) rowStub
}

func (executor executorStub) WithinTransaction(
	ctx context.Context,
	operation func(Transaction) error,
) (resultErr error) {
	transaction, err := executor.begin(ctx)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		if !complete {
			resultErr = errors.Join(resultErr, transaction.rollback(context.WithoutCancel(ctx)))
		}
	}()
	if err := operation(transaction); err != nil {
		return err
	}
	if err := transaction.commit(ctx); err != nil {
		return err
	}
	complete = true
	return nil
}

func (executor executorStub) ReadRow(
	ctx context.Context,
	statement string,
	read func(Scanner) error,
	arguments ...any,
) error {
	return read(executor.queryRow(ctx, statement, arguments...))
}
