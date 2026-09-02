package sqlite

import "context"

type Result struct{ rowsAffected func() (int64, error) }

func NewResult(rowsAffected func() (int64, error)) Result { return Result{rowsAffected: rowsAffected} }

func (result Result) RowsAffected() (int64, error) { return result.rowsAffected() }

type Row struct{ scan func(...any) error }

func NewRow(scan func(...any) error) Row { return Row{scan: scan} }

func (row Row) Scan(destinations ...any) error { return row.scan(destinations...) }

type scanner interface {
	Scan(destinations ...any) error
}

type Rows struct {
	close func() error
	err   func() error
	next  func() bool
	scan  func(...any) error
}

func NewRows(closeRows func() error, rowsErr func() error, next func() bool, scan func(...any) error) Rows {
	return Rows{close: closeRows, err: rowsErr, next: next, scan: scan}
}

func (rows Rows) Close() error                   { return rows.close() }
func (rows Rows) Err() error                     { return rows.err() }
func (rows Rows) Next() bool                     { return rows.next() }
func (rows Rows) Scan(destinations ...any) error { return rows.scan(destinations...) }

type Transaction struct {
	execute  func(context.Context, string, ...any) (Result, error)
	query    func(context.Context, string, ...any) (Rows, error)
	queryRow func(context.Context, string, ...any) Row
	commit   func(context.Context) error
	rollback func(context.Context) error
}

func NewTransaction(
	execute func(context.Context, string, ...any) (Result, error),
	query func(context.Context, string, ...any) (Rows, error),
	queryRow func(context.Context, string, ...any) Row,
	commit func(context.Context) error,
	rollback func(context.Context) error,
) Transaction {
	return Transaction{execute: execute, query: query, queryRow: queryRow, commit: commit, rollback: rollback}
}

func (transaction Transaction) Execute(ctx context.Context, statement string, arguments ...any) (Result, error) {
	return transaction.execute(ctx, statement, arguments...)
}
func (transaction Transaction) Query(ctx context.Context, statement string, arguments ...any) (Rows, error) {
	return transaction.query(ctx, statement, arguments...)
}
func (transaction Transaction) QueryRow(ctx context.Context, statement string, arguments ...any) Row {
	return transaction.queryRow(ctx, statement, arguments...)
}
func (transaction Transaction) Commit(ctx context.Context) error   { return transaction.commit(ctx) }
func (transaction Transaction) Rollback(ctx context.Context) error { return transaction.rollback(ctx) }

type Executor struct {
	begin    func(context.Context) (Transaction, error)
	queryRow func(context.Context, string, ...any) Row
}

func NewExecutor(
	begin func(context.Context) (Transaction, error),
	queryRow func(context.Context, string, ...any) Row,
) Executor {
	return Executor{begin: begin, queryRow: queryRow}
}

func (executor Executor) Begin(ctx context.Context) (Transaction, error) { return executor.begin(ctx) }
func (executor Executor) QueryRow(ctx context.Context, statement string, arguments ...any) Row {
	return executor.queryRow(ctx, statement, arguments...)
}
