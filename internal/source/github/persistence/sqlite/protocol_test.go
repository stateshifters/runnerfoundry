package sqlite

import (
	"context"
	"errors"
	"reflect"

	sqlitestate "github.com/stateshifters/runnerfoundry/internal/state/sqlite"
)

type sqliteTransactionRunner struct {
	transaction *sqliteTransaction
	err         error
	afterErr    error
}

func (runner *sqliteTransactionRunner) WithinTransaction(
	ctx context.Context,
	operation func(sqlitestate.Transaction) error,
) error {
	if runner.err != nil {
		return runner.err
	}
	if err := operation(runner.transaction); err != nil {
		return err
	}
	return runner.afterErr
}

type sqliteTransaction struct {
	execute   func(context.Context, string, ...any) (int64, error)
	readRow   func(context.Context, string, func(sqlitestate.Scanner) error, ...any) error
	visitRows func(context.Context, string, func(sqlitestate.Scanner) error, ...any) error
}

func (transaction *sqliteTransaction) Execute(
	ctx context.Context,
	statement string,
	arguments ...any,
) (int64, error) {
	return transaction.execute(ctx, statement, arguments...)
}

func (transaction *sqliteTransaction) ReadRow(
	ctx context.Context,
	statement string,
	read func(sqlitestate.Scanner) error,
	arguments ...any,
) error {
	return transaction.readRow(ctx, statement, read, arguments...)
}

func (transaction *sqliteTransaction) VisitRows(
	ctx context.Context,
	statement string,
	visit func(sqlitestate.Scanner) error,
	arguments ...any,
) error {
	return transaction.visitRows(ctx, statement, visit, arguments...)
}

type sqliteScanner struct {
	values []any
	err    error
}

func (scanner sqliteScanner) Scan(destinations ...any) error {
	if scanner.err != nil {
		return scanner.err
	}
	if len(destinations) != len(scanner.values) {
		return errors.New("destination count")
	}
	for index, destination := range destinations {
		target := reflect.ValueOf(destination)
		value := reflect.ValueOf(scanner.values[index])
		if value.Type().AssignableTo(target.Elem().Type()) {
			target.Elem().Set(value)
		} else if value.Type().ConvertibleTo(target.Elem().Type()) {
			target.Elem().Set(value.Convert(target.Elem().Type()))
		} else {
			return errors.New("value type")
		}
	}
	return nil
}

func successfulSQLiteRunner(rowsFor func(string) [][]any) *sqliteTransactionRunner {
	transaction := &sqliteTransaction{
		execute: func(context.Context, string, ...any) (int64, error) { return 1, nil },
		readRow: func(context.Context, string, func(sqlitestate.Scanner) error, ...any) error {
			return nil
		},
		visitRows: func(
			_ context.Context,
			statement string,
			visit func(sqlitestate.Scanner) error,
			_ ...any,
		) error {
			for _, values := range rowsFor(statement) {
				if err := visit(sqliteScanner{values: values}); err != nil {
					return err
				}
			}
			return nil
		},
	}
	return &sqliteTransactionRunner{transaction: transaction}
}
