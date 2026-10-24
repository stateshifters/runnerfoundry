package postgres

import (
	"context"
	"errors"
	"reflect"

	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
)

type postgresTransactionRunner struct {
	transaction *postgresTransaction
	err         error
	afterErr    error
}

func (runner *postgresTransactionRunner) WithinTransaction(
	ctx context.Context,
	operation func(postgresstate.Transaction) error,
) error {
	if runner.err != nil {
		return runner.err
	}
	if err := operation(runner.transaction); err != nil {
		return err
	}
	return runner.afterErr
}

type postgresTransaction struct {
	execute   func(context.Context, string, ...any) (int64, error)
	readRow   func(context.Context, string, func(postgresstate.Scanner) error, ...any) error
	visitRows func(context.Context, string, func(postgresstate.Scanner) error, ...any) error
}

func (transaction *postgresTransaction) Execute(
	ctx context.Context,
	statement string,
	arguments ...any,
) (int64, error) {
	return transaction.execute(ctx, statement, arguments...)
}

func (transaction *postgresTransaction) ReadRow(
	ctx context.Context,
	statement string,
	read func(postgresstate.Scanner) error,
	arguments ...any,
) error {
	return transaction.readRow(ctx, statement, read, arguments...)
}

func (transaction *postgresTransaction) VisitRows(
	ctx context.Context,
	statement string,
	visit func(postgresstate.Scanner) error,
	arguments ...any,
) error {
	return transaction.visitRows(ctx, statement, visit, arguments...)
}

type postgresScanner struct {
	values []any
	err    error
}

func (scanner postgresScanner) Scan(destinations ...any) error {
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

func successfulPostgresRunner(rowsFor func(string) [][]any) *postgresTransactionRunner {
	transaction := &postgresTransaction{
		execute: func(context.Context, string, ...any) (int64, error) { return 1, nil },
		readRow: func(context.Context, string, func(postgresstate.Scanner) error, ...any) error {
			return nil
		},
		visitRows: func(
			_ context.Context,
			statement string,
			visit func(postgresstate.Scanner) error,
			_ ...any,
		) error {
			for _, values := range rowsFor(statement) {
				if err := visit(postgresScanner{values: values}); err != nil {
					return err
				}
			}
			return nil
		},
	}
	return &postgresTransactionRunner{transaction: transaction}
}
