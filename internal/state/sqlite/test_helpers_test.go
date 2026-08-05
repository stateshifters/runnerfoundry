package sqlite

import (
	"context"
	"errors"
	"reflect"
)

func transactionBeginning(transaction Transaction) func(context.Context) (Transaction, error) {
	return func(context.Context) (Transaction, error) { return transaction, nil }
}

func successfulExecute(context.Context, string, ...any) (Result, error) {
	return successfulResult(), nil
}

func successfulCompletion(context.Context) error { return nil }

func assignDestinations(destinations, values []any) error {
	if len(destinations) != len(values) {
		return errors.New("destination count")
	}
	for index, destination := range destinations {
		reflect.ValueOf(destination).Elem().Set(reflect.ValueOf(values[index]))
	}
	return nil
}

func failingListTransactions(protocolErr error) []Transaction {
	return []Transaction{
		{query: func(context.Context, string, ...any) (Rows, error) { return Rows{}, protocolErr }, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (Rows, error) {
			return rowsWithFailure(protocolErr, nil, nil), nil
		}, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (Rows, error) {
			return rowsWithFailure(nil, protocolErr, nil), nil
		}, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (Rows, error) {
			return rowsWithFailure(nil, nil, protocolErr), nil
		}, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (Rows, error) { return rowsFromValues(nil), nil },
			commit: failingCompletion(protocolErr), rollback: successfulCompletion},
	}
}
