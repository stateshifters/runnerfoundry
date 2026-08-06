package sqlite

import (
	"context"
	"errors"
	"fmt"
)

func transactionBeginning(transaction Transaction) func(context.Context) (Transaction, error) {
	return func(context.Context) (Transaction, error) { return transaction, nil }
}

func successfulExecute(context.Context, string, ...any) (Result, error) {
	return successfulResult(), nil
}

func unstartedProcessingValues() []any {
	values := processingJobValues()
	values[5], values[6] = false, false
	return values
}

func successfulCompletion(context.Context) error { return nil }

func assignDestinations(destinations, values []any) error {
	if len(destinations) != len(values) {
		return errors.New("destination count")
	}
	for index, destination := range destinations {
		if !assignSQLiteTestValue(destination, values[index]) {
			return fmt.Errorf("cannot assign %T to %T", values[index], destination)
		}
	}
	return nil
}

func assignSQLiteTestValue(destination, value any) bool {
	switch target := destination.(type) {
	case *string:
		assigned, ok := value.(string)
		*target = assigned
		return ok
	case *int:
		assigned, ok := value.(int)
		*target = assigned
		return ok
	case *int64:
		assigned, ok := value.(int64)
		*target = assigned
		return ok
	case *bool:
		assigned, ok := value.(bool)
		*target = assigned
		return ok
	default:
		return false
	}
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

type namedTransaction struct {
	name        string
	transaction Transaction
}

func namedFailingListTransactions(protocolErr error) []namedTransaction {
	names := []string{"query", "row scan", "iteration", "close", "commit"}
	transactions := failingListTransactions(protocolErr)
	result := make([]namedTransaction, len(transactions))
	for index := range transactions {
		result[index] = namedTransaction{name: names[index], transaction: transactions[index]}
	}
	return result
}
