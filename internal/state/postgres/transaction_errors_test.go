package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestExecuteTransactionReportsEachProtocolFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	cases := transactionFailureExecutors(protocolErr, "heartbeat execute")
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			store := NewStore(testCase.executor)
			if err := store.PublishMemberState(context.Background(), testMemberIDA, fixedTime(),
				[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 2}}); err == nil {
				t.Fatal("expected protocol error")
			}
		})
	}
}

func TestPublishMemberStateReportsCapacityReplacementFailures(t *testing.T) {
	t.Parallel()

	protocolErr := errors.New("protocol")
	for name, failureCall := range map[string]int{"capacity withdrawal": 2, "capacity publication": 3} {
		t.Run(name, func(t *testing.T) {
			store := NewStore(Executor{begin: transactionBeginning(transactionFailingExecute(failureCall, protocolErr))})
			err := store.PublishMemberState(context.Background(), testMemberIDA, fixedTime(),
				[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 2}})
			if !errors.Is(err, protocolErr) {
				t.Fatalf("error = %v, want protocol error", err)
			}
		})
	}
}

func TestWithdrawMemberCapacityReportsEachProtocolFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for _, testCase := range []struct {
		name        string
		transaction Transaction
	}{
		{name: "first execute", transaction: transactionFailingExecute(1, protocolErr)},
		{name: "commit", transaction: Transaction{execute: successfulExecute,
			commit: failingCompletion(protocolErr), rollback: successfulCompletion}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := NewStore(Executor{begin: transactionBeginning(testCase.transaction)})
			if err := store.WithdrawMemberCapacity(context.Background(), testMemberIDA); err == nil {
				t.Fatal("expected withdrawal error")
			}
		})
	}
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) {
		return Transaction{}, protocolErr
	}})
	if err := store.WithdrawMemberCapacity(context.Background(), testMemberIDA); err == nil {
		t.Fatal("expected begin error")
	}
}

func TestScalarQueriesReportProtocolFailures(t *testing.T) {
	t.Parallel()
	store := NewStore(Executor{queryRow: func(context.Context, string, ...any) Row {
		return Row{scan: func(...any) error { return errors.New("protocol") }}
	}})
	if _, err := store.CountLiveMembers(context.Background(), fixedTime()); err == nil {
		t.Fatal("expected count error")
	}
	if _, err := store.AvailableRunnerPoolCapacity(context.Background(), "linux", fixedTime()); err == nil {
		t.Fatal("expected aggregate error")
	}
}

func TestJobListsReportProtocolFailures(t *testing.T) {
	t.Parallel()
	assertTransactionalListFailures(t, func(store *Store) error {
		_, err := store.ProcessingByMember(context.Background(), testMemberIDA, "linux")
		return err
	})
}

func assertTransactionalListFailures(t *testing.T, operation func(*Store) error) {
	t.Helper()
	protocolErr := errors.New("protocol")
	for _, testCase := range namedFailingListTransactions(protocolErr) {
		t.Run(testCase.name, func(t *testing.T) {
			if err := operation(NewStore(Executor{begin: transactionBeginning(testCase.transaction)})); err == nil {
				t.Fatal("expected list error")
			}
		})
	}
	if err := operation(NewStore(Executor{begin: func(context.Context) (Transaction, error) {
		return Transaction{}, protocolErr
	}})); err == nil {
		t.Fatal("expected begin error")
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

type namedExecutor struct {
	name     string
	executor Executor
}

func transactionFailureExecutors(protocolErr error, executeName string) []namedExecutor {
	return []namedExecutor{
		{name: "begin", executor: Executor{begin: func(context.Context) (Transaction, error) { return Transaction{}, protocolErr }}},
		{name: executeName, executor: Executor{begin: transactionBeginning(Transaction{
			execute: failingExecute(protocolErr), rollback: successfulCompletion,
		})}},
		{name: "commit", executor: Executor{begin: transactionBeginning(Transaction{
			execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion,
		})}},
		{name: "rollback", executor: Executor{begin: transactionBeginning(Transaction{
			execute: failingExecute(protocolErr), rollback: failingCompletion(protocolErr),
		})}},
	}
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

func transactionBeginning(transaction Transaction) func(context.Context) (Transaction, error) {
	return func(context.Context) (Transaction, error) { return transaction, nil }
}

func successfulExecute(context.Context, string, ...any) (Result, error) {
	return Result{rowsAffected: func() (int64, error) { return 1, nil }}, nil
}

func failingExecute(protocolErr error) func(context.Context, string, ...any) (Result, error) {
	return func(context.Context, string, ...any) (Result, error) { return Result{}, protocolErr }
}

func successfulCompletion(context.Context) error { return nil }

func failingCompletion(protocolErr error) func(context.Context) error {
	return func(context.Context) error { return protocolErr }
}

func transactionFailingExecute(failureCall int, protocolErr error) Transaction {
	calls := 0
	return Transaction{execute: func(ctx context.Context, _ string, _ ...any) (Result, error) {
		calls++
		if calls == failureCall {
			return Result{}, protocolErr
		}
		return successfulExecute(ctx, "")
	}, commit: successfulCompletion, rollback: successfulCompletion}
}

func rowsWithFailure(scanErr, iterationErr, closeErr error) Rows {
	advanced := false
	return Rows{close: func() error { return closeErr }, err: func() error { return iterationErr },
		next: func() bool {
			if scanErr != nil && !advanced {
				advanced = true
				return true
			}
			return false
		}, scan: func(...any) error { return scanErr }}
}
