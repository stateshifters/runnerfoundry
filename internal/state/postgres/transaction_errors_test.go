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
			store := newStore(testCase.executor)
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
			store := newStore(executorStub{begin: transactionBeginning(transactionFailingExecute(failureCall, protocolErr))})
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
		transaction transactionStub
	}{
		{name: "first execute", transaction: transactionFailingExecute(1, protocolErr)},
		{name: "commit", transaction: transactionStub{execute: successfulExecute,
			commit: failingCompletion(protocolErr), rollback: successfulCompletion}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := newStore(executorStub{begin: transactionBeginning(testCase.transaction)})
			if err := store.WithdrawMemberCapacity(context.Background(), testMemberIDA); err == nil {
				t.Fatal("expected withdrawal error")
			}
		})
	}
	store := newStore(executorStub{begin: func(context.Context) (transactionStub, error) {
		return transactionStub{}, protocolErr
	}})
	if err := store.WithdrawMemberCapacity(context.Background(), testMemberIDA); err == nil {
		t.Fatal("expected begin error")
	}
}

func TestScalarQueriesReportProtocolFailures(t *testing.T) {
	t.Parallel()
	store := newStore(executorStub{queryRow: func(context.Context, string, ...any) rowStub {
		return rowStub{scan: func(...any) error { return errors.New("protocol") }}
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
			if err := operation(newStore(executorStub{begin: transactionBeginning(testCase.transaction)})); err == nil {
				t.Fatal("expected list error")
			}
		})
	}
	if err := operation(newStore(executorStub{begin: func(context.Context) (transactionStub, error) {
		return transactionStub{}, protocolErr
	}})); err == nil {
		t.Fatal("expected begin error")
	}
}

func failingListTransactions(protocolErr error) []transactionStub {
	return []transactionStub{
		{query: func(context.Context, string, ...any) (rowsStub, error) { return rowsStub{}, protocolErr }, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsWithFailure(protocolErr, nil, nil), nil
		}, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsWithFailure(nil, protocolErr, nil), nil
		}, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsWithFailure(nil, nil, protocolErr), nil
		}, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (rowsStub, error) { return rowsFromValues(nil), nil },
			commit: failingCompletion(protocolErr), rollback: successfulCompletion},
	}
}

type namedTransaction struct {
	name        string
	transaction transactionStub
}

type namedExecutor struct {
	name     string
	executor executorStub
}

func transactionFailureExecutors(protocolErr error, executeName string) []namedExecutor {
	return []namedExecutor{
		{name: "begin", executor: executorStub{begin: func(context.Context) (transactionStub, error) { return transactionStub{}, protocolErr }}},
		{name: executeName, executor: executorStub{begin: transactionBeginning(transactionStub{
			execute: failingExecute(protocolErr), rollback: successfulCompletion,
		})}},
		{name: "commit", executor: executorStub{begin: transactionBeginning(transactionStub{
			execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion,
		})}},
		{name: "rollback", executor: executorStub{begin: transactionBeginning(transactionStub{
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

func transactionBeginning(transaction transactionStub) func(context.Context) (transactionStub, error) {
	return func(context.Context) (transactionStub, error) { return transaction, nil }
}

func successfulExecute(context.Context, string, ...any) (resultStub, error) {
	return resultStub{rowsAffected: func() (int64, error) { return 1, nil }}, nil
}

func failingExecute(protocolErr error) func(context.Context, string, ...any) (resultStub, error) {
	return func(context.Context, string, ...any) (resultStub, error) { return resultStub{}, protocolErr }
}

func successfulCompletion(context.Context) error { return nil }

func failingCompletion(protocolErr error) func(context.Context) error {
	return func(context.Context) error { return protocolErr }
}

func transactionFailingExecute(failureCall int, protocolErr error) transactionStub {
	calls := 0
	return transactionStub{execute: func(ctx context.Context, _ string, _ ...any) (resultStub, error) {
		calls++
		if calls == failureCall {
			return resultStub{}, protocolErr
		}
		return successfulExecute(ctx, "")
	}, commit: successfulCompletion, rollback: successfulCompletion}
}

func rowsWithFailure(scanErr, iterationErr, closeErr error) rowsStub {
	advanced := false
	return rowsStub{close: func() error { return closeErr }, err: func() error { return iterationErr },
		next: func() bool {
			if scanErr != nil && !advanced {
				advanced = true
				return true
			}
			return false
		}, scan: func(...any) error { return scanErr }}
}
