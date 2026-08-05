package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestExecuteTransactionReportsEachProtocolFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	cases := []struct {
		name     string
		executor Executor
	}{
		{name: "begin", executor: Executor{begin: func(context.Context) (Transaction, error) {
			return Transaction{}, protocolErr
		}}},
		{name: "heartbeat execute", executor: Executor{begin: transactionBeginning(Transaction{
			execute: failingExecute(protocolErr), rollback: successfulCompletion,
		})}},
		{name: "commit", executor: Executor{begin: transactionBeginning(Transaction{
			execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion,
		})}},
		{name: "rollback", executor: Executor{begin: transactionBeginning(Transaction{
			execute: failingExecute(protocolErr), rollback: failingCompletion(protocolErr),
		})}},
	}
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

func TestRegistrationMutationReportsEachProtocolFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	executors := []Executor{
		{begin: func(context.Context) (Transaction, error) { return Transaction{}, protocolErr }},
		{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr), rollback: successfulCompletion})},
		{begin: transactionBeginning(Transaction{execute: successfulExecute,
			commit: failingCompletion(protocolErr), rollback: successfulCompletion})},
		{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr),
			rollback: failingCompletion(protocolErr)})},
	}
	for index, executor := range executors {
		if err := NewStore(executor).DeleteRegistration(context.Background(), "repo", "linux"); err == nil {
			t.Fatalf("case %d: expected registration mutation error", index)
		}
	}
}

func TestPublishMemberStateReportsCapacityReplacementFailures(t *testing.T) {
	t.Parallel()

	protocolErr := errors.New("protocol")
	for failureCall := 2; failureCall <= 3; failureCall++ {
		store := NewStore(Executor{begin: transactionBeginning(transactionFailingExecute(failureCall, protocolErr))})
		err := store.PublishMemberState(context.Background(), testMemberIDA, fixedTime(),
			[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 2}})
		if !errors.Is(err, protocolErr) {
			t.Fatalf("execute %d error = %v, want protocol error", failureCall, err)
		}
	}
}

func TestExpireMemberReportsEachProtocolFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for _, testCase := range []struct {
		name        string
		transaction Transaction
	}{
		{name: "first execute", transaction: transactionFailingExecute(1, protocolErr)},
		{name: "second execute", transaction: transactionFailingExecute(2, protocolErr)},
		{name: "commit", transaction: Transaction{execute: successfulExecute,
			commit: failingCompletion(protocolErr), rollback: successfulCompletion}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := NewStore(Executor{begin: transactionBeginning(testCase.transaction)})
			if err := store.ExpireMember(context.Background(), testMemberIDA); err == nil {
				t.Fatal("expected expiry error")
			}
		})
	}
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) {
		return Transaction{}, protocolErr
	}})
	if err := store.ExpireMember(context.Background(), testMemberIDA); err == nil {
		t.Fatal("expected begin error")
	}
}

func TestMemberQueriesReportEachProtocolFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for index, transaction := range failingListTransactions(protocolErr) {
		store := NewStore(Executor{begin: transactionBeginning(transaction)})
		if _, err := store.ListLiveMembers(context.Background(), fixedTime()); err == nil {
			t.Fatalf("case %d: expected member query error", index)
		}
	}
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) {
		return Transaction{}, protocolErr
	}})
	if _, err := store.ListExpiredMembers(context.Background(), fixedTime()); err == nil {
		t.Fatal("expected member begin error")
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

func TestJobAndRegistrationListsReportProtocolFailures(t *testing.T) {
	t.Parallel()
	assertTransactionalListFailures(t, func(store *Store) error {
		_, err := store.PendingResults(context.Background(), 3)
		return err
	})
	assertTransactionalListFailures(t, func(store *Store) error {
		_, err := store.Registrations(context.Background())
		return err
	})
}

func assertTransactionalListFailures(t *testing.T, operation func(*Store) error) {
	t.Helper()
	protocolErr := errors.New("protocol")
	for index, transaction := range failingListTransactions(protocolErr) {
		if err := operation(NewStore(Executor{begin: transactionBeginning(transaction)})); err == nil {
			t.Fatalf("case %d: expected list error", index)
		}
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
