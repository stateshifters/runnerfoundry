package sqlite

import (
	"context"
	"errors"
	"testing"
)

func TestExecuteTransactionReportsEachProtocolFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	executors := []Executor{
		{begin: failingBegin(protocolErr)},
		{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr), rollback: successfulCompletion})},
		{begin: transactionBeginning(Transaction{execute: successfulExecute,
			commit: failingCompletion(protocolErr), rollback: successfulCompletion})},
		{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr),
			rollback: failingCompletion(protocolErr)})},
	}
	for index, executor := range executors {
		if err := NewStore(executor).HeartbeatMember(context.Background(), testMemberID, fixedTime()); err == nil {
			t.Fatalf("case %d: expected protocol error", index)
		}
	}
}

func TestExpireMemberReportsEachProtocolFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transactions := []Transaction{
		transactionFailingExecute(1, protocolErr), transactionFailingExecute(2, protocolErr),
		{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion},
	}
	for index, transaction := range transactions {
		if err := NewStore(Executor{begin: transactionBeginning(transaction)}).
			ExpireMember(context.Background(), testMemberID); err == nil {
			t.Fatalf("case %d: expected expiry error", index)
		}
	}
	if err := NewStore(Executor{begin: failingBegin(protocolErr)}).
		ExpireMember(context.Background(), testMemberID); err == nil {
		t.Fatal("expected begin error")
	}
}

func TestMemberQueriesReportEachProtocolFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transactions := failingListTransactions(protocolErr)
	for index, transaction := range transactions {
		store := NewStore(Executor{begin: transactionBeginning(transaction)})
		if _, err := store.ListLiveMembers(context.Background(), fixedTime()); err == nil {
			t.Fatalf("case %d: expected member query error", index)
		}
	}
	if _, err := NewStore(Executor{begin: failingBegin(protocolErr)}).
		ListExpiredMembers(context.Background(), fixedTime()); err == nil {
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
	if _, err := store.AggregateRunnerPoolCapacity(context.Background(), "linux", fixedTime()); err == nil {
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
	if err := operation(NewStore(Executor{begin: failingBegin(protocolErr)})); err == nil {
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

func failingBegin(protocolErr error) func(context.Context) (Transaction, error) {
	return func(context.Context) (Transaction, error) { return Transaction{}, protocolErr }
}
