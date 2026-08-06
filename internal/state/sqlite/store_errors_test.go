package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestEnqueueProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	transactions := []Transaction{
		{execute: failingExecute(protocolErr), rollback: successfulCompletion},
		{execute: func(context.Context, string, ...any) (Result, error) {
			return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
		}, rollback: successfulCompletion},
		{execute: successfulExecute, queryRow: constantRow(Row{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion},
		{execute: successfulExecute, queryRow: constantRow(rowFromValues(queuedJobValues())), commit: failingCompletion(protocolErr), rollback: successfulCompletion},
	}
	if _, _, err := NewStore(Executor{begin: failingBegin(protocolErr)}).Enqueue(context.Background(), request); err == nil {
		t.Fatal("expected begin failure")
	}
	for index, transaction := range transactions {
		if _, _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).Enqueue(context.Background(), request); err == nil {
			t.Fatalf("case %d: expected enqueue failure", index)
		}
	}
}

func TestEnqueueIgnoresAnExistingTombstone(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	tombstoned := Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return 0, nil }}, nil
	}, queryRow: constantRow(Row{scan: func(...any) error { return sql.ErrNoRows }}), commit: successfulCompletion,
		rollback: successfulCompletion}
	job, created, err := NewStore(Executor{begin: transactionBeginning(tombstoned)}).Enqueue(context.Background(), request)
	if err != nil || created || job != (core.Job{}) {
		t.Fatalf("tombstoned enqueue=%#v %t %v", job, created, err)
	}
}

func TestTakeOldestProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if _, _, err := NewStore(Executor{begin: failingBegin(protocolErr)}).TakeOldest(context.Background(), "linux", testMemberID); err == nil {
		t.Fatal("expected take begin failure")
	}
	selectionFailure := Transaction{queryRow: constantRow(Row{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion}
	if _, _, err := NewStore(Executor{begin: transactionBeginning(selectionFailure)}).TakeOldest(context.Background(), "linux", testMemberID); err == nil {
		t.Fatal("expected selection failure")
	}
	emptyCommitFailure := Transaction{queryRow: constantRow(Row{scan: func(...any) error { return sql.ErrNoRows }}),
		commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if _, _, err := NewStore(Executor{begin: transactionBeginning(emptyCommitFailure)}).TakeOldest(context.Background(), "linux", testMemberID); err == nil {
		t.Fatal("expected empty commit failure")
	}
	commitFailure := Transaction{queryRow: constantRow(rowFromValues(queuedJobValues())), execute: successfulExecute,
		commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if _, _, err := NewStore(Executor{begin: transactionBeginning(commitFailure)}).TakeOldest(context.Background(), "linux", testMemberID); err == nil {
		t.Fatal("expected move commit failure")
	}
}

func TestTerminalizeSourceProtocolFailures(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request"}
	assertTransactionalMutationFailures(t, 2,
		func(store *Store) error { return store.TerminalizeSource(context.Background(), source) })
}

func assertTransactionalMutationFailures(t *testing.T, executeCalls int, operation func(*Store) error) {
	t.Helper()
	protocolErr := errors.New("protocol")
	if err := operation(NewStore(Executor{begin: failingBegin(protocolErr)})); err == nil {
		t.Fatal("expected mutation begin failure")
	}
	for failureCall := 1; failureCall <= executeCalls; failureCall++ {
		transaction := transactionFailingExecute(failureCall, protocolErr)
		if err := operation(NewStore(Executor{begin: transactionBeginning(transaction)})); err == nil {
			t.Fatalf("execute %d: expected mutation failure", failureCall)
		}
	}
	transaction := Transaction{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if err := operation(NewStore(Executor{begin: transactionBeginning(transaction)})); err == nil {
		t.Fatal("expected mutation commit failure")
	}
}

func TestProcessingLookupProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if _, err := NewStore(Executor{begin: failingBegin(protocolErr)}).ProcessingByMember(context.Background(), testMemberID, "linux"); err == nil {
		t.Fatal("expected processing begin failure")
	}
	for index, transaction := range failingListTransactions(protocolErr) {
		if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).ProcessingByMember(context.Background(), testMemberID, "linux"); err == nil {
			t.Fatalf("case %d: expected processing failure", index)
		}
	}
}

func constantRow(row Row) func(context.Context, string, ...any) Row {
	return func(context.Context, string, ...any) Row { return row }
}

func failingBegin(protocolErr error) func(context.Context) (Transaction, error) {
	return func(context.Context) (Transaction, error) { return Transaction{}, protocolErr }
}

func failingExecute(protocolErr error) func(context.Context, string, ...any) (Result, error) {
	return func(context.Context, string, ...any) (Result, error) { return Result{}, protocolErr }
}

func failingCompletion(protocolErr error) func(context.Context) error {
	return func(context.Context) error { return protocolErr }
}

func transactionFailingExecute(failureCall int, protocolErr error) Transaction {
	calls := 0
	return Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		calls++
		if calls == failureCall {
			return Result{}, protocolErr
		}
		return successfulResult(), nil
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
		},
		scan: func(...any) error { return scanErr }}
}
