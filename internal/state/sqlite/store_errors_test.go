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
	tests := []namedTransaction{
		{name: "insert", transaction: Transaction{execute: failingExecute(protocolErr), rollback: successfulCompletion}},
		{name: "insert result", transaction: Transaction{execute: func(context.Context, string, ...any) (Result, error) {
			return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
		}, rollback: successfulCompletion}},
		{name: "outcome scan", transaction: Transaction{execute: successfulExecute, queryRow: constantRow(Row{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion}},
		{name: "commit", transaction: Transaction{execute: successfulExecute, queryRow: constantRow(rowFromValues(queuedJobValues())), commit: failingCompletion(protocolErr), rollback: successfulCompletion}},
	}
	if _, err := NewStore(Executor{begin: failingBegin(protocolErr)}).Queue(context.Background(), request); err == nil {
		t.Fatal("expected begin failure")
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(Executor{begin: transactionBeginning(testCase.transaction)}).Queue(context.Background(), request); err == nil {
				t.Fatal("expected enqueue failure")
			}
		})
	}
}

func TestEnqueueIgnoresAnExistingTombstone(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	queryCalls := 0
	tombstoned := Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return 0, nil }}, nil
	}, queryRow: func(context.Context, string, ...any) Row {
		queryCalls++
		if queryCalls == 1 {
			return Row{scan: func(...any) error { return sql.ErrNoRows }}
		}
		return rowFromValues([]any{string(core.QueueSourceTerminal)})
	}, commit: successfulCompletion,
		rollback: successfulCompletion}
	result, err := NewStore(Executor{begin: transactionBeginning(tombstoned)}).Queue(context.Background(), request)
	if err != nil || result.Outcome != core.QueueSourceTerminal || result.Job != (core.Job{}) {
		t.Fatalf("tombstoned queue=%#v %v", result, err)
	}
}

func TestTakeOldestProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if _, err := NewStore(Executor{begin: failingBegin(protocolErr)}).ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberID}); err == nil {
		t.Fatal("expected take begin failure")
	}
	selectionFailure := Transaction{queryRow: constantRow(Row{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(selectionFailure)}).ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberID}); err == nil {
		t.Fatal("expected selection failure")
	}
	emptyCommitFailure := Transaction{queryRow: constantRow(Row{scan: func(...any) error { return sql.ErrNoRows }}),
		commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(emptyCommitFailure)}).ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberID}); err == nil {
		t.Fatal("expected empty commit failure")
	}
	commitFailure := Transaction{queryRow: constantRow(rowFromValues(queuedJobValues())), execute: successfulExecute,
		commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(commitFailure)}).ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberID}); err == nil {
		t.Fatal("expected move commit failure")
	}
}

func TestTerminalizeSourceProtocolFailures(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request"}
	assertTransactionalMutationFailures(t, []string{"tombstone source", "remove queued job"},
		func(store *Store) error { return store.TerminalizeSource(context.Background(), source) })
}

func assertTransactionalMutationFailures(t *testing.T, steps []string, operation func(*Store) error) {
	t.Helper()
	protocolErr := errors.New("protocol")
	if err := operation(NewStore(Executor{begin: failingBegin(protocolErr)})); err == nil {
		t.Fatal("expected mutation begin failure")
	}
	for index, name := range steps {
		t.Run(name, func(t *testing.T) {
			transaction := transactionFailingExecute(index+1, protocolErr)
			if err := operation(NewStore(Executor{begin: transactionBeginning(transaction)})); err == nil {
				t.Fatal("expected mutation failure")
			}
		})
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
	for _, testCase := range namedFailingListTransactions(protocolErr) {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(Executor{begin: transactionBeginning(testCase.transaction)}).ProcessingByMember(context.Background(), testMemberID, "linux"); err == nil {
				t.Fatal("expected processing failure")
			}
		})
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
