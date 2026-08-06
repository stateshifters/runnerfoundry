package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestMarkRunnerStartedReportsEveryTransactionalFailure(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request"}
	assertTransactionalMutationFailures(t, 3,
		func(store *Store) error { return store.MarkRunnerStarted(context.Background(), source) })
}

func TestRetryUnstartedProcessingHandlesIneligibleTransition(t *testing.T) {
	t.Parallel()
	ineligible := Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return 0, nil }}, nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	if retried, err := NewStore(Executor{begin: transactionBeginning(ineligible)}).
		RetryUnstartedProcessing(context.Background(), testJobID); err != nil || retried {
		t.Fatalf("ineligible retry=%t error=%v", retried, err)
	}
}

func TestRetryUnstartedProcessingReportsInsertFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if _, err := NewStore(Executor{begin: failingBegin(protocolErr)}).
		RetryUnstartedProcessing(context.Background(), testJobID); err == nil {
		t.Fatal("expected processing retry begin failure")
	}
	transactions := []Transaction{
		{execute: failingExecute(protocolErr), rollback: successfulCompletion},
		{execute: func(context.Context, string, ...any) (Result, error) {
			return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
		}, rollback: successfulCompletion},
	}
	for index, transaction := range transactions {
		if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
			RetryUnstartedProcessing(context.Background(), testJobID); err == nil {
			t.Fatalf("case %d: expected retry insertion failure", index)
		}
	}
}

func TestRetryUnstartedProcessingReportsRemovalFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	deleteFailure := transactionFailingExecute(2, protocolErr)
	if _, err := NewStore(Executor{begin: transactionBeginning(deleteFailure)}).
		RetryUnstartedProcessing(context.Background(), testJobID); err == nil {
		t.Fatal("expected processing removal failure")
	}
}

func TestRetryUnstartedProcessingReportsCommitFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	ineligibleCommit := Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return 0, nil }}, nil
	}, commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(ineligibleCommit)}).
		RetryUnstartedProcessing(context.Background(), testJobID); err == nil {
		t.Fatal("expected ineligible retry commit failure")
	}
	successfulCommit := Transaction{execute: successfulExecute,
		commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(successfulCommit)}).
		RetryUnstartedProcessing(context.Background(), testJobID); err == nil {
		t.Fatal("expected successful retry commit failure")
	}
}

func TestRemoveTerminalProcessingReportsMutationFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	executors := []Executor{
		{begin: failingBegin(protocolErr)},
		{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr), rollback: successfulCompletion})},
		{begin: transactionBeginning(Transaction{execute: successfulExecute,
			commit: failingCompletion(protocolErr), rollback: successfulCompletion})},
	}
	for index, executor := range executors {
		if _, err := NewStore(executor).RemoveTerminalProcessing(context.Background(), testJobID); err == nil {
			t.Fatalf("case %d: expected terminal removal failure", index)
		}
	}
}

func TestRemoveTerminalProcessingReportsRowsAffectedFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
	}, rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RemoveTerminalProcessing(context.Background(), testJobID); err == nil {
		t.Fatal("expected affected-row failure")
	}
}
