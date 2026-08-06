package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestEnqueueReportsTransactionalFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	tests := []Transaction{
		transactionFailingAtExecute(1, protocolErr),
		transactionWithRows([]Row{{scan: func(...any) error { return protocolErr }}}, nil),
		transactionWithRows([]Row{{scan: func(...any) error { return sql.ErrNoRows }},
			{scan: func(...any) error { return protocolErr }}}, nil),
		transactionWithRows([]Row{rowFromValues(queuedJobValues(testJobID1, request))}, protocolErr),
	}
	for index, transaction := range tests {
		store := NewStore(Executor{begin: transactionBeginning(transaction)})
		if _, err := store.Queue(context.Background(), request); err == nil {
			t.Fatalf("case %d: expected enqueue error", index)
		}
	}
}

func TestTakeOldestReportsTransactionalFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	candidate := rowFromValues(queuedJobValues(testJobID1, request))
	tests := []Transaction{
		transactionFailingAtExecute(1, protocolErr),
		transactionWithRows([]Row{{scan: func(...any) error { return protocolErr }}}, nil),
		transactionWithRows([]Row{{scan: func(...any) error { return sql.ErrNoRows }}}, protocolErr),
		transactionFailingAtExecuteWithRows(2, protocolErr, candidate),
		transactionWithRows([]Row{candidate, {scan: func(...any) error { return protocolErr }}}, nil),
		transactionFailingAtExecuteWithRows(3, protocolErr, candidate, candidate),
		transactionWithRows([]Row{candidate, candidate}, protocolErr),
	}
	for index, transaction := range tests {
		store := NewStore(Executor{begin: transactionBeginning(transaction)})
		if _, err := store.ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberIDA}); err == nil {
			t.Fatalf("case %d: expected pickup error", index)
		}
	}
}

func TestTakeOldestHandlesCandidateCancelledWhileWaitingForSourceLock(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	transaction := transactionWithRows([]Row{
		rowFromValues(queuedJobValues(testJobID1, request)),
		{scan: func(...any) error { return sql.ErrNoRows }},
	}, nil)
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberIDA})
	if err != nil || result.Taken || result.Processing != (core.ProcessingJob{}) {
		t.Fatalf("vanished claim = %#v, %v", result, err)
	}
	commitFailure := transactionWithRows([]Row{
		rowFromValues(queuedJobValues(testJobID1, request)),
		{scan: func(...any) error { return sql.ErrNoRows }},
	}, errors.New("commit"))
	if _, err := NewStore(Executor{begin: transactionBeginning(commitFailure)}).
		ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberIDA}); err == nil {
		t.Fatal("expected vanished pickup commit error")
	}
}

func TestTerminalizeSourceReportsTransactionalFailures(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request"}
	assertTransactionalMutationFailures(t, 3,
		func(store *Store) error { return store.TerminalizeSource(context.Background(), source) })
}

func assertTransactionalMutationFailures(t *testing.T, executeCalls int, operation func(*Store) error) {
	t.Helper()
	protocolErr := errors.New("protocol")
	for failureCall := 1; failureCall <= executeCalls; failureCall++ {
		store := NewStore(Executor{begin: transactionBeginning(transactionFailingAtExecute(failureCall, protocolErr))})
		if err := operation(store); err == nil {
			t.Fatalf("execute %d: expected mutation failure", failureCall)
		}
	}
	transaction := Transaction{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if err := operation(NewStore(Executor{begin: transactionBeginning(transaction)})); err == nil {
		t.Fatal("expected mutation commit failure")
	}
}

func transactionWithRows(rows []Row, commitErr error) Transaction {
	index := 0
	return Transaction{
		execute: successfulExecute,
		queryRow: func(context.Context, string, ...any) Row {
			row := rows[index]
			index++
			return row
		},
		commit: func(context.Context) error { return commitErr }, rollback: successfulCompletion,
	}
}

func transactionFailingAtExecute(failureCall int, protocolErr error) Transaction {
	return transactionFailingAtExecuteWithRows(failureCall, protocolErr)
}

func transactionFailingAtExecuteWithRows(failureCall int, protocolErr error, rows ...Row) Transaction {
	transaction := transactionWithRows(rows, nil)
	executeCalls := 0
	transaction.execute = func(ctx context.Context, statement string, arguments ...any) (Result, error) {
		executeCalls++
		if executeCalls == failureCall {
			return Result{}, protocolErr
		}
		return successfulExecute(ctx, statement, arguments...)
	}
	return transaction
}
