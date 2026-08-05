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
		if _, _, err := store.Enqueue(context.Background(), request); err == nil {
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
		transactionFailingAtExecuteWithRows(4, protocolErr, candidate, candidate),
		transactionWithRows([]Row{candidate, candidate}, protocolErr),
	}
	for index, transaction := range tests {
		store := NewStore(Executor{begin: transactionBeginning(transaction)})
		if _, _, err := store.TakeOldest(context.Background(), "linux", testMemberIDA); err == nil {
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
	processing, taken, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		TakeOldest(context.Background(), "linux", testMemberIDA)
	if err != nil || taken || processing != (core.ProcessingJob{}) {
		t.Fatalf("vanished pickup = %#v, %t, %v", processing, taken, err)
	}
	commitFailure := transactionWithRows([]Row{
		rowFromValues(queuedJobValues(testJobID1, request)),
		{scan: func(...any) error { return sql.ErrNoRows }},
	}, errors.New("commit"))
	if _, _, err := NewStore(Executor{begin: transactionBeginning(commitFailure)}).
		TakeOldest(context.Background(), "linux", testMemberIDA); err == nil {
		t.Fatal("expected vanished pickup commit error")
	}
}

func TestCancelQueuedReportsTransactionalFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	source := core.SourceReference{Adapter: "github", ID: "request"}
	for failureCall := 1; failureCall <= 3; failureCall++ {
		store := NewStore(Executor{begin: transactionBeginning(transactionFailingAtExecute(failureCall, protocolErr))})
		if err := store.CancelQueued(context.Background(), source); err == nil {
			t.Fatalf("execute %d: expected cancellation error", failureCall)
		}
	}
	transaction := Transaction{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if err := NewStore(Executor{begin: transactionBeginning(transaction)}).CancelQueued(context.Background(), source); err == nil {
		t.Fatal("expected cancellation commit error")
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
