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
	tests := []namedTransaction{
		{name: "insert", transaction: transactionFailingAtExecute(1, protocolErr)},
		{name: "existing job scan", transaction: transactionWithRows([]Row{{scan: func(...any) error { return protocolErr }}}, nil)},
		{name: "outcome scan", transaction: transactionWithRows([]Row{{scan: func(...any) error { return sql.ErrNoRows }},
			{scan: func(...any) error { return protocolErr }}}, nil)},
		{name: "commit", transaction: transactionWithRows([]Row{rowFromValues(queuedJobValues(testJobID1, request))}, protocolErr)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			store := NewStore(Executor{begin: transactionBeginning(testCase.transaction)})
			if _, err := store.Queue(context.Background(), request); err == nil {
				t.Fatal("expected enqueue error")
			}
		})
	}
}

func TestTakeOldestReportsTransactionalFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	candidate := rowFromValues(queuedJobValues(testJobID1, request))
	tests := []namedTransaction{
		{name: "source lock", transaction: transactionFailingAtExecute(1, protocolErr)},
		{name: "candidate scan", transaction: transactionWithRows([]Row{{scan: func(...any) error { return protocolErr }}}, nil)},
		{name: "empty commit", transaction: transactionWithRows([]Row{{scan: func(...any) error { return sql.ErrNoRows }}}, protocolErr)},
		{name: "processing insertion", transaction: transactionFailingAtExecuteWithRows(2, protocolErr, candidate)},
		{name: "locked candidate scan", transaction: transactionWithRows([]Row{candidate, {scan: func(...any) error { return protocolErr }}}, nil)},
		{name: "queue removal", transaction: transactionFailingAtExecuteWithRows(3, protocolErr, candidate, candidate)},
		{name: "commit", transaction: transactionWithRows([]Row{candidate, candidate}, protocolErr)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			store := NewStore(Executor{begin: transactionBeginning(testCase.transaction)})
			if _, err := store.ClaimOldest(context.Background(), "linux", core.ProcessingClaim{MemberID: testMemberIDA}); err == nil {
				t.Fatal("expected pickup error")
			}
		})
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
	if err != nil || result.Outcome != core.ClaimEmpty || result.Processing != (core.ProcessingJob{}) {
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
	assertTransactionalMutationFailures(t, []string{"serialize source", "tombstone source", "remove queued job"},
		func(store *Store) error { return store.TerminalizeSource(context.Background(), source) })
}

func assertTransactionalMutationFailures(t *testing.T, steps []string, operation func(*Store) error) {
	t.Helper()
	protocolErr := errors.New("protocol")
	for index, name := range steps {
		t.Run(name, func(t *testing.T) {
			store := NewStore(Executor{begin: transactionBeginning(transactionFailingAtExecute(index+1, protocolErr))})
			if err := operation(store); err == nil {
				t.Fatal("expected mutation failure")
			}
		})
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
