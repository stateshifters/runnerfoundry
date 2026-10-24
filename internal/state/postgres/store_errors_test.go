package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestEnqueueReportsTransactionalFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"},
		Scope: core.DemandScope{Adapter: "github", ID: "repository-linux"}, RunnerPool: "linux", EnqueuedAt: fixedTime()}
	tests := []namedTransaction{
		{name: "demand domain lock", transaction: transactionFailingAtExecute(1, protocolErr)},
		{name: "demand scope lock", transaction: transactionFailingAtExecute(2, protocolErr)},
		{name: "source lock", transaction: transactionFailingAtExecute(3, protocolErr)},
		{name: "insert scan", transaction: transactionWithRows([]rowStub{rowStub{scan: func(...any) error { return protocolErr }}}, nil)},
		{name: "outcome scan", transaction: transactionWithRows([]rowStub{rowStub{scan: func(...any) error { return sql.ErrNoRows }},
			rowStub{scan: func(...any) error { return protocolErr }}}, nil)},
		{name: "commit", transaction: transactionWithRows([]rowStub{rowFromValues(queuedJobValues(testJobID1, request))}, protocolErr)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			store := newStore(executorStub{begin: transactionBeginning(testCase.transaction)})
			if _, err := store.Queue(context.Background(), request); err == nil {
				t.Fatal("expected enqueue error")
			}
		})
	}
}

func TestTakeOldestReportsTransactionalFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"},
		RunnerPool: "linux", EnqueuedAt: fixedTime()}
	candidate := rowFromValues(queuedJobValues(testJobID1, request))
	tests := []namedTransaction{
		{name: "Runner Pool lock", transaction: transactionFailingAtExecute(1, protocolErr)},
		{name: "candidate scan", transaction: transactionWithRows([]rowStub{rowStub{scan: func(...any) error { return protocolErr }}}, nil)},
		{name: "empty commit", transaction: transactionWithRows([]rowStub{rowStub{scan: func(...any) error { return sql.ErrNoRows }}}, protocolErr)},
		{name: "source lock", transaction: transactionFailingAtExecuteWithRows(2, protocolErr, candidate)},
		{name: "locked candidate scan", transaction: transactionWithRows([]rowStub{candidate, rowStub{scan: func(...any) error { return protocolErr }}}, nil)},
		{name: "processing insertion", transaction: transactionWithRows([]rowStub{
			candidate, candidate, {scan: func(...any) error { return protocolErr }},
		}, nil)},
		{name: "commit", transaction: transactionWithRows([]rowStub{
			candidate, candidate, rowFromValues([]any{validProcessingClaim().ClaimedAt}),
		}, protocolErr)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			store := newStore(executorStub{begin: transactionBeginning(testCase.transaction)})
			if _, err := store.ClaimOldest(context.Background(), "linux", validProcessingClaim()); err == nil {
				t.Fatal("expected pickup error")
			}
		})
	}
}

func TestTakeOldestHandlesCandidateCancelledWhileWaitingForSourceLock(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"},
		RunnerPool: "linux", EnqueuedAt: fixedTime()}
	transaction := transactionWithRows([]rowStub{
		rowFromValues(queuedJobValues(testJobID1, request)),
		rowStub{scan: func(...any) error { return sql.ErrNoRows }},
	}, nil)
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		ClaimOldest(context.Background(), "linux", validProcessingClaim())
	if err != nil || result.Outcome != core.ClaimEmpty || result.Processing != (core.ProcessingJob{}) {
		t.Fatalf("vanished claim = %#v, %v", result, err)
	}
	commitFailure := transactionWithRows([]rowStub{
		rowFromValues(queuedJobValues(testJobID1, request)),
		rowStub{scan: func(...any) error { return sql.ErrNoRows }},
	}, errors.New("commit"))
	if _, err := newStore(executorStub{begin: transactionBeginning(commitFailure)}).
		ClaimOldest(context.Background(), "linux", validProcessingClaim()); err == nil {
		t.Fatal("expected vanished pickup commit error")
	}
}

func TestTerminalizeSourceReportsTransactionalFailures(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request"}
	assertTransactionalMutationFailures(t, []string{"serialize source", "tombstone source", "remove queued job"},
		func(store *Store) error {
			_, err := store.Terminalize(context.Background(), core.JobIdentity{Source: source})
			return err
		})
}

func assertTransactionalMutationFailures(t *testing.T, steps []string, operation func(*Store) error) {
	t.Helper()
	protocolErr := errors.New("protocol")
	for index, name := range steps {
		t.Run(name, func(t *testing.T) {
			store := newStore(executorStub{begin: transactionBeginning(transactionFailingAtExecute(index+1, protocolErr))})
			if err := operation(store); err == nil {
				t.Fatal("expected mutation failure")
			}
		})
	}
	transaction := transactionStub{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if err := operation(newStore(executorStub{begin: transactionBeginning(transaction)})); err == nil {
		t.Fatal("expected mutation commit failure")
	}
}

func transactionWithRows(rows []rowStub, commitErr error) transactionStub {
	index := 0
	return transactionStub{
		execute: successfulExecute,
		queryRow: func(context.Context, string, ...any) rowStub {
			row := rows[index]
			index++
			return row
		},
		commit: func(context.Context) error { return commitErr }, rollback: successfulCompletion,
	}
}

func transactionFailingAtExecute(failureCall int, protocolErr error) transactionStub {
	return transactionFailingAtExecuteWithRows(failureCall, protocolErr)
}

func transactionFailingAtExecuteWithRows(failureCall int, protocolErr error, rows ...rowStub) transactionStub {
	transaction := transactionWithRows(rows, nil)
	executeCalls := 0
	transaction.execute = func(ctx context.Context, statement string, arguments ...any) (resultStub, error) {
		executeCalls++
		if executeCalls == failureCall {
			return resultStub{}, protocolErr
		}
		return successfulExecute(ctx, statement, arguments...)
	}
	return transaction
}
