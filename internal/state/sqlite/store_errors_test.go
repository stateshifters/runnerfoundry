package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestEnqueueProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux", EnqueuedAt: fixedTime()}
	tests := []namedTransaction{
		{name: "insert", transaction: transactionStub{execute: failingExecute(protocolErr), rollback: successfulCompletion}},
		{name: "insert result", transaction: transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
			return resultStub{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
		}, rollback: successfulCompletion}},
		{name: "outcome scan", transaction: transactionStub{execute: successfulExecute, queryRow: constantRow(rowStub{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion}},
		{name: "commit", transaction: transactionStub{execute: successfulExecute, queryRow: constantRow(rowFromValues(queuedJobValues())), commit: failingCompletion(protocolErr), rollback: successfulCompletion}},
	}
	if _, err := newStore(executorStub{begin: failingBegin(protocolErr)}).Queue(context.Background(), request); err == nil {
		t.Fatal("expected begin failure")
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newStore(executorStub{begin: transactionBeginning(testCase.transaction)}).Queue(context.Background(), request); err == nil {
				t.Fatal("expected enqueue failure")
			}
		})
	}
}

func TestEnqueueIgnoresAnExistingTombstone(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux", EnqueuedAt: fixedTime()}
	queryCalls := 0
	tombstoned := transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
		return resultStub{rowsAffected: func() (int64, error) { return 0, nil }}, nil
	}, queryRow: func(context.Context, string, ...any) rowStub {
		queryCalls++
		if queryCalls == 1 {
			return rowStub{scan: func(...any) error { return sql.ErrNoRows }}
		}
		return rowFromValues([]any{string(core.QueueSourceTerminal)})
	}, commit: successfulCompletion,
		rollback: successfulCompletion}
	result, err := newStore(executorStub{begin: transactionBeginning(tombstoned)}).Queue(context.Background(), request)
	if err != nil || result.Outcome != core.QueueSourceTerminal || result.Job != (core.Job{}) {
		t.Fatalf("tombstoned queue=%#v %v", result, err)
	}
}

func TestTakeOldestProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if _, err := newStore(executorStub{begin: failingBegin(protocolErr)}).ClaimOldest(context.Background(), "linux", validProcessingClaim()); err == nil {
		t.Fatal("expected take begin failure")
	}
	selectionFailure := transactionStub{queryRow: constantRow(rowStub{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion}
	if _, err := newStore(executorStub{begin: transactionBeginning(selectionFailure)}).ClaimOldest(context.Background(), "linux", validProcessingClaim()); err == nil {
		t.Fatal("expected selection failure")
	}
	emptyCommitFailure := transactionStub{queryRow: constantRow(rowStub{scan: func(...any) error { return sql.ErrNoRows }}),
		commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if _, err := newStore(executorStub{begin: transactionBeginning(emptyCommitFailure)}).ClaimOldest(context.Background(), "linux", validProcessingClaim()); err == nil {
		t.Fatal("expected empty commit failure")
	}
	assertClaimMutationFailures(t, protocolErr)
	commitFailure := transactionStub{queryRow: constantRow(rowFromValues(queuedJobValues())), execute: successfulExecute,
		commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if _, err := newStore(executorStub{begin: transactionBeginning(commitFailure)}).ClaimOldest(context.Background(), "linux",
		validProcessingClaim()); err == nil {
		t.Fatal("expected move commit failure")
	}
}

func assertClaimMutationFailures(t *testing.T, protocolErr error) {
	t.Helper()
	for name, failureCall := range map[string]int{"processing insertion": 1, "queue removal": 2} {
		t.Run(name, func(t *testing.T) {
			transaction := transactionFailingExecute(failureCall, protocolErr)
			transaction.queryRow = constantRow(rowFromValues(queuedJobValues()))
			if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).ClaimOldest(
				context.Background(), "linux", validProcessingClaim(),
			); err == nil {
				t.Fatal("expected move failure")
			}
		})
	}
}

func TestTerminalizeSourceProtocolFailures(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request"}
	assertTransactionalMutationFailures(t, []string{"tombstone source", "remove queued job"},
		func(store *Store) error {
			_, err := store.Terminalize(context.Background(), core.JobIdentity{Source: source})
			return err
		})
}

func assertTransactionalMutationFailures(t *testing.T, steps []string, operation func(*Store) error) {
	t.Helper()
	protocolErr := errors.New("protocol")
	if err := operation(newStore(executorStub{begin: failingBegin(protocolErr)})); err == nil {
		t.Fatal("expected mutation begin failure")
	}
	for index, name := range steps {
		t.Run(name, func(t *testing.T) {
			transaction := transactionFailingExecute(index+1, protocolErr)
			if err := operation(newStore(executorStub{begin: transactionBeginning(transaction)})); err == nil {
				t.Fatal("expected mutation failure")
			}
		})
	}
	transaction := transactionStub{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if err := operation(newStore(executorStub{begin: transactionBeginning(transaction)})); err == nil {
		t.Fatal("expected mutation commit failure")
	}
}

func TestProcessingLookupProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if _, err := newStore(executorStub{begin: failingBegin(protocolErr)}).ProcessingByMember(context.Background(), testMemberID, "linux"); err == nil {
		t.Fatal("expected processing begin failure")
	}
	for _, testCase := range namedFailingListTransactions(protocolErr) {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newStore(executorStub{begin: transactionBeginning(testCase.transaction)}).ProcessingByMember(context.Background(), testMemberID, "linux"); err == nil {
				t.Fatal("expected processing failure")
			}
		})
	}
}

func constantRow(row rowStub) func(context.Context, string, ...any) rowStub {
	return func(context.Context, string, ...any) rowStub { return row }
}

func failingBegin(protocolErr error) func(context.Context) (transactionStub, error) {
	return func(context.Context) (transactionStub, error) { return transactionStub{}, protocolErr }
}

func failingExecute(protocolErr error) func(context.Context, string, ...any) (resultStub, error) {
	return func(context.Context, string, ...any) (resultStub, error) { return resultStub{}, protocolErr }
}

func failingCompletion(protocolErr error) func(context.Context) error {
	return func(context.Context) error { return protocolErr }
}

func transactionFailingExecute(failureCall int, protocolErr error) transactionStub {
	calls := 0
	return transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
		calls++
		if calls == failureCall {
			return resultStub{}, protocolErr
		}
		return successfulResult(), nil
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
		},
		scan: func(...any) error { return scanErr }}
}
