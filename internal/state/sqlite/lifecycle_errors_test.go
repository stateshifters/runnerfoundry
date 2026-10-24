package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestObserveRunnerAcceptanceReportsEveryTransactionalFailure(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request"}
	assertTransactionalMutationFailures(t, []string{"mark processing accepted", "tombstone delayed acceptance", "remove queued retry"},
		func(store *Store) error {
			_, err := store.ObserveRunnerAcceptance(context.Background(), core.JobIdentity{Source: source})
			return err
		})
}

func TestConditionalTerminalObservationReportsTransactionalFailures(t *testing.T) {
	protocolErr := errors.New("protocol")
	identity := core.JobIdentity{JobID: testJobID,
		Source: core.SourceReference{Adapter: "github", ID: "aggregate"}}
	source := rowFromValues([]any{"github", "aggregate"})
	insertFailure := transactionFailingExecute(1, protocolErr)
	insertFailure.queryRow = constantRow(source)
	deleteFailure := transactionFailingExecute(2, protocolErr)
	deleteFailure.queryRow = constantRow(source)
	classificationFailure := transactionStub{
		queryRow: rowsInOrder(source, rowStub{scan: func(...any) error { return protocolErr }}),
		execute:  outcomeExecute(0), rollback: successfulCompletion,
	}
	for name, executor := range map[string]executorStub{
		"begin":          {begin: failingBegin(protocolErr)},
		"tombstone":      {begin: transactionBeginning(insertFailure)},
		"queued removal": {begin: transactionBeginning(deleteFailure)},
		"classification": {begin: transactionBeginning(classificationFailure)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := newStore(executor).ObserveTerminalIfUnaccepted(context.Background(), identity); !errors.Is(
				err, protocolErr,
			) {
				t.Fatalf("conditional terminal error = %v, want %v", err, protocolErr)
			}
		})
	}
}

func TestRetryUnstartedProcessingHandlesIneligibleTransition(t *testing.T) {
	t.Parallel()
	ineligible := transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
		return resultStub{rowsAffected: func() (int64, error) { return 0, nil }}, nil
	}, queryRow: constantRow(rowStub{scan: func(...any) error { return sql.ErrNoRows }}),
		commit: successfulCompletion, rollback: successfulCompletion}
	if result, err := newStore(executorStub{begin: transactionBeginning(ineligible)}).
		RetryUnstarted(context.Background(), retryProcessingJob()); err != nil || result.Outcome != core.RetryMissing {
		t.Fatalf("ineligible retry=%#v error=%v", result, err)
	}
}

func TestRetryUnstartedProcessingReportsInsertFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if _, err := newStore(executorStub{begin: failingBegin(protocolErr)}).
		RetryUnstarted(context.Background(), retryProcessingJob()); err == nil {
		t.Fatal("expected processing retry begin failure")
	}
	tests := []namedTransaction{
		{name: "queue insertion", transaction: transactionStub{queryRow: constantRow(rowFromValues(unstartedProcessingValues())), execute: failingExecute(protocolErr), rollback: successfulCompletion}},
		{name: "insertion result", transaction: transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
			return resultStub{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
		}, queryRow: constantRow(rowFromValues(unstartedProcessingValues())), rollback: successfulCompletion}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newStore(executorStub{begin: transactionBeginning(testCase.transaction)}).
				RetryUnstarted(context.Background(), retryProcessingJob()); err == nil {
				t.Fatal("expected retry insertion failure")
			}
		})
	}
}

func TestRetryUnstartedProcessingReportsRemovalFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	deleteFailure := transactionFailingExecute(2, protocolErr)
	deleteFailure.queryRow = constantRow(rowFromValues(unstartedProcessingValues()))
	if _, err := newStore(executorStub{begin: transactionBeginning(deleteFailure)}).
		RetryUnstarted(context.Background(), retryProcessingJob()); err == nil {
		t.Fatal("expected processing removal failure")
	}
}

func TestRetryUnstartedProcessingReportsCandidateClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := transactionStub{queryRow: constantRow(rowStub{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion}
	assertSQLiteRetryFailure(t, transaction)
}

func TestRetryUnstartedProcessingReportsLostGuardClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := transactionStub{queryRow: rowsInOrder(rowFromValues(unstartedProcessingValues()),
		rowStub{scan: func(...any) error { return protocolErr }}), execute: outcomeExecute(0), rollback: successfulCompletion}
	assertSQLiteRetryFailure(t, transaction)
}

func TestRetryUnstartedProcessingReportsRemovalCountFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := transactionStub{queryRow: constantRow(rowFromValues(unstartedProcessingValues())),
		execute: successfulThenRowsAffectedError(protocolErr), rollback: successfulCompletion}
	assertSQLiteRetryFailure(t, transaction)
}

func assertSQLiteRetryFailure(t *testing.T, transaction transactionStub) {
	t.Helper()
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), retryProcessingJob()); err == nil {
		t.Fatal("expected retry failure")
	}
}

func TestRetryUnstartedProcessingReportsCommitFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	ineligibleCommit := transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
		return resultStub{rowsAffected: func() (int64, error) { return 0, nil }}, nil
	}, queryRow: constantRow(rowStub{scan: func(...any) error { return sql.ErrNoRows }}),
		commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if _, err := newStore(executorStub{begin: transactionBeginning(ineligibleCommit)}).
		RetryUnstarted(context.Background(), retryProcessingJob()); err == nil {
		t.Fatal("expected ineligible retry commit failure")
	}
	successfulCommit := transactionStub{queryRow: constantRow(rowFromValues(unstartedProcessingValues())), execute: successfulExecute,
		commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if _, err := newStore(executorStub{begin: transactionBeginning(successfulCommit)}).
		RetryUnstarted(context.Background(), retryProcessingJob()); err == nil {
		t.Fatal("expected successful retry commit failure")
	}
}

func TestRemoveTerminalProcessingReportsMutationFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	tests := []struct {
		name     string
		executor executorStub
	}{
		{name: "begin", executor: executorStub{begin: failingBegin(protocolErr)}},
		{name: "removal", executor: executorStub{begin: transactionBeginning(transactionStub{execute: failingExecute(protocolErr), rollback: successfulCompletion})}},
		{name: "commit", executor: executorStub{begin: transactionBeginning(transactionStub{execute: successfulExecute,
			commit: failingCompletion(protocolErr), rollback: successfulCompletion})}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newStore(testCase.executor).RemoveTerminal(context.Background(), testJobID); err == nil {
				t.Fatal("expected terminal removal failure")
			}
		})
	}
}

func TestRemoveTerminalProcessingReportsRowsAffectedFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
		return resultStub{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
	}, rollback: successfulCompletion}
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RemoveTerminal(context.Background(), testJobID); err == nil {
		t.Fatal("expected affected-row failure")
	}
}

func TestRemoveTerminalProcessingReportsClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := transactionStub{execute: outcomeExecute(0),
		queryRow: constantRow(rowStub{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion}
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RemoveTerminal(context.Background(), testJobID); err == nil {
		t.Fatal("expected terminal classification failure")
	}
}

func TestQueueReportsClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	rows := []rowStub{rowStub{scan: func(...any) error { return sql.ErrNoRows }}, rowStub{scan: func(...any) error { return protocolErr }}}
	transaction := transactionStub{execute: outcomeExecute(0), queryRow: func(context.Context, string, ...any) rowStub {
		row := rows[0]
		rows = rows[1:]
		return row
	}, rollback: successfulCompletion}
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).Queue(context.Background(), core.JobRequest{
		Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux", EnqueuedAt: fixedTime(),
	}); err == nil {
		t.Fatal("expected queue classification failure")
	}
}

func TestRecoverUnstartedReportsClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID}, ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	transaction := transactionStub{execute: outcomeExecute(0),
		queryRow: constantRow(rowStub{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion}
	assertSQLiteRecoveryFailure(t, processing, transaction)
}

func TestRecoverUnstartedReportsRemovalCountFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID}, ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	transaction := transactionStub{execute: successfulThenRowsAffectedError(protocolErr), rollback: successfulCompletion}
	assertSQLiteRecoveryFailure(t, processing, transaction)
}

func assertSQLiteRecoveryFailure(t *testing.T, processing core.ProcessingJob, transaction transactionStub) {
	t.Helper()
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RecoverUnstartedProcessing(context.Background(), processing); err == nil {
		t.Fatal("expected recovery failure")
	}
}

func rowsInOrder(rows ...rowStub) func(context.Context, string, ...any) rowStub {
	return func(context.Context, string, ...any) rowStub {
		row := rows[0]
		rows = rows[1:]
		return row
	}
}

func successfulThenRowsAffectedError(wantErr error) func(context.Context, string, ...any) (resultStub, error) {
	calls := 0
	return func(context.Context, string, ...any) (resultStub, error) {
		calls++
		if calls == 2 {
			return resultStub{rowsAffected: func() (int64, error) { return 0, wantErr }}, nil
		}
		return resultStub{rowsAffected: func() (int64, error) { return 1, nil }}, nil
	}
}
