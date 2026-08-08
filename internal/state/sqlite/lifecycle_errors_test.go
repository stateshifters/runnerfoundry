package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestMarkRunnerStartedReportsEveryTransactionalFailure(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request"}
	assertTransactionalMutationFailures(t, []string{"mark processing started", "tombstone delayed start", "remove queued retry"},
		func(store *Store) error {
			_, err := store.MarkRunnerStarted(context.Background(), core.JobIdentity{Source: source})
			return err
		})
}

func TestRetryUnstartedProcessingHandlesIneligibleTransition(t *testing.T) {
	t.Parallel()
	ineligible := Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return 0, nil }}, nil
	}, queryRow: constantRow(Row{scan: func(...any) error { return sql.ErrNoRows }}),
		commit: successfulCompletion, rollback: successfulCompletion}
	if result, err := NewStore(Executor{begin: transactionBeginning(ineligible)}).
		RetryUnstarted(context.Background(), testJobID); err != nil || result.Outcome != core.RetryMissing {
		t.Fatalf("ineligible retry=%#v error=%v", result, err)
	}
}

func TestRetryUnstartedProcessingReportsInsertFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if _, err := NewStore(Executor{begin: failingBegin(protocolErr)}).
		RetryUnstarted(context.Background(), testJobID); err == nil {
		t.Fatal("expected processing retry begin failure")
	}
	tests := []namedTransaction{
		{name: "queue insertion", transaction: Transaction{queryRow: constantRow(rowFromValues(unstartedProcessingValues())), execute: failingExecute(protocolErr), rollback: successfulCompletion}},
		{name: "insertion result", transaction: Transaction{execute: func(context.Context, string, ...any) (Result, error) {
			return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
		}, queryRow: constantRow(rowFromValues(unstartedProcessingValues())), rollback: successfulCompletion}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(Executor{begin: transactionBeginning(testCase.transaction)}).
				RetryUnstarted(context.Background(), testJobID); err == nil {
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
	if _, err := NewStore(Executor{begin: transactionBeginning(deleteFailure)}).
		RetryUnstarted(context.Background(), testJobID); err == nil {
		t.Fatal("expected processing removal failure")
	}
}

func TestRetryUnstartedProcessingReportsCandidateClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := Transaction{queryRow: constantRow(Row{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion}
	assertSQLiteRetryFailure(t, transaction)
}

func TestRetryUnstartedProcessingReportsLostGuardClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := Transaction{queryRow: rowsInOrder(rowFromValues(unstartedProcessingValues()),
		Row{scan: func(...any) error { return protocolErr }}), execute: outcomeExecute(0), rollback: successfulCompletion}
	assertSQLiteRetryFailure(t, transaction)
}

func TestRetryUnstartedProcessingReportsRemovalCountFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := Transaction{queryRow: constantRow(rowFromValues(unstartedProcessingValues())),
		execute: successfulThenRowsAffectedError(protocolErr), rollback: successfulCompletion}
	assertSQLiteRetryFailure(t, transaction)
}

func assertSQLiteRetryFailure(t *testing.T, transaction Transaction) {
	t.Helper()
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), testJobID); err == nil {
		t.Fatal("expected retry failure")
	}
}

func TestRetryUnstartedProcessingReportsCommitFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	ineligibleCommit := Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return 0, nil }}, nil
	}, queryRow: constantRow(Row{scan: func(...any) error { return sql.ErrNoRows }}),
		commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(ineligibleCommit)}).
		RetryUnstarted(context.Background(), testJobID); err == nil {
		t.Fatal("expected ineligible retry commit failure")
	}
	successfulCommit := Transaction{queryRow: constantRow(rowFromValues(unstartedProcessingValues())), execute: successfulExecute,
		commit: failingCompletion(protocolErr), rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(successfulCommit)}).
		RetryUnstarted(context.Background(), testJobID); err == nil {
		t.Fatal("expected successful retry commit failure")
	}
}

func TestRemoveTerminalProcessingReportsMutationFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	tests := []struct {
		name     string
		executor Executor
	}{
		{name: "begin", executor: Executor{begin: failingBegin(protocolErr)}},
		{name: "removal", executor: Executor{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr), rollback: successfulCompletion})}},
		{name: "commit", executor: Executor{begin: transactionBeginning(Transaction{execute: successfulExecute,
			commit: failingCompletion(protocolErr), rollback: successfulCompletion})}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(testCase.executor).RemoveTerminal(context.Background(), testJobID); err == nil {
				t.Fatal("expected terminal removal failure")
			}
		})
	}
}

func TestRemoveTerminalProcessingReportsRowsAffectedFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
	}, rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RemoveTerminal(context.Background(), testJobID); err == nil {
		t.Fatal("expected affected-row failure")
	}
}

func TestRemoveTerminalProcessingReportsClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := Transaction{execute: outcomeExecute(0),
		queryRow: constantRow(Row{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RemoveTerminal(context.Background(), testJobID); err == nil {
		t.Fatal("expected terminal classification failure")
	}
}

func TestQueueReportsClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	rows := []Row{{scan: func(...any) error { return sql.ErrNoRows }}, {scan: func(...any) error { return protocolErr }}}
	transaction := Transaction{execute: outcomeExecute(0), queryRow: func(context.Context, string, ...any) Row {
		row := rows[0]
		rows = rows[1:]
		return row
	}, rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).Queue(context.Background(), core.JobRequest{
		Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux",
	}); err == nil {
		t.Fatal("expected queue classification failure")
	}
}

func TestRecoverUnstartedReportsClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID}, ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	transaction := Transaction{execute: outcomeExecute(0),
		queryRow: constantRow(Row{scan: func(...any) error { return protocolErr }}), rollback: successfulCompletion}
	assertSQLiteRecoveryFailure(t, processing, transaction)
}

func TestRecoverUnstartedReportsRemovalCountFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID}, ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	transaction := Transaction{execute: successfulThenRowsAffectedError(protocolErr), rollback: successfulCompletion}
	assertSQLiteRecoveryFailure(t, processing, transaction)
}

func assertSQLiteRecoveryFailure(t *testing.T, processing core.ProcessingJob, transaction Transaction) {
	t.Helper()
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RecoverUnstartedProcessing(context.Background(), processing); err == nil {
		t.Fatal("expected recovery failure")
	}
}

func rowsInOrder(rows ...Row) func(context.Context, string, ...any) Row {
	return func(context.Context, string, ...any) Row {
		row := rows[0]
		rows = rows[1:]
		return row
	}
}

func successfulThenRowsAffectedError(wantErr error) func(context.Context, string, ...any) (Result, error) {
	calls := 0
	return func(context.Context, string, ...any) (Result, error) {
		calls++
		if calls == 2 {
			return Result{rowsAffected: func() (int64, error) { return 0, wantErr }}, nil
		}
		return Result{rowsAffected: func() (int64, error) { return 1, nil }}, nil
	}
}
