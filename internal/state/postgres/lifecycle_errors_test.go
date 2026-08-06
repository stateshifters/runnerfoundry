package postgres

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
	assertTransactionalMutationFailures(t, 4,
		func(store *Store) error { return store.MarkRunnerStarted(context.Background(), source) })
}

func TestRetryUnstartedHandlesRejectedTransitions(t *testing.T) {
	t.Parallel()
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	terminalValues := processingJobValues(job, testMemberIDA)
	terminalValues[6] = true
	terminal := rowFromValues(terminalValues)
	noRow := Row{scan: func(...any) error { return sql.ErrNoRows }}
	for name, transaction := range map[string]Transaction{
		"missing":  transactionWithRows([]Row{noRow}, nil),
		"terminal": transactionWithRows([]Row{terminal}, nil),
	} {
		result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
			RetryUnstarted(context.Background(), testJobID1)
		want := core.RetryMissing
		if name == "terminal" {
			want = core.RetrySourceTerminal
		}
		if err != nil || result.Outcome != want {
			t.Fatalf("%s retry=%#v error=%v", name, result, err)
		}
	}
}

func TestRetryUnstartedReportsCandidateFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	processing := rowFromValues(processingJobValues(job, testMemberIDA))
	transactions := []Transaction{
		transactionWithRows([]Row{{scan: func(...any) error { return protocolErr }}}, nil),
		transactionFailingAtExecuteWithRows(1, protocolErr, processing),
		transactionWithRows([]Row{processing, {scan: func(...any) error { return protocolErr }}}, nil),
	}
	for index, transaction := range transactions {
		if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
			RetryUnstarted(context.Background(), testJobID1); err == nil {
			t.Fatalf("case %d: expected retry candidate failure", index)
		}
	}
}

func TestRetryUnstartedReportsRemovalFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	processing := rowFromValues(processingJobValues(job, testMemberIDA))
	queued := rowFromValues(queuedJobValues(testJobID1, core.JobRequest{Source: job.Source, RunnerPool: job.RunnerPool}))
	transaction := transactionFailingAtExecuteWithRows(2, protocolErr, processing, queued)
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), testJobID1); err == nil {
		t.Fatal("expected retry ownership removal failure")
	}
}

func TestRetryUnstartedReportsRemovalCountFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	job := recoveryProcessing().Job
	processing := rowFromValues(postgresProcessingOutcomeValues(job, false, false))
	queued := rowFromValues(queuedJobValues(testJobID1, recoveryRequest()))
	executeCalls := 0
	transaction := postgresOutcomeTransaction([]Row{processing, queued}, 1, new(int))
	baseExecute := transaction.execute
	transaction.execute = func(ctx context.Context, statement string, arguments ...any) (Result, error) {
		executeCalls++
		if executeCalls == 2 {
			return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
		}
		return baseExecute(ctx, statement, arguments...)
	}
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), testJobID1); err == nil {
		t.Fatal("expected retry ownership count failure")
	}
}

func TestRetryUnstartedReportsPostSerializationClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	job := recoveryProcessing().Job
	rows := []Row{rowFromValues(postgresProcessingOutcomeValues(job, false, false)), noPostgresRow(),
		{scan: func(...any) error { return protocolErr }}}
	if _, err := NewStore(Executor{begin: transactionBeginning(postgresOutcomeTransaction(rows, 1, new(int)))}).
		RetryUnstarted(context.Background(), testJobID1); err == nil {
		t.Fatal("expected post-serialization classification failure")
	}
}

func TestRetryUnstartedReportsCommitFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	processing := rowFromValues(processingJobValues(job, testMemberIDA))
	queued := rowFromValues(queuedJobValues(testJobID1, core.JobRequest{Source: job.Source, RunnerPool: job.RunnerPool}))
	noRow := Row{scan: func(...any) error { return sql.ErrNoRows }}
	transactions := []Transaction{
		transactionWithRows([]Row{processing, queued}, protocolErr),
		transactionWithRows([]Row{noRow}, protocolErr),
		transactionWithRows([]Row{processing, noRow, processing}, protocolErr),
	}
	for index, transaction := range transactions {
		if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
			RetryUnstarted(context.Background(), testJobID1); err == nil {
			t.Fatalf("case %d: expected retry commit failure", index)
		}
	}
}

func TestRemoveTerminalReportsRowsAffectedFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
	}, rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RemoveTerminal(context.Background(), testJobID1); err == nil {
		t.Fatal("expected affected-row failure")
	}
}

func TestRemoveTerminalReportsMutationFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for index, transaction := range []Transaction{
		{execute: failingExecute(protocolErr), rollback: successfulCompletion},
		{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion},
	} {
		if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
			RemoveTerminal(context.Background(), testJobID1); err == nil {
			t.Fatalf("case %d: expected terminal removal failure", index)
		}
	}
}

func TestRemoveTerminalReportsClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := postgresOutcomeTransaction([]Row{{scan: func(...any) error { return protocolErr }}}, 0, new(int))
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RemoveTerminal(context.Background(), testJobID1); err == nil {
		t.Fatal("expected terminal classification failure")
	}
}

func TestQueueReportsClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	rows := []Row{noPostgresRow(), noPostgresRow(), {scan: func(...any) error { return protocolErr }}}
	transaction := postgresOutcomeTransaction(rows, 1, new(int))
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).Queue(context.Background(), core.JobRequest{
		Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux",
	}); err == nil {
		t.Fatal("expected queue classification failure")
	}
}

func TestRecoverUnstartedReportsClassificationAndRemovalCountFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	processing := recoveryProcessing()
	tests := map[string]Transaction{
		"classification": postgresOutcomeTransaction([]Row{noPostgresRow(), {scan: func(...any) error { return protocolErr }}}, 1, new(int)),
		"removal count": Transaction{
			queryRow: func(context.Context, string, ...any) Row {
				return rowFromValues(queuedJobValues(processing.Job.ID, recoveryRequest()))
			},
			execute: func(context.Context, string, ...any) (Result, error) {
				return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
			}, rollback: successfulCompletion,
		},
	}
	for name, transaction := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
				RecoverUnstartedProcessing(context.Background(), processing); err == nil {
				t.Fatalf("expected %s failure", name)
			}
		})
	}
}
