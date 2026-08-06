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
	processing := rowFromValues(processingJobValues(job, testMemberIDA))
	noRow := Row{scan: func(...any) error { return sql.ErrNoRows }}
	for name, transaction := range map[string]Transaction{
		"missing":  transactionWithRows([]Row{noRow}, nil),
		"terminal": transactionWithRows([]Row{processing, noRow}, nil),
	} {
		result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
			RetryUnstarted(context.Background(), testJobID1)
		if err != nil || result.Applied {
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
		transactionWithRows([]Row{processing, noRow}, protocolErr),
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
