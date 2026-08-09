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
	assertTransactionalMutationFailures(t, []string{"serialize source", "mark processing started", "tombstone delayed start", "remove queued retry"},
		func(store *Store) error {
			_, err := store.MarkRunnerStarted(context.Background(), core.JobIdentity{Source: source})
			return err
		})
}

func TestLifecycleObservationsRejectInvalidIdentityBeforeMutation(t *testing.T) {
	t.Parallel()
	for name, operation := range map[string]func(*Store) (core.ObservationResult, error){
		"started": func(store *Store) (core.ObservationResult, error) {
			return store.MarkRunnerStarted(context.Background(), core.JobIdentity{})
		},
		"terminal": func(store *Store) (core.ObservationResult, error) {
			return store.Terminalize(context.Background(), core.JobIdentity{})
		},
	} {
		t.Run(name, func(t *testing.T) {
			protocol := &recordedTransaction{}
			if _, err := operation(NewStore(protocol.executor())); !errors.Is(err, core.ErrInvalidJobIdentity) ||
				protocol.commitCalls != 0 || len(protocol.statements) != 0 {
				t.Fatalf("statements=%#v commits=%d error=%v", protocol.statements, protocol.commitCalls, err)
			}
		})
	}
}

func TestJobIdentityDisappearingAfterSerializationIsUnmatched(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request"}
	protocol := &recordedTransaction{rows: []Row{
		rowFromValues([]any{string(source.Adapter), string(source.ID)}), noPostgresRow(),
	}}
	result, err := NewStore(protocol.executor()).Terminalize(context.Background(), core.JobIdentity{JobID: testJobID1})
	if err != nil || result.Outcome != core.ObservationUnmatched || protocol.commitCalls != 1 || len(protocol.statements) != 3 {
		t.Fatalf("result=%#v statements=%#v commits=%d error=%v", result, protocol.statements, protocol.commitCalls, err)
	}
}

func TestJobIdentityChangingSourceAfterSerializationIsRejected(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request"}
	changed := core.SourceReference{Adapter: "github", ID: "other"}
	protocol := &recordedTransaction{rows: []Row{
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
		rowFromValues([]any{string(changed.Adapter), string(changed.ID)}),
	}}
	result, err := NewStore(protocol.executor()).Terminalize(context.Background(), core.JobIdentity{JobID: testJobID1})
	if !errors.Is(err, core.ErrJobIdentityMismatch) || result != (core.ObservationResult{}) ||
		protocol.commitCalls != 0 || len(protocol.statements) != 3 {
		t.Fatalf("result=%#v statements=%#v commits=%d error=%v", result, protocol.statements, protocol.commitCalls, err)
	}
}

func TestLifecycleObservationReportsResolutionAndClassificationFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	source := core.SourceReference{Adapter: "github", ID: "request"}
	for name, test := range map[string]struct {
		transaction Transaction
		operation   func(*Store) (core.ObservationResult, error)
	}{
		"job resolution": {
			transaction: transactionWithRows([]Row{{scan: func(...any) error { return protocolErr }}}, nil),
			operation: func(store *Store) (core.ObservationResult, error) {
				return store.Terminalize(context.Background(), core.JobIdentity{JobID: testJobID1})
			},
		},
		"started classification": {
			transaction: postgresOutcomeTransaction([]Row{{scan: func(...any) error { return protocolErr }}}, 0, new(int)),
			operation: func(store *Store) (core.ObservationResult, error) {
				return store.MarkRunnerStarted(context.Background(), core.JobIdentity{Source: source})
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := test.operation(NewStore(Executor{begin: transactionBeginning(test.transaction)})); err == nil {
				t.Fatal("expected protocol failure")
			}
		})
	}
}

func TestLifecycleObservationReportsAffectedRowCountFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	source := core.SourceReference{Adapter: "github", ID: "request"}
	for name, operation := range map[string]func(*Store) (core.ObservationResult, error){
		"started": func(store *Store) (core.ObservationResult, error) {
			return store.MarkRunnerStarted(context.Background(), core.JobIdentity{Source: source})
		},
		"terminal": func(store *Store) (core.ObservationResult, error) {
			return store.Terminalize(context.Background(), core.JobIdentity{Source: source})
		},
	} {
		t.Run(name, func(t *testing.T) {
			transaction := Transaction{execute: func(context.Context, string, ...any) (Result, error) {
				return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
			}, rollback: successfulCompletion}
			if _, err := operation(NewStore(Executor{begin: transactionBeginning(transaction)})); err == nil {
				t.Fatal("expected affected-row failure")
			}
		})
	}
}

func TestRetryUnstartedHandlesRejectedTransitions(t *testing.T) {
	t.Parallel()
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	terminalValues := processingJobValues(job, testMemberIDA)
	terminalValues[7] = true
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
	tests := []struct {
		name        string
		transaction Transaction
	}{
		{name: "candidate scan", transaction: transactionWithRows([]Row{{scan: func(...any) error { return protocolErr }}}, nil)},
		{name: "queue insertion", transaction: transactionFailingAtExecuteWithRows(1, protocolErr, processing)},
		{name: "queued job scan", transaction: transactionWithRows([]Row{processing, {scan: func(...any) error { return protocolErr }}}, nil)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(Executor{begin: transactionBeginning(testCase.transaction)}).
				RetryUnstarted(context.Background(), testJobID1); err == nil {
				t.Fatal("expected retry candidate failure")
			}
		})
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
	tests := []struct {
		name        string
		transaction Transaction
	}{
		{name: "requeued", transaction: transactionWithRows([]Row{processing, queued}, protocolErr)},
		{name: "missing", transaction: transactionWithRows([]Row{noRow}, protocolErr)},
		{name: "claim changed", transaction: transactionWithRows([]Row{processing, noRow, processing}, protocolErr)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(Executor{begin: transactionBeginning(testCase.transaction)}).
				RetryUnstarted(context.Background(), testJobID1); err == nil {
				t.Fatal("expected retry commit failure")
			}
		})
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
	for _, testCase := range []struct {
		name        string
		transaction Transaction
	}{
		{name: "removal", transaction: Transaction{execute: failingExecute(protocolErr), rollback: successfulCompletion}},
		{name: "commit", transaction: Transaction{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(Executor{begin: transactionBeginning(testCase.transaction)}).
				RemoveTerminal(context.Background(), testJobID1); err == nil {
				t.Fatal("expected terminal removal failure")
			}
		})
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
