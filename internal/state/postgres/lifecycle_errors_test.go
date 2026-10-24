package postgres

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
	assertTransactionalMutationFailures(t, []string{"serialize source", "mark processing accepted", "tombstone delayed acceptance", "remove queued retry"},
		func(store *Store) error {
			_, err := store.ObserveRunnerAcceptance(context.Background(), core.JobIdentity{Source: source})
			return err
		})
}

func TestConditionalTerminalObservationReportsTransactionalFailures(t *testing.T) {
	protocolErr := errors.New("protocol")
	identity := core.JobIdentity{JobID: testJobID1,
		Source: core.SourceReference{Adapter: "github", ID: "aggregate"}}
	source := rowFromValues([]any{"github", "aggregate"})
	classificationFailure := rowStub{scan: func(...any) error { return protocolErr }}
	for name, executor := range map[string]executorStub{
		"begin": {begin: func(context.Context) (transactionStub, error) {
			return transactionStub{}, protocolErr
		}},
		"tombstone": {begin: transactionBeginning(transactionFailingAtExecuteWithRows(
			2, protocolErr, source, source,
		))},
		"queued removal": {begin: transactionBeginning(transactionFailingAtExecuteWithRows(
			3, protocolErr, source, source,
		))},
		"classification": {begin: transactionBeginning(postgresOutcomeTransaction(
			[]rowStub{source, source, classificationFailure}, 0, new(int),
		))},
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

func TestLifecycleObservationsRejectInvalidIdentityBeforeMutation(t *testing.T) {
	t.Parallel()
	for name, operation := range map[string]func(*Store) (core.ObservationResult, error){
		"accepted": func(store *Store) (core.ObservationResult, error) {
			return store.ObserveRunnerAcceptance(context.Background(), core.JobIdentity{})
		},
		"terminal": func(store *Store) (core.ObservationResult, error) {
			return store.Terminalize(context.Background(), core.JobIdentity{})
		},
	} {
		t.Run(name, func(t *testing.T) {
			protocol := &recordedTransaction{}
			if _, err := operation(newStore(protocol.executor())); !errors.Is(err, core.ErrInvalidJobIdentity) ||
				protocol.commitCalls != 0 || len(protocol.statements) != 0 {
				t.Fatalf("statements=%#v commits=%d error=%v", protocol.statements, protocol.commitCalls, err)
			}
		})
	}
}

func TestJobIdentityDisappearingAfterSerializationIsUnmatched(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request"}
	protocol := &recordedTransaction{rows: []rowStub{
		rowFromValues([]any{string(source.Adapter), string(source.ID)}), noPostgresRow(),
	}}
	result, err := newStore(protocol.executor()).Terminalize(context.Background(), core.JobIdentity{JobID: testJobID1})
	if err != nil || result.Outcome != core.ObservationUnmatched || protocol.commitCalls != 1 || len(protocol.statements) != 3 {
		t.Fatalf("result=%#v statements=%#v commits=%d error=%v", result, protocol.statements, protocol.commitCalls, err)
	}
}

func TestJobIdentityChangingSourceAfterSerializationIsRejected(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request"}
	changed := core.SourceReference{Adapter: "github", ID: "other"}
	protocol := &recordedTransaction{rows: []rowStub{
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
		rowFromValues([]any{string(changed.Adapter), string(changed.ID)}),
	}}
	result, err := newStore(protocol.executor()).Terminalize(context.Background(), core.JobIdentity{JobID: testJobID1})
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
		transaction transactionStub
		operation   func(*Store) (core.ObservationResult, error)
	}{
		"job resolution": {
			transaction: transactionWithRows([]rowStub{rowStub{scan: func(...any) error { return protocolErr }}}, nil),
			operation: func(store *Store) (core.ObservationResult, error) {
				return store.Terminalize(context.Background(), core.JobIdentity{JobID: testJobID1})
			},
		},
		"acceptance classification": {
			transaction: postgresOutcomeTransaction([]rowStub{rowStub{scan: func(...any) error { return protocolErr }}}, 0, new(int)),
			operation: func(store *Store) (core.ObservationResult, error) {
				return store.ObserveRunnerAcceptance(context.Background(), core.JobIdentity{Source: source})
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := test.operation(newStore(executorStub{begin: transactionBeginning(test.transaction)})); err == nil {
				t.Fatal("expected protocol failure")
			}
		})
	}
}

func TestLifecycleObservationReportsJobResolutionFailureAfterSerialization(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	source := core.SourceReference{Adapter: "github", ID: "request"}
	transaction := transactionWithRows([]rowStub{
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
		rowStub{scan: func(...any) error { return protocolErr }},
	}, nil)

	_, err := newStore(executorStub{begin: transactionBeginning(transaction)}).Terminalize(
		context.Background(),
		core.JobIdentity{JobID: testJobID1},
	)
	if !errors.Is(err, protocolErr) {
		t.Fatalf("Terminalize() error = %v, want %v", err, protocolErr)
	}
}

func TestLifecycleObservationReportsAffectedRowCountFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	source := core.SourceReference{Adapter: "github", ID: "request"}
	for name, operation := range map[string]func(*Store) (core.ObservationResult, error){
		"accepted": func(store *Store) (core.ObservationResult, error) {
			return store.ObserveRunnerAcceptance(context.Background(), core.JobIdentity{Source: source})
		},
		"terminal": func(store *Store) (core.ObservationResult, error) {
			return store.Terminalize(context.Background(), core.JobIdentity{Source: source})
		},
	} {
		t.Run(name, func(t *testing.T) {
			transaction := transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
				return resultStub{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
			}, rollback: successfulCompletion}
			if _, err := operation(newStore(executorStub{begin: transactionBeginning(transaction)})); err == nil {
				t.Fatal("expected affected-row failure")
			}
		})
	}
}

func TestRetryUnstartedHandlesRejectedTransitions(t *testing.T) {
	t.Parallel()
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	terminalValues := postgresProcessingOutcomeValues(job, false, true)
	terminal := rowFromValues(terminalValues)
	noRow := rowStub{scan: func(...any) error { return sql.ErrNoRows }}
	for name, transaction := range map[string]transactionStub{
		"missing":  transactionWithRows([]rowStub{noRow}, nil),
		"terminal": transactionWithRows([]rowStub{terminal}, nil),
	} {
		result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
			RetryUnstarted(context.Background(), retryProcessingJob())
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
	processing := rowFromValues(postgresProcessingOutcomeValues(job, false, false))
	tests := []struct {
		name        string
		transaction transactionStub
	}{
		{name: "candidate scan", transaction: transactionWithRows([]rowStub{rowStub{scan: func(...any) error { return protocolErr }}}, nil)},
		{name: "queue insertion", transaction: transactionFailingAtExecuteWithRows(1, protocolErr, processing)},
		{name: "queued job scan", transaction: transactionWithRows([]rowStub{processing, rowStub{scan: func(...any) error { return protocolErr }}}, nil)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newStore(executorStub{begin: transactionBeginning(testCase.transaction)}).
				RetryUnstarted(context.Background(), retryProcessingJob()); err == nil {
				t.Fatal("expected retry candidate failure")
			}
		})
	}
}

func TestRetryUnstartedReportsRemovalFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	processing := rowFromValues(postgresProcessingOutcomeValues(job, false, false))
	queued := rowFromValues(queuedJobValues(testJobID1, core.JobRequest{Source: job.Source, RunnerPool: job.RunnerPool}))
	transaction := transactionFailingAtExecuteWithRows(2, protocolErr, processing, queued)
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), retryProcessingJob()); err == nil {
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
	transaction := postgresOutcomeTransaction([]rowStub{processing, queued}, 1, new(int))
	baseExecute := transaction.execute
	transaction.execute = func(ctx context.Context, statement string, arguments ...any) (resultStub, error) {
		executeCalls++
		if executeCalls == 2 {
			return resultStub{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
		}
		return baseExecute(ctx, statement, arguments...)
	}
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), retryProcessingJob()); err == nil {
		t.Fatal("expected retry ownership count failure")
	}
}

func TestRetryUnstartedReportsPostSerializationClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	job := recoveryProcessing().Job
	rows := []rowStub{rowFromValues(postgresProcessingOutcomeValues(job, false, false)), noPostgresRow(),
		rowStub{scan: func(...any) error { return protocolErr }}}
	if _, err := newStore(executorStub{begin: transactionBeginning(postgresOutcomeTransaction(rows, 1, new(int)))}).
		RetryUnstarted(context.Background(), retryProcessingJob()); err == nil {
		t.Fatal("expected post-serialization classification failure")
	}
}

func TestRetryUnstartedReportsCommitFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	processing := rowFromValues(postgresProcessingOutcomeValues(job, false, false))
	queued := rowFromValues(queuedJobValues(testJobID1, core.JobRequest{Source: job.Source, RunnerPool: job.RunnerPool}))
	noRow := rowStub{scan: func(...any) error { return sql.ErrNoRows }}
	tests := []struct {
		name        string
		transaction transactionStub
	}{
		{name: "requeued", transaction: transactionWithRows([]rowStub{processing, queued}, protocolErr)},
		{name: "missing", transaction: transactionWithRows([]rowStub{noRow}, protocolErr)},
		{name: "claim changed", transaction: transactionWithRows([]rowStub{processing, noRow, processing}, protocolErr)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newStore(executorStub{begin: transactionBeginning(testCase.transaction)}).
				RetryUnstarted(context.Background(), retryProcessingJob()); err == nil {
				t.Fatal("expected retry commit failure")
			}
		})
	}
}

func TestRemoveTerminalReportsRowsAffectedFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
		return resultStub{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
	}, rollback: successfulCompletion}
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RemoveTerminal(context.Background(), testJobID1); err == nil {
		t.Fatal("expected affected-row failure")
	}
}

func TestRemoveTerminalReportsMutationFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for _, testCase := range []struct {
		name        string
		transaction transactionStub
	}{
		{name: "removal", transaction: transactionStub{execute: failingExecute(protocolErr), rollback: successfulCompletion}},
		{name: "commit", transaction: transactionStub{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newStore(executorStub{begin: transactionBeginning(testCase.transaction)}).
				RemoveTerminal(context.Background(), testJobID1); err == nil {
				t.Fatal("expected terminal removal failure")
			}
		})
	}
}

func TestRemoveTerminalReportsClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := postgresOutcomeTransaction([]rowStub{rowStub{scan: func(...any) error { return protocolErr }}}, 0, new(int))
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RemoveTerminal(context.Background(), testJobID1); err == nil {
		t.Fatal("expected terminal classification failure")
	}
}

func TestQueueReportsClassificationFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	rows := []rowStub{noPostgresRow(), noPostgresRow(), rowStub{scan: func(...any) error { return protocolErr }}}
	transaction := postgresOutcomeTransaction(rows, 1, new(int))
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).Queue(context.Background(), core.JobRequest{
		Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux",
	}); err == nil {
		t.Fatal("expected queue classification failure")
	}
}

func TestRecoverUnstartedReportsClassificationAndRemovalCountFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	processing := recoveryProcessing()
	tests := map[string]transactionStub{
		"classification": postgresOutcomeTransaction([]rowStub{noPostgresRow(), rowStub{scan: func(...any) error { return protocolErr }}}, 1, new(int)),
		"removal count": transactionStub{
			queryRow: func(context.Context, string, ...any) rowStub {
				return rowFromValues(queuedJobValues(processing.Job.ID, recoveryRequest()))
			},
			execute: func(context.Context, string, ...any) (resultStub, error) {
				return resultStub{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
			}, rollback: successfulCompletion,
		},
	}
	for name, transaction := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
				RecoverUnstartedProcessing(context.Background(), processing); err == nil {
				t.Fatalf("expected %s failure", name)
			}
		})
	}
}
