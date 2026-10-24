package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	statecontract "github.com/stateshifters/runnerfoundry/internal/state"
)

func TestDemandTransactionReconcilesActiveJobsAndMutationsUnderOneScopeLock(t *testing.T) {
	t.Parallel()
	queued := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-queued"},
		Scope: demandTransactionKey().Scope, RunnerPool: "linux", EnqueuedAt: fixedTime()}
	processingSource := core.SourceReference{Adapter: "github", ID: "request-processing"}
	protocol := demandReconciliationProtocol(queued, processingSource)
	store := newStore(protocol.executor())
	var result demandReconciliationResult
	err := store.WithDemandTransaction(context.Background(), demandTransactionKey(),
		reconcileDemandOperations(queued, &result))
	if err != nil || protocol.commitCalls != 1 {
		t.Fatalf("transaction commits=%d error=%v", protocol.commitCalls, err)
	}
	assertDemandReconciliation(t, result, protocol.statements, protocol.arguments, queued, processingSource)
}

type demandReconciliationResult struct {
	jobs    []statecontract.ActiveJob
	enqueue core.EnqueueResult
	removal statecontract.QueuedRemovalOutcome
}

func reconcileDemandOperations(
	queued core.JobRequest,
	result *demandReconciliationResult,
) func(statecontract.DemandTransaction) error {
	return func(transaction statecontract.DemandTransaction) error {
		var err error
		result.jobs, err = transaction.ActiveJobs(context.Background())
		if err != nil {
			return fmt.Errorf("load active jobs: %w", err)
		}
		result.enqueue, err = transaction.Queue(context.Background(), queued)
		if err != nil {
			return fmt.Errorf("queue job: %w", err)
		}
		result.removal, err = transaction.RemoveQueued(context.Background(), result.jobs[0])
		if err != nil {
			return fmt.Errorf("remove job: %w", err)
		}
		return nil
	}
}

func demandReconciliationProtocol(
	queued core.JobRequest,
	processingSource core.SourceReference,
) *recordedTransaction {
	return &recordedTransaction{
		queryRows: rowsFromValues([][]any{
			{testJobID1, string(queued.Source.Adapter), string(queued.Source.ID), "linux", "queued", fixedTime()},
			{uuid.MustParse("00000000-0000-0000-0000-000000000002"), string(processingSource.Adapter),
				string(processingSource.ID), "linux", "processing", fixedTime()},
		}),
		rows: []rowStub{rowFromValues(queuedJobValues(testJobID1, queued))},
	}
}

func assertDemandReconciliation(
	t *testing.T,
	result demandReconciliationResult,
	statements []string,
	arguments [][]any,
	queued core.JobRequest,
	processingSource core.SourceReference,
) {
	t.Helper()
	expectedJobs := []statecontract.ActiveJob{
		{ID: testJobID1, Source: queued.Source, RunnerPool: "linux", Phase: statecontract.ActiveJobQueued, StateChangedAt: fixedTime()},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Source: processingSource,
			RunnerPool: "linux", Phase: statecontract.ActiveJobProcessing, StateChangedAt: fixedTime()},
	}
	if !reflect.DeepEqual(result.jobs, expectedJobs) || result.enqueue.Outcome != core.QueueQueued ||
		result.removal != statecontract.QueuedJobRemoved {
		t.Fatalf("jobs=%#v enqueue=%#v removal=%q", result.jobs, result.enqueue, result.removal)
	}
	expectedStatements := []string{serializeDemandDomainSQL, serializeDemandScopeSQL, activeJobsSQL, serializeSourceSQL,
		insertQueuedJobSQL, serializeSourceSQL, removeQueuedJobSQL}
	if !reflect.DeepEqual(statements, expectedStatements) {
		t.Fatalf("statements = %#v", statements)
	}
	expectedPrefix := [][]any{
		{advisoryLockIdentity("github", "linux")},
		{advisoryLockIdentity("github", "repository-linux")},
		{"github", "linux"},
	}
	if !reflect.DeepEqual(arguments[:len(expectedPrefix)], expectedPrefix) {
		t.Fatalf("arguments = %#v, want prefix %#v", arguments, expectedPrefix)
	}
}

func TestDemandTransactionReportsScopeLifecycleFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	callbackErr := errors.New("callback")
	rollbackErr := errors.New("rollback")
	tests := []demandLifecycleFailureCase{
		{name: "begin", executor: executorStub{begin: func(context.Context) (transactionStub, error) {
			return transactionStub{}, protocolErr
		}}, callback: func(statecontract.DemandTransaction) error { return nil }, want: protocolErr},
		{name: "domain lock", executor: executorStub{begin: transactionBeginning(transactionStub{
			execute: failingExecute(protocolErr), rollback: successfulCompletion,
		})}, callback: func(statecontract.DemandTransaction) error { return nil }, want: protocolErr},
		{name: "scope lock", executor: executorStub{begin: transactionBeginning(transactionFailingExecute(2, protocolErr))},
			callback: func(statecontract.DemandTransaction) error { return nil }, want: protocolErr},
		{name: "callback and rollback", executor: executorStub{begin: transactionBeginning(transactionStub{
			execute: successfulExecute, rollback: failingCompletion(rollbackErr),
		})}, callback: func(statecontract.DemandTransaction) error { return callbackErr }, want: rollbackErr},
		{name: "commit", executor: executorStub{begin: transactionBeginning(transactionStub{
			execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion,
		})}, callback: func(statecontract.DemandTransaction) error { return nil }, want: protocolErr},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			assertDemandLifecycleFailure(t, testCase, callbackErr)
		})
	}
}

type demandLifecycleFailureCase struct {
	name     string
	executor executorStub
	callback func(statecontract.DemandTransaction) error
	want     error
}

func assertDemandLifecycleFailure(t *testing.T, testCase demandLifecycleFailureCase, callbackErr error) {
	t.Helper()
	err := newStore(testCase.executor).WithDemandTransaction(context.Background(),
		demandTransactionKey(), testCase.callback)
	if !errors.Is(err, testCase.want) {
		t.Fatalf("error = %v, want %v", err, testCase.want)
	}
	if testCase.name == "callback and rollback" && !errors.Is(err, callbackErr) {
		t.Fatalf("error = %v, want callback error", err)
	}
}

func TestDemandTransactionRejectsInvalidKeyBeforeDatabaseAccess(t *testing.T) {
	t.Parallel()
	tests := []statecontract.DemandTransactionKey{
		{RunnerPool: "linux"},
		{Scope: core.DemandScope{Adapter: "github", ID: "repository"}},
	}
	for _, key := range tests {
		called := false
		store := newStore(executorStub{begin: func(context.Context) (transactionStub, error) {
			called = true
			return transactionStub{}, nil
		}})
		err := store.WithDemandTransaction(context.Background(), key, func(statecontract.DemandTransaction) error {
			return nil
		})
		if err == nil || called {
			t.Fatalf("key = %#v, error = %v, database called = %t", key, err, called)
		}
	}
}

func TestDemandTransactionQueueRejectsKeyMismatchBeforeSourceLock(t *testing.T) {
	t.Parallel()
	key := demandTransactionKey()
	tests := []core.JobRequest{
		{Source: core.SourceReference{Adapter: "github", ID: "request"}, Scope: core.DemandScope{
			Adapter: "github", ID: "other-repository",
		}, RunnerPool: key.RunnerPool, EnqueuedAt: fixedTime()},
		{Source: core.SourceReference{Adapter: "gitlab", ID: "request"}, Scope: core.DemandScope{
			Adapter: "gitlab", ID: "repository",
		}, RunnerPool: key.RunnerPool, EnqueuedAt: fixedTime()},
		{Source: core.SourceReference{Adapter: "github", ID: "request"}, Scope: key.Scope,
			RunnerPool: "windows", EnqueuedAt: fixedTime()},
	}
	for _, request := range tests {
		protocol := &recordedTransaction{}
		result, err := (demandTransaction{transaction: protocol.transaction(), key: key}).
			Queue(context.Background(), request)
		if !errors.Is(err, statecontract.ErrDemandTransactionMismatch) ||
			result != (core.EnqueueResult{}) || len(protocol.statements) != 0 {
			t.Fatalf("request = %#v, result = %#v, error = %v, statements = %#v",
				request, result, err, protocol.statements)
		}
	}
}

func TestDemandTransactionRemoveRejectsKeyMismatchBeforePhaseAndSourceLock(t *testing.T) {
	t.Parallel()
	key := demandTransactionKey()
	tests := []statecontract.ActiveJob{
		{Source: core.SourceReference{Adapter: "gitlab", ID: "request"}, RunnerPool: key.RunnerPool,
			Phase: statecontract.ActiveJobProcessing},
		{Source: core.SourceReference{Adapter: key.Scope.Adapter, ID: "request"}, RunnerPool: "windows",
			Phase: statecontract.ActiveJobProcessing},
	}
	for _, job := range tests {
		protocol := &recordedTransaction{}
		outcome, err := (demandTransaction{transaction: protocol.transaction(), key: key}).
			RemoveQueued(context.Background(), job)
		if !errors.Is(err, statecontract.ErrDemandTransactionMismatch) ||
			outcome != "" || len(protocol.statements) != 0 {
			t.Fatalf("job = %#v, outcome = %q, error = %v, statements = %#v",
				job, outcome, err, protocol.statements)
		}
	}
}

func TestDemandTransactionActiveJobsReportsProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	tests := []struct {
		name        string
		transaction transactionStub
	}{
		{name: "active query", transaction: transactionStub{query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsStub{}, protocolErr
		}}},
		{name: "active scan", transaction: transactionStub{query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsWithFailure(protocolErr, nil, nil), nil
		}}},
		{name: "active iteration", transaction: transactionStub{query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsWithFailure(nil, protocolErr, nil), nil
		}}},
		{name: "active close", transaction: transactionStub{query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsWithFailure(nil, nil, protocolErr), nil
		}}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := (demandTransaction{transaction: testCase.transaction, key: demandTransactionKey()}).
				ActiveJobs(context.Background())
			if !errors.Is(err, protocolErr) {
				t.Fatalf("error = %v, want protocol error", err)
			}
		})
	}
}

func TestDemandTransactionMutationsReportProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for _, testCase := range demandMutationFailureCases(protocolErr) {
		t.Run(testCase.name, func(t *testing.T) {
			assertDemandMutationFailure(t, testCase, protocolErr)
		})
	}
}

func demandMutationFailureCases(protocolErr error) []demandMutationFailureCase {
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"},
		Scope: demandTransactionKey().Scope, RunnerPool: "linux", EnqueuedAt: fixedTime()}
	queued := statecontract.ActiveJob{ID: testJobID1, Source: request.Source, RunnerPool: request.RunnerPool,
		Phase: statecontract.ActiveJobQueued, StateChangedAt: request.EnqueuedAt}
	return []demandMutationFailureCase{
		{name: "queue source lock", transaction: transactionStub{execute: failingExecute(protocolErr)}, operation: func(transaction demandTransaction) error {
			_, err := transaction.Queue(context.Background(), request)
			return err
		}},
		{name: "remove source lock", transaction: transactionStub{execute: failingExecute(protocolErr)}, operation: func(transaction demandTransaction) error {
			_, err := transaction.RemoveQueued(context.Background(), queued)
			return err
		}},
		{name: "remove execute", transaction: transactionFailingAtExecute(2, protocolErr), operation: func(transaction demandTransaction) error {
			_, err := transaction.RemoveQueued(context.Background(), queued)
			return err
		}},
		{name: "remove count", transaction: transactionStub{execute: func(_ context.Context, _ string, _ ...any) (resultStub, error) {
			return resultStub{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
		}}, operation: func(transaction demandTransaction) error {
			_, err := transaction.RemoveQueued(context.Background(), queued)
			return err
		}},
	}
}

type demandMutationFailureCase struct {
	name        string
	transaction transactionStub
	operation   func(demandTransaction) error
}

func assertDemandMutationFailure(t *testing.T, testCase demandMutationFailureCase, want error) {
	t.Helper()
	if err := testCase.operation(demandTransaction{
		transaction: testCase.transaction,
		key:         demandTransactionKey(),
	}); !errors.Is(err, want) {
		t.Fatalf("error = %v, want protocol error", err)
	}
}

func TestRemoveQueuedReportsMissingWithoutMutation(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"},
		RunnerPool: "linux", EnqueuedAt: fixedTime()}
	transaction := transactionStub{execute: func(_ context.Context, _ string, _ ...any) (resultStub, error) {
		return resultStub{rowsAffected: func() (int64, error) { return 0, nil }}, nil
	}}
	outcome, err := (demandTransaction{transaction: transaction, key: demandTransactionKey()}).
		RemoveQueued(context.Background(), statecontract.ActiveJob{
			ID: testJobID1, Source: request.Source, RunnerPool: request.RunnerPool,
			Phase: statecontract.ActiveJobQueued, StateChangedAt: request.EnqueuedAt,
		})
	if err != nil || outcome != statecontract.QueuedJobMissing {
		t.Fatalf("remove = %q, %v", outcome, err)
	}
}

func TestRemoveQueuedIgnoresNonQueuedActiveJob(t *testing.T) {
	t.Parallel()
	outcome, err := (demandTransaction{transaction: transactionStub{}, key: demandTransactionKey()}).RemoveQueued(
		context.Background(),
		statecontract.ActiveJob{
			Source:     core.SourceReference{Adapter: demandTransactionKey().Scope.Adapter, ID: "request"},
			RunnerPool: demandTransactionKey().RunnerPool,
			Phase:      statecontract.ActiveJobProcessing,
		},
	)
	if err != nil || outcome != statecontract.QueuedJobMissing {
		t.Fatalf("remove = %q, %v", outcome, err)
	}
}

func TestDemandTransactionQueueClassifiesConcurrentState(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"},
		Scope: demandTransactionKey().Scope, RunnerPool: "linux", EnqueuedAt: fixedTime()}
	transaction := transactionStub{execute: successfulExecute, queryRow: func(_ context.Context, _ string, _ ...any) rowStub {
		return rowStub{scan: func(...any) error { return sql.ErrNoRows }}
	}}
	_, err := (demandTransaction{transaction: transaction, key: demandTransactionKey()}).Queue(context.Background(), request)
	if err == nil {
		t.Fatal("expected queue classification failure")
	}
}

func demandTransactionKey() statecontract.DemandTransactionKey {
	return statecontract.DemandTransactionKey{
		Scope:      core.DemandScope{Adapter: "github", ID: "repository-linux"},
		RunnerPool: "linux",
	}
}
