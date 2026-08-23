package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestReconcileAssignedDemandRejectsInvalidInputBeforeStartingTransaction(t *testing.T) {
	t.Parallel()
	beginCalls := 0
	store := NewStore(Executor{begin: func(context.Context) (Transaction, error) {
		beginCalls++
		return Transaction{}, nil
	}})
	invalidDemands := []githubsource.AssignedDemand{{}, validAssignedDemand(-1)}
	for _, demand := range invalidDemands {
		if _, err := store.ReconcileAssignedDemand(context.Background(), demand); err == nil || beginCalls != 0 {
			t.Fatalf("error = %v, begin calls = %d", err, beginCalls)
		}
	}
}

func TestReconcileAssignedDemandReportsReadProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	demand := validAssignedDemand(0)
	tests := []struct {
		name     string
		executor Executor
	}{
		{name: "begin", executor: Executor{begin: func(context.Context) (Transaction, error) {
			return Transaction{}, protocolErr
		}}},
		{name: "Runner Pool lock", executor: Executor{begin: transactionBeginning(Transaction{
			execute: failingExecute(protocolErr), rollback: successfulCompletion,
		})}},
		{name: "query", executor: assignedDemandReadFailureExecutor(protocolErr, Rows{})},
		{name: "scan", executor: assignedDemandReadFailureExecutor(nil, rowsWithFailure(protocolErr, nil, nil))},
		{name: "iteration", executor: assignedDemandReadFailureExecutor(nil, rowsWithFailure(nil, protocolErr, nil))},
		{name: "close", executor: assignedDemandReadFailureExecutor(nil, rowsWithFailure(nil, nil, protocolErr))},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(testCase.executor).ReconcileAssignedDemand(context.Background(), demand); !errors.Is(err, protocolErr) {
				t.Fatalf("error = %v, want protocol failure", err)
			}
		})
	}
}

func TestReconcileAssignedDemandReportsEnqueueFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("insert scan")
	transaction := Transaction{
		execute: successfulExecute,
		query:   func(context.Context, string, ...any) (Rows, error) { return rowsFromValues(nil), nil },
		queryRow: func(context.Context, string, ...any) Row {
			return Row{scan: func(...any) error { return protocolErr }}
		},
		rollback: successfulCompletion,
	}
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ReconcileAssignedDemand(context.Background(), validAssignedDemand(1)); !errors.Is(err, protocolErr) {
		t.Fatalf("error = %v, want enqueue failure", err)
	}
}

func TestReconcileAssignedDemandReportsSurplusRemovalFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("removal")
	for _, testCase := range []struct {
		name          string
		sourceLockErr error
		result        Result
		removalErr    error
	}{
		{name: "source lock", sourceLockErr: protocolErr},
		{name: "delete", removalErr: protocolErr},
		{name: "affected rows", result: Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			transaction := assignedDemandSurplusTransaction(t, testCase.sourceLockErr, testCase.result, testCase.removalErr)
			if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
				ReconcileAssignedDemand(context.Background(), validAssignedDemand(0)); !errors.Is(err, protocolErr) {
				t.Fatalf("error = %v, want removal failure", err)
			}
		})
	}
}

func TestReconcileAssignedDemandCountsOnlyRowsActuallyRemoved(t *testing.T) {
	t.Parallel()
	transaction := assignedDemandSurplusTransaction(t, nil,
		Result{rowsAffected: func() (int64, error) { return 0, nil }}, nil)
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ReconcileAssignedDemand(context.Background(), validAssignedDemand(0))
	if err != nil || result.ActiveJobs != 1 || result.QueuedJobs != 1 || result.JobsRemoved != 0 {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
}

func TestAddAggregateDemandJobRejectsInvalidIdentity(t *testing.T) {
	t.Parallel()
	if _, err := addAggregateDemandJob(context.Background(), Transaction{}, githubsource.AssignedDemand{}, 0); err == nil {
		t.Fatal("expected invalid aggregate identity error")
	}
}

func assignedDemandReadFailureExecutor(queryErr error, rows Rows) Executor {
	return Executor{begin: transactionBeginning(Transaction{
		execute:  successfulExecute,
		query:    func(context.Context, string, ...any) (Rows, error) { return rows, queryErr },
		rollback: successfulCompletion,
	})}
}

func assignedDemandSurplusTransaction(
	t *testing.T,
	sourceLockErr error,
	removalResult Result,
	removalErr error,
) Transaction {
	t.Helper()
	demand := validAssignedDemand(0)
	source, err := githubsource.AggregateDemandSource(githubsource.AssignedDemand{
		Repository: demand.Repository, RunnerPool: demand.RunnerPool,
		ObservationID: "previous", DesiredJobs: 1, ObservedAt: demand.ObservedAt,
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	executeCalls := 0
	return Transaction{
		execute: func(ctx context.Context, statement string, arguments ...any) (Result, error) {
			executeCalls++
			if executeCalls == 2 && sourceLockErr != nil {
				return Result{}, sourceLockErr
			}
			if executeCalls == 3 {
				return removalResult, removalErr
			}
			return successfulExecute(ctx, statement, arguments...)
		},
		query: func(context.Context, string, ...any) (Rows, error) {
			return rowsFromValues([][]any{{testJobID1, string(source.Adapter), string(source.ID), "queued", demand.ObservedAt}}), nil
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
}

func validAssignedDemand(desiredJobs int) githubsource.AssignedDemand {
	return githubsource.AssignedDemand{Repository: "owner/repository", RunnerPool: core.RunnerPool("linux"),
		ObservationID: "observation", DesiredJobs: desiredJobs, ObservedAt: fixedTime()}
}
