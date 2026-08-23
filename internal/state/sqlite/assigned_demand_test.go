package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestReconcileAssignedDemandQueuesMissingActiveJob(t *testing.T) {
	t.Parallel()
	result := reconcileAssignedDemand(t, validAssignedDemand(1), missingAssignedDemandTransaction())
	assertAssignedDemandResult(t, result, githubsource.AssignedDemandResult{ActiveJobs: 1, QueuedJobs: 1, JobsAdded: 1})
}

func TestReconcileAssignedDemandCountsOnlyMatchingRepositoryAndPreservesExactOrProcessingJobs(t *testing.T) {
	t.Parallel()
	demand := githubsource.AssignedDemand{
		Repository: "octo/repo", RunnerPool: "linux", ObservationID: "observation-1",
		DesiredJobs: 0, ObservedAt: fixedTime(),
	}
	exactQueued := exactDemandSource(t, "octo/repo", "linux", 101)
	exactProcessing := exactDemandSource(t, "octo/repo", "linux", 102)
	otherRepository := exactDemandSource(t, "octo/other", "linux", 103)
	aggregate, err := githubsource.AggregateDemandSource(demand, 1)
	if err != nil {
		t.Fatalf("aggregate source: %v", err)
	}
	rows := [][]any{
		assignedDemandRow("00000000-0000-4000-8000-000000000001", exactQueued, 1, false),
		assignedDemandRow("00000000-0000-4000-8000-000000000002", exactProcessing, 2, true),
		assignedDemandRow("00000000-0000-4000-8000-000000000003", otherRepository, 3, false),
		assignedDemandRow("00000000-0000-4000-8000-000000000004", aggregate, 4, false),
	}
	removedJobIDs := []string{}
	result := reconcileAssignedDemand(t, demand, assignedDemandRemovalTransaction(rows, &removedJobIDs))
	if len(removedJobIDs) != 1 || removedJobIDs[0] != "00000000-0000-4000-8000-000000000004" {
		t.Fatalf("removed jobs = %#v", removedJobIDs)
	}
	assertAssignedDemandResult(t, result, githubsource.AssignedDemandResult{
		ActiveJobs: 2, QueuedJobs: 1, ProcessingJobs: 1, JobsRemoved: 1,
	})
}

func TestReconcileAssignedDemandReplayDoesNotDuplicateAggregateJob(t *testing.T) {
	t.Parallel()
	demand := githubsource.AssignedDemand{
		Repository: "octo/repo", RunnerPool: "linux", ObservationID: "observation-1",
		DesiredJobs: 1, ObservedAt: fixedTime(),
	}
	source, err := githubsource.AggregateDemandSource(demand, 1)
	if err != nil {
		t.Fatalf("aggregate source: %v", err)
	}
	inserted := false
	transaction := replayAssignedDemandTransaction(source, &inserted)
	store := NewStore(Executor{begin: transactionBeginning(transaction)})

	first, err := store.ReconcileAssignedDemand(context.Background(), demand)
	if err != nil || first.JobsAdded != 1 {
		t.Fatalf("first reconciliation = %#v, %v", first, err)
	}
	replayed, err := store.ReconcileAssignedDemand(context.Background(), demand)
	if err != nil || replayed.ActiveJobs != 1 || replayed.JobsAdded != 0 {
		t.Fatalf("replayed reconciliation = %#v, %v", replayed, err)
	}
}

func TestReconcileAssignedDemandSkipsTombstonedAggregateOrdinal(t *testing.T) {
	t.Parallel()
	demand := githubsource.AssignedDemand{
		Repository: "octo/repo", RunnerPool: "linux", ObservationID: "observation-1",
		DesiredJobs: 2, ObservedAt: fixedTime(),
	}
	secondSource, err := githubsource.AggregateDemandSource(demand, 2)
	if err != nil {
		t.Fatalf("second aggregate source: %v", err)
	}
	executeCalls := 0
	transaction := tombstoneGapTransaction(secondSource, &executeCalls)

	result := reconcileAssignedDemand(t, demand, transaction)
	if result.ActiveJobs != 1 || result.JobsAdded != 1 || executeCalls != 2 {
		t.Fatalf("result = %#v, execute calls = %d", result, executeCalls)
	}
}

func tombstoneGapTransaction(secondSource core.SourceReference, executeCalls *int) Transaction {
	return Transaction{
		query:   func(context.Context, string, ...any) (Rows, error) { return rowsFromValues(nil), nil },
		execute: tombstoneGapExecute(executeCalls), queryRow: tombstoneGapQueryRow(secondSource, executeCalls),
		commit: successfulCompletion, rollback: successfulCompletion,
	}
}

func tombstoneGapExecute(executeCalls *int) func(context.Context, string, ...any) (Result, error) {
	return func(context.Context, string, ...any) (Result, error) {
		*executeCalls++
		if *executeCalls == 1 {
			return Result{rowsAffected: func() (int64, error) { return 0, nil }}, nil
		}
		return successfulResult(), nil
	}
}

func tombstoneGapQueryRow(secondSource core.SourceReference, executeCalls *int) func(context.Context, string, ...any) Row {
	return func(_ context.Context, statement string, _ ...any) Row {
		if *executeCalls == 1 && statement == queuedJobBySourceSQL {
			return Row{scan: func(...any) error { return sql.ErrNoRows }}
		}
		if *executeCalls == 1 {
			return rowFromValues([]any{string(core.QueueSourceTerminal)})
		}
		return rowFromValues([]any{testJobIDText, string(secondSource.Adapter), string(secondSource.ID),
			"linux", fixedTime().UnixNano()})
	}
}

func TestReconcileAssignedDemandReportsCommitUncertaintyAndRollbackFailure(t *testing.T) {
	t.Parallel()
	demand := githubsource.AssignedDemand{
		Repository: "octo/repo", RunnerPool: "linux", ObservationID: "observation-1",
		DesiredJobs: 0, ObservedAt: fixedTime(),
	}
	commitErr := errors.New("commit uncertain")
	rollbackErr := errors.New("rollback failed")
	rollbackCalls := 0
	transaction := Transaction{
		query:  func(context.Context, string, ...any) (Rows, error) { return rowsFromValues(nil), nil },
		commit: func(context.Context) error { return commitErr },
		rollback: func(context.Context) error {
			rollbackCalls++
			return rollbackErr
		},
	}

	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ReconcileAssignedDemand(context.Background(), demand)
	if !errors.Is(err, commitErr) || !errors.Is(err, rollbackErr) || rollbackCalls != 1 {
		t.Fatalf("result = %#v, error = %v, rollback calls = %d", result, err, rollbackCalls)
	}
	if result != (githubsource.AssignedDemandResult{}) {
		t.Fatalf("uncertain result = %#v", result)
	}
}

func reconcileAssignedDemand(
	t *testing.T,
	demand githubsource.AssignedDemand,
	transaction Transaction,
) githubsource.AssignedDemandResult {
	t.Helper()
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ReconcileAssignedDemand(context.Background(), demand)
	if err != nil {
		t.Fatalf("reconcile assigned demand: %v", err)
	}
	return result
}

func assertAssignedDemandResult(
	t *testing.T,
	actual githubsource.AssignedDemandResult,
	want githubsource.AssignedDemandResult,
) {
	t.Helper()
	if actual != want {
		t.Fatalf("result = %#v, want %#v", actual, want)
	}
}

func missingAssignedDemandTransaction() Transaction {
	var insertedArguments []any
	return Transaction{
		query: func(context.Context, string, ...any) (Rows, error) { return rowsFromValues(nil), nil },
		execute: func(_ context.Context, _ string, arguments ...any) (Result, error) {
			insertedArguments = arguments
			return successfulResult(), nil
		},
		queryRow: func(context.Context, string, ...any) Row {
			return rowFromValues([]any{testJobIDText, fmt.Sprint(insertedArguments[0]),
				fmt.Sprint(insertedArguments[1]), insertedArguments[2], insertedArguments[3]})
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
}

func assignedDemandRow(
	jobID string,
	source core.SourceReference,
	timestamp int64,
	processing bool,
) []any {
	return []any{jobID, string(source.Adapter), string(source.ID), "linux", timestamp, processing}
}

func assignedDemandRemovalTransaction(rows [][]any, removedJobIDs *[]string) Transaction {
	return Transaction{
		query: func(context.Context, string, ...any) (Rows, error) { return rowsFromValues(rows), nil },
		execute: func(_ context.Context, _ string, arguments ...any) (Result, error) {
			*removedJobIDs = append(*removedJobIDs, fmt.Sprint(arguments[0]))
			return successfulResult(), nil
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
}

func replayAssignedDemandTransaction(source core.SourceReference, inserted *bool) Transaction {
	return Transaction{
		query: func(context.Context, string, ...any) (Rows, error) {
			if !*inserted {
				return rowsFromValues(nil), nil
			}
			return rowsFromValues([][]any{assignedDemandRow(testJobIDText, source, fixedTime().UnixNano(), false)}), nil
		},
		execute: func(context.Context, string, ...any) (Result, error) {
			*inserted = true
			return successfulResult(), nil
		},
		queryRow: func(context.Context, string, ...any) Row {
			return rowFromValues(assignedDemandRow(testJobIDText, source, fixedTime().UnixNano(), false)[:5])
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
}

func exactDemandSource(
	t *testing.T,
	repository string,
	runnerPool core.RunnerPool,
	requestID int64,
) core.SourceReference {
	t.Helper()
	identifier, err := githubsource.EncodeSourceID(githubsource.SourceIdentity{
		Repository: repository, RunnerPool: runnerPool, RequestID: requestID,
	})
	if err != nil {
		t.Fatalf("encode exact demand source: %v", err)
	}
	return core.SourceReference{Adapter: githubsource.AdapterName, ID: identifier}
}

func TestReconcileAssignedDemandRemovesNewestAggregateQueuedJob(t *testing.T) {
	t.Parallel()
	demand := githubsource.AssignedDemand{
		Repository: "octo/repo", RunnerPool: "linux", ObservationID: "observation-1",
		DesiredJobs: 2, ObservedAt: fixedTime(),
	}
	olderSource, err := githubsource.AggregateDemandSource(demand, 1)
	if err != nil {
		t.Fatalf("older aggregate source: %v", err)
	}
	newerSource, err := githubsource.AggregateDemandSource(demand, 2)
	if err != nil {
		t.Fatalf("newer aggregate source: %v", err)
	}
	newestTieSource, err := githubsource.AggregateDemandSource(demand, 3)
	if err != nil {
		t.Fatalf("newest tied aggregate source: %v", err)
	}
	rows := [][]any{
		assignedDemandRow("00000000-0000-4000-8000-000000000001", olderSource,
			fixedTime().Add(-time.Minute).UnixNano(), false),
		assignedDemandRow("00000000-0000-4000-8000-000000000002", newerSource, fixedTime().UnixNano(), false),
		assignedDemandRow("00000000-0000-4000-8000-000000000003", newestTieSource, fixedTime().UnixNano(), false),
	}
	removedJobIDs := []string{}
	result := reconcileAssignedDemand(t, demand, assignedDemandRemovalTransaction(rows, &removedJobIDs))
	if len(removedJobIDs) != 1 || removedJobIDs[0] != "00000000-0000-4000-8000-000000000003" {
		t.Fatalf("removed jobs = %#v", removedJobIDs)
	}
	assertAssignedDemandResult(t, result, githubsource.AssignedDemandResult{
		ActiveJobs: 2, QueuedJobs: 2, JobsRemoved: 1,
	})
}
