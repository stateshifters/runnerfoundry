package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestReconcileAssignedDemandCountsOnlyMatchingRepositoryDemand(t *testing.T) {
	t.Parallel()
	demand := githubsource.AssignedDemand{Repository: "owner/repository", RunnerPool: "linux",
		ObservationID: "observation-1", DesiredJobs: 3, ObservedAt: fixedTime()}
	protocol := matchingRepositoryDemandProtocol(t, demand)
	result, err := NewStore(protocol.executor()).ReconcileAssignedDemand(context.Background(), demand)
	if err != nil {
		t.Fatal(err)
	}
	want := githubsource.AssignedDemandResult{ActiveJobs: 3, QueuedJobs: 2, ProcessingJobs: 1, JobsAdded: 1}
	if result != want || protocol.commitCalls != 1 {
		t.Fatalf("result = %#v, commits = %d, want %#v and one commit", result, protocol.commitCalls, want)
	}
	assertAssignedDemandScopeLock(t, demand, protocol)
}

func matchingRepositoryDemandProtocol(
	t *testing.T,
	demand githubsource.AssignedDemand,
) *recordedTransaction {
	t.Helper()
	exactQueued := githubDemandSource(t, "owner/repository", "linux", 101)
	exactProcessing := githubDemandSource(t, "owner/repository", "linux", 102)
	otherRepository := githubDemandSource(t, "owner/other", "linux", 103)
	aggregateSource, err := githubsource.AggregateDemandSource(demand, 1)
	if err != nil {
		t.Fatal(err)
	}
	return &recordedTransaction{
		queryRows: rowsFromValues([][]any{
			{testJobID1, string(exactQueued.Adapter), string(exactQueued.ID), "queued", fixedTime()},
			{assignedDemandJobID(2), string(exactProcessing.Adapter), string(exactProcessing.ID), "processing", fixedTime()},
			{assignedDemandJobID(3), string(otherRepository.Adapter), string(otherRepository.ID), "queued", fixedTime()},
		}),
		rows: []Row{rowFromValues(queuedJobValues(assignedDemandJobID(4), core.JobRequest{
			Source: aggregateSource, RunnerPool: demand.RunnerPool, EnqueuedAt: demand.ObservedAt,
		}))},
	}
}

func TestReconcileAssignedDemandRemovesNewestAggregateQueuedJobsOnly(t *testing.T) {
	t.Parallel()
	demand := githubsource.AssignedDemand{Repository: "owner/repository", RunnerPool: "linux",
		ObservationID: "observation-current", DesiredJobs: 3, ObservedAt: fixedTime()}
	protocol := surplusAssignedDemandProtocol(t, demand)
	result, err := NewStore(protocol.executor()).ReconcileAssignedDemand(context.Background(), demand)
	if err != nil {
		t.Fatal(err)
	}
	want := githubsource.AssignedDemandResult{ActiveJobs: 3, QueuedJobs: 2, ProcessingJobs: 1, JobsRemoved: 2}
	if result != want {
		t.Fatalf("result = %#v, want %#v", result, want)
	}
	oldDemand := demand
	oldDemand.ObservationID = "observation-old"
	newestSource := aggregateDemandSource(t, oldDemand, 3)
	if len(protocol.statements) != 6 || protocol.statements[2] != serializeSourceSQL ||
		!strings.Contains(protocol.statements[3], "DELETE FROM public.runnerfoundry_queued_jobs") ||
		protocol.arguments[3][0] != assignedDemandJobID(5) || protocol.statements[4] != serializeSourceSQL ||
		protocol.arguments[5][0] != assignedDemandJobID(3) ||
		!reflect.DeepEqual(protocol.arguments[2], []any{advisoryLockIdentity(
			string(newestSource.Adapter), string(newestSource.ID),
		)}) ||
		!reflect.DeepEqual(protocol.arguments[3], []any{assignedDemandJobID(5), newestSource.Adapter, newestSource.ID}) {
		t.Fatalf("surplus removal protocol = %#v %#v", protocol.statements, protocol.arguments)
	}
}

func TestReconcileAssignedDemandIsIdempotentWhenObservationRepeats(t *testing.T) {
	t.Parallel()
	demand := githubsource.AssignedDemand{Repository: "owner/repository", RunnerPool: "linux",
		ObservationID: "observation-repeat", DesiredJobs: 1, ObservedAt: fixedTime()}
	store, secondProtocol := repeatingAssignedDemandStore(t, demand)
	first, err := store.ReconcileAssignedDemand(context.Background(), demand)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.ReconcileAssignedDemand(context.Background(), demand)
	if err != nil {
		t.Fatal(err)
	}
	if first.JobsAdded != 1 || first.ActiveJobs != 1 || second.JobsAdded != 0 || second.ActiveJobs != 1 ||
		len(secondProtocol.statements) != 2 {
		t.Fatalf("first = %#v, second = %#v, repeat protocol = %#v", first, second, secondProtocol.statements)
	}
}

func surplusAssignedDemandProtocol(t *testing.T, demand githubsource.AssignedDemand) *recordedTransaction {
	t.Helper()
	oldDemand := demand
	oldDemand.ObservationID = "observation-old"
	aggregateOlder := aggregateDemandSource(t, oldDemand, 1)
	aggregateNewer := aggregateDemandSource(t, oldDemand, 2)
	aggregateNewestTie := aggregateDemandSource(t, oldDemand, 3)
	exactQueued := githubDemandSource(t, demand.Repository, demand.RunnerPool, 201)
	exactProcessing := githubDemandSource(t, demand.Repository, demand.RunnerPool, 202)
	return &recordedTransaction{queryRows: rowsFromValues([][]any{
		{testJobID1, string(exactQueued.Adapter), string(exactQueued.ID), "queued", fixedTime().Add(-4 * time.Minute)},
		{assignedDemandJobID(2), string(aggregateOlder.Adapter), string(aggregateOlder.ID), "queued", fixedTime().Add(-3 * time.Minute)},
		{assignedDemandJobID(3), string(aggregateNewer.Adapter), string(aggregateNewer.ID), "queued", fixedTime().Add(-2 * time.Minute)},
		{assignedDemandJobID(5), string(aggregateNewestTie.Adapter), string(aggregateNewestTie.ID), "queued", fixedTime().Add(-2 * time.Minute)},
		{assignedDemandJobID(4), string(exactProcessing.Adapter), string(exactProcessing.ID), "processing", fixedTime().Add(-time.Minute)},
	})}
}

func repeatingAssignedDemandStore(
	t *testing.T,
	demand githubsource.AssignedDemand,
) (*Store, *recordedTransaction) {
	t.Helper()
	source := aggregateDemandSource(t, demand, 1)
	firstProtocol := &recordedTransaction{queryRows: rowsFromValues(nil), rows: []Row{
		rowFromValues(queuedJobValues(testJobID1, core.JobRequest{
			Source: source, RunnerPool: demand.RunnerPool, EnqueuedAt: demand.ObservedAt,
		})),
	}}
	secondProtocol := &recordedTransaction{queryRows: rowsFromValues([][]any{
		{testJobID1, string(source.Adapter), string(source.ID), "queued", demand.ObservedAt},
	})}
	executors := []Executor{firstProtocol.executor(), secondProtocol.executor()}
	beginCalls := 0
	return NewStore(Executor{begin: func(ctx context.Context) (Transaction, error) {
		transaction, beginErr := executors[beginCalls].Begin(ctx)
		beginCalls++
		return transaction, beginErr
	}}), secondProtocol
}

func aggregateDemandSource(t *testing.T, demand githubsource.AssignedDemand, ordinal int) core.SourceReference {
	t.Helper()
	source, err := githubsource.AggregateDemandSource(demand, ordinal)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func assertAssignedDemandScopeLock(
	t *testing.T,
	demand githubsource.AssignedDemand,
	protocol *recordedTransaction,
) {
	t.Helper()
	aggregateRequest, err := githubsource.AggregateDemandRequest(demand, 1)
	if err != nil {
		t.Fatal(err)
	}
	wantArguments := []any{advisoryLockIdentity(
		string(aggregateRequest.Scope.Adapter), string(aggregateRequest.Scope.ID),
	)}
	if protocol.statements[0] != serializeDemandScopeSQL || !reflect.DeepEqual(protocol.arguments[0], wantArguments) {
		t.Fatalf("reconciliation scope lock = %#v %#v", protocol.statements[0], protocol.arguments[0])
	}
}

func TestReconcileAssignedDemandDoesNotRecreateTombstonedAggregateJob(t *testing.T) {
	t.Parallel()
	demand := githubsource.AssignedDemand{Repository: "owner/repository", RunnerPool: "linux",
		ObservationID: "observation-terminal", DesiredJobs: 1, ObservedAt: fixedTime()}
	protocol := &recordedTransaction{queryRows: rowsFromValues(nil), rows: []Row{
		{scan: func(...any) error { return sql.ErrNoRows }},
		{scan: func(...any) error { return sql.ErrNoRows }},
		rowFromValues([]any{string(core.QueueSourceTerminal)}),
	}}
	result, err := NewStore(protocol.executor()).ReconcileAssignedDemand(context.Background(), demand)
	if err != nil {
		t.Fatal(err)
	}
	if result != (githubsource.AssignedDemandResult{}) || protocol.commitCalls != 1 {
		t.Fatalf("result = %#v, commits = %d", result, protocol.commitCalls)
	}
}

func TestReconcileAssignedDemandReportsCommitAndRollbackUncertainty(t *testing.T) {
	t.Parallel()
	commitErr := errors.New("commit outcome uncertain")
	rollbackErr := errors.New("rollback outcome uncertain")
	transaction := Transaction{
		execute: successfulExecute,
		query:   func(context.Context, string, ...any) (Rows, error) { return rowsFromValues(nil), nil },
		commit:  failingCompletion(commitErr), rollback: failingCompletion(rollbackErr),
	}
	demand := githubsource.AssignedDemand{Repository: "owner/repository", RunnerPool: "linux",
		ObservationID: "observation-commit", DesiredJobs: 0, ObservedAt: fixedTime()}
	_, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ReconcileAssignedDemand(context.Background(), demand)
	if !errors.Is(err, commitErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("error = %v, want commit and rollback uncertainty", err)
	}
}

func TestReconcileAssignedDemandReportsMutationAndRollbackFailures(t *testing.T) {
	t.Parallel()
	mutationErr := errors.New("source lock failed")
	rollbackErr := errors.New("rollback failed")
	executeCalls := 0
	transaction := Transaction{
		execute: func(ctx context.Context, statement string, arguments ...any) (Result, error) {
			executeCalls++
			if executeCalls == 2 {
				return Result{}, mutationErr
			}
			return successfulExecute(ctx, statement, arguments...)
		},
		query:  func(context.Context, string, ...any) (Rows, error) { return rowsFromValues(nil), nil },
		commit: successfulCompletion, rollback: failingCompletion(rollbackErr),
	}
	demand := githubsource.AssignedDemand{Repository: "owner/repository", RunnerPool: "linux",
		ObservationID: "observation-mutation", DesiredJobs: 1, ObservedAt: fixedTime()}
	_, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		ReconcileAssignedDemand(context.Background(), demand)
	if !errors.Is(err, mutationErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("error = %v, want mutation and rollback failures", err)
	}
}

func assignedDemandJobID(ordinal int) core.JobID {
	return uuid.MustParse("00000000-0000-0000-0000-" + fmt.Sprintf("%012d", ordinal))
}

func githubDemandSource(
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
		t.Fatal(err)
	}
	return core.SourceReference{Adapter: githubsource.AdapterName, ID: identifier}
}
