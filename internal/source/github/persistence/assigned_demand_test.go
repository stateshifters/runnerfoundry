package persistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

func TestAssignedDemandAddsMissingAggregateJobs(t *testing.T) {
	demand := assignedDemand(2)
	existingSource := aggregateSource(t, demand, 1)
	addedSource := aggregateSource(t, demand, 2)
	transaction := &demandTransaction{
		jobs: []state.ActiveJob{activeJob("00000000-0000-0000-0000-000000000001", existingSource,
			demand.RunnerPool, state.ActiveJobQueued, demand.ObservedAt)},
		queueOutcomes: []core.QueueOutcome{core.QueueAlreadyQueued, core.QueueQueued},
	}
	transactions := &demandTransactionStore{transaction: transaction}
	store := NewAssignedDemandStore(transactions)

	result, err := store.ReconcileAssignedDemand(context.Background(), demand)

	want := githubsource.AssignedDemandResult{ActiveJobs: 2, QueuedJobs: 2, JobsAdded: 1}
	if err != nil || result != want {
		t.Fatalf("result = %#v, error = %v, want %#v", result, err, want)
	}
	if len(transaction.queued) != 2 {
		t.Fatalf("queued requests = %d, want 2", len(transaction.queued))
	}
	if transaction.queued[0].Source != existingSource || transaction.queued[1].Source != addedSource {
		t.Fatalf("queued sources = %#v", transaction.queued)
	}
	if transactions.key.Scope != transaction.queued[0].Scope || transactions.key.RunnerPool != demand.RunnerPool {
		t.Fatalf("transaction key = %#v, queued request = %#v", transactions.key, transaction.queued[0])
	}
}

func TestAssignedDemandRemovesNewestAggregateQueuedJobs(t *testing.T) {
	demand := assignedDemand(3)
	transaction := &demandTransaction{jobs: assignedDemandContractionJobs(t, demand)}
	store := NewAssignedDemandStore(&demandTransactionStore{transaction: transaction})

	result, err := store.ReconcileAssignedDemand(context.Background(), demand)

	want := githubsource.AssignedDemandResult{ActiveJobs: 3, QueuedJobs: 2, ProcessingJobs: 1, JobsRemoved: 1}
	if err != nil || result != want {
		t.Fatalf("result = %#v, error = %v, want %#v", result, err, want)
	}
	if len(transaction.removed) != 1 || transaction.removed[0].ID != jobID("00000000-0000-0000-0000-000000000003") {
		t.Fatalf("removed jobs = %#v", transaction.removed)
	}
}

func assignedDemandContractionJobs(t *testing.T, demand githubsource.AssignedDemand) []state.ActiveJob {
	t.Helper()
	previous := demand
	previous.ObservationID = "previous"
	otherRepository := githubsource.AssignedDemand{Repository: "other/repo", RunnerPool: demand.RunnerPool}
	return []state.ActiveJob{
		activeJob("00000000-0000-0000-0000-000000000001", aggregateSource(t, previous, 1),
			demand.RunnerPool, state.ActiveJobQueued, demand.ObservedAt.Add(-time.Minute)),
		activeJob("00000000-0000-0000-0000-000000000002", aggregateSource(t, previous, 2),
			demand.RunnerPool, state.ActiveJobQueued, demand.ObservedAt),
		activeJob("00000000-0000-0000-0000-000000000003", aggregateSource(t, previous, 3),
			demand.RunnerPool, state.ActiveJobQueued, demand.ObservedAt),
		activeJob("00000000-0000-0000-0000-000000000004", exactSource(t, demand, 42),
			demand.RunnerPool, state.ActiveJobProcessing, demand.ObservedAt),
		activeJob("00000000-0000-0000-0000-000000000005", exactSource(t, otherRepository, 43),
			demand.RunnerPool, state.ActiveJobQueued, demand.ObservedAt),
	}
}

func TestAssignedDemandReturnsNoResultWhenActiveJobLookupFails(t *testing.T) {
	expectedErr := errors.New("state failure")
	store := assignedDemandTestStore(&demandTransaction{activeJobsErr: expectedErr}, nil)
	assertAssignedDemandFailure(t, store, assignedDemand(1), expectedErr)
}

func TestAssignedDemandReturnsNoResultWhenQueueingFails(t *testing.T) {
	expectedErr := errors.New("state failure")
	transaction := &demandTransaction{queue: func(core.JobRequest) (core.EnqueueResult, error) {
		return core.EnqueueResult{}, expectedErr
	}}
	assertAssignedDemandFailure(t, assignedDemandTestStore(transaction, nil), assignedDemand(1), expectedErr)
}

func TestAssignedDemandReturnsNoResultWhenRemovalFails(t *testing.T) {
	expectedErr := errors.New("state failure")
	demand := assignedDemand(1)
	previous := demand
	previous.ObservationID = "previous"
	transaction := &demandTransaction{jobs: []state.ActiveJob{
		activeJob("00000000-0000-0000-0000-000000000001", aggregateSource(t, previous, 1),
			demand.RunnerPool, state.ActiveJobQueued, demand.ObservedAt),
		activeJob("00000000-0000-0000-0000-000000000002", aggregateSource(t, previous, 2),
			demand.RunnerPool, state.ActiveJobQueued, demand.ObservedAt),
	}, removeErr: expectedErr}
	assertAssignedDemandFailure(t, assignedDemandTestStore(transaction, nil), demand, expectedErr)
}

func TestAssignedDemandReturnsNoResultWhenTransactionCompletionFails(t *testing.T) {
	expectedErr := errors.New("state failure")
	store := assignedDemandTestStore(&demandTransaction{}, expectedErr)
	assertAssignedDemandFailure(t, store, assignedDemand(1), expectedErr)
}

func TestAssignedDemandContinuesAfterAQueuedCandidateIsAlreadyMissing(t *testing.T) {
	demand := assignedDemand(1)
	previous := demand
	previous.ObservationID = "previous"
	transaction := &demandTransaction{
		jobs: []state.ActiveJob{
			{ID: jobID("00000000-0000-0000-0000-000000000001"), Source: aggregateSource(t, previous, 1),
				RunnerPool: demand.RunnerPool, Phase: state.ActiveJobQueued, StateChangedAt: demand.ObservedAt},
			{ID: jobID("00000000-0000-0000-0000-000000000002"), Source: aggregateSource(t, previous, 2),
				RunnerPool: demand.RunnerPool, Phase: state.ActiveJobQueued, StateChangedAt: demand.ObservedAt},
		},
		remove: func(job state.ActiveJob) (state.QueuedRemovalOutcome, error) {
			if job.ID == jobID("00000000-0000-0000-0000-000000000002") {
				return state.QueuedJobMissing, nil
			}
			return state.QueuedJobRemoved, nil
		},
	}
	store := NewAssignedDemandStore(&demandTransactionStore{transaction: transaction})

	result, err := store.ReconcileAssignedDemand(context.Background(), demand)

	want := githubsource.AssignedDemandResult{ActiveJobs: 1, QueuedJobs: 1, JobsRemoved: 1}
	if err != nil || result != want {
		t.Fatalf("result = %#v, error = %v, want %#v", result, err, want)
	}
	if len(transaction.removed) != 2 {
		t.Fatalf("removal attempts = %d, want 2", len(transaction.removed))
	}
}

func TestAssignedDemandRejectsInvalidDemandBeforeStartingTransaction(t *testing.T) {
	transactionStore := &demandTransactionStore{transaction: &demandTransaction{}}
	store := NewAssignedDemandStore(transactionStore)

	result, err := store.ReconcileAssignedDemand(context.Background(), githubsource.AssignedDemand{})

	if err == nil {
		t.Fatal("expected invalid demand error")
	}
	if transactionStore.called {
		t.Fatal("transaction started for invalid demand")
	}
	if result != (githubsource.AssignedDemandResult{}) {
		t.Fatalf("result = %#v, want zero", result)
	}
}

type demandTransactionStore struct {
	transaction    state.DemandTransaction
	transactionErr error
	called         bool
	key            state.DemandTransactionKey
}

func (store *demandTransactionStore) WithDemandTransaction(
	_ context.Context,
	key state.DemandTransactionKey,
	reconcile func(state.DemandTransaction) error,
) error {
	store.called = true
	store.key = key
	if err := reconcile(store.transaction); err != nil {
		return err
	}
	return store.transactionErr
}

type demandTransaction struct {
	jobs          []state.ActiveJob
	activeJobsErr error
	queue         func(core.JobRequest) (core.EnqueueResult, error)
	queueOutcomes []core.QueueOutcome
	remove        func(state.ActiveJob) (state.QueuedRemovalOutcome, error)
	removeErr     error
	queued        []core.JobRequest
	removed       []state.ActiveJob
}

func (transaction *demandTransaction) ActiveJobs(
	context.Context,
) ([]state.ActiveJob, error) {
	return transaction.jobs, transaction.activeJobsErr
}

func (transaction *demandTransaction) Queue(
	_ context.Context,
	request core.JobRequest,
) (core.EnqueueResult, error) {
	transaction.queued = append(transaction.queued, request)
	if transaction.queue != nil {
		return transaction.queue(request)
	}
	if transaction.queueOutcomes != nil {
		return core.EnqueueResult{Outcome: transaction.queueOutcomes[len(transaction.queued)-1]}, nil
	}
	return core.EnqueueResult{Outcome: core.QueueQueued}, nil
}

func (transaction *demandTransaction) RemoveQueued(
	_ context.Context,
	job state.ActiveJob,
) (state.QueuedRemovalOutcome, error) {
	transaction.removed = append(transaction.removed, job)
	if transaction.removeErr != nil {
		return "", transaction.removeErr
	}
	if transaction.remove != nil {
		return transaction.remove(job)
	}
	return state.QueuedJobRemoved, nil
}

func assignedDemand(desiredJobs int) githubsource.AssignedDemand {
	return githubsource.AssignedDemand{
		Repository: "owner/repo", RunnerPool: "linux", ObservationID: "observation",
		DesiredJobs: desiredJobs, ObservedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
}

func assignedDemandTestStore(
	transaction *demandTransaction,
	transactionErr error,
) *AssignedDemandStore {
	return NewAssignedDemandStore(&demandTransactionStore{
		transaction: transaction, transactionErr: transactionErr,
	})
}

func assertAssignedDemandFailure(
	t *testing.T,
	store *AssignedDemandStore,
	demand githubsource.AssignedDemand,
	expectedErr error,
) {
	t.Helper()
	result, err := store.ReconcileAssignedDemand(context.Background(), demand)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("error = %v, want %v", err, expectedErr)
	}
	if result != (githubsource.AssignedDemandResult{}) {
		t.Fatalf("result = %#v, want zero", result)
	}
}

func activeJob(
	identifier string,
	source core.SourceReference,
	runnerPool core.RunnerPool,
	phase state.ActiveJobPhase,
	stateChangedAt time.Time,
) state.ActiveJob {
	return state.ActiveJob{
		ID: jobID(identifier), Source: source, RunnerPool: runnerPool, Phase: phase, StateChangedAt: stateChangedAt,
	}
}

func aggregateSource(t *testing.T, demand githubsource.AssignedDemand, ordinal int) core.SourceReference {
	t.Helper()
	source, err := githubsource.AggregateDemandSource(demand, ordinal)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func exactSource(t *testing.T, demand githubsource.AssignedDemand, requestID int64) core.SourceReference {
	t.Helper()
	identifier, err := githubsource.EncodeSourceID(githubsource.SourceIdentity{
		Repository: demand.Repository, RunnerPool: demand.RunnerPool, RequestID: requestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return core.SourceReference{Adapter: githubsource.AdapterName, ID: identifier}
}

func jobID(value string) core.JobID { return uuid.MustParse(value) }
