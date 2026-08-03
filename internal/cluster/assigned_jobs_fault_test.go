package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestObserveAssignedJobsReportsEveryPersistenceFailure(t *testing.T) {
	failure := errors.New("failed")
	observation := assignedObservationFixture()
	if _, err := storeWithConnection(&fakeDatabaseConnection{beginErr: failure}).ObserveAssignedJobs(
		context.Background(), observation); err == nil {
		t.Fatal("expected assigned observation begin failure")
	}
}

func TestObserveAssignedJobsReportsEveryWriteFailure(t *testing.T) {
	failure := errors.New("failed")
	observation := assignedObservationFixture()
	for failureIndex := range 7 {
		t.Run(fmt.Sprintf("write %d", failureIndex), func(t *testing.T) {
			executionErrors := make([]error, 7)
			executionErrors[failureIndex] = failure
			transaction := &fakeDatabaseTransaction{execErrors: executionErrors}
			_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ObserveAssignedJobs(
				context.Background(), observation)
			if err == nil {
				t.Fatal("expected assigned observation write failure")
			}
		})
	}
}

func TestObserveAssignedJobsReportsCapacityAndCommitFailures(t *testing.T) {
	failure := errors.New("failed")
	observation := assignedObservationFixture()
	for name, transaction := range map[string]*fakeDatabaseTransaction{
		"capacity": {rowResults: []fakeRow{{err: failure}}},
		"commit": {
			rowResults: []fakeRow{
				{scan: func(destinations ...any) {
					*destination[int](t, destinations[0]) = 0
					*destination[int](t, destinations[1]) = 0
				}},
				{scan: func(destinations ...any) { *destination[int](t, destinations[0]) = 0 }},
			},
			commitErr: failure,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ObserveAssignedJobs(
				context.Background(), observation)
			if err == nil {
				t.Fatal("expected assigned observation lifecycle failure")
			}
		})
	}
}

func TestObserveAssignedJobsReportsQueuedObligationCountFailure(t *testing.T) {
	failure := errors.New("failed")
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{
		assignedJobCapacityRow(t, 0, 3), {err: failure},
	}}
	assertAssignedObservationFailure(t, transaction, assignedObservationFixture(), failure)
}

func TestObserveAssignedJobsReportsQueuedObligationCancellationFailure(t *testing.T) {
	failure := errors.New("failed")
	transaction := &fakeDatabaseTransaction{
		rowResults: []fakeRow{assignedJobCapacityRow(t, 0, 3), queuedObligationCountRow(t, 3)},
		execErrors: assignedObservationWritesThen(failure),
	}
	observation := assignedObservationFixture()
	observation.AssignedJobs = 0
	assertAssignedObservationFailure(t, transaction, observation, failure)
}

func TestObserveAssignedJobsReportsQueuedObligationCreationFailure(t *testing.T) {
	failure := errors.New("failed")
	transaction := &fakeDatabaseTransaction{
		rowResults: []fakeRow{assignedJobCapacityRow(t, 0, 3), queuedObligationCountRow(t, 0)},
		execErrors: assignedObservationWritesThen(failure),
	}
	assertAssignedObservationFailure(t, transaction, assignedObservationFixture(), failure)
}

func assignedJobCapacityRow(t *testing.T, active int, available int) fakeRow {
	t.Helper()
	return fakeRow{scan: func(destinations ...any) {
		*destination[int](t, destinations[0]) = active
		*destination[int](t, destinations[1]) = available
	}}
}

func queuedObligationCountRow(t *testing.T, count int) fakeRow {
	t.Helper()
	return fakeRow{scan: func(destinations ...any) {
		*destination[int](t, destinations[0]) = count
	}}
}

func assignedObservationWritesThen(failure error) []error {
	return append(make([]error, 7), failure)
}

func assertAssignedObservationFailure(
	t *testing.T,
	transaction *fakeDatabaseTransaction,
	observation AssignedJobObservation,
	failure error,
) {
	t.Helper()
	_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ObserveAssignedJobs(
		context.Background(), observation,
	)
	if !errors.Is(err, failure) {
		t.Fatalf("obligation reconciliation error = %v, want %v", err, failure)
	}
}

func TestAssignedJobOperationsRejectInvalidInputAndConnectionFailures(t *testing.T) {
	store := NewStore(Settings{})
	store.connector = func(context.Context) (*databaseConnection, error) { return nil, errors.New("failed") }
	if _, err := store.ObserveAssignedJobs(context.Background(), assignedObservationFixture()); err == nil {
		t.Fatal("expected assigned observation connection failure")
	}
	invalid := assignedObservationFixture()
	invalid.AppID = 0
	if _, err := storeWithConnection(&fakeDatabaseConnection{}).ObserveAssignedJobs(context.Background(), invalid); !errors.Is(err, ErrInvalidDemand) {
		t.Fatalf("invalid assigned observation error = %v", err)
	}
}
