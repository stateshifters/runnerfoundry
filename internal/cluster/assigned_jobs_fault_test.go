package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
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
			rowResults: []fakeRow{{scan: func(destinations ...any) {
				*destination[int](t, destinations[0]) = 0
				*destination[int](t, destinations[1]) = 0
			}}},
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

func TestAssignedJobOperationsRejectInvalidInputAndConnectionFailures(t *testing.T) {
	store := NewStore(Settings{})
	store.connector = func(context.Context) (*databaseConnection, error) { return nil, errors.New("failed") }
	if _, err := store.ObserveAssignedJobs(context.Background(), assignedObservationFixture()); err == nil {
		t.Fatal("expected assigned observation connection failure")
	}
	if _, err := store.AssignAssignedJob(
		context.Background(), assignedObservationFixture(), time.Now().UTC(), time.Minute,
	); err == nil {
		t.Fatal("expected assigned assignment connection failure")
	}
	invalid := assignedObservationFixture()
	invalid.AppID = 0
	if _, err := storeWithConnection(&fakeDatabaseConnection{}).ObserveAssignedJobs(context.Background(), invalid); !errors.Is(err, ErrInvalidDemand) {
		t.Fatalf("invalid assigned observation error = %v", err)
	}
	if _, err := storeWithConnection(&fakeDatabaseConnection{}).AssignAssignedJob(
		context.Background(), assignedObservationFixture(), time.Time{}, time.Minute,
	); !errors.Is(err, ErrInvalidDemand) {
		t.Fatalf("invalid assigned assignment error = %v", err)
	}
}

func TestAssignAssignedJobReportsDatabaseFailure(t *testing.T) {
	failure := errors.New("failed")
	store := storeWithConnection(&fakeDatabaseConnection{rowResults: []fakeRow{{err: failure}}})
	_, err := store.AssignAssignedJob(
		context.Background(), assignedObservationFixture(), time.Now().UTC(), time.Minute,
	)
	if err == nil {
		t.Fatal("expected assigned assignment database failure")
	}
}
