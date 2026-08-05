package scaler

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

var sqliteAssignedObservationForFailure = AssignedCapacityObservation{
	ScaleSet: "linux", Repository: "repo", ControllerEpoch: 1,
	AssignedJobs: 1, ObservedAt: time.Unix(1, 0).UTC(),
}

func TestSQLiteAssignedCapacityObservationReportsTransactionFailures(t *testing.T) {
	failure := errors.New("failed")
	t.Run("invalid", func(t *testing.T) {
		fixture := mockDemandStore(t)
		if _, err := fixture.store.ObserveAssignedCapacity(
			context.Background(), AssignedCapacityObservation{}, 0,
		); !errors.Is(err, ErrInvalidAssignedCapacityObservation) {
			t.Fatalf("invalid assigned observation error = %v", err)
		}
	})
	t.Run("begin", func(t *testing.T) {
		fixture := mockDemandStore(t)
		fixture.mock.ExpectBegin().WillReturnError(failure)
		assertAssignedCapacityObservationFailure(t, fixture.store, sqliteAssignedObservationForFailure, 0, failure)
	})
	t.Run("commit", func(t *testing.T) {
		fixture := mockDemandStore(t)
		expectAssignedObservationThroughCount(fixture.mock, 0)
		fixture.mock.ExpectCommit().WillReturnError(failure)
		assertAssignedCapacityObservationFailure(t, fixture.store, sqliteAssignedObservationForFailure, 1, failure)
	})
}

func TestSQLiteAssignedCapacityObservationReportsCurrentObservationFailure(t *testing.T) {
	assertConfiguredAssignedObservationFailure(t, 0, func(mock sqlmock.Sqlmock, failure error) {
		expectAssignedObservationBegin(mock)
		mock.ExpectQuery("SELECT controller_epoch").WillReturnError(failure)
	})
}

func TestSQLiteAssignedCapacityObservationReportsRecordFailure(t *testing.T) {
	assertConfiguredAssignedObservationFailure(t, 0, func(mock sqlmock.Sqlmock, failure error) {
		expectAssignedObservationBegin(mock)
		expectNoAssignedObservation(mock)
		mock.ExpectExec("INSERT INTO assigned_capacity_observations").WillReturnError(failure)
	})
}

func TestSQLiteAssignedCapacityObservationReportsRecordedObservationFailure(t *testing.T) {
	assertConfiguredAssignedObservationFailure(t, 0, func(mock sqlmock.Sqlmock, failure error) {
		expectAssignedObservationBegin(mock)
		expectNoAssignedObservation(mock)
		mock.ExpectExec("INSERT INTO assigned_capacity_observations").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery("SELECT assigned_observation_id").WillReturnError(failure)
	})
}

func TestSQLiteAssignedCapacityObservationReportsObsoleteRemovalFailure(t *testing.T) {
	assertConfiguredAssignedObservationFailure(t, 0, func(mock sqlmock.Sqlmock, failure error) {
		expectAssignedObservationThroughRecord(mock)
		expectAssignedObligationAdoption(mock)
		mock.ExpectExec("DELETE FROM assigned_capacity_observations").WillReturnError(failure)
	})
}

func TestSQLiteAssignedCapacityObservationReportsAdoptionFailure(t *testing.T) {
	assertConfiguredAssignedObservationFailure(t, 0, func(mock sqlmock.Sqlmock, failure error) {
		expectAssignedObservationThroughRecord(mock)
		mock.ExpectExec("UPDATE demand").WillReturnError(failure)
	})
}

func TestSQLiteAssignedCapacityObservationReportsQueuedCountFailure(t *testing.T) {
	assertConfiguredAssignedObservationFailure(t, 0, func(mock sqlmock.Sqlmock, failure error) {
		expectAssignedObservationThroughObsoleteRemoval(mock)
		mock.ExpectQuery("SELECT\\s+COALESCE\\(SUM").WillReturnError(failure)
	})
}

func TestSQLiteAssignedCapacityObservationReportsCancellationFailure(t *testing.T) {
	assertConfiguredAssignedObservationFailure(t, 1, func(mock sqlmock.Sqlmock, failure error) {
		expectAssignedObservationThroughCount(mock, 1)
		mock.ExpectExec("DELETE FROM demand WHERE demand_id IN").WillReturnError(failure)
	})
}

func TestSQLiteAssignedCapacityObservationReportsCreationFailure(t *testing.T) {
	assertConfiguredAssignedObservationFailure(t, 0, func(mock sqlmock.Sqlmock, failure error) {
		expectAssignedObservationThroughCount(mock, 0)
		mock.ExpectExec("INSERT INTO demand").WillReturnError(failure)
	})
}

func TestSQLiteAssignedCapacityObservationReportsBlockingFailure(t *testing.T) {
	observation := sqliteAssignedObservationForFailure
	observation.AssignedJobs = 1
	observation.BusyRunners = 1
	assertCustomAssignedObservationFailure(t, observation, 0, func(mock sqlmock.Sqlmock, failure error) {
		expectAssignedObservationThroughCounts(mock, 0, 0)
		mock.ExpectExec("INSERT INTO demand").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("UPDATE demand").WillReturnError(failure)
	})
}

func TestSQLiteAssignedCapacityObservationReportsUnblockingFailure(t *testing.T) {
	observation := sqliteAssignedObservationForFailure
	observation.AssignedJobs = 1
	assertCustomAssignedObservationFailure(t, observation, 0, func(mock sqlmock.Sqlmock, failure error) {
		expectAssignedObservationThroughCounts(mock, 0, 1)
		mock.ExpectExec("UPDATE demand").WillReturnError(failure)
	})
}

func assertCustomAssignedObservationFailure(
	t *testing.T,
	observation AssignedCapacityObservation,
	active int,
	configure func(sqlmock.Sqlmock, error),
) {
	t.Helper()
	failure := errors.New("failed")
	fixture := mockDemandStore(t)
	configure(fixture.mock, failure)
	fixture.mock.ExpectRollback()
	assertAssignedCapacityObservationFailure(t, fixture.store, observation, active, failure)
}

func assertConfiguredAssignedObservationFailure(
	t *testing.T,
	active int,
	configure func(sqlmock.Sqlmock, error),
) {
	t.Helper()
	failure := errors.New("failed")
	fixture := mockDemandStore(t)
	configure(fixture.mock, failure)
	fixture.mock.ExpectRollback()
	assertAssignedCapacityObservationFailure(t, fixture.store, sqliteAssignedObservationForFailure, active, failure)
}

func expectAssignedObservationBegin(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
}

func expectNoAssignedObservation(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT controller_epoch").WillReturnRows(sqlmock.NewRows([]string{"controller_epoch", "observed_at"}))
}

func expectAssignedObservationThroughRecord(mock sqlmock.Sqlmock) {
	expectAssignedObservationBegin(mock)
	expectNoAssignedObservation(mock)
	mock.ExpectExec("INSERT INTO assigned_capacity_observations").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT assigned_observation_id").WillReturnRows(
		sqlmock.NewRows([]string{"assigned_observation_id"}).AddRow(1),
	)
}

func expectAssignedObservationThroughObsoleteRemoval(mock sqlmock.Sqlmock) {
	expectAssignedObservationThroughRecord(mock)
	expectAssignedObligationAdoption(mock)
	mock.ExpectExec("DELETE FROM assigned_capacity_observations").WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectAssignedObligationAdoption(mock sqlmock.Sqlmock) {
	mock.ExpectExec("UPDATE demand").WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectAssignedObservationThroughCount(mock sqlmock.Sqlmock, queued int) {
	expectAssignedObservationThroughCounts(mock, queued, 0)
}

func expectAssignedObservationThroughCounts(mock sqlmock.Sqlmock, queued, blocked int) {
	expectAssignedObservationThroughObsoleteRemoval(mock)
	mock.ExpectQuery("SELECT\\s+COALESCE\\(SUM").WillReturnRows(
		sqlmock.NewRows([]string{"queued", "blocked"}).AddRow(queued, blocked),
	)
}

func assertAssignedCapacityObservationFailure(
	t *testing.T,
	store *SQLiteDemandStore,
	observation AssignedCapacityObservation,
	active int,
	want error,
) {
	t.Helper()
	if _, err := store.ObserveAssignedCapacity(context.Background(), observation, active); !errors.Is(err, want) {
		t.Fatalf("assigned observation error = %v, want %v", err, want)
	}
}

func TestSQLiteCapacityObligationRetryFailures(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2, 0).UTC()
	failure := errors.New("failed")

	fixture := mockDemandStore(t)
	if err := fixture.store.RecordObligationFailure(ctx, 0, now); !errors.Is(err, ErrInvalidCapacityObligation) {
		t.Fatalf("invalid retry error = %v", err)
	}
	fixture.mock.ExpectExec("UPDATE demand").WillReturnError(failure)
	if err := fixture.store.RecordObligationFailure(ctx, 1, now); !errors.Is(err, failure) {
		t.Fatalf("retry update error = %v", err)
	}
	fixture.mock.ExpectExec("UPDATE demand").WillReturnResult(failingSQLResult{})
	if err := fixture.store.RecordObligationFailure(ctx, 1, now); err == nil {
		t.Fatalf("retry result error = %v", err)
	}
	fixture.mock.ExpectExec("UPDATE demand").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := fixture.store.RecordObligationFailure(ctx, 1, now); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing retry error = %v", err)
	}
}

func TestSQLiteCapacityObligationDeletionFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("failed")
	fixture := mockDemandStore(t)
	if err := fixture.store.DeleteObligation(ctx, 0); !errors.Is(err, ErrInvalidCapacityObligation) {
		t.Fatalf("invalid delete error = %v", err)
	}
	fixture.mock.ExpectExec("DELETE FROM demand WHERE demand_id").WillReturnError(failure)
	if err := fixture.store.DeleteObligation(ctx, 1); !errors.Is(err, failure) {
		t.Fatalf("delete error = %v", err)
	}
	fixture.mock.ExpectExec("DELETE FROM demand WHERE demand_id").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := fixture.store.DeleteObligation(ctx, 1); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteLifecycleOwnedObligationDeletionFailures(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("failed")
	fixture := mockDemandStore(t)
	if err := fixture.store.DeleteLifecycleOwnedObligations(ctx, " "); !errors.Is(err, ErrInvalidCapacityObligation) {
		t.Fatalf("invalid lifecycle cleanup error = %v", err)
	}
	fixture.mock.ExpectExec("DELETE FROM demand").WillReturnError(failure)
	if err := fixture.store.DeleteLifecycleOwnedObligations(ctx, "linux"); !errors.Is(err, failure) {
		t.Fatalf("lifecycle cleanup error = %v", err)
	}
}

func TestSQLiteRemoveCapacityAuthorityReportsTransactionFailures(t *testing.T) {
	failure := errors.New("failed")
	tests := []struct {
		name      string
		configure func(sqlmock.Sqlmock)
	}{
		{name: "observation-delete", configure: func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM assigned_capacity_observations").WillReturnError(failure)
			mock.ExpectRollback()
		}},
		{name: "demand-delete", configure: func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM assigned_capacity_observations").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("DELETE FROM demand").WillReturnError(failure)
			mock.ExpectRollback()
		}},
		{name: "commit", configure: func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM assigned_capacity_observations").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectCommit().WillReturnError(failure)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := mockDemandStore(t)
			test.configure(fixture.mock)
			if err := fixture.store.RemoveRepository(context.Background(), "linux", "repo"); !errors.Is(err, failure) {
				t.Fatalf("remove capacity authority error = %v", err)
			}
		})
	}
}
