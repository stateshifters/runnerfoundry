package scaler

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLiteMessageUpdateRejectsInvalidUpdate(t *testing.T) {
	fixture := newSQLiteMessageUpdateFixture(t)
	if _, err := fixture.store.ApplyMessageUpdate(context.Background(), MessageUpdate{}); !errors.Is(err, ErrInvalidMessageUpdate) {
		t.Fatalf("invalid update error = %v", err)
	}
}

func TestSQLiteMessageUpdateReportsBeginFailure(t *testing.T) {
	want := errors.New("begin failed")
	fixture := newSQLiteMessageUpdateFixture(t)
	fixture.mock.ExpectBegin().WillReturnError(want)
	if _, err := fixture.store.ApplyMessageUpdate(context.Background(), validSQLiteMessageUpdate()); !errors.Is(err, want) {
		t.Fatalf("begin error = %v, want %v", err, want)
	}
}

func TestSQLiteMessageUpdateReportsCommitFailure(t *testing.T) {
	want := errors.New("commit failed")
	fixture := newSQLiteMessageUpdateFixture(t)
	expectSQLiteMessageThroughCapacityReconciliation(fixture.mock)
	fixture.mock.ExpectCommit().WillReturnError(want)
	if _, err := fixture.store.ApplyMessageUpdate(context.Background(), validSQLiteMessageUpdate()); !errors.Is(err, want) {
		t.Fatalf("commit error = %v, want %v", err, want)
	}
}

func expectSQLiteMessageThroughCapacityReconciliation(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM repository_controls").
		WillReturnRows(sqlmock.NewRows([]string{"current"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT.*runner_lifecycle").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	expectNoAssignedObservation(mock)
	mock.ExpectExec("INSERT INTO assigned_capacity_observations").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT assigned_observation_id").WillReturnRows(
		sqlmock.NewRows([]string{"assigned_observation_id"}).AddRow(1),
	)
	expectAssignedObligationAdoption(mock)
	mock.ExpectExec("DELETE FROM assigned_capacity_observations").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
}

func TestSQLiteMessageUpdateReportsEpochQueryFailure(t *testing.T) {
	want := errors.New("epoch query failed")
	fixture := newSQLiteMessageUpdateFixture(t)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectQuery("SELECT 1 FROM repository_controls").WillReturnError(want)
	fixture.mock.ExpectRollback()
	if _, err := fixture.store.ApplyMessageUpdate(context.Background(), validSQLiteMessageUpdate()); !errors.Is(err, want) {
		t.Fatalf("epoch query error = %v, want %v", err, want)
	}
}

func TestSQLiteMessageUpdateReportsActiveCapacityFailure(t *testing.T) {
	want := errors.New("capacity query failed")
	fixture := newSQLiteMessageUpdateFixture(t)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectQuery("SELECT 1 FROM repository_controls").
		WillReturnRows(sqlmock.NewRows([]string{"current"}).AddRow(1))
	fixture.mock.ExpectQuery("SELECT COUNT.*runner_lifecycle").WillReturnError(want)
	fixture.mock.ExpectRollback()
	if _, err := fixture.store.ApplyMessageUpdate(context.Background(), validSQLiteMessageUpdate()); !errors.Is(err, want) {
		t.Fatalf("capacity query error = %v, want %v", err, want)
	}
}
