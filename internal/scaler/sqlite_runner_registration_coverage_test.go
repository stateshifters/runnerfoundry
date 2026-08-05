package scaler

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLiteRegistrationReportsInsertedRowCountFailure(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	runner := genericSQLiteRunner()
	expectMissingSQLiteRegistrationState(fixture.mock)
	fixture.mock.ExpectExec("INSERT INTO runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := registerSQLiteRunner(context.Background(), transaction, runner); err == nil {
		t.Fatal("expected registration affected-row failure")
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteRegistrationStateReportsInspectionFailure(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	runner := genericSQLiteRunner()
	fixture.mock.ExpectQuery("SELECT provider").WillReturnError(errors.New("inspect failed"))
	if _, err := loadSQLiteLifecycleRegistrationState(context.Background(), transaction, runner); err == nil {
		t.Fatal("expected mirror registration inspection failure")
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteRegistrationStateReportsRequestOwnerFailure(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	runner := genericSQLiteRunner()
	fixture.mock.ExpectQuery("SELECT provider").WillReturnError(sql.ErrNoRows)
	fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runner_lifecycle").WillReturnError(errors.New("inspect failed"))
	if _, err := loadSQLiteLifecycleRegistrationState(context.Background(), transaction, runner); err == nil {
		t.Fatal("expected mirror request owner failure")
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteGenericRegistrationValidationRejectsConflictingIdentity(t *testing.T) {
	runner := genericSQLiteRunner()
	err := validateSQLiteGenericRegistrationState(runner, sqliteLifecycleRegistrationState{
		genericExists: true, provider: runner.Provider, requestID: runner.RequestID + 1,
		assignmentID: runner.AssignmentID, phase: runner.Phase,
	})
	if !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("generic registration error = %v, want lifecycle consistency", err)
	}
}

func TestSQLiteGenericRegistrationValidationRejectsPendingReap(t *testing.T) {
	runner := genericSQLiteRunner()
	err := validateSQLiteGenericRegistrationState(runner, sqliteLifecycleRegistrationState{
		genericExists: true, provider: runner.Provider, requestID: runner.RequestID,
		assignmentID: runner.AssignmentID, phase: RunnerReapPending,
	})
	if !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("generic registration error = %v, want lifecycle consistency", err)
	}
}

func TestSQLiteGenericRegistrationValidationRejectsOwnerAndProviderConflicts(t *testing.T) {
	runner := genericSQLiteRunner()
	for name, state := range map[string]sqliteLifecycleRegistrationState{
		"owner":    {genericRequestOwnedByOtherName: true},
		"provider": {genericExists: true, provider: RunnerProviderLibvirt},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateSQLiteGenericRegistrationState(runner, state); !errors.Is(err, ErrRunnerLifecycleConsistency) {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
}

func TestSQLiteRegistrationReturnsValidationFailure(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	runner := genericSQLiteRunner()
	fixture.mock.ExpectQuery("SELECT provider").WillReturnRows(sqlmock.NewRows(
		[]string{"provider", "request_id", "assignment_id", "phase"},
	).AddRow("libvirt", runner.RequestID, runner.AssignmentID, runner.Phase))
	fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runner_lifecycle").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(0))
	if err := registerSQLiteRunner(context.Background(), transaction, runner); !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("registration error = %v", err)
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func expectMissingSQLiteRegistrationState(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT provider").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runner_lifecycle").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(0),
	)
}
