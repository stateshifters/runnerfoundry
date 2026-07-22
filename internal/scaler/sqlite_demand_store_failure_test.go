package scaler

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLiteDemandStoreReportsDatabaseConfigurationFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	fixture.mock.ExpectExec("PRAGMA journal_mode").WillReturnError(errors.New("configure failed"))
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected database configuration error")
	}
}

func TestSQLiteDemandStoreReportsSchemaVersionReadFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	fixture.mock.ExpectExec("PRAGMA journal_mode").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectQuery("PRAGMA user_version").WillReturnError(errors.New("read failed"))
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected schema read error")
	}
}

func TestSQLiteDemandStoreReportsMigrationBeginFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 0)
	fixture.mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected migration begin error")
	}
}

func TestSQLiteDemandStoreReportsSchemaWriteAndRollbackFailures(t *testing.T) {
	fixture := mockDemandStore(t)
	writeErr := errors.New("write failed")
	rollbackErr := errors.New("rollback failed")
	expectSchemaVersion(fixture.mock, 0)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("CREATE TABLE demand").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectExec("PRAGMA user_version").WillReturnError(writeErr)
	fixture.mock.ExpectRollback().WillReturnError(rollbackErr)
	err := fixture.store.migrate(context.Background())
	if !errors.Is(err, writeErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("migration error = %v, want write and rollback failures", err)
	}
}

func TestSQLiteDemandStoreReportsMigrationCommitFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 0)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("CREATE TABLE demand").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectExec("PRAGMA user_version").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
	assertMigrationCommitFailure(t, fixture.store)
}

func TestSQLiteDemandStoreReportsLegacyMigrationBeginFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 1)
	fixture.mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected legacy migration begin error")
	}
}

func TestSQLiteDemandStoreReportsLegacyDemandRemovalFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 1)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("DELETE FROM demand").WillReturnError(errors.New("delete failed"))
	fixture.mock.ExpectRollback()
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected legacy demand removal error")
	}
}

func TestSQLiteDemandStoreReportsLegacyVersionWriteFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 1)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
	fixture.mock.ExpectExec("PRAGMA user_version").WillReturnError(errors.New("write failed"))
	fixture.mock.ExpectRollback()
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected legacy version write error")
	}
}

func TestSQLiteDemandStoreReportsLegacyMigrationCommitFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 1)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
	fixture.mock.ExpectExec("PRAGMA user_version").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
	assertMigrationCommitFailure(t, fixture.store)
}

func TestSQLiteDemandStoreReportsDemandCommitFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
	if err := fixture.store.Apply(context.Background(), "linux", nil, nil); err == nil {
		t.Fatal("expected demand commit error")
	}
}

func TestSQLiteDemandStoreReportsRowCloseFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	closeErr := errors.New("close failed")
	rows := sqlmock.NewRows([]string{"repository", "request_id", "queue_time", "acquired", "observation_order"}).
		AddRow("repo", "invalid", 0, false, 1).
		CloseError(closeErr)
	fixture.mock.ExpectQuery("SELECT repository").WillReturnRows(rows)
	if _, err := fixture.store.Ordered(context.Background(), "linux"); !errors.Is(err, closeErr) {
		t.Fatalf("scan error = %v, want row close failure", err)
	}
}

func TestSQLiteDemandStoreReportsRowIterationFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	rows := sqlmock.NewRows([]string{"repository", "request_id", "queue_time", "acquired", "observation_order"}).
		AddRow("repo", 1, 0, false, 1).
		RowError(0, errors.New("iteration failed"))
	fixture.mock.ExpectQuery("SELECT repository").WillReturnRows(rows)
	if _, err := fixture.store.Ordered(context.Background(), "linux"); err == nil {
		t.Fatal("expected iteration error")
	}
}

type demandStoreMock struct {
	store *SQLiteDemandStore
	mock  sqlmock.Sqlmock
}

func mockDemandStore(t *testing.T) demandStoreMock {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mock.ExpectClose()
		if err := database.Close(); err != nil {
			t.Error(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	return demandStoreMock{store: &SQLiteDemandStore{database: database}, mock: mock}
}

func expectSchemaVersion(mock sqlmock.Sqlmock, version int) {
	mock.ExpectExec("PRAGMA journal_mode").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(version))
}

func assertMigrationCommitFailure(t *testing.T, store *SQLiteDemandStore) {
	t.Helper()
	if err := store.migrate(context.Background()); err == nil {
		t.Fatal("expected migration commit error")
	}
}
