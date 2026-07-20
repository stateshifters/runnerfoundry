package scaler

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLiteDemandStoreReportsMutationFailures(t *testing.T) {
	store, err := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if _, err := store.database.Exec(`CREATE TRIGGER reject_insert BEFORE INSERT ON demand BEGIN SELECT RAISE(FAIL, 'rejected'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, "linux", []JobDemand{{Repository: "repo", RequestID: 1}}, nil); err == nil {
		t.Fatal("expected insert failure")
	}
	if _, err := store.database.Exec("DROP TRIGGER reject_insert"); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, "linux", []JobDemand{{Repository: "repo", RequestID: 1}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.database.Exec(`CREATE TRIGGER reject_delete BEFORE DELETE ON demand BEGIN SELECT RAISE(FAIL, 'rejected'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, "linux", nil, []DemandRemoval{{Repository: "repo", RequestID: 1}}); err == nil {
		t.Fatal("expected removal failure")
	}
	if _, err := store.database.Exec("DROP TRIGGER reject_delete"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.database.Exec("PRAGMA user_version = 0"); err != nil {
		t.Fatal(err)
	}
	if err := store.migrate(ctx); err == nil {
		t.Fatal("expected duplicate table migration failure")
	}
}

func TestSQLiteDemandStoreRejectsUnavailableDirectory(t *testing.T) {
	if _, err := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "missing", "state.db")); err == nil {
		t.Fatal("expected lock path failure")
	}
}

func TestSQLiteDemandStoreReleasesLockWhenDatabaseOpenFails(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	if _, err := createSQLiteDemandStore(databasePath, func(string) (*sql.DB, error) { return nil, errors.New("open failed") }); err == nil {
		t.Fatal("expected database open failure")
	}
	store, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatalf("expected released lock, got %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteDemandStoreReportsDatabaseProtocolFailures(t *testing.T) {
	t.Run("database configuration", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		mock.ExpectExec("PRAGMA journal_mode").WillReturnError(errors.New("configure failed"))
		if err := store.migrate(context.Background()); err == nil {
			t.Fatal("expected database configuration error")
		}
	})
	t.Run("schema version read", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		mock.ExpectExec("PRAGMA journal_mode").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("PRAGMA user_version").WillReturnError(errors.New("read failed"))
		if err := store.migrate(context.Background()); err == nil {
			t.Fatal("expected schema read error")
		}
	})
	t.Run("migration begin", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		expectSchemaVersion(mock, 0)
		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
		if err := store.migrate(context.Background()); err == nil {
			t.Fatal("expected migration begin error")
		}
	})
	t.Run("schema version write", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		expectSchemaVersion(mock, 0)
		mock.ExpectBegin()
		mock.ExpectExec("CREATE TABLE demand").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("PRAGMA user_version").WillReturnError(errors.New("write failed"))
		mock.ExpectRollback()
		if err := store.migrate(context.Background()); err == nil {
			t.Fatal("expected schema write error")
		}
	})
	t.Run("migration commit", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		expectSchemaVersion(mock, 0)
		mock.ExpectBegin()
		mock.ExpectExec("CREATE TABLE demand").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("PRAGMA user_version").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
		if err := store.migrate(context.Background()); err == nil {
			t.Fatal("expected migration commit error")
		}
	})
	legacyMigrationFailures := map[string]func(sqlmock.Sqlmock){
		"legacy migration begin": func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
		},
		"legacy demand removal": func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM demand").WillReturnError(errors.New("delete failed"))
			mock.ExpectRollback()
		},
		"legacy version write": func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("PRAGMA user_version").WillReturnError(errors.New("write failed"))
			mock.ExpectRollback()
		},
		"legacy migration commit": func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("PRAGMA user_version").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
		},
	}
	for testName, configureFailure := range legacyMigrationFailures {
		t.Run(testName, func(t *testing.T) {
			store, mock := mockDemandStore(t)
			expectSchemaVersion(mock, 1)
			configureFailure(mock)
			if err := store.migrate(context.Background()); err == nil {
				t.Fatal("expected legacy migration failure")
			}
		})
	}
	t.Run("demand commit", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		mock.ExpectBegin()
		mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
		if err := store.Apply(context.Background(), "linux", nil, nil); err == nil {
			t.Fatal("expected demand commit error")
		}
	})
	t.Run("demand scan", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		rows := sqlmock.NewRows([]string{"repository", "request_id", "queue_time", "acquired", "observation_order"}).AddRow("repo", "invalid", 0, false, 1)
		mock.ExpectQuery("SELECT repository").WillReturnRows(rows)
		if _, err := store.Ordered(context.Background(), "linux"); err == nil {
			t.Fatal("expected scan error")
		}
	})
	t.Run("demand iteration", func(t *testing.T) {
		store, mock := mockDemandStore(t)
		rows := sqlmock.NewRows([]string{"repository", "request_id", "queue_time", "acquired", "observation_order"}).AddRow("repo", 1, 0, false, 1).RowError(0, errors.New("iteration failed"))
		mock.ExpectQuery("SELECT repository").WillReturnRows(rows)
		if _, err := store.Ordered(context.Background(), "linux"); err == nil {
			t.Fatal("expected iteration error")
		}
	})
}

func mockDemandStore(t *testing.T) (*SQLiteDemandStore, sqlmock.Sqlmock) { //nolint:ireturn // sqlmock exposes its expectation API through this interface.
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
	})
	return &SQLiteDemandStore{database: database}, mock
}

func expectSchemaVersion(mock sqlmock.Sqlmock, version int) {
	mock.ExpectExec("PRAGMA journal_mode").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(version))
}
