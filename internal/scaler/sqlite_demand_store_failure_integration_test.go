//go:build integration

package scaler

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestSQLiteDemandStoreReportsInsertTriggerFailure(t *testing.T) {
	store := newSQLiteFailureStore(t)
	if _, err := store.database.Exec(`CREATE TRIGGER reject_insert BEFORE INSERT ON demand BEGIN SELECT RAISE(FAIL, 'rejected'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(context.Background(), "linux", []JobDemand{{Repository: "repo", RequestID: 1}}, nil); err == nil {
		t.Fatal("expected insert failure")
	}
}

func TestSQLiteDemandStoreReportsDeleteTriggerFailure(t *testing.T) {
	store := newSQLiteFailureStore(t)
	if err := store.Apply(context.Background(), "linux", []JobDemand{{Repository: "repo", RequestID: 1}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.database.Exec(`CREATE TRIGGER reject_delete BEFORE DELETE ON demand BEGIN SELECT RAISE(FAIL, 'rejected'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(context.Background(), "linux", nil, []DemandRemoval{{Repository: "repo", RequestID: 1}}); err == nil {
		t.Fatal("expected removal failure")
	}
}

func TestSQLiteDemandStoreReportsDuplicateSchemaMigrationFailure(t *testing.T) {
	store := newSQLiteFailureStore(t)
	if _, err := store.database.Exec("PRAGMA user_version = 0"); err != nil {
		t.Fatal(err)
	}
	if err := store.migrate(context.Background()); err == nil {
		t.Fatal("expected duplicate table migration failure")
	}
}

func newSQLiteFailureStore(t *testing.T) *SQLiteDemandStore {
	t.Helper()
	store, err := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteDemandStore(t, store)
	return store
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
		t.Fatalf("expected released lock: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
}

func closeSQLiteDemandStore(t *testing.T, store *SQLiteDemandStore) {
	t.Helper()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close SQLite demand store: %v", err)
		}
	})
}
