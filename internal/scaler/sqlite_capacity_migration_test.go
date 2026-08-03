package scaler

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteMigrationSixPreservesExactPositiveDemandFIFO(t *testing.T) {
	store := openSQLiteVersionFiveStore(t)
	demand, err := store.Ordered(context.Background(), "linux")
	if err != nil {
		t.Fatal(err)
	}
	if len(demand) != 2 || demand[0].RequestID != 22 || demand[1].RequestID != 11 ||
		demand[0].QueueTime != time.Unix(1, 0).UTC() || demand[0].observationOrder != 9 ||
		demand[0].Source != CapacityObligationJobAvailable || demand[0].ObligationID != 9 {
		t.Fatalf("migrated demand = %#v", demand)
	}
}

func openSQLiteVersionFiveStore(t *testing.T) *SQLiteDemandStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	statements := []string{
		`CREATE TABLE demand (
			observation_order INTEGER PRIMARY KEY AUTOINCREMENT,
			scale_set TEXT NOT NULL, repository TEXT NOT NULL, request_id INTEGER NOT NULL,
			queue_time INTEGER NOT NULL, acquired INTEGER NOT NULL,
			UNIQUE(scale_set, repository, request_id))`,
		`INSERT INTO demand(observation_order, scale_set, repository, request_id, queue_time, acquired)
		 VALUES(9, 'linux', 'repo-b', 22, 1000000000, 0),
		       (10, 'linux', 'repo-a', 11, 2000000000, 1)`,
		`CREATE TABLE runner_lifecycle (
			scale_set TEXT NOT NULL, repository TEXT NOT NULL, runner_name TEXT NOT NULL,
			assignment_id INTEGER NOT NULL DEFAULT 0, phase TEXT NOT NULL)`,
		`PRAGMA user_version = 5`,
	}
	for _, statement := range statements {
		if _, err = database.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenSQLiteDemandStore(path)
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteStoreDuringCleanup(t, store)
	return store
}
