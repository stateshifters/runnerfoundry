package scaler

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestSQLiteLifecycleBridgeContractionFreshInstall(t *testing.T) {
	_, database := openSQLiteMigrationStore(t, 8)
	assertSQLiteTablePresent(t, database, "runner_lifecycle")
	assertSQLiteTableAbsent(t, database, "libvirt_runner_lifecycle")
}

func TestSQLiteLifecycleBridgeContractionPreservesExactGenericHistory(t *testing.T) {
	store, database := openSQLiteMigrationStore(t, 7)
	mustExecuteSQLiteStatements(t, database,
		`INSERT INTO runner_lifecycle(node_id, scale_set, repository, provider, runner_name,
			request_id, assignment_id, installation_id, github_scale_set_id, github_runner_id,
			phase, created_at, waiting_since, updated_at, reap_reason)
		 VALUES('node', 'linux', 'repo', 'libvirt', 'runner', 42, NULL, 7, 8, 9,
			'reap_pending', 100, 0, 200, 'job_completed')`,
		`INSERT INTO libvirt_runner_lifecycle(scale_set, repository, request_id, runner_name, reap_requested)
		 VALUES('linux', 'repo', 42, 'runner', 1)`)
	var before string
	if err := database.QueryRow(`SELECT printf('%s|%s|%s|%s|%s|%d|%d|%d|%d|%d|%s|%d|%d|%d|%s',
		node_id, scale_set, repository, provider, runner_name, request_id, assignment_id,
		installation_id, github_scale_set_id, github_runner_id, phase, created_at,
		waiting_since, updated_at, reap_reason) FROM runner_lifecycle`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := store.migrateTo(context.Background(), 8); err != nil {
		t.Fatal(err)
	}
	var after string
	if err := database.QueryRow(`SELECT printf('%s|%s|%s|%s|%s|%d|%d|%d|%d|%d|%s|%d|%d|%d|%s',
		node_id, scale_set, repository, provider, runner_name, request_id, assignment_id,
		installation_id, github_scale_set_id, github_runner_id, phase, created_at,
		waiting_since, updated_at, reap_reason) FROM runner_lifecycle`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("generic lifecycle changed during contraction: before=%q after=%q", before, after)
	}
	assertSQLiteTableAbsent(t, database, "libvirt_runner_lifecycle")
}

func TestSQLiteLifecycleBridgeContractionRefusesUnsafeStateBeforeMutation(t *testing.T) {
	store, database := openSQLiteMigrationStore(t, 7)
	mustExecuteSQLiteStatements(t, database,
		`DROP TRIGGER mirror_libvirt_runner_lifecycle_insert;
		 DROP TRIGGER mirror_libvirt_runner_lifecycle_update;
		 DROP TRIGGER mirror_libvirt_runner_lifecycle_delete`,
		`INSERT INTO runner_lifecycle(scale_set, repository, provider, runner_name, request_id,
			phase, created_at, waiting_since, updated_at, reap_reason)
		 VALUES('secret-set', 'secret-repo', 'libvirt', 'conflict', 50,
			'waiting_for_job', 100, 100, 100, '')`,
		`INSERT INTO libvirt_runner_lifecycle(scale_set, repository, request_id, runner_name, reap_requested)
		 VALUES('secret-set', 'secret-repo', 1, 'legacy-only', 0),
		       ('secret-set', 'secret-repo', 51, 'conflict', 0)`)
	err := store.migrateTo(context.Background(), 8)
	if err == nil || !strings.Contains(err.Error(), "legacy_only=1 conflicting=1") {
		t.Fatalf("contraction error = %v", err)
	}
	assertErrorOmitsSQLitePayloads(t, err)
	assertSQLiteTablePresent(t, database, "libvirt_runner_lifecycle")
	assertSQLiteMigrationState(t, database, 7, 1)
}

func sqliteTableExists(t *testing.T, database *sql.DB, table string) bool {
	t.Helper()
	var count int
	if err := database.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table).
		Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count == 1
}

func assertSQLiteTablePresent(t *testing.T, database *sql.DB, table string) {
	t.Helper()
	if !sqliteTableExists(t, database, table) {
		t.Fatalf("table %q is absent", table)
	}
}

func assertSQLiteTableAbsent(t *testing.T, database *sql.DB, table string) {
	t.Helper()
	if sqliteTableExists(t, database, table) {
		t.Fatalf("table %q is present", table)
	}
}
