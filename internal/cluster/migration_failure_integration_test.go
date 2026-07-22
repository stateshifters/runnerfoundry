//go:build integration

package cluster

import (
	"context"
	"testing"
	"testing/fstest"
	"time"
)

func TestPostgreSQLFailedUpMigrationLeavesTheLastCompleteVersion(t *testing.T) {
	fixture := newMigrationFailureFixture(t)
	fixture.store.migrationFiles = fstest.MapFS{
		"migrations/001_initial.sql": {Data: fixture.initial},
		"migrations/002_failure.sql": {Data: []byte(`
CREATE TABLE partial_migration_marker (value integer NOT NULL);
SELECT missing_up_migration_function();
---- create above / drop below ----
DROP TABLE partial_migration_marker;
`)},
	}
	err := fixture.store.Migrate(context.Background())
	if err == nil {
		t.Fatal("expected failing Up migration")
	}
	assertErrorDiagnostics(t, err, "function missing_up_migration_function() does not exist", "SQLSTATE 42883")
	assertMigrationCurrentVersion(t, fixture.store, 1)
	assertPostgreSQLTableExists(t, fixture.dsn, "stable_migration_marker")
	assertPostgreSQLTableMissing(t, fixture.dsn, "partial_migration_marker")
}

func TestPostgreSQLFailedDownMigrationKeepsCurrentVersion(t *testing.T) {
	fixture := newMigrationFailureFixture(t)
	fixture.store.migrationFiles = fstest.MapFS{
		"migrations/001_initial.sql": {Data: fixture.initial},
		"migrations/002_failure.sql": {Data: []byte(`
CREATE TABLE down_failure_marker (value integer NOT NULL);
---- create above / drop below ----
DROP TABLE down_failure_marker;
SELECT missing_down_migration_function();
`)},
	}
	if err := fixture.store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	err := fixture.store.MigrateDown(context.Background(), 1, time.Now(), time.Minute)
	if err == nil {
		t.Fatal("expected failing Down migration")
	}
	assertErrorDiagnostics(t, err, "function missing_down_migration_function() does not exist", "SQLSTATE 42883")
	assertMigrationCurrentVersion(t, fixture.store, 2)
	assertPostgreSQLTableExists(t, fixture.dsn, "down_failure_marker")
}

type migrationFailureFixture struct {
	store   *Store
	dsn     string
	initial []byte
}

func newMigrationFailureFixture(t *testing.T) migrationFailureFixture {
	t.Helper()
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	if err := store.MigrateDown(ctx, 0, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	connection := postgreSQLAdminConnection(t, dsn)
	if _, err := connection.Exec(ctx, `DROP TABLE scaler_schema_version`); err != nil {
		t.Fatal(err)
	}
	return migrationFailureFixture{store: store, dsn: dsn, initial: []byte(`
CREATE TABLE scaler_schema_compatibility (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    oldest_binary_schema_generation integer NOT NULL
);
INSERT INTO scaler_schema_compatibility (oldest_binary_schema_generation) VALUES (1);
CREATE TABLE scaler_nodes (node_id text PRIMARY KEY, heartbeat_at timestamptz NOT NULL);
CREATE TABLE stable_migration_marker (value integer NOT NULL);
---- create above / drop below ----
DROP TABLE stable_migration_marker;
DROP TABLE scaler_nodes;
DROP TABLE scaler_schema_compatibility;
`)}
}

func assertMigrationCurrentVersion(t *testing.T, store *Store, expected int32) {
	t.Helper()
	status, err := store.MigrationStatus(context.Background())
	if err != nil || status.Current != expected {
		t.Fatalf("migration status = %#v, %v; want current version %d", status, err, expected)
	}
}
