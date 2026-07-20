//go:build integration

package cluster

import (
	"context"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPostgreSQLFailedMigrationsLeaveTheLastCompleteVersion(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	if err := store.MigrateDown(ctx, 0, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = connection.Exec(ctx, `DROP TABLE scaler_schema_version`); err != nil {
		t.Fatal(err)
	}
	if err = connection.Close(ctx); err != nil {
		t.Fatal(err)
	}
	initial := []byte(`
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
`)
	store.migrationFiles = fstest.MapFS{
		"migrations/001_initial.sql": {Data: initial},
		"migrations/002_failure.sql": {Data: []byte(`
CREATE TABLE partial_migration_marker (value integer NOT NULL);
SELECT missing_up_migration_function();
---- create above / drop below ----
DROP TABLE partial_migration_marker;
`)},
	}
	if err = store.Migrate(ctx); err == nil {
		t.Fatal("expected failing Up migration")
	}
	status, err := store.MigrationStatus(ctx)
	if err != nil || status.Current != 1 {
		t.Fatalf("status after failed Up = %#v, %v", status, err)
	}
	assertPostgreSQLTableExists(t, dsn, "stable_migration_marker")
	assertPostgreSQLTableMissing(t, dsn, "partial_migration_marker")

	store.migrationFiles = fstest.MapFS{
		"migrations/001_initial.sql": {Data: initial},
		"migrations/002_failure.sql": {Data: []byte(`
CREATE TABLE down_failure_marker (value integer NOT NULL);
---- create above / drop below ----
DROP TABLE down_failure_marker;
SELECT missing_down_migration_function();
`)},
	}
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.MigrateDown(ctx, 1, time.Now(), time.Minute); err == nil {
		t.Fatal("expected failing Down migration")
	}
	status, err = store.MigrationStatus(ctx)
	if err != nil || status.Current != 2 {
		t.Fatalf("status after failed Down = %#v, %v", status, err)
	}
	assertPostgreSQLTableExists(t, dsn, "down_failure_marker")
}
