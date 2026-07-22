//go:build integration

package cluster

import (
	"context"
	"testing"
	"testing/fstest"
)

func TestPostgreSQLStartupNeverDowngradesANewerCompatibleSchema(t *testing.T) {
	olderStore, dsn := olderBinaryAgainstFutureSchema(t)
	if err := olderStore.Migrate(context.Background()); err != nil {
		t.Fatalf("compatible previous binary rejected expanded schema: %v", err)
	}
	assertPostgreSQLTableExists(t, dsn, "future_schema_marker")
}

func TestPostgreSQLStartupRejectsIncompatibleNewerSchemaWithoutDowngrade(t *testing.T) {
	olderStore, dsn := olderBinaryAgainstFutureSchema(t)
	setSchemaCompatibilityFloor(t, dsn, 2)
	if err := olderStore.Migrate(context.Background()); err == nil {
		t.Fatal("incompatible previous binary accepted a newer schema")
	}
	assertPostgreSQLTableExists(t, dsn, "future_schema_marker")
}

func olderBinaryAgainstFutureSchema(t *testing.T) (*Store, string) {
	t.Helper()
	newerStore, dsn := postgreSQLTestStore(t)
	newerStore.migrationFiles = fstest.MapFS{
		"migrations/001_initial.sql": {Data: []byte("SELECT 1;\n---- create above / drop below ----\nSELECT 1;")},
		"migrations/002_future.sql": {Data: []byte(`
CREATE TABLE future_schema_marker (value integer NOT NULL);
UPDATE scaler_schema_compatibility SET oldest_binary_schema_generation = 1 WHERE singleton;
---- create above / drop below ----
DROP TABLE future_schema_marker;
`)},
	}
	if err := newerStore.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	olderStore := NewStore(newerStore.settings)
	olderStore.migrationFiles = fstest.MapFS{
		"migrations/001_initial.sql": {Data: []byte("SELECT 1;\n---- create above / drop below ----\nSELECT 1;")},
	}
	return olderStore, dsn
}

func setSchemaCompatibilityFloor(t *testing.T, dsn string, floor int) {
	t.Helper()
	connection := postgreSQLAdminConnection(t, dsn)
	if _, err := connection.Exec(context.Background(),
		`UPDATE scaler_schema_compatibility SET oldest_binary_schema_generation = $1 WHERE singleton`, floor,
	); err != nil {
		t.Fatal(err)
	}
}
