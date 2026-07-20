//go:build integration

package cluster

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5"
)

func TestPostgreSQLStartupNeverDowngradesANewerCompatibleSchema(t *testing.T) {
	newerStore, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	newerStore.migrationFiles = fstest.MapFS{
		"migrations/001_initial.sql": {Data: []byte("SELECT 1;\n---- create above / drop below ----\nSELECT 1;")},
		"migrations/002_future.sql": {Data: []byte(`
CREATE TABLE future_schema_marker (value integer NOT NULL);
UPDATE scaler_schema_compatibility SET oldest_binary_schema_generation = 1 WHERE singleton;
---- create above / drop below ----
DROP TABLE future_schema_marker;
`)},
	}
	if err := newerStore.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	olderStore := NewStore(newerStore.settings)
	olderStore.migrationFiles = fstest.MapFS{
		"migrations/001_initial.sql": {Data: []byte("SELECT 1;\n---- create above / drop below ----\nSELECT 1;")},
	}
	if err := olderStore.Migrate(ctx); err != nil {
		t.Fatalf("compatible previous binary rejected expanded schema: %v", err)
	}
	assertPostgreSQLTableExists(t, dsn, "future_schema_marker")
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = connection.Exec(ctx, `UPDATE scaler_schema_compatibility SET oldest_binary_schema_generation = 2 WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	if err = connection.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = olderStore.Migrate(ctx); err == nil {
		t.Fatal("incompatible previous binary accepted a newer schema")
	}
	assertPostgreSQLTableExists(t, dsn, "future_schema_marker")
}
