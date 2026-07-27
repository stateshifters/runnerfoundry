//go:build integration

package cluster

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
)

func TestMigrationStatusLeavesAnEmptyDatabaseEmpty(t *testing.T) {
	dsn := postgreSQLTestDSN(t)
	store := NewStore(postgresconfig.Settings{DSN: dsn, Role: "postgres"})
	status, err := store.MigrationStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Current != 0 || status.Target != 3 {
		t.Fatalf("MigrationStatus() = %#v, want current 0 and target 3", status)
	}
	connection, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := connection.Close(context.Background()); closeErr != nil {
			t.Errorf("close PostgreSQL inspection connection: %v", closeErr)
		}
	}()
	var versionTable *string
	if err = connection.QueryRow(context.Background(), `SELECT to_regclass('public.scaler_schema_version')::text`).Scan(&versionTable); err != nil {
		t.Fatal(err)
	}
	if versionTable != nil {
		t.Fatalf("MigrationStatus() created version table %q", *versionTable)
	}
}
