//go:build integration

package cluster

import (
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func credentialFileURL(path string) string {
	urlPath := filepath.ToSlash(path)
	if filepath.VolumeName(path) != "" && !strings.HasPrefix(urlPath, "/") {
		urlPath = "/" + urlPath
	}
	return (&url.URL{Scheme: "file", Path: urlPath}).String()
}

func TestPostgreSQLMigrationOrchestrationReportsInvalidDatabaseStates(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	assertMigratorInitializationFailures(t, store)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgreSQLConnection(ctx, admin) })
	assertInvalidVersionMetadata(t, store, admin)
	assertUnavailableSchemaObjects(t, store, dsn, admin)
}

func assertMigratorInitializationFailures(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	store.migrationFiles = failingSubFS{}
	if err := store.Migrate(ctx); err == nil {
		t.Fatal("expected startup migrator initialization failure")
	}
	if _, err := store.MigrationStatus(ctx); err == nil {
		t.Fatal("expected status migrator initialization failure")
	}
	if err := store.MigrateDown(ctx, 0, time.Now(), time.Minute); err == nil {
		t.Fatal("expected rollback migrator initialization failure")
	}
	store.migrationFiles = embeddedMigrations
}

func assertInvalidVersionMetadata(t *testing.T, store *Store, admin *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	if _, err := admin.Exec(ctx, `ALTER TABLE scaler_schema_version ALTER COLUMN version TYPE text USING version::text; UPDATE scaler_schema_version SET version = 'invalid'`); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err == nil {
		t.Fatal("expected startup version read failure")
	}
	if _, err := store.MigrationStatus(ctx); err == nil {
		t.Fatal("expected status version read failure")
	}
	if err := store.MigrateDown(ctx, 0, time.Now(), time.Minute); err == nil {
		t.Fatal("expected rollback version read failure")
	}
	if _, err := admin.Exec(ctx, `UPDATE scaler_schema_version SET version = '1'; ALTER TABLE scaler_schema_version ALTER COLUMN version TYPE integer USING version::integer`); err != nil {
		t.Fatal(err)
	}
}

func assertUnavailableSchemaObjects(t *testing.T, store *Store, dsn string, admin *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	if err := store.MigrateDown(ctx, 1, time.Now(), time.Minute); err != nil {
		t.Fatalf("same-version rollback should be a no-op: %v", err)
	}
	if _, err := admin.Exec(ctx, `ALTER TABLE scaler_schema_compatibility RENAME TO unavailable_schema_compatibility`); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err == nil {
		t.Fatal("expected startup compatibility read failure")
	}
	if _, err := store.MigrationStatus(ctx); err == nil {
		t.Fatal("expected status compatibility read failure")
	} else {
		for _, diagnostic := range []string{
			`relation "scaler_schema_compatibility" does not exist`,
			"SQLSTATE 42P01",
		} {
			if !strings.Contains(err.Error(), diagnostic) {
				t.Fatalf("migration status error = %q, want %q", err, diagnostic)
			}
		}
	}
	if _, err := admin.Exec(ctx, `ALTER TABLE unavailable_schema_compatibility RENAME TO scaler_schema_compatibility`); err != nil {
		t.Fatal(err)
	}

	closed, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = closed.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = store.newMigrator(ctx, closed); err == nil {
		t.Fatal("expected closed connection to reject migration metadata bootstrap")
	}
	if _, err = store.lockedMigrator(ctx, closed); err == nil {
		t.Fatal("expected closed connection to reject migration lock")
	}
	if err = store.ensureRollbackAllowed(ctx, closed, time.Now()); err == nil {
		t.Fatal("expected closed connection to reject leader check")
	}

	if _, err = admin.Exec(ctx, `ALTER TABLE scaler_nodes RENAME TO unavailable_scaler_nodes`); err != nil {
		t.Fatal(err)
	}
	rollbackConnection, err := store.connectDirectPostgreSQL(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ensureRollbackAllowed(ctx, rollbackConnection, time.Now()); err == nil {
		t.Fatal("expected missing node table to reject live-node check")
	}
	closePostgreSQLConnection(ctx, rollbackConnection)
	if _, err = admin.Exec(ctx, `ALTER TABLE unavailable_scaler_nodes RENAME TO scaler_nodes`); err != nil {
		t.Fatal(err)
	}
}
