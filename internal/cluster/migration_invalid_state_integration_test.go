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

func TestPostgreSQLMigrationOperationsReportMigratorInitializationFailures(t *testing.T) {
	store, _ := postgreSQLTestStore(t)
	store.migrationFiles = failingSubFS{}
	assertMigrationOperationsFail(t, store, "migrator initialization")
}

func TestPostgreSQLMigrationOperationsRejectInvalidVersionMetadata(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	admin := postgreSQLAdminConnection(t, dsn)
	ctx := context.Background()
	if _, err := admin.Exec(ctx, `ALTER TABLE scaler_schema_version ALTER COLUMN version TYPE text USING version::text; UPDATE scaler_schema_version SET version = 'invalid'`); err != nil {
		t.Fatal(err)
	}
	assertMigrationOperationsFail(t, store, "invalid version metadata")
}

func TestPostgreSQLMigrationOperationsRejectVersionMetadataView(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	admin := postgreSQLAdminConnection(t, dsn)
	ctx := context.Background()
	if _, err := admin.Exec(ctx, `DROP TABLE scaler_schema_version; CREATE VIEW scaler_schema_version AS SELECT 1::integer AS version`); err != nil {
		t.Fatal(err)
	}
	assertMigrationOperationsFail(t, store, "unsupported version metadata")
}

func TestPostgreSQLMigrationStatusReportsMissingCompatibilityTable(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	admin := postgreSQLAdminConnection(t, dsn)
	ctx := context.Background()
	if _, err := admin.Exec(ctx, `ALTER TABLE scaler_schema_compatibility RENAME TO unavailable_schema_compatibility`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MigrationStatus(ctx); err == nil {
		t.Fatal("expected status compatibility read failure")
	} else {
		assertErrorDiagnostics(t, err, `relation "scaler_schema_compatibility" does not exist`, "SQLSTATE 42P01")
	}
}

func TestPostgreSQLStartupReportsMissingCompatibilityTable(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	admin := postgreSQLAdminConnection(t, dsn)
	if _, err := admin.Exec(context.Background(),
		`ALTER TABLE scaler_schema_compatibility RENAME TO unavailable_schema_compatibility`,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(context.Background()); err == nil {
		t.Fatal("expected startup compatibility read failure")
	}
}

func TestPostgreSQLMigrationStatusReportsMetadataInspectionFailure(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	admin := postgreSQLAdminConnection(t, dsn)
	if _, err := admin.Exec(context.Background(), `
CREATE FUNCTION public.to_regclass(text) RETURNS regclass
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'metadata inspection failed'; END $$`); err != nil {
		t.Fatal(err)
	}
	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsedDSN.Query()
	query.Set("options", "-c search_path=public,pg_catalog")
	parsedDSN.RawQuery = query.Encode()
	store.settings.DSN = parsedDSN.String()
	if _, err := store.MigrationStatus(context.Background()); err == nil {
		t.Fatal("expected migration metadata inspection failure")
	}
}

func TestPostgreSQLSameVersionRollbackIsNoOp(t *testing.T) {
	store, _ := postgreSQLTestStore(t)
	if err := store.MigrateDown(context.Background(), 1, time.Now(), time.Minute); err != nil {
		t.Fatalf("same-version rollback should be a no-op: %v", err)
	}
}

func TestPostgreSQLMigratorRejectsClosedConnection(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
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
}

func TestPostgreSQLRollbackReportsMissingNodeTable(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	admin := postgreSQLAdminConnection(t, dsn)
	ctx := context.Background()
	if _, err := admin.Exec(ctx, `ALTER TABLE scaler_nodes RENAME TO unavailable_scaler_nodes`); err != nil {
		t.Fatal(err)
	}
	rollbackConnection, err := store.connectDirectPostgreSQL(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ensureRollbackAllowed(ctx, rollbackConnection, time.Now()); err == nil {
		t.Fatal("expected missing node table to reject live-node check")
	}
	if err := closePostgreSQLConnection(ctx, rollbackConnection); err != nil {
		t.Fatalf("close rollback connection: %v", err)
	}
}

func assertMigrationOperationsFail(t *testing.T, store *Store, condition string) {
	t.Helper()
	ctx := context.Background()
	if err := store.Migrate(ctx); err == nil {
		t.Fatalf("expected startup to reject %s", condition)
	}
	if _, err := store.MigrationStatus(ctx); err == nil {
		t.Fatalf("expected status to reject %s", condition)
	}
	if err := store.MigrateDown(ctx, 0, time.Now(), time.Minute); err == nil {
		t.Fatalf("expected rollback to reject %s", condition)
	}
}

func postgreSQLAdminConnection(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	connection, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := closePostgreSQLConnection(context.Background(), connection); closeErr != nil {
			t.Errorf("close PostgreSQL admin connection: %v", closeErr)
		}
	})
	return connection
}

func assertErrorDiagnostics(t *testing.T, err error, diagnostics ...string) {
	t.Helper()
	for _, diagnostic := range diagnostics {
		if !strings.Contains(err.Error(), diagnostic) {
			t.Fatalf("error = %q, want diagnostic %q", err, diagnostic)
		}
	}
}
