package cluster

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/tern/v2/migrate"
)

func TestEmptyMigrationCatalogTargetsVersionZero(t *testing.T) {
	if target := migrationTarget(&migrate.Migrator{}); target != 0 {
		t.Fatalf("empty migration target = %d", target)
	}
}

func TestRollbackTargetRejectsNegativeVersion(t *testing.T) {
	if err := validateRollbackTarget(-1, 1); err == nil {
		t.Fatal("expected negative rollback target rejection")
	}
}

func TestMigrationConnectionCloseReportsFailure(t *testing.T) {
	closeErr := errors.New("close failed")
	connection := &fakeDatabaseConnection{closeErr: closeErr}
	if err := closePostgreSQLConnection(context.Background(), connection); !errors.Is(err, closeErr) {
		t.Fatalf("close error = %v, want %v", err, closeErr)
	}
	if !connection.closed {
		t.Fatal("migration connection was not closed")
	}
}

func TestMigrationCatalogLoadsWithoutDatabaseBootstrap(t *testing.T) {
	store := NewStore(ConnectionSettings{Role: "github_scaler"})
	catalog, err := store.migrationCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if target := migrationTarget(catalog); target != 1 {
		t.Fatalf("migration target = %d, want 1", target)
	}
}

func TestPostgreSQLMigrationErrorsRetainSQLStateWithoutServerDetails(t *testing.T) {
	serverError := &pgconn.PgError{Code: "42501", Message: "secret role cannot alter secret table"}
	err := safePostgreSQLError("apply PostgreSQL migrations", serverError)
	if !strings.Contains(err.Error(), "SQLSTATE 42501") {
		t.Fatalf("migration error = %q, want SQLSTATE", err)
	}
	for _, forbidden := range []string{"secret role", "secret table"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("migration error exposed server details: %q", err)
		}
	}
}

func TestNewMigratorRejectsInvalidMigrationFiles(t *testing.T) {
	store := NewStore(ConnectionSettings{Role: "github_scaler"})
	store.migrationFiles = failingSubFS{}
	if _, err := store.migrationCatalog(); err == nil {
		t.Fatal("expected migration filesystem error")
	}
	store.migrationFiles = fstest.MapFS{
		"migrations/invalid.sql": {Data: []byte("not a tern migration")},
	}
	if _, err := store.migrationCatalog(); err == nil {
		t.Fatal("expected malformed migration error")
	}
	store.migrationFiles = fstest.MapFS{
		"migrations/001_missing_down.sql": {Data: []byte("SELECT 1;\n---- create above / drop below ----\n")},
	}
	if _, err := store.migrationCatalog(); err == nil {
		t.Fatal("expected missing Down SQL error")
	}
}

func TestMigrationStatusReportsMetadataInspectionFailure(t *testing.T) {
	failure := &pgconn.PgError{Code: "42501"}
	store := storeWithConnection(&fakeDatabaseConnection{rows: map[string]fakeRow{
		"to_regclass": {err: failure},
	}})
	if _, err := store.MigrationStatus(context.Background()); err == nil || !strings.Contains(err.Error(), "SQLSTATE 42501") {
		t.Fatalf("MigrationStatus() error = %v, want safe SQLSTATE", err)
	}
}

type failingSubFS struct{}

func (failingSubFS) Open(string) (fs.File, error) {
	return nil, errors.New("open failed")
}

func (failingSubFS) Sub(string) (fs.FS, error) {
	return nil, errors.New("sub failed")
}
