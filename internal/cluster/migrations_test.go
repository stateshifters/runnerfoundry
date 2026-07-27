package cluster

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/tern/v2/migrate"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
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
	store := NewStore(postgresconfig.Settings{Role: "runnerfoundry"})
	catalog, err := store.migrationCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if target := migrationTarget(catalog); target != 3 {
		t.Fatalf("migration target = %d, want 3", target)
	}
}

func TestPostgreSQLMigrationErrorsRetainAllowedServerDiagnostics(t *testing.T) {
	serverError := &pgconn.PgError{
		Severity:         "ERROR",
		Code:             "23505",
		Message:          "duplicate key value violates unique constraint",
		Detail:           "Key (name)=(runner) already exists.",
		Hint:             "Choose another runner name.",
		Position:         17,
		InternalPosition: 11,
		InternalQuery:    "INSERT INTO scaler_assignments VALUES ('secret')",
		Where:            "SQL statement in migration 2",
		SchemaName:       "public",
		TableName:        "scaler_assignments",
		ColumnName:       "name",
		DataTypeName:     "text",
		ConstraintName:   "scaler_assignments_name_key",
		File:             "nbtinsert.c",
		Line:             664,
		Routine:          "_bt_check_unique",
	}
	err := safePostgreSQLError("apply PostgreSQL migrations", fmt.Errorf("wrapped PostgreSQL error: %w", serverError))
	expected := "apply PostgreSQL migrations: duplicate key value violates unique constraint (SQLSTATE 23505); " +
		"detail: Key (name)=(runner) already exists.; hint: Choose another runner name.; " +
		"table: scaler_assignments; constraint: scaler_assignments_name_key"
	if err.Error() != expected {
		t.Fatalf("migration error snapshot mismatch\nexpected: %q\nactual:   %q", expected, err)
	}
}

func TestPostgreSQLMigrationErrorsOmitEmptyDiagnostics(t *testing.T) {
	serverError := &pgconn.PgError{Code: "42601", Message: "syntax error at or near \"INVALID\""}
	err := safePostgreSQLError("apply PostgreSQL migrations", serverError)
	expected := "apply PostgreSQL migrations: syntax error at or near \"INVALID\" (SQLSTATE 42601)"
	if err.Error() != expected {
		t.Fatalf("migration error = %q, want %q", err, expected)
	}
}

func TestPostgreSQLMigrationErrorsWithoutSQLStateDiscardServerDetails(t *testing.T) {
	operation := "apply PostgreSQL migrations"
	serverError := &pgconn.PgError{
		Message:        "diagnostic without SQLSTATE",
		Detail:         "server detail",
		Hint:           "server hint",
		TableName:      "server_table",
		ConstraintName: "server_constraint",
	}
	err := safePostgreSQLError(operation, serverError)
	if err.Error() != operation {
		t.Fatalf("migration error = %q, want %q", err, operation)
	}
}

func TestPostgreSQLMigrationErrorsDiscardUnknownErrorDetails(t *testing.T) {
	operation := "apply PostgreSQL migrations"
	sensitiveError := errors.New(
		"postgres://secret-user:secret-password@postgres.internal/runnerfoundry: SELECT * FROM secret_table",
	)
	err := safePostgreSQLError(operation, sensitiveError)
	if err.Error() != operation {
		t.Fatalf("migration error = %q, want %q", err, operation)
	}
	for _, forbidden := range []string{
		"secret-user",
		"secret-password",
		"postgres.internal",
		"runnerfoundry",
		"SELECT * FROM secret_table",
	} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("migration error exposed sensitive value %q: %q", forbidden, err)
		}
	}
	if errors.Is(err, sensitiveError) {
		t.Fatal("migration error retained the sensitive underlying error")
	}
}

func TestNewMigratorRejectsInvalidMigrationFiles(t *testing.T) {
	store := NewStore(postgresconfig.Settings{Role: "runnerfoundry"})
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

type failingSubFS struct{}

func (failingSubFS) Open(string) (fs.File, error) {
	return nil, errors.New("open failed")
}

func (failingSubFS) Sub(string) (fs.FS, error) {
	return nil, errors.New("sub failed")
}
