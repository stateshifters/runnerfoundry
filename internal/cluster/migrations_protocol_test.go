package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIncompatibleSchemaErrorReportsSentinelMessage(t *testing.T) {
	err := IncompatibleSchemaError{}
	if err.Error() != ErrIncompatibleSchema.Error() {
		t.Fatalf("IncompatibleSchemaError.Error() = %q, want %q", err.Error(), ErrIncompatibleSchema.Error())
	}
}

func TestMigrateInspectsUnversionedSchemaBeforeApplyingMigrations(t *testing.T) {
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{schemaCompatibilityRow(t, false, false)}}
	executor := successfulMigrationExecutor(0)
	var migratedTo int32
	executor.migrateTo = func(_ context.Context, target int32) error {
		migratedTo = target
		return nil
	}

	err := storeWithMigrationExecutor(connection, executor, nil).Migrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if migratedTo != executor.target {
		t.Fatalf("migration target = %d, want %d", migratedTo, executor.target)
	}
	if len(connection.queries) != 1 {
		t.Fatalf("schema compatibility query count = %d, want 1", len(connection.queries))
	}
}

func TestMigrateReportsUnversionedSchemaInspectionFailure(t *testing.T) {
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{{err: errors.New("inspect failed")}}}
	err := storeWithMigrationExecutor(connection, successfulMigrationExecutor(0), nil).Migrate(context.Background())
	if err == nil || err.Error() != "inspect PostgreSQL schema compatibility" {
		t.Fatalf("Migrate() error = %v, want sanitized schema inspection failure", err)
	}
}

func TestMigrateRejectsUnversionedLegacySchemaWithoutApplyingMigrations(t *testing.T) {
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{schemaCompatibilityRow(t, false, true)}}
	executor := successfulMigrationExecutor(0)
	executor.migrateTo = func(context.Context, int32) error {
		t.Fatal("legacy schema was migrated")
		return nil
	}

	err := storeWithMigrationExecutor(connection, executor, nil).Migrate(context.Background())
	if !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("Migrate() error = %v, want incompatible schema", err)
	}
}

func TestSafePostgreSQLErrorPreservesIncompatibleSchemaError(t *testing.T) {
	incompatibleErr := fmt.Errorf("migration guard: %w", IncompatibleSchemaError{})
	if err := safePostgreSQLError("inspect PostgreSQL schema", incompatibleErr); !errors.Is(err, incompatibleErr) {
		t.Fatalf("safePostgreSQLError() = %v, want original incompatible schema error", err)
	}
}

func TestSafePostgreSQLErrorReportsSQLStateWithoutServerMessage(t *testing.T) {
	err := safePostgreSQLError("apply PostgreSQL migrations", &pgconn.PgError{Code: "55000"})
	if err.Error() != "apply PostgreSQL migrations (SQLSTATE 55000)" {
		t.Fatalf("safePostgreSQLError() = %q", err)
	}
}

func schemaCompatibilityRow(t *testing.T, versionTableExists bool, legacySchema bool) fakeRow {
	t.Helper()
	return fakeRow{scan: func(destinations ...any) {
		*destination[bool](t, destinations[0]) = versionTableExists
		*destination[bool](t, destinations[1]) = legacySchema
	}}
}
