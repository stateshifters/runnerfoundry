package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/tern/v2/migrate"
)

func TestMigratorUpAppliesCatalogTargetAndClosesSession(t *testing.T) {
	connections := connectionsForTest(t, Settings{Role: "runnerfoundry"}, operatingSystemConnectionDriver())
	if NewMigrator(connections, &Catalog{}) == nil {
		t.Fatal("migrator was not constructed")
	}
	session := &fakeDatabaseConnection{executor: successfulMigrationExecutor(1)}
	var migratedTo int32
	session.executor.migrateTo = func(_ context.Context, target int32) error { migratedTo = target; return nil }
	migrator := migratorWithSessions(fakeSessionOpener{connection: session}, &Catalog{target: 4})
	if err := migrator.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	if migratedTo != 4 || !session.closed {
		t.Fatalf("target=%d closed=%t", migratedTo, session.closed)
	}
}

func TestMigratorUpProtectsCompatibilityAndKnownRange(t *testing.T) {
	legacy := &fakeDatabaseConnection{
		executor:   successfulMigrationExecutor(0),
		rowResults: []fakeRow{schemaCompatibilityRow(t, false, true)},
	}
	if err := migratorWithSessions(fakeSessionOpener{connection: legacy}, &Catalog{target: 4}).Up(context.Background()); !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("legacy error = %v", err)
	}
	newer := &fakeDatabaseConnection{executor: successfulMigrationExecutor(5)}
	if err := migratorWithSessions(fakeSessionOpener{connection: newer}, &Catalog{target: 4}).Up(context.Background()); err == nil {
		t.Fatal("newer schema was accepted")
	}
}

func TestMigratorUpReportsBoundaryFailures(t *testing.T) {
	failure := errors.New("failed")
	tests := map[string]*fakeDatabaseConnection{
		"lock":       {execErrors: []error{failure}},
		"executor":   {executorErr: failure},
		"version":    {executor: &migrationExecutor{currentVersion: func(context.Context) (int32, error) { return 0, failure }}},
		"inspection": {executor: successfulMigrationExecutor(0), rowResults: []fakeRow{{err: failure}}},
		"apply": {
			executor: &migrationExecutor{
				currentVersion: currentMigrationVersion(1), target: 4,
				migrateTo: func(context.Context, int32) error { return failure },
			},
		},
	}
	for name, session := range tests {
		t.Run(name, func(t *testing.T) {
			if err := migratorWithSessions(fakeSessionOpener{connection: session}, &Catalog{target: 4}).Up(context.Background()); err == nil {
				t.Fatal("failure was hidden")
			}
		})
	}
	if err := migratorWithSessions(fakeSessionOpener{err: failure}, &Catalog{}).Up(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("open session error = %v", err)
	}
}

func TestMigratorStatusReportsEmptyCurrentAndLegacySchemas(t *testing.T) {
	empty := &fakeDatabaseConnection{rowResults: []fakeRow{schemaCompatibilityRow(t, false, false)}}
	status, err := migratorWithSessions(fakeSessionOpener{connection: empty}, &Catalog{target: 8}).Status(context.Background())
	if err != nil || status != (MigrationStatus{Target: 8}) {
		t.Fatalf("empty status = %#v, %v", status, err)
	}
	current := &fakeDatabaseConnection{
		executor:   successfulMigrationExecutor(6),
		rowResults: []fakeRow{schemaCompatibilityRow(t, true, false)},
	}
	status, err = migratorWithSessions(fakeSessionOpener{connection: current}, &Catalog{target: 8}).Status(context.Background())
	if err != nil || status.Current != 6 || status.Target != 8 {
		t.Fatalf("current status = %#v, %v", status, err)
	}
	legacy := &fakeDatabaseConnection{rowResults: []fakeRow{schemaCompatibilityRow(t, false, true)}}
	if _, err = migratorWithSessions(fakeSessionOpener{connection: legacy}, &Catalog{target: 8}).Status(context.Background()); !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("legacy status error = %v", err)
	}
}

func TestMigratorStatusReportsBoundaryFailures(t *testing.T) {
	failure := errors.New("failed")
	if _, err := migratorWithSessions(fakeSessionOpener{err: failure}, &Catalog{}).Status(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("open error = %v", err)
	}
	tests := []*fakeDatabaseConnection{
		{rowResults: []fakeRow{{err: failure}}},
		{rowResults: []fakeRow{schemaCompatibilityRow(t, true, false)}, executorErr: failure},
		{rowResults: []fakeRow{schemaCompatibilityRow(t, true, false)}, executor: &migrationExecutor{
			currentVersion: func(context.Context) (int32, error) { return 0, failure },
		}},
	}
	for _, session := range tests {
		if _, err := migratorWithSessions(fakeSessionOpener{connection: session}, &Catalog{}).Status(context.Background()); err == nil {
			t.Fatal("status failure was hidden")
		}
	}
}

func TestMigratorRollbackUsesAbsoluteCutoffAfterSafetyChecks(t *testing.T) {
	cutoff := time.Unix(100, 0)
	session := &fakeDatabaseConnection{
		executor:   successfulMigrationExecutor(3),
		rowResults: []fakeRow{booleanRow(t, true), booleanRow(t, false)},
	}
	var migratedTo int32
	session.executor.migrateTo = func(_ context.Context, target int32) error { migratedTo = target; return nil }
	if err := migratorWithSessions(fakeSessionOpener{connection: session}, &Catalog{target: 3}).RollbackTo(context.Background(), 1, cutoff); err != nil {
		t.Fatal(err)
	}
	if migratedTo != 1 || len(session.queries) != 2 || session.queries[1].arguments[0] != cutoff {
		t.Fatalf("target=%d queries=%#v", migratedTo, session.queries)
	}
}

func TestMigratorRollbackRejectsInvalidAndUnsafeRequests(t *testing.T) {
	tests := map[string]*fakeDatabaseConnection{
		"active leader": {executor: successfulMigrationExecutor(3), rowResults: []fakeRow{booleanRow(t, false)}},
		"live members": {
			executor:   successfulMigrationExecutor(3),
			rowResults: []fakeRow{booleanRow(t, true), booleanRow(t, true)},
		},
		"leader query": {executor: successfulMigrationExecutor(3), rowResults: []fakeRow{{err: errors.New("query")}}},
		"member query": {
			executor:   successfulMigrationExecutor(3),
			rowResults: []fakeRow{booleanRow(t, true), {err: errors.New("query")}},
		},
	}
	for name, session := range tests {
		t.Run(name, func(t *testing.T) {
			if err := migratorWithSessions(fakeSessionOpener{connection: session}, &Catalog{target: 3}).RollbackTo(context.Background(), 1, time.Time{}); err == nil {
				t.Fatal("unsafe rollback was accepted")
			}
		})
	}
	invalid := &fakeDatabaseConnection{executor: successfulMigrationExecutor(3)}
	if err := migratorWithSessions(fakeSessionOpener{connection: invalid}, &Catalog{target: 3}).RollbackTo(context.Background(), -1, time.Time{}); err == nil {
		t.Fatal("invalid target was accepted")
	}
	current := &fakeDatabaseConnection{executor: successfulMigrationExecutor(3)}
	if err := migratorWithSessions(fakeSessionOpener{connection: current}, &Catalog{target: 3}).RollbackTo(context.Background(), 3, time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func TestMigratorRollbackReportsBoundaryFailures(t *testing.T) {
	failure := errors.New("failed")
	if err := migratorWithSessions(fakeSessionOpener{err: failure}, &Catalog{}).RollbackTo(context.Background(), 0, time.Time{}); !errors.Is(err, failure) {
		t.Fatalf("open error = %v", err)
	}
	tests := map[string]*fakeDatabaseConnection{
		"executor": {executorErr: failure},
		"version": {
			executor: &migrationExecutor{currentVersion: func(context.Context) (int32, error) { return 0, failure }},
		},
		"apply": {
			executor: &migrationExecutor{
				currentVersion: currentMigrationVersion(3), target: 3,
				migrateTo: func(context.Context, int32) error { return failure },
			},
			rowResults: []fakeRow{booleanRow(t, true), booleanRow(t, false)},
		},
	}
	for name, session := range tests {
		t.Run(name, func(t *testing.T) {
			if err := migratorWithSessions(fakeSessionOpener{connection: session}, &Catalog{target: 3}).RollbackTo(context.Background(), 1, time.Time{}); err == nil {
				t.Fatal("failure was hidden")
			}
		})
	}
}

func TestMigratorReportsSessionCloseFailure(t *testing.T) {
	session := &fakeDatabaseConnection{
		closeErr:   errors.New("close failed"),
		rowResults: []fakeRow{schemaCompatibilityRow(t, false, false)},
	}
	if _, err := migratorWithSessions(fakeSessionOpener{connection: session}, &Catalog{}).Status(context.Background()); err == nil {
		t.Fatal("session close failure was hidden")
	}
}

func TestPostgreSQLSessionConstructsCatalogExecutorWithoutTypeEscape(t *testing.T) {
	catalog := &Catalog{target: 1, migrations: []catalogMigration{{name: "test", upSQL: "up", downSQL: "down"}}}
	connection := &pgx.Conn{}
	newTernMigrator := func(context.Context, *pgx.Conn, string) (*migrate.Migrator, error) {
		return &migrate.Migrator{}, nil
	}
	session := databaseSessionForConnection(connection, connectionDriver{newTernMigrator: newTernMigrator})
	executor, err := session.newMigrationExecutor(context.Background(), catalog)
	if err != nil || executor.target != 1 {
		t.Fatalf("executor = %#v, %v", executor, err)
	}
	newTernMigrator = func(context.Context, *pgx.Conn, string) (*migrate.Migrator, error) {
		return nil, errors.New("failed")
	}
	session = databaseSessionForConnection(connection, connectionDriver{newTernMigrator: newTernMigrator})
	if _, err = session.newMigrationExecutor(context.Background(), catalog); err == nil {
		t.Fatal("construction failure was hidden")
	}
}

func TestSafePostgreSQLErrorPreservesOnlyApprovedDiagnostics(t *testing.T) {
	operation := "apply PostgreSQL migrations"
	postgresError := &pgconn.PgError{Code: "23505", Message: "sensitive detail"}
	if err := safePostgreSQLError(operation, fmt.Errorf("wrapped: %w", postgresError)); err.Error() != operation+" (SQLSTATE 23505)" {
		t.Fatalf("PostgreSQL error = %q", err)
	}
	privateErr := errors.New("postgres://secret@database/private")
	if err := safePostgreSQLError(operation, privateErr); err.Error() != operation || strings.Contains(err.Error(), "secret") {
		t.Fatalf("private error = %q", err)
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, IncompatibleSchemaError{}} {
		if err := safePostgreSQLError(operation, fmt.Errorf("wrapped: %w", cause)); !errors.Is(err, cause) {
			t.Fatalf("error %v does not preserve %v", err, cause)
		}
	}
}

func successfulMigrationExecutor(current int32) *migrationExecutor {
	return &migrationExecutor{
		currentVersion: currentMigrationVersion(current),
		migrateTo:      func(context.Context, int32) error { return nil },
		target:         4,
	}
}

func currentMigrationVersion(version int32) func(context.Context) (int32, error) {
	return func(context.Context) (int32, error) { return version, nil }
}

func schemaCompatibilityRow(t *testing.T, versionTableExists bool, legacySchema bool) fakeRow {
	t.Helper()
	return fakeRow{scan: func(destinations ...any) {
		*destination[bool](t, destinations[0]) = versionTableExists
		*destination[bool](t, destinations[1]) = legacySchema
	}}
}
