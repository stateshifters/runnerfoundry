package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/tern/v2/migrate"
)

func TestMigrateAppliesKnownMigrations(t *testing.T) {
	connection := &fakeDatabaseConnection{}
	executor := &migrationExecutor{currentVersion: currentMigrationVersion(1), target: 4}
	var migratedTo int32
	executor.migrateTo = func(_ context.Context, target int32) error {
		migratedTo = target
		return nil
	}
	store := storeWithMigrationExecutor(connection, executor, nil)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if migratedTo != 4 || !connection.closed {
		t.Fatalf("migration target=%d connectionClosed=%t", migratedTo, connection.closed)
	}
}

func TestMigrateRejectsNewerSchemaWithoutApplyingMigrations(t *testing.T) {
	connection := &fakeDatabaseConnection{}
	executor := &migrationExecutor{
		currentVersion: currentMigrationVersion(5),
		migrateTo: func(context.Context, int32) error {
			t.Fatal("newer schema was migrated")
			return nil
		},
		target: 4,
	}
	if err := storeWithMigrationExecutor(connection, executor, nil).Migrate(context.Background()); err == nil {
		t.Fatal("newer schema was accepted")
	}
}

func TestMigrateReportsInitializationFailures(t *testing.T) {
	failure := errors.New("failed")
	tests := map[string]struct {
		connection *fakeDatabaseConnection
		executor   *migrationExecutor
		factoryErr error
	}{
		"lock": {
			connection: &fakeDatabaseConnection{execErrors: []error{failure}},
			executor:   successfulMigrationExecutor(1),
		},
		"factory": {
			connection: &fakeDatabaseConnection{}, factoryErr: failure,
		},
		"current version": {
			connection: &fakeDatabaseConnection{},
			executor: &migrationExecutor{
				currentVersion: func(context.Context) (int32, error) { return 0, failure },
			},
		},
	}
	assertMigrationFailures(t, tests)
}

func TestMigrationEntryPointsReportConnectionFailures(t *testing.T) {
	failure := errors.New("connect")
	store := NewStore(Settings{})
	store.directConnector = func(context.Context) (leaderConnection, error) { return nil, failure }
	if err := store.Migrate(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("Migrate error = %v", err)
	}
	if _, err := store.MigrationStatus(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("MigrationStatus error = %v", err)
	}
	if err := store.MigrateDown(context.Background(), 0, time.Time{}, 0); !errors.Is(err, failure) {
		t.Fatalf("MigrateDown error = %v", err)
	}
}

func TestMigrationStatusReportsCatalogFailure(t *testing.T) {
	store := storeWithConnection(&fakeDatabaseConnection{})
	store.migrationFiles = failingSubFS{}
	if _, err := store.MigrationStatus(context.Background()); err == nil {
		t.Fatal("catalog failure was hidden")
	}
}

func TestMigrateReportsApplicationFailures(t *testing.T) {
	failure := errors.New("failed")
	tests := map[string]struct {
		connection *fakeDatabaseConnection
		executor   *migrationExecutor
		factoryErr error
	}{
		"apply": {
			connection: &fakeDatabaseConnection{},
			executor: &migrationExecutor{
				currentVersion: currentMigrationVersion(1), target: 3,
				migrateTo: func(context.Context, int32) error { return failure },
			},
		},
	}
	assertMigrationFailures(t, tests)
}

func assertMigrationFailures(t *testing.T, tests map[string]struct {
	connection *fakeDatabaseConnection
	executor   *migrationExecutor
	factoryErr error
}) {
	t.Helper()
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			store := storeWithMigrationExecutor(test.connection, test.executor, test.factoryErr)
			if err := store.Migrate(context.Background()); err == nil {
				t.Fatal("expected migration failure")
			}
		})
	}
}

func TestMigrationStatusReportsEmptyZeroAndCurrentSchemas(t *testing.T) {
	tests := map[string]struct {
		rows        []fakeRow
		executor    *migrationExecutor
		wantCurrent int32
	}{
		"empty database": {rows: []fakeRow{booleanRow(t, false)}},
		"version zero": {
			rows: []fakeRow{booleanRow(t, true)}, executor: successfulMigrationExecutor(0),
		},
		"current schema": {
			rows: []fakeRow{booleanRow(t, true)}, executor: successfulMigrationExecutor(3), wantCurrent: 3,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			store := storeWithMigrationExecutor(&fakeDatabaseConnection{rowResults: test.rows}, test.executor, nil)
			status, err := store.MigrationStatus(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if status.Current != test.wantCurrent || status.Target != 1 {
				t.Fatalf("migration status = %#v", status)
			}
		})
	}
}

func TestMigrationStatusRejectsLegacySchema(t *testing.T) {
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{{scan: func(destinations ...any) {
		*destination[bool](t, destinations[0]) = false
		*destination[bool](t, destinations[1]) = true
	}}}}
	_, err := storeWithMigrationExecutor(connection, successfulMigrationExecutor(0), nil).
		MigrationStatus(context.Background())
	if !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("MigrationStatus() error = %v, want incompatible schema", err)
	}
	if len(connection.queries) != 1 || !strings.Contains(connection.queries[0].statement, "scaler\\_%") {
		t.Fatalf("schema compatibility query = %#v", connection.queries)
	}
}

func TestMigrationStatusReportsInspectionFailures(t *testing.T) {
	failure := errors.New("failed")
	tests := map[string]struct {
		rows       []fakeRow
		executor   *migrationExecutor
		factoryErr error
	}{
		"metadata": {rows: []fakeRow{{err: failure}}},
		"factory":  {rows: []fakeRow{booleanRow(t, true)}, factoryErr: failure},
		"version": {
			rows:     []fakeRow{booleanRow(t, true)},
			executor: &migrationExecutor{currentVersion: func(context.Context) (int32, error) { return 0, failure }},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			store := storeWithMigrationExecutor(&fakeDatabaseConnection{rowResults: test.rows}, test.executor, test.factoryErr)
			if _, err := store.MigrationStatus(context.Background()); err == nil {
				t.Fatal("expected migration status failure")
			}
		})
	}
}

func TestMigrateDownEnforcesRollbackSafetyAndAppliesTarget(t *testing.T) {
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{booleanRow(t, true), booleanRow(t, false)}}
	executor := successfulMigrationExecutor(3)
	var migratedTo int32
	executor.migrateTo = func(_ context.Context, target int32) error {
		migratedTo = target
		return nil
	}
	store := storeWithMigrationExecutor(connection, executor, nil)
	if err := store.MigrateDown(context.Background(), 1, time.Unix(100, 0), time.Minute); err != nil {
		t.Fatal(err)
	}
	if migratedTo != 1 {
		t.Fatalf("rollback target = %d", migratedTo)
	}
	if len(connection.queries) != 2 || !strings.Contains(connection.queries[1].statement, "runnerfoundry_members") {
		t.Fatalf("rollback safety queries = %#v", connection.queries)
	}
}

func TestMigrateDownAcceptsCurrentTargetWithoutSafetyQueries(t *testing.T) {
	connection := &fakeDatabaseConnection{}
	store := storeWithMigrationExecutor(connection, successfulMigrationExecutor(3), nil)
	if err := store.MigrateDown(context.Background(), 3, time.Unix(100, 0), time.Minute); err != nil {
		t.Fatal(err)
	}
	if len(connection.rowResults) != 0 {
		t.Fatalf("unexpected rollback safety queries: %#v", connection.rowResults)
	}
}

func TestMigrateDownRejectsInvalidOrFailedExecution(t *testing.T) {
	failure := errors.New("failed")
	tests := map[string]struct {
		target     int32
		rows       []fakeRow
		executor   *migrationExecutor
		factoryErr error
	}{
		"invalid target": {target: -1, executor: successfulMigrationExecutor(3)},
		"factory":        {target: 1, factoryErr: failure},
		"version": {
			target: 1, executor: &migrationExecutor{currentVersion: func(context.Context) (int32, error) { return 0, failure }},
		},
		"migration": {
			target: 1, rows: []fakeRow{booleanRow(t, true), booleanRow(t, false)},
			executor: &migrationExecutor{
				currentVersion: currentMigrationVersion(3), target: 3,
				migrateTo: func(context.Context, int32) error { return failure },
			},
		},
	}
	assertRollbackFailures(t, tests)
}

func TestMigrateDownRejectsUnsafeClusterState(t *testing.T) {
	failure := errors.New("failed")
	tests := map[string]struct {
		target     int32
		rows       []fakeRow
		executor   *migrationExecutor
		factoryErr error
	}{
		"leader query": {
			target: 1, rows: []fakeRow{{err: failure}}, executor: successfulMigrationExecutor(3),
		},
		"active leader": {
			target: 1, rows: []fakeRow{booleanRow(t, false)}, executor: successfulMigrationExecutor(3),
		},
		"node query": {
			target: 1, rows: []fakeRow{booleanRow(t, true), {err: failure}}, executor: successfulMigrationExecutor(3),
		},
		"live nodes": {
			target: 1, rows: []fakeRow{booleanRow(t, true), booleanRow(t, true)}, executor: successfulMigrationExecutor(3),
		},
	}
	assertRollbackFailures(t, tests)
}

func assertRollbackFailures(t *testing.T, tests map[string]struct {
	target     int32
	rows       []fakeRow
	executor   *migrationExecutor
	factoryErr error
}) {
	t.Helper()
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			store := storeWithMigrationExecutor(&fakeDatabaseConnection{rowResults: test.rows}, test.executor, test.factoryErr)
			if err := store.MigrateDown(context.Background(), test.target, time.Unix(100, 0), time.Minute); err == nil {
				t.Fatal("expected rollback failure")
			}
		})
	}
}

func TestNewMigrationExecutorRequiresPostgreSQLConnectionAndLoadsCatalog(t *testing.T) {
	store := NewStore(Settings{Role: "runnerfoundry"})
	if _, err := store.newMigrationExecutor(context.Background(), &fakeDatabaseConnection{}); err == nil {
		t.Fatal("expected concrete PostgreSQL migration connection requirement")
	}
	store.migrationFiles = migrationTestFiles()
	executor, err := store.newMigrationExecutor(context.Background(), &postgreSQLLeaderConnection{})
	if err != nil || executor.target != 1 {
		t.Fatalf("migration executor = %#v, %v", executor, err)
	}
}

func TestNewMigrationExecutorReportsConstructionAndCatalogFailures(t *testing.T) {
	connection := &postgreSQLLeaderConnection{}
	store := NewStore(Settings{Role: "runnerfoundry"})
	store.newTernMigrator = func(context.Context, *pgx.Conn, string) (*migrate.Migrator, error) {
		return nil, errors.New("construct")
	}
	if _, err := store.newMigrationExecutor(context.Background(), connection); err == nil {
		t.Fatal("migrator construction failure was hidden")
	}

	store = NewStore(Settings{Role: "runnerfoundry"})
	store.migrationFiles = failingSubFS{}
	store.newTernMigrator = func(context.Context, *pgx.Conn, string) (*migrate.Migrator, error) {
		return &migrate.Migrator{}, nil
	}
	if _, err := store.newMigrationExecutor(context.Background(), connection); err == nil {
		t.Fatal("migration catalog failure was hidden")
	}
}

func storeWithMigrationExecutor(
	connection *fakeDatabaseConnection,
	executor *migrationExecutor,
	factoryErr error,
) *Store {
	store := storeWithConnection(connection)
	store.migrationFiles = migrationTestFiles()
	store.migrationFactory = func(context.Context, leaderConnection) (*migrationExecutor, error) {
		return executor, factoryErr
	}
	return store
}

func migrationTestFiles() fstest.MapFS {
	return fstest.MapFS{
		"migrations/001_test.sql": {
			Data: []byte("SELECT 1;\n---- create above / drop below ----\nSELECT 1;\n"),
		},
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

func booleanRow(t *testing.T, value bool) fakeRow {
	t.Helper()
	return fakeRow{scan: func(destinations ...any) { *destination[bool](t, destinations[0]) = value }}
}
