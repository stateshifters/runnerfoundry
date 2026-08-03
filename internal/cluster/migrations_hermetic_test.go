package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMigrateAppliesKnownMigrationsAndValidatesCompatibility(t *testing.T) {
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{int32Row(t, 1)}}
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

func TestMigrateAcceptsCompatibleNewerSchemaWithoutApplyingMigrations(t *testing.T) {
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{int32Row(t, 2)}}
	executor := &migrationExecutor{
		currentVersion: currentMigrationVersion(5),
		migrateTo: func(context.Context, int32) error {
			t.Fatal("newer compatible schema was migrated")
			return nil
		},
		target: 4,
	}
	if err := storeWithMigrationExecutor(connection, executor, nil).Migrate(context.Background()); err != nil {
		t.Fatal(err)
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

func TestMigrateReportsApplicationAndCompatibilityFailures(t *testing.T) {
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
		"compatibility read": {
			connection: &fakeDatabaseConnection{rowResults: []fakeRow{{err: failure}}},
			executor:   successfulMigrationExecutor(1),
		},
		"incompatible": {
			connection: &fakeDatabaseConnection{rowResults: []fakeRow{int32Row(t, 5)}},
			executor:   successfulMigrationExecutor(1),
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
		wantFloor   int32
	}{
		"empty database": {rows: []fakeRow{booleanRow(t, false)}},
		"version zero": {
			rows: []fakeRow{booleanRow(t, true)}, executor: successfulMigrationExecutor(0),
		},
		"current schema": {
			rows:     []fakeRow{booleanRow(t, true), int32Row(t, 2)},
			executor: successfulMigrationExecutor(3), wantCurrent: 3, wantFloor: 2,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			store := storeWithMigrationExecutor(&fakeDatabaseConnection{rowResults: test.rows}, test.executor, nil)
			status, err := store.MigrationStatus(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if status.Current != test.wantCurrent || status.CompatibilityFloor != test.wantFloor || status.Target != 6 {
				t.Fatalf("migration status = %#v", status)
			}
		})
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
		"compatibility": {
			rows: []fakeRow{booleanRow(t, true), {err: failure}}, executor: successfulMigrationExecutor(3),
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
	executor, err := store.newMigrationExecutor(context.Background(), &postgreSQLLeaderConnection{})
	if err != nil || executor.target != 6 {
		t.Fatalf("migration executor = %#v, %v", executor, err)
	}
}

func storeWithMigrationExecutor(
	connection *fakeDatabaseConnection,
	executor *migrationExecutor,
	factoryErr error,
) *Store {
	store := storeWithConnection(connection)
	store.migrationFactory = func(context.Context, leaderConnection) (*migrationExecutor, error) {
		return executor, factoryErr
	}
	return store
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

func int32Row(t *testing.T, value int32) fakeRow {
	t.Helper()
	return fakeRow{scan: func(destinations ...any) { *destination[int32](t, destinations[0]) = value }}
}
