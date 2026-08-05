package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestMigrationTenFreshInstallAndCleanRoundTrip(t *testing.T) {
	model := &strandedCapacityMigrationFake{version: 9, observationCount: 2, demandCount: 3}
	mustMigrateStrandedCapacityModel(t, model)
	if model.version != 10 || !model.statisticsPresent || !model.blockedStatePresent || model.blockedCount != 0 {
		t.Fatalf("upgraded stranded-capacity schema = %#v", model)
	}
	mustRollBackStrandedCapacityModel(t, model)
	if model.version != 9 || model.statisticsPresent || model.blockedStatePresent ||
		model.observationCount != 2 || model.demandCount != 3 {
		t.Fatalf("downgraded stranded-capacity schema = %#v", model)
	}
	mustMigrateStrandedCapacityModel(t, model)
}

func TestMigrationTenRefusesDowngradeWhileBlockedStateExists(t *testing.T) {
	model := &strandedCapacityMigrationFake{
		version: 10, statisticsPresent: true, blockedStatePresent: true,
		observationCount: 1, demandCount: 1, blockedCount: 1,
	}
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{booleanRow(t, true), booleanRow(t, false)}}
	err := storeWithMigrationExecutor(connection, model.executor(), nil).
		MigrateDown(context.Background(), 9, time.Unix(100, 0), time.Minute)
	if err == nil {
		t.Fatal("downgrade with blocked obligations succeeded")
	}
	if model.version != 10 || !model.blockedStatePresent || model.blockedCount != 1 {
		t.Fatalf("refused downgrade mutated state: %#v", model)
	}
}

type strandedCapacityMigrationFake struct {
	version             int32
	statisticsPresent   bool
	blockedStatePresent bool
	observationCount    int
	demandCount         int
	blockedCount        int
}

func (model *strandedCapacityMigrationFake) executor() *migrationExecutor {
	return &migrationExecutor{
		currentVersion: func(context.Context) (int32, error) { return model.version, nil },
		migrateTo: func(_ context.Context, target int32) error {
			switch target {
			case 10:
				model.statisticsPresent = true
				model.blockedStatePresent = true
			case 9:
				if model.blockedCount > 0 {
					return &pgconn.PgError{Code: "P0001", Message: "blocked obligations remain"}
				}
				model.statisticsPresent = false
				model.blockedStatePresent = false
			default:
				return errors.New("unsupported fake migration target")
			}
			model.version = target
			return nil
		},
		target: 10,
	}
}

func mustMigrateStrandedCapacityModel(t *testing.T, model *strandedCapacityMigrationFake) {
	t.Helper()
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{int32Row(t, 10)}}
	if err := storeWithMigrationExecutor(connection, model.executor(), nil).Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func mustRollBackStrandedCapacityModel(t *testing.T, model *strandedCapacityMigrationFake) {
	t.Helper()
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{booleanRow(t, true), booleanRow(t, false)}}
	if err := storeWithMigrationExecutor(connection, model.executor(), nil).
		MigrateDown(context.Background(), 9, time.Unix(100, 0), time.Minute); err != nil {
		t.Fatal(err)
	}
}
