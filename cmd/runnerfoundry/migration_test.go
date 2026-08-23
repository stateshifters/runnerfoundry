package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/cluster"
	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
)

func TestLoadMigrationConfigIgnoresUnrelatedDaemonConfiguration(t *testing.T) {
	configText := "github: invalid\nstate:\n  postgres:\n    role: runnerfoundry\nscale_sets: invalid\n"
	configured, err := loadMigrationConfig([]byte(configText))
	if err != nil {
		t.Fatalf("load migration config: %v", err)
	}
	if configured.Role != "runnerfoundry" {
		t.Fatalf("PostgreSQL role = %q", configured.Role)
	}
}

func TestLoadMigrationConfigReportsInvalidContent(t *testing.T) {
	if _, err := loadMigrationConfig([]byte("state: [")); err == nil {
		t.Fatal("expected invalid config error")
	}
}

func TestExecuteMigrationUsesOnlyPostgreSQLConfiguration(t *testing.T) {
	store := &fakeMigrationStore{status: migrationStatus{Current: 1, Target: 1}}
	status, err := executeMigration(t.Context(), migrationRequest{
		action: migrationStatusAction, configPath: "config.yaml",
	}, migrationDependencies{
		readConfigFile: func(path string) ([]byte, error) {
			if path != "config.yaml" {
				t.Fatalf("config path = %q", path)
			}
			return []byte("github: invalid\nstate:\n  postgres:\n    role: runnerfoundry\n"), nil
		},
		newStore: func(postgresConfig postgresconfig.Settings) migrationStore {
			if postgresConfig.Role != "runnerfoundry" {
				t.Fatalf("PostgreSQL config = %#v", postgresConfig)
			}
			return store
		},
		now: func() time.Time { return time.Unix(1, 0) },
	})
	if err != nil || status.Current != 1 {
		t.Fatalf("migration status = %#v, %v", status, err)
	}
}

func TestExecuteMigrationReportsInvalidConfiguration(t *testing.T) {
	_, err := executeMigration(t.Context(), migrationRequest{
		action: migrationStatusAction, configPath: "missing.yaml",
	}, migrationDependencies{readConfigFile: func(string) ([]byte, error) { return nil, errors.New("missing") }})
	if err == nil {
		t.Fatal("expected invalid migration config")
	}
}

func TestExecuteMigrationReportsInvalidConfigurationContent(t *testing.T) {
	_, err := executeMigration(t.Context(), migrationRequest{configPath: "config.yaml"}, migrationDependencies{
		readConfigFile: func(string) ([]byte, error) { return []byte("state: ["), nil },
	})
	if err == nil {
		t.Fatal("expected invalid migration config content")
	}
}

func TestExecuteMigrationUsesConfiguredStore(t *testing.T) {
	store := &fakeMigrationStore{status: migrationStatus{Current: 1, Target: 1}}
	dependencies := migrationDependencies{
		readConfigFile: func(string) ([]byte, error) {
			return []byte("state:\n  postgres:\n    role: runnerfoundry\n"), nil
		},
		newStore: func(postgresconfig.Settings) migrationStore { return store },
		now:      func() time.Time { return time.Unix(1, 0) },
	}
	status, err := (migrationCommandExecutor{dependencies}).run(
		t.Context(), migrationRequest{action: migrationStatusAction},
	)
	if err != nil || status.Current != 1 {
		t.Fatalf("migration status = %#v, %v", status, err)
	}
}

func TestProductionMigrationStoreFactoryConstructsWithoutConnecting(t *testing.T) {
	if store := newProductionMigrationDependencies().newStore(postgresconfig.Settings{Role: "runnerfoundry"}); store == nil {
		t.Fatal("expected migration store")
	}
}

func TestExecuteStoreMigrationDispatchesTypedActions(t *testing.T) {
	store := &fakeMigrationStore{status: migrationStatus{Current: 1, Target: 1}}
	for _, request := range []migrationRequest{
		{action: migrationStatusAction},
		{action: migrationUpAction},
		{action: migrationDownAction, target: 0},
	} {
		status, err := executeStoreMigration(t.Context(), store, request, time.Unix(1, 0))
		if err != nil || status.Current != 1 {
			t.Fatalf("execute %q = %#v, %v", request.action, status, err)
		}
	}
	if store.migrateCalls != 1 || store.migrateDownCalls != 1 || store.statusCalls != 3 || store.target != 0 {
		t.Fatalf("migration calls = %#v", store)
	}
}

func TestExecuteStoreMigrationReportsFailures(t *testing.T) {
	failure := errors.New("failed")
	for name, scenario := range map[string]struct {
		store   *fakeMigrationStore
		request migrationRequest
	}{
		"status":      {&fakeMigrationStore{statusErr: failure}, migrationRequest{action: migrationStatusAction}},
		"up":          {&fakeMigrationStore{migrateErr: failure}, migrationRequest{action: migrationUpAction}},
		"up status":   {&fakeMigrationStore{statusErr: failure}, migrationRequest{action: migrationUpAction}},
		"down":        {&fakeMigrationStore{migrateDownErr: failure}, migrationRequest{action: migrationDownAction}},
		"down status": {&fakeMigrationStore{statusErr: failure}, migrationRequest{action: migrationDownAction}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := executeStoreMigration(t.Context(), scenario.store, scenario.request, time.Unix(1, 0)); err == nil {
				t.Fatal("expected migration failure")
			}
		})
	}
}

func TestExecuteStoreMigrationRejectsUnsupportedAction(t *testing.T) {
	if _, err := executeStoreMigration(t.Context(), nil, migrationRequest{action: "unsupported"}, time.Unix(1, 0)); err == nil {
		t.Fatal("expected unsupported migration action")
	}
}

func TestMigrationStatusErrorPreservesNilAndWrapsFailure(t *testing.T) {
	failure := errors.New("failed")
	if err := migrationStatusError(nil); err != nil {
		t.Fatalf("nil migration error = %v", err)
	}
	if err := migrationStatusError(failure); err == nil {
		t.Fatal("expected wrapped migration error")
	}
}

type fakeMigrationStore struct {
	status           migrationStatus
	statusErr        error
	migrateErr       error
	migrateDownErr   error
	statusCalls      int
	migrateCalls     int
	migrateDownCalls int
	target           int32
}

func (store *fakeMigrationStore) MigrationStatus(context.Context) (cluster.MigrationStatus, error) {
	store.statusCalls++
	return store.status, store.statusErr
}

func (store *fakeMigrationStore) Migrate(context.Context) error {
	store.migrateCalls++
	return store.migrateErr
}

func (store *fakeMigrationStore) MigrateDown(_ context.Context, target int32, _ time.Time, liveFor time.Duration) error {
	store.migrateDownCalls++
	store.target = target
	if liveFor != cluster.NodeLifetime {
		return errors.New("unexpected node lifetime")
	}
	return store.migrateDownErr
}
