package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/postgresconfig"
)

func TestLoadMigrationConfigIgnoresUnrelatedDaemonConfiguration(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	configText := "github: invalid\nstate:\n  postgres:\n    role: github_scaler\nscale_sets: invalid\n"
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	configured, err := loadMigrationConfig(configPath)
	if err != nil {
		t.Fatalf("load migration config: %v", err)
	}
	if configured.Role != "github_scaler" {
		t.Fatalf("PostgreSQL role = %q", configured.Role)
	}
}

func TestLoadMigrationConfigReportsFileFailures(t *testing.T) {
	missingDirectory := filepath.Join(t.TempDir(), "missing", "config.yaml")
	if _, err := loadMigrationConfig(missingDirectory); err == nil {
		t.Fatal("expected missing config directory error")
	}
	if _, err := loadMigrationConfig("missing-config.yaml"); err == nil {
		t.Fatal("expected missing config error")
	}
	if _, err := loadMigrationConfig(t.TempDir()); err == nil {
		t.Fatal("expected config directory read error")
	}
}

func TestExecuteMigrationUsesOnlyPostgreSQLConfiguration(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("github: invalid\nstate:\n  postgres:\n    role: github_scaler\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := &fakeMigrationStore{status: migrationStatus{Current: 1, Target: 1}}
	status, err := executeMigration(t.Context(), migrationRequest{
		action: migrationStatusAction, configPath: configPath,
	}, func(postgresConfig postgresconfig.Settings) migrationStore {
		if postgresConfig.Role != "github_scaler" {
			t.Fatalf("PostgreSQL config = %#v", postgresConfig)
		}
		return store
	})
	if err != nil || status.Current != 1 {
		t.Fatalf("migration status = %#v, %v", status, err)
	}
}

func TestExecuteMigrationReportsInvalidConfiguration(t *testing.T) {
	store := &fakeMigrationStore{}
	_, err := executeMigration(t.Context(), migrationRequest{
		action: migrationStatusAction, configPath: filepath.Join(t.TempDir(), "missing.yaml"),
	}, func(postgresconfig.Settings) migrationStore { return store })
	if err == nil {
		t.Fatal("expected invalid migration config")
	}
}

func TestExecuteStoreMigrationDispatchesTypedActions(t *testing.T) {
	store := &fakeMigrationStore{status: migrationStatus{Current: 1, Target: 1}}
	for _, request := range []migrationRequest{
		{action: migrationStatusAction},
		{action: migrationUpAction},
		{action: migrationDownAction, target: 0},
	} {
		status, err := executeStoreMigration(t.Context(), store, request)
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
			if _, err := executeStoreMigration(t.Context(), scenario.store, scenario.request); err == nil {
				t.Fatal("expected migration failure")
			}
		})
	}
}

func TestExecuteStoreMigrationRejectsUnsupportedAction(t *testing.T) {
	if _, err := executeStoreMigration(t.Context(), nil, migrationRequest{action: "unsupported"}); err == nil {
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
