package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
)

type migrationStatus = cluster.MigrationStatus

type migrationStore interface {
	MigrationStatus(ctx context.Context) (cluster.MigrationStatus, error)
	Migrate(ctx context.Context) error
	MigrateDown(ctx context.Context, target int32, now time.Time, liveFor time.Duration) error
}

type migrationStoreFactory func(postgresconfig.Settings) migrationStore

type migrationDependencies struct {
	readConfigFile config.ReadFile
	newStore       migrationStoreFactory
	now            func() time.Time
}

var productionMigrationDependencies = migrationDependencies{
	readConfigFile: os.ReadFile,
	newStore: func(postgresConfig postgresconfig.Settings) migrationStore {
		return cluster.NewStore(postgresConfig)
	},
	now: time.Now,
}

func runPostgreSQLMigration(ctx context.Context, request migrationRequest) (migrationStatus, error) {
	return executeMigration(ctx, request, productionMigrationDependencies)
}

func executeMigration(
	ctx context.Context,
	request migrationRequest,
	dependencies migrationDependencies,
) (migrationStatus, error) {
	configBytes, err := dependencies.readConfigFile(request.configPath)
	if err != nil {
		return migrationStatus{}, fmt.Errorf("read migration config: %w", err)
	}
	configured, err := loadMigrationConfig(configBytes)
	if err != nil {
		return migrationStatus{}, err
	}
	return executeStoreMigration(ctx, dependencies.newStore(configured), request, dependencies.now())
}

func loadMigrationConfig(configBytes []byte) (postgresconfig.Settings, error) {
	configured, err := config.LoadPostgreSQLState(bytes.NewReader(configBytes))
	if err != nil {
		return postgresconfig.Settings{}, fmt.Errorf("load migration config: %w", err)
	}
	return configured, nil
}

func executeStoreMigration(
	ctx context.Context,
	store migrationStore,
	request migrationRequest,
	now time.Time,
) (migrationStatus, error) {
	switch request.action {
	case migrationStatusAction:
		status, err := store.MigrationStatus(ctx)
		return status, migrationStatusError(err)
	case migrationUpAction:
		if err := store.Migrate(ctx); err != nil {
			return migrationStatus{}, fmt.Errorf("migrate PostgreSQL up: %w", err)
		}
	case migrationDownAction:
		if err := store.MigrateDown(ctx, request.target, now, cluster.NodeLifetime); err != nil {
			return migrationStatus{}, fmt.Errorf("migrate PostgreSQL down: %w", err)
		}
	default:
		return migrationStatus{}, errors.New("unsupported migration command")
	}
	status, err := store.MigrationStatus(ctx)
	return status, migrationStatusError(err)
}

func migrationStatusError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("read migration status: %w", err)
}
