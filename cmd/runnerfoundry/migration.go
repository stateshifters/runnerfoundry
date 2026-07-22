package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

func runPostgreSQLMigration(ctx context.Context, request migrationRequest) (migrationStatus, error) {
	return executeMigration(ctx, request, func(postgresConfig postgresconfig.Settings) migrationStore {
		return cluster.NewStore(postgresConfig)
	})
}

func executeMigration(
	ctx context.Context,
	request migrationRequest,
	newStore migrationStoreFactory,
) (migrationStatus, error) {
	configured, err := loadMigrationConfig(request.configPath)
	if err != nil {
		return migrationStatus{}, err
	}
	return executeStoreMigration(ctx, newStore(configured), request)
}

func loadMigrationConfig(configPath string) (postgresconfig.Settings, error) {
	configRoot, err := os.OpenRoot(filepath.Dir(configPath))
	if err != nil {
		return postgresconfig.Settings{}, fmt.Errorf("open config directory: %w", err)
	}
	configFile, err := configRoot.Open(filepath.Base(configPath))
	if err != nil {
		return postgresconfig.Settings{}, errors.Join(fmt.Errorf("open config: %w", err), configRoot.Close())
	}
	configured, loadErr := config.LoadPostgreSQLState(configFile)
	closeErr := errors.Join(configFile.Close(), configRoot.Close())
	if err = errors.Join(loadErr, closeErr); err != nil {
		return postgresconfig.Settings{}, fmt.Errorf("load migration config: %w", err)
	}
	return configured, nil
}

func executeStoreMigration(ctx context.Context, store migrationStore, request migrationRequest) (migrationStatus, error) {
	switch request.action {
	case migrationStatusAction:
		status, err := store.MigrationStatus(ctx)
		return status, migrationStatusError(err)
	case migrationUpAction:
		if err := store.Migrate(ctx); err != nil {
			return migrationStatus{}, fmt.Errorf("migrate PostgreSQL up: %w", err)
		}
	case migrationDownAction:
		if err := store.MigrateDown(ctx, request.target, time.Now(), cluster.NodeLifetime); err != nil {
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
