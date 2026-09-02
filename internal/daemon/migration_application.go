package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/cluster"
	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
	githubpostgres "github.com/stateshifters/runnerfoundry/internal/source/github/persistence/postgres"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
)

const (
	neutralPostgreSQLMigrationOrder = 100
	githubPostgreSQLMigrationOrder  = 200
)

type MigrationStatus = cluster.MigrationStatus

type migrationBackend interface {
	Status(ctx context.Context) (MigrationStatus, error)
	Up(ctx context.Context) error
	RollbackTo(ctx context.Context, target int32, liveCutoff time.Time) error
}

type migrationExecutor struct {
	backend migrationBackend
}

type migrationApplicationDependencies struct {
	readConfigFile config.ReadFile
	newExecutor    func(postgresconfig.Settings) (migrationExecutor, error)
	currentTime    func() time.Time
}

type MigrationApplication struct {
	dependencies migrationApplicationDependencies
}

func NewMigrationApplication() MigrationApplication {
	return MigrationApplication{dependencies: migrationApplicationDependencies{
		readConfigFile: os.ReadFile,
		newExecutor:    newProductionMigrationExecutor,
		currentTime:    time.Now,
	}}
}

func (application MigrationApplication) Status(ctx context.Context, configPath string) (MigrationStatus, error) {
	executor, err := application.executor(configPath)
	if err != nil {
		return MigrationStatus{}, err
	}
	return migrationStatus(ctx, executor)
}

func (application MigrationApplication) Up(ctx context.Context, configPath string) (MigrationStatus, error) {
	executor, err := application.executor(configPath)
	if err != nil {
		return MigrationStatus{}, err
	}
	if err = executor.backend.Up(ctx); err != nil {
		return MigrationStatus{}, fmt.Errorf("migrate PostgreSQL up: %w", err)
	}
	return migrationStatus(ctx, executor)
}

func (application MigrationApplication) RollbackTo(
	ctx context.Context,
	configPath string,
	target int32,
) (MigrationStatus, error) {
	executor, err := application.executor(configPath)
	if err != nil {
		return MigrationStatus{}, err
	}
	liveCutoff := application.dependencies.currentTime().Add(-runtimeMemberTTL)
	if err = executor.backend.RollbackTo(ctx, target, liveCutoff); err != nil {
		return MigrationStatus{}, fmt.Errorf("migrate PostgreSQL down: %w", err)
	}
	return migrationStatus(ctx, executor)
}

func (application MigrationApplication) executor(configPath string) (migrationExecutor, error) {
	configBytes, err := application.dependencies.readConfigFile(configPath)
	if err != nil {
		return migrationExecutor{}, fmt.Errorf("read migration config: %w", err)
	}
	settings, err := config.LoadPostgreSQLState(bytes.NewReader(configBytes))
	if err != nil {
		return migrationExecutor{}, fmt.Errorf("load migration config: %w", err)
	}
	executor, err := application.dependencies.newExecutor(settings)
	if err != nil {
		return migrationExecutor{}, fmt.Errorf("configure PostgreSQL migrations: %w", err)
	}
	return executor, nil
}

func newProductionMigrationExecutor(settings postgresconfig.Settings) (migrationExecutor, error) {
	connections, err := cluster.NewConnections(settings)
	if err != nil {
		return migrationExecutor{}, fmt.Errorf("create PostgreSQL connections: %w", err)
	}
	catalog, err := newPostgreSQLMigrationCatalog()
	if err != nil {
		return migrationExecutor{}, err
	}
	return migrationExecutor{backend: cluster.NewMigrator(connections, catalog)}, nil
}

func newPostgreSQLMigrationCatalog() (*cluster.Catalog, error) {
	return composePostgreSQLMigrationCatalog(postgresstate.Migrations(), githubpostgres.Migrations())
}

func composePostgreSQLMigrationCatalog(
	neutralFiles fs.FS,
	githubFiles fs.FS,
) (*cluster.Catalog, error) {
	neutralMigrations, err := cluster.NewMigrationSet(
		"state-postgres", neutralPostgreSQLMigrationOrder, neutralFiles,
	)
	if err != nil {
		return nil, fmt.Errorf("register neutral PostgreSQL migrations: %w", err)
	}
	githubMigrations, err := cluster.NewMigrationSet(
		"source-github-postgres", githubPostgreSQLMigrationOrder, githubFiles,
	)
	if err != nil {
		return nil, fmt.Errorf("register GitHub PostgreSQL migrations: %w", err)
	}
	catalog, err := cluster.NewCatalog(neutralMigrations, githubMigrations)
	if err != nil {
		return nil, fmt.Errorf("compose PostgreSQL migration catalog: %w", err)
	}
	return catalog, nil
}

func migrationStatus(ctx context.Context, executor migrationExecutor) (MigrationStatus, error) {
	status, err := executor.backend.Status(ctx)
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("read migration status: %w", err)
	}
	return status, nil
}
