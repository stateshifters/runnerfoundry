package cluster

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/tern/v2/migrate"
)

const (
	migrationAdvisoryLockKey int64 = 0x6769746875626d67
	migrationVersionTable          = "public.runnerfoundry_schema_version"
)

const inspectSchemaCompatibilitySQL = `SELECT
	to_regclass($1) IS NOT NULL,
	to_regclass($2) IS NOT NULL OR EXISTS (
		SELECT 1 FROM pg_catalog.pg_class objects
		JOIN pg_catalog.pg_namespace namespaces ON namespaces.oid = objects.relnamespace
		WHERE namespaces.nspname = 'public' AND objects.relname LIKE 'scaler\_%' ESCAPE '\')`

var ErrIncompatibleSchema = errors.New("incompatible legacy RunnerFoundry schema")

type IncompatibleSchemaError struct{}

func (IncompatibleSchemaError) Error() string        { return ErrIncompatibleSchema.Error() }
func (IncompatibleSchemaError) Is(target error) bool { return target == ErrIncompatibleSchema }

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// MigrationStatus describes the database version and the migration range known to this binary.
type MigrationStatus struct {
	Current int32
	Target  int32
	Minimum int32
}

// Migrate serializes startup migration and applies only known forward migrations.
func (store *Store) Migrate(ctx context.Context) (resultErr error) {
	connection, err := store.directConnector(ctx)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, closePostgreSQLConnection(ctx, connection))
	}()
	migrator, err := store.lockedMigrator(ctx, connection)
	if err != nil {
		return safePostgreSQLError("initialize PostgreSQL migrations", err)
	}
	current, err := migrator.currentVersion(ctx)
	if err != nil {
		return safePostgreSQLError("read PostgreSQL migration version", err)
	}
	if current == 0 {
		_, legacy, inspectErr := inspectSchemaCompatibility(ctx, connection)
		if inspectErr != nil {
			return safePostgreSQLError("inspect PostgreSQL schema compatibility", inspectErr)
		}
		if legacy {
			return IncompatibleSchemaError{}
		}
	}
	target := migrator.target
	if current > target {
		return errors.New("PostgreSQL schema is newer than this binary")
	}
	if err = migrator.migrateTo(ctx, target); err != nil {
		return safePostgreSQLError("apply PostgreSQL migrations", err)
	}
	return nil
}

// MigrationStatus reports the database version without creating migration metadata or domain objects.
func (store *Store) MigrationStatus(ctx context.Context) (status MigrationStatus, resultErr error) {
	connection, err := store.directConnector(ctx)
	if err != nil {
		return MigrationStatus{}, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, closePostgreSQLConnection(ctx, connection))
	}()
	catalog, err := store.migrationCatalog()
	if err != nil {
		return MigrationStatus{}, safePostgreSQLError("load PostgreSQL migrations", err)
	}
	status.Target = migrationTarget(catalog)
	versionTableExists, legacySchema, err := inspectSchemaCompatibility(ctx, connection)
	if err != nil {
		return MigrationStatus{}, safePostgreSQLError("inspect PostgreSQL migration metadata", err)
	}
	if !versionTableExists {
		if legacySchema {
			return MigrationStatus{}, IncompatibleSchemaError{}
		}
		return status, nil
	}
	if status.Current, err = store.inspectMigrationVersion(ctx, connection); err != nil {
		return MigrationStatus{}, err
	}
	return status, nil
}

func inspectSchemaCompatibility(
	ctx context.Context,
	connection leaderConnection,
) (versionTableExists bool, legacySchema bool, err error) {
	err = connection.QueryRow(ctx, inspectSchemaCompatibilitySQL,
		migrationVersionTable, "public.scaler_schema_version").Scan(&versionTableExists, &legacySchema)
	return versionTableExists, legacySchema, err
}

func (store *Store) inspectMigrationVersion(ctx context.Context, connection leaderConnection) (int32, error) {
	migrator, err := store.migrationFactory(ctx, connection)
	if err != nil {
		return 0, safePostgreSQLError("initialize PostgreSQL migration metadata", err)
	}
	current, err := migrator.currentVersion(ctx)
	if err != nil {
		return 0, safePostgreSQLError("read PostgreSQL migration version", err)
	}
	return current, nil
}

// MigrateDown rolls back to a known version while holding the migration and
// leader locks. It refuses rollback until every published member heartbeat expires.
func (store *Store) MigrateDown(
	ctx context.Context,
	target int32,
	now time.Time,
	liveFor time.Duration,
) (resultErr error) {
	connection, err := store.directConnector(ctx)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, closePostgreSQLConnection(ctx, connection))
	}()
	migrator, err := store.lockedMigrator(ctx, connection)
	if err != nil {
		return safePostgreSQLError("initialize PostgreSQL migrations", err)
	}
	current, err := migrator.currentVersion(ctx)
	if err != nil {
		return safePostgreSQLError("read PostgreSQL migration version", err)
	}
	if err = validateRollbackTarget(target, current); err != nil || target == current {
		return err
	}
	if err = store.ensureRollbackAllowed(ctx, connection, now.Add(-liveFor)); err != nil {
		return err
	}
	if err = migrator.migrateTo(ctx, target); err != nil {
		return safePostgreSQLError("roll back PostgreSQL migrations", err)
	}
	return nil
}

func validateRollbackTarget(target int32, current int32) error {
	if target < 0 || target > current {
		return errors.New("invalid PostgreSQL rollback target")
	}
	return nil
}

func (store *Store) ensureRollbackAllowed(ctx context.Context, connection leaderConnection, liveCutoff time.Time) error {
	var leaderAvailable bool
	if err := connection.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderAdvisoryLockKey).Scan(&leaderAvailable); err != nil {
		return errors.New("check PostgreSQL leader lease")
	}
	if !leaderAvailable {
		return errors.New("PostgreSQL leader lease is active")
	}
	var liveMembers bool
	if err := connection.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.runnerfoundry_members WHERE heartbeat_at >= $1)`, liveCutoff).Scan(&liveMembers); err != nil {
		return errors.New("check live PostgreSQL cluster members")
	}
	if liveMembers {
		return errors.New("PostgreSQL cluster members are still live")
	}
	return nil
}

func migrationTarget(migrator *migrate.Migrator) int32 {
	if len(migrator.Migrations) == 0 {
		return 0
	}
	return migrator.Migrations[len(migrator.Migrations)-1].Sequence
}

func (store *Store) lockedMigrator(ctx context.Context, connection leaderConnection) (*migrationExecutor, error) {
	if _, err := connection.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationAdvisoryLockKey); err != nil {
		return nil, fmt.Errorf("acquire PostgreSQL migration lock: %w", err)
	}
	return store.migrationFactory(ctx, connection)
}

func (store *Store) newMigrationExecutor(ctx context.Context, connection leaderConnection) (*migrationExecutor, error) {
	postgresConnection, valid := connection.(*postgreSQLLeaderConnection)
	if !valid {
		return nil, errors.New("PostgreSQL migration connection is required")
	}
	migrator, err := store.newTernMigrator(ctx, postgresConnection.connection, migrationVersionTable)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL migrator: %w", err)
	}
	if err = store.loadMigrations(migrator); err != nil {
		return nil, err
	}
	return &migrationExecutor{
		currentVersion: migrator.GetCurrentVersion,
		migrateTo:      migrator.MigrateTo,
		target:         migrationTarget(migrator),
	}, nil
}

func (store *Store) migrationCatalog() (*migrate.Migrator, error) {
	catalog := &migrate.Migrator{}
	if err := store.loadMigrations(catalog); err != nil {
		return nil, err
	}
	return catalog, nil
}

func (store *Store) loadMigrations(migrator *migrate.Migrator) error {
	migrationFiles, err := fs.Sub(store.migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("open embedded PostgreSQL migrations: %w", err)
	}
	if err = migrator.LoadMigrations(migrationFiles); err != nil {
		return fmt.Errorf("load embedded PostgreSQL migrations: %w", err)
	}
	return validateReversibleMigrations(migrator)
}

func validateReversibleMigrations(migrator *migrate.Migrator) error {
	for _, migration := range migrator.Migrations {
		if strings.TrimSpace(migration.DownSQL) == "" && migration.DownFunc == nil {
			return errors.New("PostgreSQL migration is missing Down SQL")
		}
	}
	return nil
}

type migrationExecutor struct {
	currentVersion func(context.Context) (int32, error)
	migrateTo      func(context.Context, int32) error
	target         int32
}

type postgreSQLCloser interface {
	Close(ctx context.Context) error
}

func closePostgreSQLConnection(ctx context.Context, connection postgreSQLCloser) error {
	if err := connection.Close(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("close PostgreSQL migration connection: %w", err)
	}
	return nil
}

func safePostgreSQLError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: %w", operation, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", operation, context.DeadlineExceeded)
	}
	if errors.Is(err, ErrIncompatibleSchema) {
		return err
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code != "" {
		return fmt.Errorf("%s (SQLSTATE %s)", operation, postgresError.Code)
	}
	return errors.New(operation)
}
