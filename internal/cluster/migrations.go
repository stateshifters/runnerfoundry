package cluster

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/tern/v2/migrate"
)

const (
	migrationAdvisoryLockKey int64 = 0x6769746875626d67
	migrationVersionTable          = "public.scaler_schema_version"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// MigrationStatus describes the database version and the migration range known
// to this binary. CompatibilityFloor is the oldest binary schema generation
// allowed to use the current database schema.
type MigrationStatus struct {
	Current            int32
	Target             int32
	Minimum            int32
	CompatibilityFloor int32
}

// Migrate serializes startup migration, applies only known forward migrations,
// and accepts a newer schema only when its compatibility floor includes this binary.
func (store *Store) Migrate(ctx context.Context) (resultErr error) {
	connection, err := store.connectDirectPostgreSQL(ctx)
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
	current, err := migrator.GetCurrentVersion(ctx)
	if err != nil {
		return safePostgreSQLError("read PostgreSQL migration version", err)
	}
	target := migrationTarget(migrator)
	if current > target {
		return validateCompatibility(ctx, connection, target)
	}
	if err = migrator.MigrateTo(ctx, target); err != nil {
		return safePostgreSQLError("apply PostgreSQL migrations", err)
	}
	return validateCompatibility(ctx, connection, target)
}

// MigrationStatus reports the database version without creating migration metadata or domain objects.
func (store *Store) MigrationStatus(ctx context.Context) (status MigrationStatus, resultErr error) {
	connection, err := store.connectDirectPostgreSQL(ctx)
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
	var versionTableExists bool
	if err = connection.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, migrationVersionTable).Scan(&versionTableExists); err != nil {
		return MigrationStatus{}, safePostgreSQLError("inspect PostgreSQL migration metadata", err)
	}
	if !versionTableExists {
		return status, nil
	}
	if status.Current, err = inspectMigrationVersion(ctx, connection); err != nil {
		return MigrationStatus{}, err
	}
	if status.Current == 0 {
		return status, nil
	}
	if err = connection.QueryRow(ctx, `SELECT oldest_binary_schema_generation FROM scaler_schema_compatibility WHERE singleton`).Scan(&status.CompatibilityFloor); err != nil {
		return MigrationStatus{}, safePostgreSQLError("read PostgreSQL schema compatibility", err)
	}
	return status, nil
}

func inspectMigrationVersion(ctx context.Context, connection *pgx.Conn) (int32, error) {
	migrator, err := migrate.NewMigrator(ctx, connection, migrationVersionTable)
	if err != nil {
		return 0, safePostgreSQLError("initialize PostgreSQL migration metadata", err)
	}
	current, err := migrator.GetCurrentVersion(ctx)
	if err != nil {
		return 0, safePostgreSQLError("read PostgreSQL migration version", err)
	}
	return current, nil
}

// MigrateDown rolls back to a known version while holding the migration and
// leader locks. It refuses rollback until every published node heartbeat expires.
func (store *Store) MigrateDown(
	ctx context.Context,
	target int32,
	now time.Time,
	liveFor time.Duration,
) (resultErr error) {
	connection, err := store.connectDirectPostgreSQL(ctx)
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
	current, err := migrator.GetCurrentVersion(ctx)
	if err != nil {
		return safePostgreSQLError("read PostgreSQL migration version", err)
	}
	if err = validateRollbackTarget(target, current); err != nil || target == current {
		return err
	}
	if err = store.ensureRollbackAllowed(ctx, connection, now.Add(-liveFor)); err != nil {
		return err
	}
	if err = migrator.MigrateTo(ctx, target); err != nil {
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

func (store *Store) ensureRollbackAllowed(ctx context.Context, connection *pgx.Conn, liveCutoff time.Time) error {
	var leaderAvailable bool
	if err := connection.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderAdvisoryLockKey).Scan(&leaderAvailable); err != nil {
		return errors.New("check PostgreSQL leader lease")
	}
	if !leaderAvailable {
		return errors.New("PostgreSQL leader lease is active")
	}
	var liveNodes bool
	if err := connection.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM scaler_nodes WHERE heartbeat_at >= $1)`, liveCutoff).Scan(&liveNodes); err != nil {
		return errors.New("check live PostgreSQL cluster nodes")
	}
	if liveNodes {
		return errors.New("PostgreSQL cluster nodes are still live")
	}
	return nil
}

func migrationTarget(migrator *migrate.Migrator) int32 {
	if len(migrator.Migrations) == 0 {
		return 0
	}
	return migrator.Migrations[len(migrator.Migrations)-1].Sequence
}

func (store *Store) lockedMigrator(ctx context.Context, connection *pgx.Conn) (*migrate.Migrator, error) {
	if _, err := connection.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationAdvisoryLockKey); err != nil {
		return nil, fmt.Errorf("acquire PostgreSQL migration lock: %w", err)
	}
	return store.newMigrator(ctx, connection)
}

func (store *Store) newMigrator(ctx context.Context, connection *pgx.Conn) (*migrate.Migrator, error) {
	migrator, err := migrate.NewMigrator(ctx, connection, migrationVersionTable)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL migrator: %w", err)
	}
	if err = store.loadMigrations(migrator); err != nil {
		return nil, err
	}
	return migrator, nil
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

func validateCompatibility(ctx context.Context, connection *pgx.Conn, binaryTarget int32) error {
	var compatibilityFloor int32
	if err := connection.QueryRow(ctx, `SELECT oldest_binary_schema_generation FROM scaler_schema_compatibility WHERE singleton`).Scan(&compatibilityFloor); err != nil {
		return errors.New("read PostgreSQL schema compatibility")
	}
	if binaryTarget < compatibilityFloor {
		return errors.New("PostgreSQL schema is incompatible with this binary")
	}
	return nil
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
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code != "" {
		message := operation
		if postgresError.Message != "" {
			message += ": " + postgresError.Message
		}
		message += fmt.Sprintf(" (SQLSTATE %s)", postgresError.Code)
		for _, diagnostic := range []struct {
			label string
			value string
		}{
			{label: "detail", value: postgresError.Detail},
			{label: "hint", value: postgresError.Hint},
			{label: "table", value: postgresError.TableName},
			{label: "constraint", value: postgresError.ConstraintName},
		} {
			if diagnostic.value != "" {
				message += "; " + diagnostic.label + ": " + diagnostic.value
			}
		}
		return errors.New(message)
	}
	return errors.New(operation)
}
