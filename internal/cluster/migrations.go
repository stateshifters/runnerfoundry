package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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

type MigrationStatus struct {
	Current int32
	Target  int32
	Minimum int32
}

type Migrator struct {
	sessions sessionOpener
	catalog  *Catalog
}

func NewMigrator(connections *Connections, catalog *Catalog) *Migrator {
	return &Migrator{sessions: connections, catalog: catalog}
}

func migratorWithSessions(sessions sessionOpener, catalog *Catalog) *Migrator {
	return &Migrator{sessions: sessions, catalog: catalog}
}

func (migrator *Migrator) Up(ctx context.Context) (resultErr error) {
	session, err := migrator.sessions.openSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, closePostgreSQLSession(ctx, session))
	}()
	executor, err := migrator.lockedExecutor(ctx, session)
	if err != nil {
		return safePostgreSQLError("initialize PostgreSQL migrations", err)
	}
	current, err := executor.currentVersion(ctx)
	if err != nil {
		return safePostgreSQLError("read PostgreSQL migration version", err)
	}
	if current == 0 {
		_, legacy, inspectErr := inspectSchemaCompatibility(ctx, session)
		if inspectErr != nil {
			return safePostgreSQLError("inspect PostgreSQL schema compatibility", inspectErr)
		}
		if legacy {
			return IncompatibleSchemaError{}
		}
	}
	if current > executor.target {
		return errors.New("PostgreSQL schema is newer than this binary")
	}
	if err = executor.migrateTo(ctx, executor.target); err != nil {
		return safePostgreSQLError("apply PostgreSQL migrations", err)
	}
	return nil
}

func (migrator *Migrator) Status(ctx context.Context) (status MigrationStatus, resultErr error) {
	session, err := migrator.sessions.openSession(ctx)
	if err != nil {
		return MigrationStatus{}, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, closePostgreSQLSession(ctx, session))
	}()
	status.Target = migrator.catalog.Target()
	versionTableExists, legacySchema, err := inspectSchemaCompatibility(ctx, session)
	if err != nil {
		return MigrationStatus{}, safePostgreSQLError("inspect PostgreSQL migration metadata", err)
	}
	if !versionTableExists {
		if legacySchema {
			return MigrationStatus{}, IncompatibleSchemaError{}
		}
		return status, nil
	}
	executor, err := session.newMigrationExecutor(ctx, migrator.catalog)
	if err != nil {
		return MigrationStatus{}, safePostgreSQLError("initialize PostgreSQL migration metadata", err)
	}
	status.Current, err = executor.currentVersion(ctx)
	if err != nil {
		return MigrationStatus{}, safePostgreSQLError("read PostgreSQL migration version", err)
	}
	return status, nil
}

func inspectSchemaCompatibility(
	ctx context.Context,
	session *databaseSession,
) (versionTableExists bool, legacySchema bool, err error) {
	err = session.QueryRow(ctx, inspectSchemaCompatibilitySQL,
		migrationVersionTable, "public.scaler_schema_version").Scan(&versionTableExists, &legacySchema)
	return versionTableExists, legacySchema, err
}

func (migrator *Migrator) RollbackTo(
	ctx context.Context,
	target int32,
	liveCutoff time.Time,
) (resultErr error) {
	session, err := migrator.sessions.openSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, closePostgreSQLSession(ctx, session))
	}()
	executor, err := migrator.lockedExecutor(ctx, session)
	if err != nil {
		return safePostgreSQLError("initialize PostgreSQL migrations", err)
	}
	current, err := executor.currentVersion(ctx)
	if err != nil {
		return safePostgreSQLError("read PostgreSQL migration version", err)
	}
	if err = validateRollbackTarget(target, current); err != nil || target == current {
		return err
	}
	if err = ensureRollbackAllowed(ctx, session, liveCutoff); err != nil {
		return err
	}
	if err = executor.migrateTo(ctx, target); err != nil {
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

func ensureRollbackAllowed(ctx context.Context, session *databaseSession, liveCutoff time.Time) error {
	var leaderAvailable bool
	if err := session.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderAdvisoryLockKey).Scan(&leaderAvailable); err != nil {
		return errors.New("check PostgreSQL leader lease")
	}
	if !leaderAvailable {
		return errors.New("PostgreSQL leader lease is active")
	}
	var liveMembers bool
	if err := session.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.runnerfoundry_members WHERE heartbeat_at >= $1)`, liveCutoff).Scan(&liveMembers); err != nil {
		return errors.New("check live PostgreSQL cluster members")
	}
	if liveMembers {
		return errors.New("PostgreSQL cluster members are still live")
	}
	return nil
}

func (migrator *Migrator) lockedExecutor(
	ctx context.Context,
	session *databaseSession,
) (*migrationExecutor, error) {
	if _, err := session.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationAdvisoryLockKey); err != nil {
		return nil, fmt.Errorf("acquire PostgreSQL migration lock: %w", err)
	}
	return session.newMigrationExecutor(ctx, migrator.catalog)
}

type migrationExecutor struct {
	currentVersion func(context.Context) (int32, error)
	migrateTo      func(context.Context, int32) error
	target         int32
}

func closePostgreSQLSession(ctx context.Context, session *databaseSession) error {
	if err := session.Close(context.WithoutCancel(ctx)); err != nil {
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
