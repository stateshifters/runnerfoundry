package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "modernc.org/sqlite" // Register the production SQLite database/sql driver.
)

func NewDatabaseStore(database *sql.DB) *Store {
	database.SetMaxOpenConns(1)
	store := NewStore(Executor{
		begin: func(ctx context.Context) (Transaction, error) {
			transaction, err := database.BeginTx(ctx, nil)
			if err != nil {
				return Transaction{}, fmt.Errorf("begin SQLite transaction: %w", err)
			}
			return adaptTransaction(transaction), nil
		},
		queryRow: func(ctx context.Context, statement string, arguments ...any) Row {
			return Row{scan: database.QueryRowContext(ctx, statement, arguments...).Scan}
		},
	})
	store.close = database.Close
	return store
}

func Open(ctx context.Context, path string) (*Store, error) {
	return openWithDatabaseOpener(ctx, path, sql.Open)
}

func openWithDatabaseOpener(
	ctx context.Context,
	path string,
	opener func(driverName string, dataSourceName string) (*sql.DB, error),
) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("open SQLite job state: %w", err)
	}
	database, err := opener("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite job state: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	if err := acquireExclusiveSQLiteOwnership(ctx, database); err != nil {
		return nil, errors.Join(err, database.Close())
	}
	if err := initialize(ctx, database); err != nil {
		return nil, errors.Join(err, database.Close())
	}
	return NewDatabaseStore(database), nil
}

func acquireExclusiveSQLiteOwnership(ctx context.Context, database *sql.DB) (resultErr error) {
	connection, err := database.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire exclusive SQLite ownership: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, connection.Close()) }()
	var mode string
	if err := connection.QueryRowContext(ctx, "PRAGMA main.locking_mode=EXCLUSIVE").Scan(&mode); err != nil {
		return fmt.Errorf("acquire exclusive SQLite ownership: set locking mode: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(mode), "exclusive") {
		return fmt.Errorf("acquire exclusive SQLite ownership: locking mode is %q", mode)
	}
	if _, err := connection.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
		return fmt.Errorf("acquire exclusive SQLite ownership: another process may own the database: %w", err)
	}
	if _, err := connection.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("acquire exclusive SQLite ownership: commit lock acquisition: %w", err)
	}
	return nil
}

func sqliteSchemaStatements() []string {
	return append(sqliteJobSchemaStatements(), sqliteCapacitySchemaStatements()...)
}

func sqliteJobSchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS runnerfoundry_members (
		member_id TEXT NOT NULL PRIMARY KEY CHECK(length(trim(member_id)) > 0),
		heartbeat_at INTEGER NOT NULL
	)`,
		`CREATE TABLE IF NOT EXISTS runnerfoundry_queued_jobs (
		job_id TEXT NOT NULL PRIMARY KEY CHECK(length(job_id) = 36
			AND substr(job_id, 9, 1) = '-' AND substr(job_id, 14, 1) = '-'
			AND substr(job_id, 19, 1) = '-' AND substr(job_id, 24, 1) = '-'
			AND length(replace(job_id, '-', '')) = 32
			AND replace(job_id, '-', '') NOT GLOB '*[^0-9a-f]*'),
		source_name TEXT NOT NULL CHECK(length(trim(source_name)) > 0),
		source_reference TEXT NOT NULL CHECK(length(trim(source_reference)) > 0),
		runner_pool TEXT NOT NULL CHECK(length(trim(runner_pool)) > 0),
		enqueued_at INTEGER NOT NULL,
		UNIQUE(source_name, source_reference)
	)`,
		`CREATE INDEX IF NOT EXISTS runnerfoundry_queued_jobs_fifo
		ON runnerfoundry_queued_jobs(runner_pool, enqueued_at)`,
		`CREATE TABLE IF NOT EXISTS runnerfoundry_processing_jobs (
		job_id TEXT NOT NULL PRIMARY KEY,
		source_name TEXT NOT NULL CHECK(length(trim(source_name)) > 0),
		source_reference TEXT NOT NULL CHECK(length(trim(source_reference)) > 0),
		runner_pool TEXT NOT NULL CHECK(length(trim(runner_pool)) > 0),
		member_id TEXT NOT NULL REFERENCES runnerfoundry_members(member_id),
		runner_name TEXT NOT NULL CHECK(length(trim(runner_name)) > 0),
		claimed_at INTEGER NOT NULL,
		inventory_endpoint_id TEXT NOT NULL CHECK(length(trim(inventory_endpoint_id)) > 0),
		runner_started INTEGER NOT NULL DEFAULT 0 CHECK(runner_started IN (0, 1)),
		provisioning_failure_reserved INTEGER NOT NULL DEFAULT 0
			CHECK(provisioning_failure_reserved IN (0, 1)),
		CHECK(NOT (runner_started = 1 AND provisioning_failure_reserved = 1)),
		UNIQUE(source_name, source_reference)
	)`,
		`CREATE INDEX IF NOT EXISTS runnerfoundry_processing_jobs_member_pool
		ON runnerfoundry_processing_jobs(member_id, runner_pool)`,
		`CREATE INDEX IF NOT EXISTS runnerfoundry_processing_jobs_pool
		ON runnerfoundry_processing_jobs(runner_pool)`,
		sqliteProvisioningFailureSchema,
		`CREATE TABLE IF NOT EXISTS runnerfoundry_source_tombstones (
		source_name TEXT NOT NULL CHECK(length(trim(source_name)) > 0),
		source_reference TEXT NOT NULL CHECK(length(trim(source_reference)) > 0),
		PRIMARY KEY(source_name, source_reference)
	)`,
	}
}

const sqliteProvisioningFailureSchema = `CREATE TABLE IF NOT EXISTS runnerfoundry_provisioning_failures (
	job_id TEXT NOT NULL PRIMARY KEY
		REFERENCES runnerfoundry_processing_jobs(job_id) ON DELETE RESTRICT,
	runner_name TEXT NOT NULL CHECK(length(trim(runner_name)) > 0),
	is_reported INTEGER NOT NULL DEFAULT 0 CHECK(is_reported IN (0, 1))
)`

func sqliteCapacitySchemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS runnerfoundry_member_runner_pools (
		member_id TEXT NOT NULL REFERENCES runnerfoundry_members(member_id) ON DELETE CASCADE,
		runner_pool TEXT NOT NULL CHECK(length(trim(runner_pool)) > 0),
		capacity INTEGER NOT NULL CHECK(capacity > 0),
		PRIMARY KEY(member_id, runner_pool)
	)`,
		sqliteGitHubRegistrationsSchema,
		sqliteGitHubMemberRunnerPoolsSchema,
		sqliteGitHubOwnershipSchema,
	}
}

const sqliteGitHubRegistrationsSchema = `CREATE TABLE IF NOT EXISTS runnerfoundry_github_registrations (
		github_app_id INTEGER NOT NULL CHECK(github_app_id > 0),
		repository_url TEXT NOT NULL CHECK(length(trim(repository_url)) > 0),
		runner_pool TEXT NOT NULL CHECK(length(trim(runner_pool)) > 0),
		installation_id INTEGER NOT NULL CHECK(installation_id > 0),
		runner_group_id INTEGER NOT NULL CHECK(runner_group_id > 0),
		scale_set_id INTEGER NOT NULL CHECK(scale_set_id > 0),
		PRIMARY KEY(repository_url, runner_pool)
		)`

const sqliteGitHubOwnershipSchema = `CREATE TABLE IF NOT EXISTS runnerfoundry_github_scale_set_ownership (
		github_app_id INTEGER NOT NULL CHECK(github_app_id > 0),
		repository_url TEXT NOT NULL CHECK(length(trim(repository_url)) > 0),
		runner_pool TEXT NOT NULL CHECK(length(trim(runner_pool)) > 0),
		installation_id INTEGER NOT NULL CHECK(installation_id > 0),
		runner_group_id INTEGER NOT NULL CHECK(runner_group_id > 0),
		scale_set_id INTEGER NOT NULL CHECK(scale_set_id > 0),
		PRIMARY KEY(repository_url, scale_set_id)
	)`

const sqliteGitHubMemberRunnerPoolsSchema = `CREATE TABLE IF NOT EXISTS runnerfoundry_github_member_runner_pools (
	member_id TEXT NOT NULL REFERENCES runnerfoundry_members(member_id) ON DELETE CASCADE,
	runner_pool TEXT NOT NULL CHECK(length(trim(runner_pool)) > 0),
	runner_group TEXT NOT NULL CHECK(length(trim(runner_group)) > 0),
	PRIMARY KEY(member_id, runner_pool)
)`

const sqliteSchemaVersion = 109
const sqliteSchemaVersionSQL = "PRAGMA user_version = 109"
const sqliteSchemaVersion108SQL = "PRAGMA user_version = 108"

const addSQLiteProvisioningFailureFenceSQL = `ALTER TABLE runnerfoundry_processing_jobs
	ADD COLUMN provisioning_failure_reserved INTEGER NOT NULL DEFAULT 0
	CHECK(provisioning_failure_reserved IN (0, 1)
		AND NOT (runner_started = 1 AND provisioning_failure_reserved = 1))`

func initialize(ctx context.Context, database *sql.DB) (resultErr error) {
	needsInitialization, err := validateSQLiteSchema(ctx, database)
	if err != nil || !needsInitialization {
		return err
	}
	return createSQLiteSchema(ctx, database)
}

func validateSQLiteSchema(ctx context.Context, database *sql.DB) (bool, error) {
	if _, err := database.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return false, fmt.Errorf("enable SQLite foreign keys: %w", err)
	}
	var version int
	if err := database.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return false, fmt.Errorf("read SQLite job schema version: %w", err)
	}
	if version == sqliteSchemaVersion {
		return false, nil
	}
	if version >= 106 && version <= 108 {
		return false, migrateSQLiteSchema(ctx, database, version)
	}
	if version != 0 {
		return false, fmt.Errorf("SQLite job schema version %d requires database recreation", version)
	}
	var applicationTables int
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&applicationTables); err != nil {
		return false, fmt.Errorf("inspect unversioned SQLite database: %w", err)
	}
	if applicationTables != 0 {
		return false, errors.New("unversioned SQLite database contains application tables and requires recreation")
	}
	return true, nil
}

func migrateSQLiteSchema(ctx context.Context, database *sql.DB, version int) error {
	if version < 108 {
		migration := migrateSQLite107
		if version == 106 {
			migration = migrateSQLite106
		}
		if err := migration(ctx, database); err != nil {
			return err
		}
	}
	return migrateSQLite108(ctx, database)
}

func migrateSQLite106(ctx context.Context, database *sql.DB) error {
	return runSQLiteMigration(ctx, database, "GitHub declaration schema upgrade", sqliteSchemaVersion108SQL,
		func(ctx context.Context, transaction *sql.Tx) error {
			if _, err := transaction.ExecContext(ctx, sqliteGitHubMemberRunnerPoolsSchema); err != nil {
				return fmt.Errorf("add SQLite GitHub Runner Pool declarations: %w", err)
			}
			return replaceSQLiteGitHubAdapterState(ctx, transaction)
		})
}

func migrateSQLite107(ctx context.Context, database *sql.DB) error {
	return runSQLiteMigration(ctx, database, "GitHub App schema upgrade", sqliteSchemaVersion108SQL,
		replaceSQLiteGitHubAdapterState)
}

func migrateSQLite108(ctx context.Context, database *sql.DB) error {
	return runSQLiteMigration(ctx, database, "provisioning failure schema upgrade", sqliteSchemaVersionSQL,
		func(ctx context.Context, transaction *sql.Tx) error {
			if _, err := transaction.ExecContext(ctx, addSQLiteProvisioningFailureFenceSQL); err != nil {
				return fmt.Errorf("add SQLite provisioning failure fence: %w", err)
			}
			if _, err := transaction.ExecContext(ctx, sqliteProvisioningFailureSchema); err != nil {
				return fmt.Errorf("add SQLite provisioning failure state: %w", err)
			}
			return nil
		})
}

func runSQLiteMigration(
	ctx context.Context,
	database *sql.DB,
	description string,
	versionStatement string,
	upgrade func(context.Context, *sql.Tx) error,
) (resultErr error) {
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite %s: %w", description, err)
	}
	open := true
	defer func() {
		if open {
			resultErr = errors.Join(resultErr, transaction.Rollback())
		}
	}()
	if err = upgrade(ctx, transaction); err != nil {
		return err
	}
	if _, err = transaction.ExecContext(ctx, versionStatement); err != nil {
		return fmt.Errorf("record SQLite %s: %w", description, err)
	}
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("commit SQLite %s: %w", description, err)
	}
	open = false
	return nil
}

func replaceSQLiteGitHubAdapterState(ctx context.Context, transaction *sql.Tx) error {
	for _, statement := range []string{
		`DROP TABLE runnerfoundry_github_registrations`,
		`DROP TABLE runnerfoundry_github_scale_set_ownership`,
		sqliteGitHubRegistrationsSchema,
		sqliteGitHubOwnershipSchema,
	} {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("replace SQLite GitHub App state: %w", err)
		}
	}
	return nil
}

func createSQLiteSchema(ctx context.Context, database *sql.DB) (resultErr error) {
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite job schema: %w", err)
	}
	open := true
	defer func() {
		if open {
			resultErr = errors.Join(resultErr, transaction.Rollback())
		}
	}()
	for _, statement := range sqliteSchemaStatements() {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize SQLite job schema: %w", err)
		}
	}
	if _, err := transaction.ExecContext(ctx, sqliteSchemaVersionSQL); err != nil {
		return fmt.Errorf("record SQLite job schema version: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit SQLite job schema: %w", err)
	}
	open = false
	return nil
}

func adaptTransaction(transaction *sql.Tx) Transaction {
	return Transaction{
		execute: sqliteTransactionExecutor(transaction), query: sqliteTransactionQuery(transaction),
		queryRow: func(ctx context.Context, statement string, arguments ...any) Row {
			return Row{scan: transaction.QueryRowContext(ctx, statement, arguments...).Scan}
		},
		commit: sqliteTransactionCommitter(transaction), rollback: sqliteTransactionRollbacker(transaction),
	}
}

func sqliteTransactionExecutor(transaction *sql.Tx) func(context.Context, string, ...any) (Result, error) {
	return func(ctx context.Context, statement string, arguments ...any) (Result, error) {
		result, err := transaction.ExecContext(ctx, statement, arguments...)
		if err != nil {
			return Result{}, fmt.Errorf("execute SQLite statement: %w", err)
		}
		return Result{rowsAffected: result.RowsAffected}, nil
	}
}

func sqliteTransactionQuery(transaction *sql.Tx) func(context.Context, string, ...any) (Rows, error) {
	return func(ctx context.Context, statement string, arguments ...any) (Rows, error) {
		rows, err := transaction.QueryContext(ctx, statement, arguments...)
		if err != nil {
			return Rows{}, fmt.Errorf("query SQLite rows: %w", err)
		}
		return Rows{close: rows.Close, err: rows.Err, next: rows.Next, scan: rows.Scan}, nil
	}
}

func sqliteTransactionCommitter(transaction *sql.Tx) func(context.Context) error {
	return func(context.Context) error {
		if err := transaction.Commit(); err != nil {
			return fmt.Errorf("commit SQLite transaction: %w", err)
		}
		return nil
	}
}

func sqliteTransactionRollbacker(transaction *sql.Tx) func(context.Context) error {
	return func(context.Context) error {
		if err := transaction.Rollback(); err != nil {
			return fmt.Errorf("rollback SQLite transaction: %w", err)
		}
		return nil
	}
}
