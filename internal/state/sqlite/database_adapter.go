package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite" // Register the production SQLite database/sql driver.
)

var openSQLiteDatabase = sql.Open

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
	database, err := openSQLiteDatabase("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite job state: %w", err)
	}
	if err := initialize(ctx, database); err != nil {
		return nil, errors.Join(err, database.Close())
	}
	return NewDatabaseStore(database), nil
}

var sqliteSchemaStatements = []string{
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
		source_name TEXT NOT NULL,
		source_reference TEXT NOT NULL,
		runner_pool TEXT NOT NULL,
		enqueued_at INTEGER NOT NULL,
		UNIQUE(source_name, source_reference)
	)`,
	`CREATE INDEX IF NOT EXISTS runnerfoundry_queued_jobs_fifo
		ON runnerfoundry_queued_jobs(runner_pool, enqueued_at)`,
	`CREATE TABLE IF NOT EXISTS runnerfoundry_processing_jobs (
		job_id TEXT NOT NULL PRIMARY KEY,
		source_name TEXT NOT NULL,
		source_reference TEXT NOT NULL,
		runner_pool TEXT NOT NULL,
		member_id TEXT NOT NULL REFERENCES runnerfoundry_members(member_id),
		runner_started INTEGER NOT NULL DEFAULT 0 CHECK(runner_started IN (0, 1)),
		UNIQUE(source_name, source_reference)
	)`,
	`CREATE INDEX IF NOT EXISTS runnerfoundry_processing_jobs_member_pool
		ON runnerfoundry_processing_jobs(member_id, runner_pool)`,
	`CREATE INDEX IF NOT EXISTS runnerfoundry_processing_jobs_pool
		ON runnerfoundry_processing_jobs(runner_pool)`,
	`CREATE TABLE IF NOT EXISTS runnerfoundry_source_tombstones (
		source_name TEXT NOT NULL,
		source_reference TEXT NOT NULL,
		PRIMARY KEY(source_name, source_reference)
	)`,
	`CREATE TABLE IF NOT EXISTS runnerfoundry_member_runner_pools (
		member_id TEXT NOT NULL REFERENCES runnerfoundry_members(member_id) ON DELETE CASCADE,
		runner_pool TEXT NOT NULL,
		capacity INTEGER NOT NULL CHECK(capacity > 0),
		PRIMARY KEY(member_id, runner_pool)
	)`,
	`CREATE TABLE IF NOT EXISTS runnerfoundry_github_registrations (
		repository_url TEXT NOT NULL,
		runner_pool TEXT NOT NULL,
		installation_id INTEGER NOT NULL,
		runner_group_id INTEGER NOT NULL,
		scale_set_id INTEGER NOT NULL,
		PRIMARY KEY(repository_url, runner_pool)
	)`,
}

const sqliteSchemaVersion = 103
const sqliteSchemaVersionSQL = "PRAGMA user_version = 103"

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
	for _, statement := range sqliteSchemaStatements {
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
