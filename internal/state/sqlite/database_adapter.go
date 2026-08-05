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
	`CREATE TABLE IF NOT EXISTS runnerfoundry_jobs (
		job_id TEXT NOT NULL PRIMARY KEY CHECK(length(job_id) = 36
			AND substr(job_id, 9, 1) = '-' AND substr(job_id, 14, 1) = '-'
			AND substr(job_id, 19, 1) = '-' AND substr(job_id, 24, 1) = '-'
			AND length(replace(job_id, '-', '')) = 32
			AND replace(job_id, '-', '') NOT GLOB '*[^0-9a-f]*'),
		source_name TEXT NOT NULL,
		source_reference TEXT NOT NULL,
		runner_pool TEXT NOT NULL,
		state TEXT NOT NULL CHECK(state IN ('queued', 'attempting', 'result_pending')),
		enqueued_at INTEGER NOT NULL,
		member_id TEXT,
		claimed_at INTEGER,
		started_at INTEGER,
		acquisition_started_at INTEGER,
		acquired_at INTEGER,
		result TEXT CHECK(result IN ('success', 'failure')),
		result_origin TEXT CHECK(result_origin IN ('runner', 'service')),
		result_reason TEXT,
		UNIQUE(source_name, source_reference),
		CHECK(acquired_at IS NULL OR acquisition_started_at IS NOT NULL),
		CHECK(acquisition_started_at IS NULL OR acquisition_started_at >= claimed_at),
		CHECK(acquired_at IS NULL OR acquired_at >= acquisition_started_at),
		CHECK(started_at IS NULL OR (acquired_at IS NOT NULL AND started_at >= acquired_at)),
		CHECK((state = 'queued' AND member_id IS NULL AND claimed_at IS NULL AND started_at IS NULL
			AND acquisition_started_at IS NULL AND acquired_at IS NULL AND result IS NULL
			AND result_origin IS NULL AND result_reason IS NULL)
		 OR (state = 'attempting' AND member_id IS NOT NULL AND claimed_at IS NOT NULL
			AND result IS NULL AND result_origin IS NULL AND result_reason IS NULL)
		 OR (state = 'result_pending' AND result IS NOT NULL AND result_origin IS NOT NULL
			AND ((member_id IS NULL AND claimed_at IS NULL AND started_at IS NULL
				AND acquisition_started_at IS NULL AND acquired_at IS NULL)
			 OR (member_id IS NOT NULL AND claimed_at IS NOT NULL))))
	)`,
	`CREATE INDEX IF NOT EXISTS runnerfoundry_jobs_queued_fifo
		ON runnerfoundry_jobs(runner_pool, enqueued_at) WHERE state = 'queued'`,
	`CREATE INDEX IF NOT EXISTS runnerfoundry_jobs_member_attempts
		ON runnerfoundry_jobs(member_id, runner_pool, claimed_at) WHERE state = 'attempting'`,
	`CREATE INDEX IF NOT EXISTS runnerfoundry_jobs_pending_results
		ON runnerfoundry_jobs(enqueued_at) WHERE state = 'result_pending'`,
	`CREATE INDEX IF NOT EXISTS runnerfoundry_jobs_pending_acquisitions
		ON runnerfoundry_jobs(claimed_at)
		WHERE state = 'attempting' AND acquisition_started_at IS NOT NULL AND acquired_at IS NULL`,
	`CREATE TABLE IF NOT EXISTS runnerfoundry_source_receipts (
		source_name TEXT NOT NULL,
		source_reference TEXT NOT NULL,
		result TEXT NOT NULL CHECK(result IN ('success', 'failure')),
		result_origin TEXT NOT NULL CHECK(result_origin IN ('runner', 'service')),
		delivery TEXT NOT NULL CHECK(delivery IN ('reported', 'runner_managed', 'unsupported')),
		PRIMARY KEY(source_name, source_reference)
	)`,
	`CREATE TABLE IF NOT EXISTS runnerfoundry_members (
		member_id TEXT NOT NULL PRIMARY KEY CHECK(length(member_id) = 36
			AND substr(member_id, 9, 1) = '-' AND substr(member_id, 14, 1) = '-'
			AND substr(member_id, 19, 1) = '-' AND substr(member_id, 24, 1) = '-'
			AND length(replace(member_id, '-', '')) = 32
			AND replace(member_id, '-', '') NOT GLOB '*[^0-9a-f]*'),
		heartbeat_at INTEGER NOT NULL
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

const sqliteSchemaVersion = 101
const sqliteSchemaVersionSQL = "PRAGMA user_version = 101"

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
	open = false
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit SQLite job schema: %w", err)
	}
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
