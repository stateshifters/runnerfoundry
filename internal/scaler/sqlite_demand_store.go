package scaler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/flock"
	// Register the pure-Go SQLite database driver used by sql.Open below.
	_ "modernc.org/sqlite"
)

const currentStateSchemaVersion = 4

type SQLiteDemandStore struct {
	database *sql.DB
	lock     *flock.Flock
}

// OpenSQLiteDemandStore opens and exclusively owns the durable demand queue.
// The caller must close the store to release both the database and its adjacent
// process lock. A second daemon using the same path is rejected.
func OpenSQLiteDemandStore(path string) (*SQLiteDemandStore, error) {
	return createSQLiteDemandStore(path, func(path string) (*sql.DB, error) { return sql.Open("sqlite", path) })
}

func createSQLiteDemandStore(path string, openDatabase func(path string) (*sql.DB, error)) (*SQLiteDemandStore, error) {
	stateLock := flock.New(path + ".lock")
	locked, err := stateLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("lock state database: %w", err)
	}
	if !locked {
		return nil, errors.New("state database is already in use")
	}
	database, err := openDatabase(path)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open state database: %w", err), stateLock.Unlock())
	}
	database.SetMaxOpenConns(1)
	store := &SQLiteDemandStore{database: database, lock: stateLock}
	if err := store.migrate(context.Background()); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return store, nil
}

func (store *SQLiteDemandStore) migrate(ctx context.Context) error {
	version, err := store.schemaVersion(ctx)
	if err != nil {
		return err
	}
	if version > currentStateSchemaVersion {
		return fmt.Errorf("state schema version %d is newer than supported version %d", version, currentStateSchemaVersion)
	}
	migrations := []func(context.Context) error{
		store.createSchema,
		store.removeLegacyZeroIdentityDemand,
		store.addLibvirtRunnerLifecycle,
		store.addRunnerLifecycle,
	}
	for ; version < currentStateSchemaVersion; version++ {
		if err = migrations[version](ctx); err != nil {
			return err
		}
	}
	return nil
}

type sqliteMigrationStep struct {
	statement string
	operation string
}

var runnerLifecycleMigrationSteps = []sqliteMigrationStep{
	{operation: "create generic runner lifecycle table", statement: `CREATE TABLE runner_lifecycle (
			node_id TEXT NOT NULL DEFAULT '',
			scale_set TEXT NOT NULL,
			repository TEXT NOT NULL,
			provider TEXT NOT NULL CHECK(provider IN ('docker', 'libvirt')),
			runner_name TEXT NOT NULL,
			request_id INTEGER NOT NULL,
			assignment_id INTEGER NOT NULL DEFAULT 0,
			installation_id INTEGER NOT NULL DEFAULT 0,
			github_scale_set_id INTEGER NOT NULL DEFAULT 0,
			github_runner_id INTEGER NOT NULL DEFAULT 0,
			phase TEXT NOT NULL CHECK(phase IN ('provisioning', 'waiting_for_job', 'busy', 'reap_pending')),
			created_at INTEGER NOT NULL,
			waiting_since INTEGER NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL,
			reap_reason TEXT NOT NULL DEFAULT '',
			PRIMARY KEY(scale_set, repository, runner_name)
		)`},
	{operation: "index pending runner lifecycle", statement: `CREATE INDEX runner_lifecycle_pending_reaps
			ON runner_lifecycle(scale_set, repository, phase)`},
	{operation: "backfill generic runner lifecycle", statement: `INSERT INTO runner_lifecycle(
			scale_set, repository, provider, runner_name, request_id, phase,
			created_at, waiting_since, updated_at, reap_reason)
			SELECT scale_set, repository, 'libvirt', runner_name, request_id,
				CASE WHEN reap_requested = 1 THEN 'reap_pending' ELSE 'waiting_for_job' END,
				unixepoch('now') * 1000000000,
				CASE WHEN reap_requested = 1 THEN 0 ELSE unixepoch('now') * 1000000000 END,
				unixepoch('now') * 1000000000,
				CASE WHEN reap_requested = 1 THEN 'legacy_reap_requested' ELSE '' END
			FROM libvirt_runner_lifecycle`},
	{operation: "mirror legacy libvirt lifecycle inserts", statement: `CREATE TRIGGER mirror_libvirt_runner_lifecycle_insert
			AFTER INSERT ON libvirt_runner_lifecycle BEGIN
				INSERT INTO runner_lifecycle(
					scale_set, repository, provider, runner_name, request_id, phase,
					created_at, waiting_since, updated_at, reap_reason)
				VALUES(NEW.scale_set, NEW.repository, 'libvirt', NEW.runner_name, NEW.request_id,
					CASE WHEN NEW.reap_requested = 1 THEN 'reap_pending' ELSE 'waiting_for_job' END,
					unixepoch('now') * 1000000000,
					CASE WHEN NEW.reap_requested = 1 THEN 0 ELSE unixepoch('now') * 1000000000 END,
					unixepoch('now') * 1000000000,
					CASE WHEN NEW.reap_requested = 1 THEN 'legacy_reap_requested' ELSE '' END)
				ON CONFLICT(scale_set, repository, runner_name) DO UPDATE SET
					request_id = excluded.request_id,
					phase = CASE WHEN NEW.reap_requested = 1 THEN 'reap_pending' ELSE runner_lifecycle.phase END,
					reap_reason = CASE
						WHEN NEW.reap_requested = 1 AND runner_lifecycle.reap_reason = '' THEN 'legacy_reap_requested'
						ELSE runner_lifecycle.reap_reason END;
			END`},
	{operation: "mirror legacy libvirt lifecycle updates", statement: `CREATE TRIGGER mirror_libvirt_runner_lifecycle_update
			AFTER UPDATE ON libvirt_runner_lifecycle BEGIN
				UPDATE runner_lifecycle SET request_id = NEW.request_id,
					phase = CASE WHEN NEW.reap_requested = 1 THEN 'reap_pending' ELSE phase END,
					reap_reason = CASE
						WHEN NEW.reap_requested = 1 AND reap_reason = '' THEN 'legacy_reap_requested'
						ELSE reap_reason END
				WHERE scale_set = NEW.scale_set AND repository = NEW.repository AND runner_name = NEW.runner_name;
			END`},
	{operation: "mirror legacy libvirt lifecycle deletes", statement: `CREATE TRIGGER mirror_libvirt_runner_lifecycle_delete
			AFTER DELETE ON libvirt_runner_lifecycle BEGIN
				DELETE FROM runner_lifecycle
				WHERE scale_set = OLD.scale_set AND repository = OLD.repository AND runner_name = OLD.runner_name;
			END`},
	{operation: "record generic runner lifecycle migration", statement: "PRAGMA user_version = 4"},
}

func (store *SQLiteDemandStore) addRunnerLifecycle(ctx context.Context) error {
	return store.runMigration(ctx, "generic runner lifecycle migration", func(transaction *sql.Tx) error {
		for _, step := range runnerLifecycleMigrationSteps {
			if _, err := transaction.ExecContext(ctx, step.statement); err != nil {
				return fmt.Errorf("%s: %w", step.operation, err)
			}
		}
		return nil
	})
}

func (store *SQLiteDemandStore) schemaVersion(ctx context.Context) (int, error) {
	if _, err := store.database.ExecContext(ctx, "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;"); err != nil {
		return 0, fmt.Errorf("configure state database: %w", err)
	}
	var version int
	if err := store.database.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return 0, fmt.Errorf("read state schema version: %w", err)
	}
	return version, nil
}

func (store *SQLiteDemandStore) createSchema(ctx context.Context) error {
	return store.runMigration(ctx, "state migration", func(transaction *sql.Tx) error {
		if _, err := transaction.ExecContext(ctx, `CREATE TABLE demand (
		observation_order INTEGER PRIMARY KEY AUTOINCREMENT,
		scale_set TEXT NOT NULL,
		repository TEXT NOT NULL,
		request_id INTEGER NOT NULL,
		queue_time INTEGER NOT NULL,
		acquired INTEGER NOT NULL,
		UNIQUE(scale_set, repository, request_id)
	)`); err != nil {
			return fmt.Errorf("create demand table: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, "PRAGMA user_version = 1"); err != nil {
			return fmt.Errorf("record state schema version: %w", err)
		}
		return nil
	})
}

func (store *SQLiteDemandStore) removeLegacyZeroIdentityDemand(ctx context.Context) error {
	return store.runMigration(ctx, "legacy demand migration", func(transaction *sql.Tx) error {
		if _, err := transaction.ExecContext(ctx, "DELETE FROM demand WHERE request_id <= 0"); err != nil {
			return fmt.Errorf("remove legacy zero-identity demand: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, "PRAGMA user_version = 2"); err != nil {
			return fmt.Errorf("record legacy demand migration: %w", err)
		}
		return nil
	})
}

func (store *SQLiteDemandStore) addLibvirtRunnerLifecycle(ctx context.Context) error {
	return store.runMigration(ctx, "libvirt runner lifecycle migration", func(transaction *sql.Tx) error {
		if _, err := transaction.ExecContext(ctx, `CREATE TABLE libvirt_runner_lifecycle (
			scale_set TEXT NOT NULL,
			repository TEXT NOT NULL,
			request_id INTEGER NOT NULL,
			runner_name TEXT NOT NULL,
			reap_requested INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(scale_set, repository, runner_name)
		)`); err != nil {
			return fmt.Errorf("create libvirt runner lifecycle table: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, "PRAGMA user_version = 3"); err != nil {
			return fmt.Errorf("record libvirt runner lifecycle migration: %w", err)
		}
		return nil
	})
}

func (store *SQLiteDemandStore) runMigration(ctx context.Context, description string, migrate func(*sql.Tx) error) (resultErr error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin %s: %w", description, err)
	}
	transactionOpen := true
	defer finishSQLiteTransaction(transaction, &transactionOpen, description, &resultErr)
	if err := migrate(transaction); err != nil {
		return err
	}
	transactionOpen = false
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", description, err)
	}
	return nil
}

func (store *SQLiteDemandStore) Apply(ctx context.Context, scaleSet string, observed []JobDemand, removed []DemandRemoval) (resultErr error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin demand update: %w", err)
	}
	transactionOpen := true
	defer finishSQLiteTransaction(transaction, &transactionOpen, "demand update", &resultErr)
	if err = applyDemandChanges(ctx, transaction, scaleSet, observed, removed); err != nil {
		return err
	}
	transactionOpen = false
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit demand update: %w", err)
	}
	return nil
}

func applyDemandChanges(ctx context.Context, transaction *sql.Tx, scaleSet string, observed []JobDemand, removed []DemandRemoval) error {
	for _, demand := range observed {
		_, err := transaction.ExecContext(ctx, `INSERT INTO demand(scale_set, repository, request_id, queue_time, acquired)
			VALUES(?, ?, ?, ?, ?)
			ON CONFLICT(scale_set, repository, request_id) DO UPDATE SET acquired = MAX(acquired, excluded.acquired)`,
			scaleSet, demand.Repository, demand.RequestID, demand.QueueTime.UnixNano(), demand.Acquired)
		if err != nil {
			return fmt.Errorf("record queued demand: %w", err)
		}
	}
	for _, removal := range removed {
		if _, err := transaction.ExecContext(ctx, "DELETE FROM demand WHERE scale_set = ? AND repository = ? AND request_id = ?", scaleSet, removal.Repository, removal.RequestID); err != nil {
			return fmt.Errorf("remove queued demand: %w", err)
		}
	}
	return nil
}

func finishSQLiteTransaction(transaction *sql.Tx, transactionOpen *bool, description string, resultErr *error) {
	if !*transactionOpen {
		return
	}
	*transactionOpen = false
	if rollbackErr := transaction.Rollback(); rollbackErr != nil {
		*resultErr = errors.Join(*resultErr, fmt.Errorf("rollback %s: %w", description, rollbackErr))
	}
}

func (store *SQLiteDemandStore) RemoveRepository(ctx context.Context, scaleSet string, repository string) error {
	_, err := store.database.ExecContext(ctx, "DELETE FROM demand WHERE scale_set = ? AND repository = ?", scaleSet, repository)
	if err != nil {
		return fmt.Errorf("remove repository demand: %w", err)
	}
	return nil
}

func (store *SQLiteDemandStore) RemoveScaleSet(ctx context.Context, scaleSet string) error {
	_, err := store.database.ExecContext(ctx, "DELETE FROM demand WHERE scale_set = ?", scaleSet)
	if err != nil {
		return fmt.Errorf("remove scale set demand: %w", err)
	}
	return nil
}

func (store *SQLiteDemandStore) Ordered(ctx context.Context, scaleSet string) (result []QueuedDemand, resultErr error) {
	rows, err := store.database.QueryContext(ctx, `SELECT repository, request_id, queue_time, acquired, observation_order
		FROM demand WHERE scale_set = ? ORDER BY queue_time, observation_order`, scaleSet)
	if err != nil {
		return nil, fmt.Errorf("query queued demand: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close queued demand rows: %w", closeErr))
		}
	}()
	for rows.Next() {
		var demand QueuedDemand
		var queueNanoseconds int64
		if err := rows.Scan(&demand.Repository, &demand.RequestID, &queueNanoseconds, &demand.Acquired, &demand.observationOrder); err != nil {
			return nil, fmt.Errorf("scan queued demand: %w", err)
		}
		demand.QueueTime = time.Unix(0, queueNanoseconds).UTC()
		result = append(result, demand)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queued demand: %w", err)
	}
	return result, nil
}

func (store *SQLiteDemandStore) Delete(ctx context.Context, scaleSet string, repository string, requestID int64) error {
	_, err := store.database.ExecContext(ctx, "DELETE FROM demand WHERE scale_set = ? AND repository = ? AND request_id = ?", scaleSet, repository, requestID)
	if err != nil {
		return fmt.Errorf("delete queued demand: %w", err)
	}
	return nil
}

func (store *SQLiteDemandStore) Close() error {
	databaseErr := store.database.Close()
	lockErr := store.lock.Unlock()
	return errors.Join(databaseErr, lockErr)
}
