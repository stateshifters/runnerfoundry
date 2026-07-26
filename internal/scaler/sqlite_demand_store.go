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

const currentStateSchemaVersion = 3

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
	if version == 0 {
		if err := store.createSchema(ctx); err != nil {
			return err
		}
		version = 1
	}
	if version == 1 {
		if err := store.removeLegacyZeroIdentityDemand(ctx); err != nil {
			return err
		}
		version = 2
	}
	if version == 2 {
		return store.addLibvirtRunnerLifecycle(ctx)
	}
	return nil
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
