package scaler

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/flock"
	// Register the pure-Go SQLite database driver used by sql.Open below.
	_ "modernc.org/sqlite"
)

const currentStateSchemaVersion = 7

//go:embed migrations/007_normalize_assignment_source.sql
var sqliteAssignmentSourceMigrationSQL string

type SQLiteDemandStore struct {
	database *sql.DB
	lock     stateDatabaseLock
}

type stateDatabaseLock interface {
	TryLock() (bool, error)
	Unlock() error
}

type sqliteStoreDependencies struct {
	newLock      func(string) stateDatabaseLock
	openDatabase func(string) (*sql.DB, error)
}

var productionSQLiteStoreDependencies = sqliteStoreDependencies{
	newLock: func(path string) stateDatabaseLock { return flock.New(path) },
	openDatabase: func(path string) (*sql.DB, error) {
		return sql.Open("sqlite", path)
	},
}

// OpenSQLiteDemandStore opens and exclusively owns the durable demand queue.
// The caller must close the store to release both the database and its adjacent
// process lock. A second daemon using the same path is rejected.
func OpenSQLiteDemandStore(path string) (*SQLiteDemandStore, error) {
	return createSQLiteDemandStore(path, productionSQLiteStoreDependencies)
}

func createSQLiteDemandStore(path string, dependencies sqliteStoreDependencies) (*SQLiteDemandStore, error) {
	stateLock := dependencies.newLock(path + ".lock")
	locked, err := stateLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("lock state database: %w", err)
	}
	if !locked {
		return nil, errors.New("state database is already in use")
	}
	database, err := dependencies.openDatabase(path)
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
	return store.migrateTo(ctx, currentStateSchemaVersion)
}

func (store *SQLiteDemandStore) migrateTo(ctx context.Context, targetVersion int) error {
	version, err := store.schemaVersion(ctx)
	if err != nil {
		return err
	}
	if version > targetVersion {
		return fmt.Errorf("state schema version %d is newer than supported version %d", version, targetVersion)
	}
	migrations := []func(context.Context) error{
		store.createSchema,
		store.removeLegacyZeroIdentityDemand,
		store.addLibvirtRunnerLifecycle,
		store.addRunnerLifecycle,
		store.addRepositoryState,
		store.addCapacityObligations,
		store.normalizeAssignmentSource,
	}
	for ; version < targetVersion; version++ {
		if err = migrations[version](ctx); err != nil {
			return err
		}
	}
	return nil
}

func (store *SQLiteDemandStore) normalizeAssignmentSource(ctx context.Context) error {
	return store.runSQLiteTransaction(ctx, "assignment-source migration", func(transaction *sql.Tx) error {
		counts, err := loadSQLiteAssignmentPreflightCounts(ctx, transaction)
		if err != nil {
			return err
		}
		if counts.invalid() {
			return fmt.Errorf("refuse assignment-source normalization: missing_demand=%d scope=%d identity=%d provider=%d phase_timestamp=%d source=%d",
				counts.missingDemand, counts.scope, counts.identity, counts.provider, counts.phaseTimestamp, counts.source)
		}
		if _, err = transaction.ExecContext(ctx, sqliteAssignmentSourceMigrationSQL); err != nil {
			return fmt.Errorf("normalize assignment source: %w", err)
		}
		return nil
	})
}

type sqliteAssignmentPreflightCounts struct {
	missingDemand  int
	scope          int
	identity       int
	provider       int
	phaseTimestamp int
	source         int
}

func (counts sqliteAssignmentPreflightCounts) invalid() bool {
	return counts.missingDemand+counts.scope+counts.identity+counts.provider+counts.phaseTimestamp+counts.source > 0
}

func loadSQLiteAssignmentPreflightCounts(
	ctx context.Context,
	transaction *sql.Tx,
) (sqliteAssignmentPreflightCounts, error) {
	var counts sqliteAssignmentPreflightCounts
	err := transaction.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN lifecycle.assignment_id > 0 AND demand.demand_id IS NULL THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN demand.demand_id IS NOT NULL AND
			(lifecycle.scale_set <> demand.scale_set OR lifecycle.repository <> demand.repository) THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN demand.demand_id IS NOT NULL AND
			((demand.source = 'job_available' AND lifecycle.request_id <> demand.request_id)
			 OR (demand.source = 'assigned_count' AND
				NOT (lifecycle.request_id = 0 OR
					(lifecycle.request_id > 0 AND lifecycle.phase IN ('busy', 'reap_pending')))))
			THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN lifecycle.provider NOT IN ('docker', 'libvirt') THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN lifecycle.created_at <= 0 OR lifecycle.updated_at < lifecycle.created_at
			OR lifecycle.github_runner_id < 0
			OR (lifecycle.phase = 'provisioning' AND lifecycle.waiting_since <> 0)
			OR (lifecycle.phase IN ('waiting_for_job', 'busy') AND
				(lifecycle.waiting_since < lifecycle.created_at OR lifecycle.waiting_since > lifecycle.updated_at))
			OR (lifecycle.phase = 'reap_pending' AND lifecycle.waiting_since <> 0 AND
				(lifecycle.waiting_since < lifecycle.created_at OR lifecycle.waiting_since > lifecycle.updated_at))
			OR (lifecycle.phase = 'reap_pending') <> (lifecycle.reap_reason <> '')
			THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN demand.demand_id IS NOT NULL AND
			((demand.source = 'job_available' AND lifecycle.request_id <= 0)
			 OR (demand.source = 'assigned_count' AND lifecycle.request_id < 0)) THEN 1 ELSE 0 END), 0)
		FROM runner_lifecycle lifecycle
		LEFT JOIN demand ON demand.demand_id = lifecycle.assignment_id`).Scan(
		&counts.missingDemand, &counts.scope, &counts.identity, &counts.provider,
		&counts.phaseTimestamp, &counts.source,
	)
	if err != nil {
		return sqliteAssignmentPreflightCounts{}, fmt.Errorf("inspect assignment-source aggregates: %w", err)
	}
	return counts, nil
}

var capacityObligationMigrationSteps = []sqliteMigrationStep{
	{operation: "retain legacy demand during capacity-obligation migration", statement: `ALTER TABLE demand RENAME TO demand_v5`},
	{operation: "create assigned capacity observations", statement: `CREATE TABLE assigned_capacity_observations (
		assigned_observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
		scale_set TEXT NOT NULL CHECK(trim(scale_set) <> ''),
		repository TEXT NOT NULL CHECK(trim(repository) <> ''),
		controller_epoch INTEGER NOT NULL CHECK(controller_epoch > 0),
		assigned_jobs INTEGER NOT NULL CHECK(assigned_jobs >= 0),
		observed_at INTEGER NOT NULL,
		UNIQUE(scale_set, repository, controller_epoch)
	)`},
	{operation: "create unified capacity obligations", statement: `CREATE TABLE demand (
		demand_id INTEGER PRIMARY KEY AUTOINCREMENT,
		observation_order INTEGER NOT NULL UNIQUE,
		scale_set TEXT NOT NULL,
		repository TEXT NOT NULL,
		request_id INTEGER,
		queue_time INTEGER NOT NULL,
		acquired INTEGER NOT NULL CHECK(acquired IN (0, 1)),
		source TEXT NOT NULL CHECK(source IN ('job_available', 'assigned_count')),
		assigned_observation_id INTEGER REFERENCES assigned_capacity_observations(assigned_observation_id) ON DELETE CASCADE,
		controller_epoch INTEGER NOT NULL DEFAULT 0 CHECK(controller_epoch >= 0),
		attempt INTEGER NOT NULL DEFAULT 0 CHECK(attempt >= 0),
		next_attempt_at INTEGER NOT NULL,
		UNIQUE(scale_set, repository, request_id, source),
		CHECK((source = 'job_available' AND request_id > 0 AND assigned_observation_id IS NULL AND controller_epoch = 0)
		   OR (source = 'assigned_count' AND request_id IS NULL AND assigned_observation_id IS NOT NULL AND controller_epoch > 0))
	)`},
	{operation: "preserve exact FIFO capacity obligations", statement: `INSERT INTO demand(
		demand_id, observation_order, scale_set, repository, request_id, queue_time, acquired,
		source, assigned_observation_id, controller_epoch, attempt, next_attempt_at)
		SELECT observation_order, observation_order, scale_set, repository, request_id, queue_time, acquired,
		       'job_available', NULL, 0, 0, queue_time
		FROM demand_v5 ORDER BY observation_order`},
	{operation: "remove migrated legacy demand", statement: `DROP TABLE demand_v5`},
	{operation: "index runnable capacity obligations", statement: `CREATE INDEX demand_runnable_fifo
		ON demand(scale_set, next_attempt_at, queue_time, observation_order)`},
	{operation: "index assigned capacity obligations", statement: `CREATE INDEX demand_assigned_observation
		ON demand(assigned_observation_id, source)`},
	{operation: "record capacity-obligation migration", statement: "PRAGMA user_version = 6"},
}

func (store *SQLiteDemandStore) addCapacityObligations(ctx context.Context) error {
	return store.runSQLiteTransaction(ctx, "capacity-obligation migration", func(transaction *sql.Tx) error {
		for _, step := range capacityObligationMigrationSteps {
			if _, err := transaction.ExecContext(ctx, step.statement); err != nil {
				return fmt.Errorf("%s: %w", step.operation, err)
			}
		}
		return nil
	})
}

var repositoryStateMigrationSteps = []sqliteMigrationStep{
	{operation: "create repository control table", statement: `CREATE TABLE repository_controls (
		app_id INTEGER PRIMARY KEY CHECK(app_id > 0),
		discovery_generation INTEGER NOT NULL DEFAULT 0 CHECK(discovery_generation >= 0),
		last_successful_discovery INTEGER NOT NULL DEFAULT 0,
		controller_epoch INTEGER NOT NULL DEFAULT 0 CHECK(controller_epoch >= 0),
		initialized INTEGER NOT NULL DEFAULT 0 CHECK(initialized IN (0, 1))
	)`},
	{operation: "create installation state table", statement: `CREATE TABLE repository_installations (
		app_id INTEGER NOT NULL REFERENCES repository_controls(app_id) ON DELETE CASCADE,
		installation_id INTEGER NOT NULL CHECK(installation_id > 0),
		last_seen_generation INTEGER NOT NULL CHECK(last_seen_generation > 0),
		PRIMARY KEY(app_id, installation_id)
	)`},
	{operation: "create repository state table", statement: `CREATE TABLE repositories (
		app_id INTEGER NOT NULL,
		repository_url TEXT NOT NULL CHECK(trim(repository_url) <> ''),
		installation_id INTEGER NOT NULL,
		last_seen_generation INTEGER NOT NULL CHECK(last_seen_generation > 0),
		PRIMARY KEY(app_id, repository_url),
		FOREIGN KEY(app_id, installation_id) REFERENCES repository_installations(app_id, installation_id) ON DELETE CASCADE
	)`},
	{operation: "index repositories by installation generation", statement: `CREATE INDEX repositories_by_installation_generation
		ON repositories(app_id, installation_id, last_seen_generation)`},
	{operation: "create registration state table", statement: `CREATE TABLE repository_registrations (
		app_id INTEGER NOT NULL,
		repository_url TEXT NOT NULL,
		scale_set_name TEXT NOT NULL CHECK(trim(scale_set_name) <> ''),
		runner_group TEXT NOT NULL CHECK(trim(runner_group) <> ''),
		github_scale_set_id INTEGER NOT NULL CHECK(github_scale_set_id > 0),
		PRIMARY KEY(app_id, repository_url, scale_set_name),
		FOREIGN KEY(app_id, repository_url) REFERENCES repositories(app_id, repository_url) ON DELETE CASCADE
	)`},
	{operation: "record repository state migration", statement: "PRAGMA user_version = 5"},
}

func (store *SQLiteDemandStore) addRepositoryState(ctx context.Context) error {
	return store.runSQLiteTransaction(ctx, "repository state migration", func(transaction *sql.Tx) error {
		for _, step := range repositoryStateMigrationSteps {
			if _, err := transaction.ExecContext(ctx, step.statement); err != nil {
				return fmt.Errorf("%s: %w", step.operation, err)
			}
		}
		return nil
	})
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
	return store.runSQLiteTransaction(ctx, "generic runner lifecycle migration", func(transaction *sql.Tx) error {
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
	return store.runSQLiteTransaction(ctx, "state migration", func(transaction *sql.Tx) error {
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
	return store.runSQLiteTransaction(ctx, "legacy demand migration", func(transaction *sql.Tx) error {
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
	return store.runSQLiteTransaction(ctx, "libvirt runner lifecycle migration", func(transaction *sql.Tx) error {
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

func (store *SQLiteDemandStore) runSQLiteTransaction(
	ctx context.Context,
	description string,
	apply func(*sql.Tx) error,
) (resultErr error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin %s: %w", description, err)
	}
	transactionOpen := true
	defer finishSQLiteTransaction(transaction, &transactionOpen, description, &resultErr)
	if err := apply(transaction); err != nil {
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
		_, err := transaction.ExecContext(ctx, `INSERT INTO demand(
			observation_order, scale_set, repository, request_id, queue_time, acquired,
			source, controller_epoch, attempt, next_attempt_at)
			VALUES((SELECT COALESCE(MAX(observation_order), 0) + 1 FROM demand), ?, ?, ?, ?, ?,
			       'job_available', 0, 0, ?)
			ON CONFLICT(scale_set, repository, request_id, source)
			DO UPDATE SET acquired = MAX(acquired, excluded.acquired)`,
			scaleSet, demand.Repository, demand.RequestID, demand.QueueTime.UnixNano(), demand.Acquired, demand.QueueTime.UnixNano())
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
	return store.removeCapacityAuthority(ctx, "repository", scaleSet, repository)
}

func (store *SQLiteDemandStore) RemoveScaleSet(ctx context.Context, scaleSet string) error {
	return store.removeCapacityAuthority(ctx, "scale set", scaleSet, "")
}

func (store *SQLiteDemandStore) removeCapacityAuthority(ctx context.Context, description, scaleSet, repository string) (resultErr error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin remove %s demand: %w", description, err)
	}
	transactionOpen := true
	defer finishSQLiteTransaction(transaction, &transactionOpen, "remove "+description+" demand", &resultErr)
	observationStatement := "DELETE FROM assigned_capacity_observations WHERE scale_set = ?"
	demandStatement := "DELETE FROM demand WHERE scale_set = ?"
	arguments := []any{scaleSet}
	if repository != "" {
		observationStatement += " AND repository = ?"
		demandStatement += " AND repository = ?"
		arguments = append(arguments, repository)
	}
	if _, err = transaction.ExecContext(ctx, observationStatement, arguments...); err != nil {
		return fmt.Errorf("remove %s assigned capacity observations: %w", description, err)
	}
	if _, err = transaction.ExecContext(ctx, demandStatement, arguments...); err != nil {
		return fmt.Errorf("remove %s demand: %w", description, err)
	}
	transactionOpen = false
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("commit remove %s demand: %w", description, err)
	}
	return nil
}

func (store *SQLiteDemandStore) Ordered(ctx context.Context, scaleSet string) (result []QueuedDemand, resultErr error) {
	return store.loadCapacityObligations(ctx, scaleSet, time.Time{})
}

// OrderedReady returns runnable obligations in the shared FIFO order. A zero
// cutoff returns every obligation and is used by startup recovery inspection.
func (store *SQLiteDemandStore) OrderedReady(ctx context.Context, scaleSet string, cutoff time.Time) (result []QueuedDemand, resultErr error) {
	return store.loadCapacityObligations(ctx, scaleSet, cutoff)
}

func (store *SQLiteDemandStore) loadCapacityObligations(
	ctx context.Context,
	scaleSet string,
	cutoff time.Time,
) (result []QueuedDemand, resultErr error) {
	query, arguments := runnableCapacityObligationQuery(scaleSet, cutoff)
	rows, err := store.database.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query queued demand: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close queued demand rows: %w", closeErr))
		}
	}()
	return scanCapacityObligations(rows)
}

func runnableCapacityObligationQuery(scaleSet string, cutoff time.Time) (string, []any) {
	query := `SELECT demand_id, repository, COALESCE(request_id, 0), queue_time, acquired,
		observation_order, source, COALESCE(assigned_observation_id, 0), controller_epoch,
		attempt, next_attempt_at
		FROM demand WHERE scale_set = ?
		AND (source = 'job_available' OR NOT EXISTS (SELECT 1 FROM runner_lifecycle lifecycle
			WHERE lifecycle.assignment_id = demand.demand_id))
		ORDER BY queue_time, observation_order`
	arguments := []any{scaleSet}
	if !cutoff.IsZero() {
		query = `SELECT demand_id, repository, COALESCE(request_id, 0), queue_time, acquired,
			observation_order, source, COALESCE(assigned_observation_id, 0), controller_epoch,
			attempt, next_attempt_at
			FROM demand WHERE scale_set = ? AND next_attempt_at <= ?
			AND (source = 'job_available' OR NOT EXISTS (SELECT 1 FROM runner_lifecycle lifecycle
				WHERE lifecycle.assignment_id = demand.demand_id))
			ORDER BY queue_time, observation_order`
		arguments = append(arguments, cutoff.UnixNano())
	}
	return query, arguments
}

func scanCapacityObligations(rows *sql.Rows) ([]QueuedDemand, error) {
	var result []QueuedDemand
	for rows.Next() {
		var demand QueuedDemand
		var queueNanoseconds, nextAttemptNanoseconds int64
		if err := rows.Scan(&demand.ObligationID, &demand.Repository, &demand.RequestID, &queueNanoseconds,
			&demand.Acquired, &demand.observationOrder, &demand.Source, &demand.AssignedObservationID,
			&demand.ControllerEpoch, &demand.Attempt, &nextAttemptNanoseconds); err != nil {
			return nil, fmt.Errorf("scan queued demand: %w", err)
		}
		demand.QueueTime = time.Unix(0, queueNanoseconds).UTC()
		demand.NextAttemptAt = time.Unix(0, nextAttemptNanoseconds).UTC()
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
