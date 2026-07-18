package cluster

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const leaderAdvisoryLockKey int64 = 0x6769746875627363

// NodeLifetime is the heartbeat age after which a clustered node no longer
// contributes capacity or prevents an explicit schema rollback.
const NodeLifetime = 30 * time.Second

var (
	ErrNoAssignment      = errors.New("no schedulable demand")
	ErrNoReadyAssignment = errors.New("no local assignment is ready")
	ErrNotLeader         = errors.New("cluster leader lease is held elsewhere")
)

type Contribution struct {
	ScaleSet    string
	RunnerGroup string
	MaxRunners  int
	Healthy     bool
}

type ScaleSetStatus struct {
	Name              string
	RunnerGroup       string
	EffectiveCapacity int
	Compatible        bool
}

type Demand struct {
	ScaleSet   string
	Repository string
	RequestID  int64
	QueueTime  time.Time
}

type Assignment struct {
	ID               int64
	NodeID           string
	ScaleSet         string
	Repository       string
	RequestID        int64
	QueueTime        time.Time
	InstallationID   int64
	GitHubScaleSetID int
}

type Store struct {
	settings        ConnectionSettings
	connector       func(ctx context.Context) (databaseConnection, error)
	directConnector func(ctx context.Context) (databaseConnection, error)
	migrationFiles  fs.FS
}

func NewStore(settings ConnectionSettings) *Store {
	store := &Store{settings: settings}
	store.connector = store.connectPostgreSQL
	store.directConnector = func(ctx context.Context) (databaseConnection, error) {
		connection, err := store.connectDirectPostgreSQL(ctx)
		if err != nil {
			return nil, err
		}
		return postgreSQLConnection{Conn: connection}, nil
	}
	store.migrationFiles = embeddedMigrations
	return store
}

type databaseConnection interface { //nolint:interfacebloat // The adapter mirrors the small pgx connection surface used by the store.
	Begin(ctx context.Context) (databaseTransaction, error)
	Close(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	IsClosed() bool
	Ping(ctx context.Context) error
	Query(ctx context.Context, sql string, arguments ...any) (databaseRows, error)
	QueryRow(ctx context.Context, sql string, arguments ...any) databaseRow
}

type databaseTransaction interface {
	Commit(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, arguments ...any) databaseRow
	Rollback(ctx context.Context) error
}

type databaseRows interface {
	Close()
	Err() error
	Next() bool
	Scan(destinations ...any) error
}

type databaseRow interface {
	Scan(destinations ...any) error
}

type postgreSQLConnection struct {
	*pgx.Conn
}

type postgreSQLTransaction struct {
	pgx.Tx
}

func (connection postgreSQLConnection) Begin(ctx context.Context) (databaseTransaction, error) { //nolint:ireturn // The store uses only the transaction contract.
	transaction, err := connection.Conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin PostgreSQL transaction: %w", err)
	}
	return postgreSQLTransaction{Tx: transaction}, nil
}

//nolint:ireturn // The store reads only the portable row iterator contract.
func (connection postgreSQLConnection) Query(
	ctx context.Context,
	sql string,
	arguments ...any,
) (databaseRows, error) {
	rows, err := connection.Conn.Query(ctx, sql, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query PostgreSQL: %w", err)
	}
	return rows, nil
}

func (connection postgreSQLConnection) QueryRow(ctx context.Context, sql string, arguments ...any) databaseRow { //nolint:ireturn // The store reads only Scan.
	return connection.Conn.QueryRow(ctx, sql, arguments...)
}

func (transaction postgreSQLTransaction) QueryRow(ctx context.Context, sql string, arguments ...any) databaseRow { //nolint:ireturn // The store reads only Scan.
	return transaction.Tx.QueryRow(ctx, sql, arguments...)
}

func (store *Store) connectPostgreSQL(ctx context.Context) (databaseConnection, error) { //nolint:ireturn // Connection behavior is abstracted for deterministic fault tests.
	config, err := store.settings.config()
	if err != nil {
		return nil, err
	}
	connection, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, errors.New("connect to PostgreSQL")
	}
	return postgreSQLConnection{Conn: connection}, nil
}

func (store *Store) connectDirectPostgreSQL(ctx context.Context) (*pgx.Conn, error) {
	config, err := store.settings.directConfig()
	if err != nil {
		return nil, err
	}
	connection, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, errors.New("connect to PostgreSQL")
	}
	return connection, nil
}

func (store *Store) Publish( //nolint:maintidx // Transactional replacement keeps the node snapshot atomic and readable in one place.
	ctx context.Context,
	nodeID string,
	healthy bool,
	contributions []Contribution,
	now time.Time,
) error {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer closeConnection(ctx, connection)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin contribution publication: %w", err)
	}
	defer rollbackTransaction(ctx, transaction)
	if _, err = transaction.Exec(ctx, `
		INSERT INTO scaler_nodes (node_id, heartbeat_at, healthy)
		VALUES ($1, $2, $3)
		ON CONFLICT (node_id) DO UPDATE
		SET heartbeat_at = EXCLUDED.heartbeat_at, healthy = EXCLUDED.healthy`, nodeID, now, healthy); err != nil {
		return fmt.Errorf("publish node state: %w", err)
	}
	if _, err = transaction.Exec(ctx, `DELETE FROM scaler_contributions WHERE node_id = $1`, nodeID); err != nil {
		return fmt.Errorf("replace node contributions: %w", err)
	}
	for _, contribution := range contributions {
		if _, err = transaction.Exec(ctx, `
			INSERT INTO scaler_contributions (node_id, scale_set, runner_group, max_runners, healthy)
			VALUES ($1, $2, $3, $4, $5)`,
			nodeID, contribution.ScaleSet, contribution.RunnerGroup, contribution.MaxRunners, contribution.Healthy,
		); err != nil {
			return fmt.Errorf("publish scale-set contribution: %w", err)
		}
	}
	if _, err = transaction.Exec(ctx, `
		DELETE FROM scaler_demand demand
		WHERE NOT EXISTS (
		    SELECT 1 FROM scaler_contributions contribution
		    WHERE contribution.scale_set = demand.scale_set
		)`); err != nil {
		return fmt.Errorf("remove demand for fully withdrawn scale sets: %w", err)
	}
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit contribution publication: %w", err)
	}
	return nil
}

func (store *Store) ScaleSets(ctx context.Context, now time.Time, liveFor time.Duration) ([]ScaleSetStatus, error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer closeConnection(ctx, connection)
	rows, err := connection.Query(ctx, `
		SELECT contributions.scale_set,
		       MIN(contributions.runner_group),
		       COUNT(DISTINCT contributions.runner_group) = 1,
		       COALESCE(SUM(contributions.max_runners) FILTER (
		           WHERE nodes.healthy AND contributions.healthy AND nodes.heartbeat_at >= $1
		       ), 0)
		FROM scaler_contributions contributions
		JOIN scaler_nodes nodes ON nodes.node_id = contributions.node_id
		GROUP BY contributions.scale_set
		ORDER BY contributions.scale_set`, now.Add(-liveFor))
	if err != nil {
		return nil, fmt.Errorf("load scale-set contributions: %w", err)
	}
	defer rows.Close()
	return scanScaleSetStatuses(rows)
}

func scanScaleSetStatuses(rows databaseRows) ([]ScaleSetStatus, error) {
	statuses := make([]ScaleSetStatus, 0)
	for rows.Next() {
		var status ScaleSetStatus
		if err := rows.Scan(&status.Name, &status.RunnerGroup, &status.Compatible, &status.EffectiveCapacity); err != nil {
			return nil, fmt.Errorf("scan scale-set contribution: %w", err)
		}
		if !status.Compatible {
			status.EffectiveCapacity = 0
		}
		statuses = append(statuses, status)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read scale-set contributions: %w", err)
	}
	return statuses, nil
}

func (store *Store) Enqueue(ctx context.Context, demand Demand) error {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer closeConnection(ctx, connection)
	if _, err = connection.Exec(ctx, `
		INSERT INTO scaler_demand (scale_set, repository, request_id, queue_time)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (scale_set, repository, request_id) DO NOTHING`,
		demand.ScaleSet, demand.Repository, demand.RequestID, demand.QueueTime,
	); err != nil {
		return fmt.Errorf("enqueue scale-set demand: %w", err)
	}
	return nil
}

func (store *Store) AssignOldest(
	ctx context.Context,
	scaleSet string,
	now time.Time,
	liveFor time.Duration,
) (*Assignment, error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer closeConnection(ctx, connection)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin FIFO assignment: %w", err)
	}
	defer rollbackTransaction(ctx, transaction)
	assignment, err := assignOldestInTransaction(ctx, transaction, scaleSet, now, liveFor)
	if err != nil {
		return nil, err
	}
	if err = transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit FIFO assignment: %w", err)
	}
	return assignment, nil
}

func assignOldestInTransaction(
	ctx context.Context,
	transaction databaseTransaction,
	scaleSet string,
	now time.Time,
	liveFor time.Duration,
) (*Assignment, error) {
	if err := releaseUnavailableAssignments(ctx, transaction, now.Add(-liveFor)); err != nil {
		return nil, err
	}
	demandID, assignment, err := loadOldestDemand(ctx, transaction, scaleSet)
	if err != nil {
		return nil, err
	}
	if err = selectContributor(ctx, transaction, scaleSet, now.Add(-liveFor), assignment); err != nil {
		return nil, err
	}
	assignment.ScaleSet = scaleSet
	if err = transaction.QueryRow(ctx, `
		INSERT INTO scaler_assignments (demand_id, node_id, scale_set, state)
		VALUES ($1, $2, $3, 'reserved')
		RETURNING assignment_id`, demandID, assignment.NodeID, scaleSet).Scan(&assignment.ID); err != nil {
		return nil, fmt.Errorf("persist FIFO assignment: %w", err)
	}
	return assignment, nil
}

func releaseUnavailableAssignments(ctx context.Context, transaction databaseTransaction, expiredBefore time.Time) error {
	if _, err := transaction.Exec(ctx, `
		UPDATE scaler_assignments assignment
		SET state = 'complete'
		FROM scaler_nodes node
		WHERE assignment.node_id = node.node_id
		  AND assignment.state <> 'complete'
		  AND node.heartbeat_at < $1`, expiredBefore); err != nil {
		return fmt.Errorf("release assignments from unavailable nodes: %w", err)
	}
	return nil
}

func loadOldestDemand(ctx context.Context, transaction databaseTransaction, scaleSet string) (int64, *Assignment, error) {
	var demandID int64
	var assignment Assignment
	err := transaction.QueryRow(ctx, `
		SELECT demand_id, repository, request_id, queue_time
		FROM scaler_demand
		WHERE scale_set = $1
		  AND NOT EXISTS (SELECT 1 FROM scaler_assignments WHERE scaler_assignments.demand_id = scaler_demand.demand_id AND state <> 'complete')
		ORDER BY queue_time, observation_id
		LIMIT 1
		FOR UPDATE`, scaleSet).Scan(&demandID, &assignment.Repository, &assignment.RequestID, &assignment.QueueTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, ErrNoAssignment
	}
	if err != nil {
		return 0, nil, fmt.Errorf("load oldest scale-set demand: %w", err)
	}
	return demandID, &assignment, nil
}

func selectContributor(
	ctx context.Context,
	transaction databaseTransaction,
	scaleSet string,
	liveAfter time.Time,
	assignment *Assignment,
) error {
	err := transaction.QueryRow(ctx, `
		SELECT contribution.node_id
		FROM scaler_contributions contribution
		JOIN scaler_nodes node ON node.node_id = contribution.node_id
		WHERE contribution.scale_set = $1
		  AND contribution.healthy
		  AND node.healthy
		  AND node.heartbeat_at >= $2
		  AND NOT EXISTS (
		      SELECT 1 FROM scaler_contributions conflicting
		      WHERE conflicting.scale_set = contribution.scale_set
		        AND conflicting.runner_group <> contribution.runner_group
		  )
		  AND contribution.max_runners > (
		      SELECT COUNT(*) FROM scaler_assignments assignment
		      WHERE assignment.node_id = contribution.node_id
		        AND assignment.scale_set = contribution.scale_set
		        AND assignment.state <> 'complete'
		  )
		ORDER BY contribution.node_id
		LIMIT 1
		FOR UPDATE OF contribution`, scaleSet, liveAfter).Scan(&assignment.NodeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoAssignment
	}
	if err != nil {
		return fmt.Errorf("select scale-set contributor: %w", err)
	}
	return nil
}

func (store *Store) Ready(
	ctx context.Context,
	assignmentID int64,
	installationID int64,
	githubScaleSetID int,
) error {
	return store.updateAssignment(ctx, `
		UPDATE scaler_assignments
		SET state = 'ready', installation_id = $2, github_scale_set_id = $3
		WHERE assignment_id = $1 AND state = 'reserved'`, assignmentID, installationID, githubScaleSetID)
}

func (store *Store) Retry(ctx context.Context, assignmentID int64) error {
	return store.updateAssignment(ctx, `UPDATE scaler_assignments SET state = 'ready' WHERE assignment_id = $1`, assignmentID)
}

func (store *Store) Started(ctx context.Context, assignmentID int64) error {
	return store.updateAssignment(ctx, `UPDATE scaler_assignments SET state = 'running' WHERE assignment_id = $1`, assignmentID)
}

func (store *Store) ClaimReady(ctx context.Context, nodeID string) (*Assignment, error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer closeConnection(ctx, connection)
	var assignment Assignment
	err = connection.QueryRow(ctx, `
		UPDATE scaler_assignments assignment
		SET state = 'starting'
		FROM scaler_demand demand
		WHERE assignment.assignment_id = (
		    SELECT candidate.assignment_id
		    FROM scaler_assignments candidate
		    WHERE candidate.node_id = $1 AND candidate.state = 'ready'
		    ORDER BY candidate.assignment_id
		    LIMIT 1
		    FOR UPDATE SKIP LOCKED
		)
		AND demand.demand_id = assignment.demand_id
		RETURNING assignment.assignment_id, assignment.node_id, assignment.scale_set,
		          demand.repository, demand.request_id, demand.queue_time,
		          assignment.installation_id, assignment.github_scale_set_id`, nodeID).Scan(
		&assignment.ID, &assignment.NodeID, &assignment.ScaleSet, &assignment.Repository,
		&assignment.RequestID, &assignment.QueueTime, &assignment.InstallationID, &assignment.GitHubScaleSetID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoReadyAssignment
	}
	if err != nil {
		return nil, fmt.Errorf("claim local runner assignment: %w", err)
	}
	return &assignment, nil
}

func (store *Store) RemoveDemand(ctx context.Context, scaleSet string, repository string, requestID int64) error {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer closeConnection(ctx, connection)
	if _, err = connection.Exec(ctx, `
		WITH removed AS (
		    DELETE FROM scaler_demand
		    WHERE scale_set = $1 AND repository = $2 AND request_id = $3
		    RETURNING demand_id
		)
		UPDATE scaler_assignments SET state = 'complete'
		WHERE demand_id IN (SELECT demand_id FROM removed)`, scaleSet, repository, requestID); err != nil {
		return fmt.Errorf("remove completed scale-set demand: %w", err)
	}
	return nil
}

func (store *Store) Release(ctx context.Context, assignmentID int64) error {
	return store.updateAssignment(ctx, `UPDATE scaler_assignments SET state = 'complete' WHERE assignment_id = $1`, assignmentID)
}

func (store *Store) updateAssignment(ctx context.Context, statement string, arguments ...any) error {
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer closeConnection(ctx, connection)
	if _, err = connection.Exec(ctx, statement, arguments...); err != nil {
		return fmt.Errorf("update runner assignment: %w", err)
	}
	return nil
}

func (store *Store) Complete(ctx context.Context, assignmentID int64) error {
	return store.Release(ctx, assignmentID)
}

type LeaderLease struct {
	connection databaseConnection
}

type Lease interface {
	Alive(ctx context.Context) bool
	Close(ctx context.Context) error
}

func (store *Store) TryLeader(ctx context.Context) (Lease, error) { //nolint:ireturn // Callers depend on lease behavior, not the pgx-backed implementation.
	connection, err := store.directConnector(ctx)
	if err != nil {
		return nil, err
	}
	var acquired bool
	if err = connection.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderAdvisoryLockKey).Scan(&acquired); err != nil {
		closeConnection(ctx, connection)
		return nil, fmt.Errorf("try clustered leader lease: %w", err)
	}
	if !acquired {
		closeConnection(ctx, connection)
		return nil, ErrNotLeader
	}
	return &LeaderLease{connection: connection}, nil
}

// WaitForLeader blocks on PostgreSQL's session advisory lock. Canceling ctx
// closes the waiting connection; a returned lease owns its connection until Close.
func (store *Store) WaitForLeader(ctx context.Context) (*LeaderLease, error) {
	connection, err := store.directConnector(ctx)
	if err != nil {
		return nil, err
	}
	if _, err = connection.Exec(ctx, `SELECT pg_advisory_lock($1)`, leaderAdvisoryLockKey); err != nil {
		closeConnection(ctx, connection)
		if ctx.Err() != nil {
			return nil, fmt.Errorf("wait for clustered leader lease: %w", ctx.Err())
		}
		return nil, fmt.Errorf("wait for clustered leader lease: %w", err)
	}
	return &LeaderLease{connection: connection}, nil
}

func (lease *LeaderLease) Close(ctx context.Context) error {
	if lease == nil || lease.connection == nil {
		return nil
	}
	if err := lease.connection.Close(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("close PostgreSQL leader lease: %w", err)
	}
	return nil
}

func closeConnection(ctx context.Context, connection databaseConnection) {
	_ = connection.Close(context.WithoutCancel(ctx)) //nolint:errcheck,gosec // Deferred connection cleanup is best effort.
}

func rollbackTransaction(ctx context.Context, transaction databaseTransaction) {
	_ = transaction.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck,gosec // Rollback is best effort after commit or failure.
}

func (lease *LeaderLease) Alive(ctx context.Context) bool {
	if lease == nil || lease.connection == nil || lease.connection.IsClosed() {
		return false
	}
	return lease.connection.Ping(ctx) == nil
}
