package cluster

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/github-scaler/internal/githubapp"
)

const leaderAdvisoryLockKey int64 = 0x6769746875627363

// NodeLifetime is the heartbeat age after which a clustered node no longer
// contributes capacity or prevents an explicit schema rollback.
const NodeLifetime = 30 * time.Second

var (
	ErrNoAssignment      = errors.New("no schedulable demand")
	ErrNoReadyAssignment = errors.New("no local assignment is ready")
	ErrNotLeader         = errors.New("cluster leader lease is held elsewhere")
	// ErrInvalidDemand reports invalid demand input. Callers should use
	// errors.Is and correct or discard the input rather than retry it unchanged.
	ErrInvalidDemand = errors.New("invalid scale-set demand")
	// ErrInvalidPublication reports invalid node or contribution input. Callers
	// should use errors.Is and correct the publication rather than retry it.
	ErrInvalidPublication = errors.New("invalid node contribution publication")
	// ErrAssignmentConflict reports that an assignment no longer has the state
	// or owner required for a transition. Callers should use errors.Is and
	// reconcile current state; errors.As exposes AssignmentTransitionError details.
	ErrAssignmentConflict = errors.New("assignment state changed")
)

// AssignmentState identifies a persisted stage in an assignment's lifecycle.
type AssignmentState string

// AssignmentID identifies a persisted runner assignment.
type AssignmentID int64

// NodeID identifies a host participating in clustered coordination.
type NodeID string

const (
	// AssignmentReserved has selected a demand and node but has not recorded a
	// GitHub acquisition attempt.
	AssignmentReserved AssignmentState = "reserved"
	// AssignmentAcquiring has persisted the GitHub identifiers for an acquisition
	// that has not yet been confirmed.
	AssignmentAcquiring AssignmentState = "acquiring"
	// AssignmentReady has a confirmed GitHub acquisition and can be claimed by
	// its owning node for provider startup.
	AssignmentReady AssignmentState = "ready"
	// AssignmentStarting has been claimed by its owning node and requires provider
	// startup or recovery.
	AssignmentStarting AssignmentState = "starting"
	// AssignmentRunning has been observed running by its owning node.
	AssignmentRunning AssignmentState = "running"
	// AssignmentComplete is terminal and no longer consumes contributor capacity.
	AssignmentComplete AssignmentState = "complete"
)

// AssignmentTransitionError reports that an assignment no longer has the
// state and owner required by a requested transition.
type AssignmentTransitionError struct {
	AssignmentID AssignmentID
	Expected     AssignmentState
}

func (transitionErr *AssignmentTransitionError) Error() string {
	return fmt.Sprintf("assignment %d is not %s: %v", transitionErr.AssignmentID, transitionErr.Expected, ErrAssignmentConflict)
}

func (transitionErr *AssignmentTransitionError) Unwrap() error { return ErrAssignmentConflict }

// Contribution declares one node's capacity and health for a scale set.
type Contribution struct {
	// ScaleSet identifies the shared scale set receiving this contribution.
	ScaleSet string
	// RunnerGroup is the GitHub-facing setting that must agree across eligible
	// contributors to the same scale set.
	RunnerGroup string
	// MaxRunners limits concurrent active assignments on this node and scale set.
	// Zero publishes a declaration without contributing capacity.
	MaxRunners int
	// Healthy controls whether this contribution is eligible for capacity and
	// assignment while retaining its declaration.
	Healthy bool
}

// NodeHealth controls whether any of a node's contributions are eligible. Its
// zero value is NodeUnhealthy; health changes do not withdraw declarations.
type NodeHealth bool

const (
	// NodeUnhealthy excludes all contributions from capacity and assignment.
	NodeUnhealthy NodeHealth = false
	// NodeHealthy allows individually healthy contributions from a live node to
	// participate in capacity and assignment.
	NodeHealthy NodeHealth = true
)

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
	ID               AssignmentID
	NodeID           NodeID
	ScaleSet         string
	Repository       string
	RequestID        int64
	QueueTime        time.Time
	InstallationID   githubapp.InstallationID
	GitHubScaleSetID int
	State            AssignmentState
}

type Store struct {
	settings        ConnectionSettings
	connector       func(ctx context.Context) (*databaseConnection, error)
	directConnector func(ctx context.Context) (leaderConnection, error)
	migrationFiles  fs.FS
}

func NewStore(settings ConnectionSettings) *Store {
	store := &Store{settings: settings}
	store.connector = store.connectPostgreSQL
	store.directConnector = func(ctx context.Context) (leaderConnection, error) {
		connection, err := store.connectDirectPostgreSQL(ctx)
		if err != nil {
			return nil, err
		}
		return &postgreSQLLeaderConnection{Conn: connection}, nil
	}
	store.migrationFiles = embeddedMigrations
	return store
}

type databaseConnection struct {
	begin    func(context.Context) (*databaseTransaction, error)
	close    func(context.Context) error
	exec     func(context.Context, string, ...any) (pgconn.CommandTag, error)
	query    func(context.Context, string, ...any) (*databaseRows, error)
	queryRow func(context.Context, string, ...any) databaseRow
}

func (connection *databaseConnection) Begin(ctx context.Context) (*databaseTransaction, error) {
	return connection.begin(ctx)
}

func (connection *databaseConnection) Close(ctx context.Context) error {
	return connection.close(ctx)
}

func (connection *databaseConnection) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return connection.exec(ctx, sql, arguments...)
}

func (connection *databaseConnection) Query(ctx context.Context, sql string, arguments ...any) (*databaseRows, error) {
	return connection.query(ctx, sql, arguments...)
}

func (connection *databaseConnection) QueryRow(ctx context.Context, sql string, arguments ...any) databaseRow {
	return connection.queryRow(ctx, sql, arguments...)
}

type leaderConnection interface {
	Close(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	IsClosed() bool
	Ping(ctx context.Context) error
	QueryRow(ctx context.Context, sql string, arguments ...any) databaseRow
}

type databaseTransaction struct {
	commit   func(context.Context) error
	exec     func(context.Context, string, ...any) (pgconn.CommandTag, error)
	queryRow func(context.Context, string, ...any) databaseRow
	rollback func(context.Context) error
}

func (transaction *databaseTransaction) Commit(ctx context.Context) error {
	return transaction.commit(ctx)
}

func (transaction *databaseTransaction) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return transaction.exec(ctx, sql, arguments...)
}

func (transaction *databaseTransaction) QueryRow(ctx context.Context, sql string, arguments ...any) databaseRow {
	return transaction.queryRow(ctx, sql, arguments...)
}

func (transaction *databaseTransaction) Rollback(ctx context.Context) error {
	return transaction.rollback(ctx)
}

type rowIterator interface {
	Close()
	Err() error
	Next() bool
	Scan(destinations ...any) error
}

type databaseRows struct {
	iterator rowIterator
}

func (rows *databaseRows) Close() { rows.iterator.Close() }
func (rows *databaseRows) Err() error {
	if err := rows.iterator.Err(); err != nil {
		return fmt.Errorf("read PostgreSQL rows: %w", err)
	}
	return nil
}
func (rows *databaseRows) Next() bool { return rows.iterator.Next() }
func (rows *databaseRows) Scan(destinations ...any) error {
	if err := rows.iterator.Scan(destinations...); err != nil {
		return fmt.Errorf("scan PostgreSQL rows: %w", err)
	}
	return nil
}

type rowScanner interface {
	Scan(destinations ...any) error
}

type databaseRow struct {
	scanner rowScanner
}

type postgreSQLLeaderConnection struct {
	*pgx.Conn
}

func (connection *postgreSQLLeaderConnection) QueryRow(ctx context.Context, sql string, arguments ...any) databaseRow {
	return databaseRow{scanner: connection.Conn.QueryRow(ctx, sql, arguments...)}
}

func (row databaseRow) Scan(destinations ...any) error {
	if err := row.scanner.Scan(destinations...); err != nil {
		return fmt.Errorf("scan PostgreSQL row: %w", err)
	}
	return nil
}

func newDatabaseRows(iterator rowIterator) *databaseRows {
	return &databaseRows{iterator: iterator}
}

func newPostgreSQLTransaction(transaction pgx.Tx) *databaseTransaction {
	return &databaseTransaction{
		commit: transaction.Commit,
		exec:   transaction.Exec,
		queryRow: func(ctx context.Context, sql string, arguments ...any) databaseRow {
			return databaseRow{scanner: transaction.QueryRow(ctx, sql, arguments...)}
		},
		rollback: transaction.Rollback,
	}
}

func newPostgreSQLDatabaseConnection(connection *pgx.Conn) *databaseConnection {
	return &databaseConnection{
		begin: func(ctx context.Context) (*databaseTransaction, error) {
			transaction, err := connection.Begin(ctx)
			if err != nil {
				return nil, fmt.Errorf("begin PostgreSQL transaction: %w", err)
			}
			return newPostgreSQLTransaction(transaction), nil
		},
		close: connection.Close,
		exec:  connection.Exec,
		query: func(ctx context.Context, sql string, arguments ...any) (*databaseRows, error) {
			rows, err := connection.Query(ctx, sql, arguments...)
			if err != nil {
				return nil, fmt.Errorf("query PostgreSQL: %w", err)
			}
			return newDatabaseRows(rows), nil
		},
		queryRow: func(ctx context.Context, sql string, arguments ...any) databaseRow {
			return databaseRow{scanner: connection.QueryRow(ctx, sql, arguments...)}
		},
	}
}

func (store *Store) connectPostgreSQL(ctx context.Context) (*databaseConnection, error) {
	config, err := store.settings.Primary()
	if err != nil {
		return nil, fmt.Errorf("resolve primary PostgreSQL connection: %w", err)
	}
	connection, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, errors.New("connect to PostgreSQL")
	}
	return newPostgreSQLDatabaseConnection(connection), nil
}

func (store *Store) connectDirectPostgreSQL(ctx context.Context) (*pgx.Conn, error) {
	config, err := store.settings.DirectConfig()
	if err != nil {
		return nil, fmt.Errorf("resolve direct PostgreSQL connection: %w", err)
	}
	connection, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, errors.New("connect to PostgreSQL")
	}
	return connection, nil
}
