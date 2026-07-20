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
	AssignmentID int64
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
	ID               int64
	NodeID           string
	ScaleSet         string
	Repository       string
	RequestID        int64
	QueueTime        time.Time
	InstallationID   int64
	GitHubScaleSetID int
	State            AssignmentState
}

type Store struct {
	settings        ConnectionSettings
	connector       func(ctx context.Context) (databaseConnection, error)
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
		return postgreSQLConnection{Conn: connection}, nil
	}
	store.migrationFiles = embeddedMigrations
	return store
}

type databaseConnection interface {
	Begin(ctx context.Context) (databaseTransaction, error)
	Close(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, arguments ...any) (databaseRows, error)
	QueryRow(ctx context.Context, sql string, arguments ...any) databaseRow
}

type leaderConnection interface {
	Close(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	IsClosed() bool
	Ping(ctx context.Context) error
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

type rowScanner interface {
	Scan(destinations ...any) error
}

type databaseRow struct {
	scanner rowScanner
}

func (row databaseRow) Scan(destinations ...any) error {
	if err := row.scanner.Scan(destinations...); err != nil {
		return fmt.Errorf("scan PostgreSQL row: %w", err)
	}
	return nil
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

func (connection postgreSQLConnection) QueryRow(ctx context.Context, sql string, arguments ...any) databaseRow {
	return databaseRow{scanner: connection.Conn.QueryRow(ctx, sql, arguments...)}
}

func (transaction postgreSQLTransaction) QueryRow(ctx context.Context, sql string, arguments ...any) databaseRow {
	return databaseRow{scanner: transaction.Tx.QueryRow(ctx, sql, arguments...)}
}

func (store *Store) connectPostgreSQL(ctx context.Context) (databaseConnection, error) { //nolint:ireturn // Connection behavior is abstracted for deterministic fault tests.
	config, err := store.settings.Primary()
	if err != nil {
		return nil, fmt.Errorf("resolve primary PostgreSQL connection: %w", err)
	}
	connection, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, errors.New("connect to PostgreSQL")
	}
	return postgreSQLConnection{Conn: connection}, nil
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
