package cluster

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/tern/v2/migrate"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
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
	ControllerEpoch  int64
	IdentityFree     bool
	State            AssignmentState
}

// AssignedJobObservation is an authoritative aggregate assigned-job count for
// one repository scale set in the current repository-controller epoch.
type AssignedJobObservation struct {
	AppID           int64
	Repository      string
	ScaleSet        string
	ControllerEpoch int64
	AssignedJobs    int
	ObservedAt      time.Time
}

type LibvirtRunnerLifecycle struct {
	NodeID     NodeID
	ScaleSet   string
	Repository string
	RequestID  int64
	RunnerName string
}

type JobCompletion struct {
	ScaleSet   string
	Repository string
	RequestID  int64
	RunnerName string
}

type Store struct {
	settings           postgresconfig.Settings
	connector          func(ctx context.Context) (*databaseConnection, error)
	directConnector    func(ctx context.Context) (leaderConnection, error)
	connectConfig      func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error)
	migrationFactory   func(context.Context, leaderConnection) (*migrationExecutor, error)
	newTernMigrator    func(context.Context, *pgx.Conn, string) (*migrate.Migrator, error)
	connectionAdapter  postgreSQLConnectionAdapter
	transactionAdapter postgreSQLTransactionAdapter
	migrationFiles     fs.FS
}

func NewStore(settings postgresconfig.Settings) *Store {
	store := &Store{settings: settings}
	store.connectConfig = pgx.ConnectConfig
	store.migrationFactory = store.newMigrationExecutor
	store.newTernMigrator = migrate.NewMigrator
	store.connectionAdapter = operatingSystemPostgreSQLConnectionAdapter()
	store.transactionAdapter = operatingSystemPostgreSQLTransactionAdapter()
	store.connector = store.connectPostgreSQL
	store.directConnector = func(ctx context.Context) (leaderConnection, error) {
		connection, err := store.connectDirectPostgreSQL(ctx)
		if err != nil {
			return nil, err
		}
		return newPostgreSQLLeaderConnection(connection, store.connectionAdapter), nil
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
	query    func(context.Context, string, ...any) (*databaseRows, error)
	queryRow func(context.Context, string, ...any) databaseRow
	rollback func(context.Context) error
}

func (transaction *databaseTransaction) Commit(ctx context.Context) error {
	return transaction.commit(ctx)
}

func (transaction *databaseTransaction) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return transaction.exec(ctx, sql, arguments...)
}

func (transaction *databaseTransaction) Query(ctx context.Context, sql string, arguments ...any) (*databaseRows, error) {
	return transaction.query(ctx, sql, arguments...)
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
	connection *pgx.Conn
	adapter    postgreSQLConnectionAdapter
}

func (connection *postgreSQLLeaderConnection) QueryRow(ctx context.Context, sql string, arguments ...any) databaseRow {
	return databaseRow{scanner: connection.adapter.queryRow(connection.connection, ctx, sql, arguments...)}
}

func (connection *postgreSQLLeaderConnection) Close(ctx context.Context) error {
	return connection.adapter.close(connection.connection, ctx)
}

func (connection *postgreSQLLeaderConnection) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return connection.adapter.exec(connection.connection, ctx, sql, arguments...)
}

func (connection *postgreSQLLeaderConnection) IsClosed() bool {
	return connection.adapter.isClosed(connection.connection)
}

func (connection *postgreSQLLeaderConnection) Ping(ctx context.Context) error {
	return connection.adapter.ping(connection.connection, ctx)
}

func newPostgreSQLLeaderConnection(
	connection *pgx.Conn,
	adapter postgreSQLConnectionAdapter,
) *postgreSQLLeaderConnection {
	return &postgreSQLLeaderConnection{connection: connection, adapter: adapter}
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

type postgreSQLTransactionAdapter struct {
	commit   func(pgx.Tx, context.Context) error
	exec     func(pgx.Tx, context.Context, string, ...any) (pgconn.CommandTag, error)
	query    func(pgx.Tx, context.Context, string, ...any) (pgx.Rows, error)
	queryRow func(pgx.Tx, context.Context, string, ...any) pgx.Row
	rollback func(pgx.Tx, context.Context) error
}

func operatingSystemPostgreSQLTransactionAdapter() postgreSQLTransactionAdapter {
	return postgreSQLTransactionAdapter{
		commit:   pgx.Tx.Commit,
		exec:     pgx.Tx.Exec,
		query:    pgx.Tx.Query,
		queryRow: pgx.Tx.QueryRow,
		rollback: pgx.Tx.Rollback,
	}
}

func newPostgreSQLTransaction(transaction pgx.Tx, adapter postgreSQLTransactionAdapter) *databaseTransaction {
	return &databaseTransaction{
		commit: func(ctx context.Context) error { return adapter.commit(transaction, ctx) },
		exec: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			return adapter.exec(transaction, ctx, sql, arguments...)
		},
		query: func(ctx context.Context, sql string, arguments ...any) (*databaseRows, error) {
			rows, err := adapter.query(transaction, ctx, sql, arguments...)
			if err != nil {
				return nil, fmt.Errorf("query PostgreSQL: %w", err)
			}
			return newDatabaseRows(rows), nil
		},
		queryRow: func(ctx context.Context, sql string, arguments ...any) databaseRow {
			return databaseRow{scanner: adapter.queryRow(transaction, ctx, sql, arguments...)}
		},
		rollback: func(ctx context.Context) error { return adapter.rollback(transaction, ctx) },
	}
}

type postgreSQLConnectionAdapter struct {
	begin    func(*pgx.Conn, context.Context) (pgx.Tx, error)
	close    func(*pgx.Conn, context.Context) error
	exec     func(*pgx.Conn, context.Context, string, ...any) (pgconn.CommandTag, error)
	query    func(*pgx.Conn, context.Context, string, ...any) (pgx.Rows, error)
	queryRow func(*pgx.Conn, context.Context, string, ...any) pgx.Row
	isClosed func(*pgx.Conn) bool
	ping     func(*pgx.Conn, context.Context) error
}

func operatingSystemPostgreSQLConnectionAdapter() postgreSQLConnectionAdapter {
	return postgreSQLConnectionAdapter{
		begin: (*pgx.Conn).Begin, close: (*pgx.Conn).Close, exec: (*pgx.Conn).Exec,
		query: (*pgx.Conn).Query, queryRow: (*pgx.Conn).QueryRow,
		isClosed: (*pgx.Conn).IsClosed, ping: (*pgx.Conn).Ping,
	}
}

func newPostgreSQLDatabaseConnection(
	connection *pgx.Conn,
	connectionAdapter postgreSQLConnectionAdapter,
	transactionAdapter postgreSQLTransactionAdapter,
	role string,
) *databaseConnection {
	beginWithRole := newRoleScopedTransactionStarter(connection, connectionAdapter, transactionAdapter, role)
	return &databaseConnection{
		begin:    beginWithRole,
		close:    func(ctx context.Context) error { return connectionAdapter.close(connection, ctx) },
		exec:     roleScopedStatementExecutor(beginWithRole),
		query:    roleScopedRowsQuery(beginWithRole),
		queryRow: roleScopedRowQuery(beginWithRole),
	}
}

func newRoleScopedTransactionStarter(
	connection *pgx.Conn,
	connectionAdapter postgreSQLConnectionAdapter,
	transactionAdapter postgreSQLTransactionAdapter,
	role string,
) func(context.Context) (*databaseTransaction, error) {
	localRoleStatement := "SET LOCAL ROLE " + quotePostgreSQLIdentifier(role)
	return func(ctx context.Context) (*databaseTransaction, error) {
		transaction, err := connectionAdapter.begin(connection, ctx)
		if err != nil {
			return nil, fmt.Errorf("begin PostgreSQL transaction: %w", err)
		}
		if _, err = transactionAdapter.exec(transaction, ctx, localRoleStatement); err != nil {
			rollbackError := transactionAdapter.rollback(transaction, context.WithoutCancel(ctx))
			return nil, errors.Join(fmt.Errorf("assume PostgreSQL role: %w", err), rollbackError)
		}
		return newPostgreSQLTransaction(transaction, transactionAdapter), nil
	}
}

func roleScopedStatementExecutor(
	beginWithRole func(context.Context) (*databaseTransaction, error),
) func(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return func(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, resultErr error) {
		transaction, err := beginWithRole(ctx)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		transactionOpen := true
		defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
		commandTag, err = transaction.Exec(ctx, sql, arguments...)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		transactionOpen = false
		if err = transaction.Commit(ctx); err != nil {
			return pgconn.CommandTag{}, fmt.Errorf("commit PostgreSQL statement: %w", err)
		}
		return commandTag, nil
	}
}

func roleScopedRowsQuery(
	beginWithRole func(context.Context) (*databaseTransaction, error),
) func(context.Context, string, ...any) (*databaseRows, error) {
	return func(ctx context.Context, sql string, arguments ...any) (*databaseRows, error) {
		transaction, err := beginWithRole(ctx)
		if err != nil {
			return nil, err
		}
		rows, err := transaction.Query(ctx, sql, arguments...)
		if err != nil {
			return nil, rollbackTransactionalRowsQuery(ctx, transaction, err)
		}
		return newTransactionalRows(context.WithoutCancel(ctx), rows, transaction), nil
	}
}

func rollbackTransactionalRowsQuery(ctx context.Context, transaction *databaseTransaction, queryError error) error {
	return errors.Join(queryError, rollbackTransaction(ctx, transaction))
}

func roleScopedRowQuery(
	beginWithRole func(context.Context) (*databaseTransaction, error),
) func(context.Context, string, ...any) databaseRow {
	return func(ctx context.Context, sql string, arguments ...any) databaseRow {
		transaction, err := beginWithRole(ctx)
		if err != nil {
			return databaseRow{scanner: failedRowScanner{error: err}}
		}
		return databaseRow{
			scanner: &transactionalRowScanner{
				row:      transaction.QueryRow(ctx, sql, arguments...),
				finalize: transactionalFinalizer(context.WithoutCancel(ctx), transaction),
			},
		}
	}
}

func (store *Store) connectPostgreSQL(ctx context.Context) (*databaseConnection, error) {
	role, err := resolvedConnectionRole(store.settings, productionConnectionSettingResolver)
	if err != nil {
		return nil, fmt.Errorf("resolve PostgreSQL role: %w", err)
	}
	config, err := primaryConnectionConfig(store.settings)
	if err != nil {
		return nil, fmt.Errorf("resolve primary PostgreSQL connection: %w", err)
	}
	connection, err := store.connectConfig(ctx, config)
	if err != nil {
		return nil, newPostgreSQLConnectionError(err)
	}
	return newPostgreSQLDatabaseConnection(connection, store.connectionAdapter, store.transactionAdapter, role), nil
}

func (store *Store) connectDirectPostgreSQL(ctx context.Context) (*pgx.Conn, error) {
	role, err := resolvedConnectionRole(store.settings, productionConnectionSettingResolver)
	if err != nil {
		return nil, fmt.Errorf("resolve PostgreSQL role: %w", err)
	}
	config, err := directConnectionConfig(store.settings)
	if err != nil {
		return nil, fmt.Errorf("resolve direct PostgreSQL connection: %w", err)
	}
	connection, err := store.connectConfig(ctx, config)
	if err != nil {
		return nil, newPostgreSQLConnectionError(err)
	}
	if _, err = store.connectionAdapter.exec(connection, ctx, "SET ROLE "+quotePostgreSQLIdentifier(role)); err != nil {
		discardPostgreSQLConnectionCloseError(ctx, store.connectionAdapter, connection)
		return nil, errors.New("assume PostgreSQL role")
	}
	return connection, nil
}

type postgreSQLConnectionError struct {
	cause error
	kind  string
}

func newPostgreSQLConnectionError(cause error) error {
	return &postgreSQLConnectionError{cause: cause, kind: postgreSQLConnectionFailureKind(cause)}
}

func (connectionErr *postgreSQLConnectionError) Error() string {
	return "connect to PostgreSQL: " + connectionErr.kind
}

func (connectionErr *postgreSQLConnectionError) Unwrap() error { return connectionErr.cause }

func postgreSQLConnectionFailureKind(cause error) string {
	if errors.Is(cause, context.Canceled) {
		return "canceled"
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return "timeout"
	}
	var networkErr net.Error
	if errors.As(cause, &networkErr) && networkErr.Timeout() {
		return "timeout"
	}
	if errors.Is(cause, syscall.ECONNREFUSED) {
		return "refused"
	}
	return "unavailable"
}

func discardPostgreSQLConnectionCloseError(
	ctx context.Context,
	connectionAdapter postgreSQLConnectionAdapter,
	connection *pgx.Conn,
) {
	if closeErr := connectionAdapter.close(connection, context.WithoutCancel(ctx)); closeErr != nil {
		return
	}
}

func resolvedConnectionRole(settings postgresconfig.Settings, resolver connectionSettingResolver) (string, error) {
	if settings.Role == "" {
		return "", errors.New("PostgreSQL role is required")
	}
	return resolver.resolve(settings.Role)
}

func quotePostgreSQLIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

type failedRowScanner struct {
	error error
}

func (scanner failedRowScanner) Scan(...any) error {
	return scanner.error
}

type transactionalRowScanner struct {
	row      pgx.Row
	finalize func(error) error
	finished bool
}

func (scanner *transactionalRowScanner) Scan(destinations ...any) error {
	if scanner.finished {
		return errors.New("scan PostgreSQL row: row already consumed")
	}
	scanner.finished = true
	return scanner.finalize(scanner.row.Scan(destinations...))
}

type transactionalRowIterator struct {
	rows           *databaseRows
	finalize       func(error) error
	finished       bool
	failed         bool
	finalizeResult error
}

func (iterator *transactionalRowIterator) Close() {
	iterator.rows.Close()
	iterator.recordCompletion(iterator.rows.Err())
}

func (iterator *transactionalRowIterator) Err() error {
	iterator.recordCompletion(iterator.rows.Err())
	return iterator.finalizeResult
}

func (iterator *transactionalRowIterator) Next() bool {
	hasNext := iterator.rows.Next()
	if !hasNext {
		iterator.recordCompletion(iterator.rows.Err())
	}
	return hasNext
}

func (iterator *transactionalRowIterator) Scan(destinations ...any) error {
	if err := iterator.rows.Scan(destinations...); err != nil {
		iterator.failed = true
		iterator.recordCompletion(err)
		return err
	}
	return nil
}

func (iterator *transactionalRowIterator) recordCompletion(resultErr error) {
	iterator.finalizeResult = iterator.finish(resultErr)
}

func (iterator *transactionalRowIterator) finish(resultErr error) error {
	if iterator.finished {
		return errors.Join(resultErr, iterator.finalizeResult)
	}
	iterator.finished = true
	if resultErr != nil || iterator.failed {
		return iterator.finalize(resultErr)
	}
	return iterator.finalize(resultErr)
}

func newTransactionalRows(ctx context.Context, rows *databaseRows, transaction *databaseTransaction) *databaseRows {
	return &databaseRows{
		iterator: &transactionalRowIterator{
			rows:     rows,
			finalize: transactionalFinalizer(ctx, transaction),
		},
	}
}

func transactionalFinalizer(ctx context.Context, transaction *databaseTransaction) func(error) error {
	return func(resultErr error) error {
		if resultErr != nil {
			return errors.Join(resultErr, rollbackTransaction(ctx, transaction))
		}
		return errors.Join(nil, transaction.Commit(ctx))
	}
}
