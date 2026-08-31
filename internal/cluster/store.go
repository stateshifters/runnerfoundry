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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/tern/v2/migrate"
	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
)

const (
	NodeLifetime          = 30 * time.Second
	leaderAdvisoryLockKey = int64(0x6769746875627363)
)

var ErrNotLeader = errors.New("PostgreSQL leadership is held by another member")

type Store struct {
	settings           postgresconfig.Settings
	directConnector    func(context.Context) (leaderConnection, error)
	connectConfig      func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error)
	parsePoolConfig    func(string) (*pgxpool.Config, error)
	connectPool        func(context.Context, *pgxpool.Config) (*pgxpool.Pool, error)
	assumePoolRole     func(context.Context, roleConnection, string) error
	migrationFactory   func(context.Context, leaderConnection) (*migrationExecutor, error)
	newTernMigrator    func(context.Context, *pgx.Conn, string) (*migrate.Migrator, error)
	connectionAdapter  postgreSQLConnectionAdapter
	migrationFiles     fs.FS
	connectionResolver connectionSettingResolver
}

func NewStore(settings postgresconfig.Settings) *Store {
	return newStoreWithResolver(settings, operatingSystemConnectionSettingResolver())
}

func newStoreWithResolver(settings postgresconfig.Settings, resolver connectionSettingResolver) *Store {
	store := &Store{
		settings:           settings,
		connectConfig:      pgx.ConnectConfig,
		parsePoolConfig:    pgxpool.ParseConfig,
		connectPool:        pgxpool.NewWithConfig,
		assumePoolRole:     assumePostgreSQLRole,
		newTernMigrator:    migrate.NewMigrator,
		connectionAdapter:  operatingSystemPostgreSQLConnectionAdapter(),
		migrationFiles:     embeddedMigrations,
		connectionResolver: resolver,
	}
	store.migrationFactory = store.newMigrationExecutor
	store.directConnector = func(ctx context.Context) (leaderConnection, error) {
		connection, err := store.connectDirectPostgreSQL(ctx)
		if err != nil {
			return nil, err
		}
		return newPostgreSQLLeaderConnection(connection, store.connectionAdapter), nil
	}
	return store
}

type leaderConnection interface {
	Close(ctx context.Context) error
	Exec(ctx context.Context, statement string, arguments ...any) (pgconn.CommandTag, error)
	IsClosed() bool
	Ping(ctx context.Context) error
	QueryRow(ctx context.Context, statement string, arguments ...any) databaseRow
}

type rowScanner interface {
	Scan(destinations ...any) error
}

type databaseRow struct{ scanner rowScanner }

func (row databaseRow) Scan(destinations ...any) error {
	if err := row.scanner.Scan(destinations...); err != nil {
		return fmt.Errorf("scan PostgreSQL row: %w", err)
	}
	return nil
}

type postgreSQLLeaderConnection struct {
	connection *pgx.Conn
	adapter    postgreSQLConnectionAdapter
}

func newPostgreSQLLeaderConnection(
	connection *pgx.Conn,
	adapter postgreSQLConnectionAdapter,
) *postgreSQLLeaderConnection {
	return &postgreSQLLeaderConnection{connection: connection, adapter: adapter}
}

func (connection *postgreSQLLeaderConnection) Close(ctx context.Context) error {
	return connection.adapter.close(connection.connection, ctx)
}

func (connection *postgreSQLLeaderConnection) Exec(
	ctx context.Context,
	statement string,
	arguments ...any,
) (pgconn.CommandTag, error) {
	return connection.adapter.exec(connection.connection, ctx, statement, arguments...)
}

func (connection *postgreSQLLeaderConnection) IsClosed() bool {
	return connection.adapter.isClosed(connection.connection)
}

func (connection *postgreSQLLeaderConnection) Ping(ctx context.Context) error {
	return connection.adapter.ping(connection.connection, ctx)
}

func (connection *postgreSQLLeaderConnection) QueryRow(
	ctx context.Context,
	statement string,
	arguments ...any,
) databaseRow {
	return databaseRow{scanner: connection.adapter.queryRow(connection.connection, ctx, statement, arguments...)}
}

type postgreSQLConnectionAdapter struct {
	close    func(*pgx.Conn, context.Context) error
	exec     func(*pgx.Conn, context.Context, string, ...any) (pgconn.CommandTag, error)
	queryRow func(*pgx.Conn, context.Context, string, ...any) pgx.Row
	isClosed func(*pgx.Conn) bool
	ping     func(*pgx.Conn, context.Context) error
}

func operatingSystemPostgreSQLConnectionAdapter() postgreSQLConnectionAdapter {
	return postgreSQLConnectionAdapter{
		close: (*pgx.Conn).Close, exec: (*pgx.Conn).Exec, queryRow: (*pgx.Conn).QueryRow,
		isClosed: (*pgx.Conn).IsClosed, ping: (*pgx.Conn).Ping,
	}
}

func (store *Store) connectDirectPostgreSQL(ctx context.Context) (*pgx.Conn, error) {
	if err := postgresconfig.Validate(store.settings); err != nil {
		return nil, fmt.Errorf("validate PostgreSQL settings: %w", err)
	}
	role, err := resolvedConnectionRole(store.settings, store.connectionResolver)
	if err != nil {
		return nil, fmt.Errorf("resolve PostgreSQL role: %w", err)
	}
	config, err := directConnectionConfigWithResolver(store.settings, store.connectionResolver)
	if err != nil {
		return nil, fmt.Errorf("resolve direct PostgreSQL connection: %w", err)
	}
	connection, err := store.connectConfig(ctx, config)
	if err != nil {
		return nil, newPostgreSQLConnectionError(err)
	}
	if _, err = store.connectionAdapter.exec(
		connection,
		ctx,
		"SET ROLE "+quotePostgreSQLIdentifier(role),
	); err != nil {
		return nil, errors.Join(
			errors.New("assume PostgreSQL role"),
			closePostgreSQLAfterFailure(ctx, store.connectionAdapter, connection),
		)
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

func closePostgreSQLAfterFailure(
	ctx context.Context,
	adapter postgreSQLConnectionAdapter,
	connection *pgx.Conn,
) error {
	if err := adapter.close(connection, context.WithoutCancel(ctx)); err != nil {
		return errors.New("close failed PostgreSQL connection")
	}
	return nil
}

func resolvedConnectionRole(settings postgresconfig.Settings, resolver connectionSettingResolver) (string, error) {
	return resolver.resolve(settings.Role)
}

func quotePostgreSQLIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
