package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/tern/v2/migrate"
	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
)

const leaderAdvisoryLockKey = int64(0x6769746875627363)

var ErrNotLeader = errors.New("PostgreSQL leadership is held by another member")

type Connections struct {
	settings postgresconfig.Settings
	resolver connectionSettingResolver
	driver   connectionDriver
}

type connectionDriver struct {
	connectConfig     func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error)
	parsePoolConfig   func(string) (*pgxpool.Config, error)
	connectPool       func(context.Context, *pgxpool.Config) (*pgxpool.Pool, error)
	assumePoolRole    func(context.Context, roleConnection, string) error
	newTernMigrator   func(context.Context, *pgx.Conn, string) (*migrate.Migrator, error)
	connectionAdapter postgreSQLConnectionAdapter
}

func NewConnections(settings postgresconfig.Settings) (*Connections, error) {
	return connectionsWithDependencies(settings, operatingSystemConnectionSettingResolver(), operatingSystemConnectionDriver())
}

func connectionsWithDependencies(
	settings postgresconfig.Settings,
	resolver connectionSettingResolver,
	driver connectionDriver,
) (*Connections, error) {
	if err := postgresconfig.Validate(settings); err != nil {
		return nil, fmt.Errorf("validate PostgreSQL settings: %w", err)
	}
	return &Connections{settings: clonePostgreSQLSettings(settings), resolver: resolver, driver: driver}, nil
}

func clonePostgreSQLSettings(settings postgresconfig.Settings) postgresconfig.Settings {
	if settings.Direct != nil {
		direct := *settings.Direct
		settings.Direct = &direct
	}
	return settings
}

func operatingSystemConnectionDriver() connectionDriver {
	return connectionDriver{
		connectConfig: pgx.ConnectConfig, parsePoolConfig: pgxpool.ParseConfig,
		connectPool: pgxpool.NewWithConfig, assumePoolRole: assumePostgreSQLRole,
		newTernMigrator: migrate.NewMigrator, connectionAdapter: operatingSystemPostgreSQLConnectionAdapter(),
	}
}

type databaseSession struct {
	close       func(context.Context) error
	exec        func(context.Context, string, ...any) (pgconn.CommandTag, error)
	isClosed    func() bool
	ping        func(context.Context) error
	queryRow    func(context.Context, string, ...any) databaseRow
	newExecutor func(context.Context, *Catalog) (*migrationExecutor, error)
}

type sessionOpener interface {
	openSession(ctx context.Context) (*databaseSession, error)
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

func (session *databaseSession) Close(ctx context.Context) error {
	return session.close(ctx)
}

func (session *databaseSession) Exec(
	ctx context.Context,
	statement string,
	arguments ...any,
) (pgconn.CommandTag, error) {
	return session.exec(ctx, statement, arguments...)
}

func (session *databaseSession) IsClosed() bool {
	return session.isClosed()
}

func (session *databaseSession) Ping(ctx context.Context) error {
	return session.ping(ctx)
}

func (session *databaseSession) QueryRow(
	ctx context.Context,
	statement string,
	arguments ...any,
) databaseRow {
	return session.queryRow(ctx, statement, arguments...)
}

func (session *databaseSession) newMigrationExecutor(
	ctx context.Context,
	catalog *Catalog,
) (*migrationExecutor, error) {
	return session.newExecutor(ctx, catalog)
}

func createMigrationExecutor(
	ctx context.Context,
	connection *pgx.Conn,
	catalog *Catalog,
	newTernMigrator func(context.Context, *pgx.Conn, string) (*migrate.Migrator, error),
) (*migrationExecutor, error) {
	migrator, err := newTernMigrator(ctx, connection, migrationVersionTable)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL migrator: %w", err)
	}
	catalog.appendTo(migrator)
	return &migrationExecutor{
		currentVersion: migrator.GetCurrentVersion,
		migrateTo:      migrator.MigrateTo,
		target:         catalog.Target(),
	}, nil
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

func (connections *Connections) openSession(ctx context.Context) (*databaseSession, error) {
	role, config, err := connections.resolveSessionSettings()
	if err != nil {
		return nil, err
	}
	connection, err := connections.driver.connectConfig(ctx, config)
	if err != nil {
		return nil, newPostgreSQLConnectionError(err)
	}
	if err = assumeDirectPostgreSQLRole(ctx, connection, role, connections.driver.connectionAdapter); err != nil {
		return nil, err
	}
	return databaseSessionForConnection(connection, connections.driver), nil
}

func (connections *Connections) resolveSessionSettings() (string, *pgx.ConnConfig, error) {
	role, err := resolvedConnectionRole(connections.settings, connections.resolver)
	if err != nil {
		return "", nil, fmt.Errorf("resolve PostgreSQL role: %w", err)
	}
	config, err := directConnectionConfigWithResolver(connections.settings, connections.resolver)
	if err != nil {
		return "", nil, fmt.Errorf("resolve direct PostgreSQL connection: %w", err)
	}
	return role, config, nil
}

func assumeDirectPostgreSQLRole(
	ctx context.Context,
	connection *pgx.Conn,
	role string,
	adapter postgreSQLConnectionAdapter,
) error {
	if _, err := adapter.exec(connection, ctx, "SET ROLE "+quotePostgreSQLIdentifier(role)); err != nil {
		return errors.Join(errors.New("assume PostgreSQL role"), closePostgreSQLAfterFailure(ctx, adapter, connection))
	}
	return nil
}

func databaseSessionForConnection(connection *pgx.Conn, driver connectionDriver) *databaseSession {
	adapter := driver.connectionAdapter
	return &databaseSession{
		close: func(closeCtx context.Context) error { return adapter.close(connection, closeCtx) },
		exec: func(execCtx context.Context, statement string, arguments ...any) (pgconn.CommandTag, error) {
			return adapter.exec(connection, execCtx, statement, arguments...)
		},
		isClosed: func() bool { return adapter.isClosed(connection) },
		ping:     func(pingCtx context.Context) error { return adapter.ping(connection, pingCtx) },
		queryRow: func(queryCtx context.Context, statement string, arguments ...any) databaseRow {
			return databaseRow{scanner: adapter.queryRow(connection, queryCtx, statement, arguments...)}
		},
		newExecutor: func(executorCtx context.Context, catalog *Catalog) (*migrationExecutor, error) {
			return createMigrationExecutor(executorCtx, connection, catalog, driver.newTernMigrator)
		},
	}
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
