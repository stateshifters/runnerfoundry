package cluster

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNewConnectionsValidatesStructureWithoutResolvingCredentials(t *testing.T) {
	resolver := testConnectionResolver(nil)
	resolver.lookupEnvironment = func(string) (string, bool) {
		t.Fatal("credentials were resolved during construction")
		return "", false
	}
	connections, err := connectionsWithDependencies(
		Settings{Role: "env://POSTGRES_ROLE", Host: "database"},
		resolver,
		operatingSystemConnectionDriver(),
	)
	if err != nil || connections == nil {
		t.Fatalf("NewConnections = (%#v, %v)", connections, err)
	}
	if _, err = NewConnections(Settings{}); err == nil {
		t.Fatal("missing stable role was accepted")
	}
}

func TestNewConnectionsOwnsAnImmutableSettingsSnapshot(t *testing.T) {
	settings := Settings{
		Role: "runnerfoundry", Host: "primary",
		Direct: &Endpoint{Host: "direct"},
	}
	connections := connectionsForTest(t, settings, operatingSystemConnectionDriver())
	settings.Role = "changed"
	settings.Direct.Host = "changed"
	if connections.settings.Role != "runnerfoundry" || connections.settings.Direct.Host != "direct" {
		t.Fatalf("settings snapshot changed: %#v", connections.settings)
	}
}

func TestConnectionsOpenPoolResolvesCredentialsAndScopesEveryConnection(t *testing.T) {
	driver := operatingSystemConnectionDriver()
	var configured *pgxpool.Config
	wantPool := &pgxpool.Pool{}
	driver.connectPool = func(_ context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
		configured = config
		return wantPool, nil
	}
	var assumedRole string
	driver.assumePoolRole = func(_ context.Context, _ roleConnection, role string) error {
		assumedRole = role
		return nil
	}
	connections := connectionsForTest(t, Settings{Role: "runnerfoundry", Host: "database"}, driver)
	pool, err := connections.OpenPool(context.Background())
	if err != nil || pool != wantPool || configured == nil || configured.AfterConnect == nil {
		t.Fatalf("OpenPool = (%#v, %v), config=%#v", pool, err, configured)
	}
	if err = configured.AfterConnect(context.Background(), nil); err != nil || assumedRole != "runnerfoundry" {
		t.Fatalf("AfterConnect role = %q, %v", assumedRole, err)
	}
}

func TestConnectionsOpenPoolSanitizesBoundaryFailures(t *testing.T) {
	driver := operatingSystemConnectionDriver()
	driver.parsePoolConfig = func(string) (*pgxpool.Config, error) { return nil, errors.New("sensitive parser detail") }
	connections := connectionsForTest(t, Settings{Role: "runnerfoundry", Host: "database"}, driver)
	if _, err := connections.OpenPool(context.Background()); err == nil || err.Error() != "prepare PostgreSQL pool" {
		t.Fatalf("parser error = %v", err)
	}

	driver = operatingSystemConnectionDriver()
	driver.connectPool = func(context.Context, *pgxpool.Config) (*pgxpool.Pool, error) {
		return nil, syscall.ECONNREFUSED
	}
	connections = connectionsForTest(t, Settings{Role: "runnerfoundry", Host: "database"}, driver)
	if _, err := connections.OpenPool(context.Background()); !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("connection error = %v", err)
	}

	connections = connectionsForTest(t, Settings{Role: "env://MISSING", Host: "database"}, operatingSystemConnectionDriver())
	if _, err := connections.OpenPool(context.Background()); err == nil {
		t.Fatal("missing role credential was accepted")
	}
	connections = connectionsForTest(t, Settings{Role: "runnerfoundry", Port: "invalid"}, operatingSystemConnectionDriver())
	if _, err := connections.OpenPool(context.Background()); err == nil {
		t.Fatal("invalid endpoint was accepted")
	}
}

func TestConnectionsOpenSessionAssumesStableRoleAndClosesFailedSession(t *testing.T) {
	driver := operatingSystemConnectionDriver()
	connection := &pgx.Conn{}
	driver.connectConfig = func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error) { return connection, nil }
	var statement string
	closed := false
	driver.connectionAdapter.exec = func(_ *pgx.Conn, _ context.Context, executed string, _ ...any) (pgconn.CommandTag, error) {
		statement = executed
		return pgconn.CommandTag{}, nil
	}
	driver.connectionAdapter.close = func(*pgx.Conn, context.Context) error { closed = true; return nil }
	connections := connectionsForTest(t, Settings{Role: `role"name`, Host: "database"}, driver)
	session, err := connections.openSession(context.Background())
	if err != nil || session == nil || statement != `SET ROLE "role""name"` {
		t.Fatalf("openSession = (%#v, %v), statement=%q", session, err, statement)
	}

	driver.connectionAdapter.exec = func(*pgx.Conn, context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, errors.New("role failed")
	}
	connections = connectionsForTest(t, Settings{Role: "runnerfoundry", Host: "database"}, driver)
	if _, err = connections.openSession(context.Background()); err == nil || !closed {
		t.Fatalf("role failure = %v, closed=%t", err, closed)
	}
}

func TestConnectionsOpenSessionReportsResolutionConnectionAndCleanupFailures(t *testing.T) {
	connections := connectionsForTest(t, Settings{Role: "env://MISSING", Host: "database"}, operatingSystemConnectionDriver())
	if _, err := connections.openSession(context.Background()); err == nil {
		t.Fatal("missing role credential was accepted")
	}
	connections = connectionsForTest(t, Settings{Role: "runnerfoundry", Port: "invalid"}, operatingSystemConnectionDriver())
	if _, err := connections.openSession(context.Background()); err == nil {
		t.Fatal("invalid direct endpoint was accepted")
	}
	driver := operatingSystemConnectionDriver()
	driver.connectConfig = func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error) {
		return nil, syscall.ECONNREFUSED
	}
	connections = connectionsForTest(t, Settings{Role: "runnerfoundry", Host: "database"}, driver)
	if _, err := connections.openSession(context.Background()); !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("connect error = %v", err)
	}
	driver = operatingSystemConnectionDriver()
	driver.connectConfig = func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error) { return &pgx.Conn{}, nil }
	driver.connectionAdapter.exec = func(*pgx.Conn, context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, errors.New("failed")
	}
	driver.connectionAdapter.close = func(*pgx.Conn, context.Context) error { return errors.New("close failed") }
	connections = connectionsForTest(t, Settings{Role: "runnerfoundry", Host: "database"}, driver)
	if _, err := connections.openSession(context.Background()); err == nil || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("cleanup error = %v", err)
	}
}

func TestPostgreSQLSessionSupportsSessionOwnedOperations(t *testing.T) {
	adapter := postgreSQLConnectionAdapter{
		close: func(*pgx.Conn, context.Context) error { return nil },
		exec: func(*pgx.Conn, context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("SELECT 1"), nil
		},
		queryRow: func(*pgx.Conn, context.Context, string, ...any) pgx.Row { return fakeRow{} },
		isClosed: func(*pgx.Conn) bool { return false },
		ping:     func(*pgx.Conn, context.Context) error { return nil },
	}
	session := databaseSessionForConnection(&pgx.Conn{}, connectionDriver{connectionAdapter: adapter})
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Exec(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if session.IsClosed() || session.Ping(context.Background()) != nil {
		t.Fatal("healthy session reported unavailable")
	}
	if err := session.QueryRow(context.Background(), "SELECT 1").Scan(); err != nil {
		t.Fatal(err)
	}
}

func TestAssumePostgreSQLRoleUsesQuotedIdentifier(t *testing.T) {
	session := &fakeDatabaseConnection{}
	if err := assumePostgreSQLRole(context.Background(), session, `role"name`); err != nil {
		t.Fatal(err)
	}
	if session.executions[0].statement != `SET ROLE "role""name"` {
		t.Fatalf("statement = %q", session.executions[0].statement)
	}
	session.execErrors = []error{errors.New("failed")}
	if err := assumePostgreSQLRole(context.Background(), session, "runnerfoundry"); err == nil {
		t.Fatal("role failure was hidden")
	}
}

func TestPostgreSQLConnectionFailureKinds(t *testing.T) {
	tests := map[string]struct {
		cause error
		kind  string
	}{
		"canceled": {cause: context.Canceled, kind: "canceled"},
		"deadline": {cause: context.DeadlineExceeded, kind: "timeout"},
		"network":  {cause: timeoutNetworkError{}, kind: "timeout"},
		"refused":  {cause: syscall.ECONNREFUSED, kind: "refused"},
		"other":    {cause: errors.New("private detail"), kind: "unavailable"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := newPostgreSQLConnectionError(test.cause)
			if err.Error() != "connect to PostgreSQL: "+test.kind || !errors.Is(err, test.cause) {
				t.Fatalf("connection error = %q", err)
			}
		})
	}
}

func connectionsForTest(t *testing.T, settings Settings, driver connectionDriver) *Connections {
	t.Helper()
	connections, err := connectionsWithDependencies(settings, testConnectionResolver(nil), driver)
	if err != nil {
		t.Fatal(err)
	}
	return connections
}

type timeoutNetworkError struct{}

func (timeoutNetworkError) Error() string   { return "timeout" }
func (timeoutNetworkError) Timeout() bool   { return true }
func (timeoutNetworkError) Temporary() bool { return false }

var _ net.Error = timeoutNetworkError{}
