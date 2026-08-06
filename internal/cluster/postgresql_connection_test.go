package cluster

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgreSQLLeaderConnectionDelegatesClose(t *testing.T) {
	called := false
	connection := protocolConnection(postgreSQLConnectionAdapter{close: func(*pgx.Conn, context.Context) error {
		called = true
		return nil
	}})
	if err := connection.Close(context.Background()); err != nil || !called {
		t.Fatalf("Close called=%t error=%v", called, err)
	}
}

func TestPostgreSQLLeaderConnectionDelegatesExec(t *testing.T) {
	called := false
	connection := protocolConnection(postgreSQLConnectionAdapter{exec: func(*pgx.Conn, context.Context, string, ...any) (pgconn.CommandTag, error) {
		called = true
		return pgconn.CommandTag{}, nil
	}})
	if _, err := connection.Exec(context.Background(), "SELECT 1"); err != nil || !called {
		t.Fatalf("Exec called=%t error=%v", called, err)
	}
}

func TestPostgreSQLLeaderConnectionDelegatesState(t *testing.T) {
	connection := protocolConnection(postgreSQLConnectionAdapter{
		isClosed: func(*pgx.Conn) bool { return true },
		ping:     func(*pgx.Conn, context.Context) error { return nil },
	})
	if !connection.IsClosed() || connection.Ping(context.Background()) != nil {
		t.Fatal("connection state was not delegated")
	}
}

func TestPostgreSQLLeaderConnectionDelegatesQuery(t *testing.T) {
	connection := protocolConnection(postgreSQLConnectionAdapter{
		queryRow: func(*pgx.Conn, context.Context, string, ...any) pgx.Row { return fakeRow{} },
	})
	if err := connection.QueryRow(context.Background(), "SELECT 1").Scan(); err != nil {
		t.Fatal(err)
	}
}

func protocolConnection(adapter postgreSQLConnectionAdapter) *postgreSQLLeaderConnection {
	return newPostgreSQLLeaderConnection(&pgx.Conn{}, adapter)
}

func TestConnectDirectPostgreSQLRejectsMissingRole(t *testing.T) {
	assertDirectConnection(t, directConnectionExpectation{settings: Settings{}})
}

func TestConnectDirectPostgreSQLRejectsInvalidConfiguration(t *testing.T) {
	assertDirectConnection(t, directConnectionExpectation{settings: Settings{Role: "runnerfoundry", Port: "invalid"}})
}

func TestConnectDirectPostgreSQLReportsConnectionFailure(t *testing.T) {
	assertDirectConnection(t, directConnectionExpectation{
		settings: Settings{Role: "runnerfoundry", Host: "database"}, connectErr: syscall.ECONNREFUSED, wantConnect: true,
	})
}

func TestConnectDirectPostgreSQLReportsRoleFailure(t *testing.T) {
	assertDirectConnection(t, directConnectionExpectation{
		settings: Settings{Role: "runnerfoundry", Host: "database"}, execErr: errors.New("set role"),
		wantConnect: true, wantExecuted: true,
	})
}

func TestConnectDirectPostgreSQLReportsRoleAndCloseFailures(t *testing.T) {
	assertDirectConnection(t, directConnectionExpectation{
		settings: Settings{Role: "runnerfoundry", Host: "database"}, execErr: errors.New("set role"), closeErr: errors.New("close"),
		wantConnect: true, wantExecuted: true,
	})
}

func TestConnectDirectPostgreSQLReturnsRoleScopedConnection(t *testing.T) {
	assertDirectConnection(t, directConnectionExpectation{
		settings: Settings{Role: `role"name`, Host: "database"}, wantSuccess: true, wantConnect: true, wantExecuted: true,
	})
}

type directConnectionExpectation struct {
	settings     Settings
	connectErr   error
	execErr      error
	closeErr     error
	wantSuccess  bool
	wantConnect  bool
	wantExecuted bool
}

func assertDirectConnection(t *testing.T, expectation directConnectionExpectation) {
	t.Helper()
	store := newStoreWithResolver(expectation.settings, testConnectionResolver(nil))
	connected := false
	executed := false
	store.connectConfig = func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error) {
		connected = true
		return &pgx.Conn{}, expectation.connectErr
	}
	store.connectionAdapter.exec = func(*pgx.Conn, context.Context, string, ...any) (pgconn.CommandTag, error) {
		executed = true
		return pgconn.CommandTag{}, expectation.execErr
	}
	store.connectionAdapter.close = func(*pgx.Conn, context.Context) error { return expectation.closeErr }
	connection, err := store.connectDirectPostgreSQL(context.Background())
	if (err == nil) != expectation.wantSuccess || (connection != nil) != expectation.wantSuccess {
		t.Fatalf("connectDirectPostgreSQL = (%#v, %v), wantSuccess=%t", connection, err, expectation.wantSuccess)
	}
	if connected != expectation.wantConnect || executed != expectation.wantExecuted {
		t.Fatalf("connected=%t executed=%t", connected, executed)
	}
}

func TestNewStoreConnectorUsesDirectConnectionBoundary(t *testing.T) {
	store := NewStore(Settings{})
	if _, err := store.directConnector(context.Background()); err == nil {
		t.Fatal("default direct connector accepted missing role")
	}

	store = newStoreWithResolver(Settings{Role: "runnerfoundry", Host: "database"}, testConnectionResolver(nil))
	postgresConnection := &pgx.Conn{}
	store.connectConfig = func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error) {
		return postgresConnection, nil
	}
	store.connectionAdapter.exec = func(*pgx.Conn, context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, nil
	}
	connection, err := store.directConnector(context.Background())
	if err != nil || connection == nil {
		t.Fatalf("default direct connector = (%#v, %v)", connection, err)
	}
}

func TestPostgreSQLConnectionFailureKinds(t *testing.T) {
	tests := map[string]struct {
		cause error
		kind  string
	}{
		"canceled":        {cause: context.Canceled, kind: "canceled"},
		"deadline":        {cause: context.DeadlineExceeded, kind: "timeout"},
		"network timeout": {cause: timeoutNetworkError{}, kind: "timeout"},
		"refused":         {cause: syscall.ECONNREFUSED, kind: "refused"},
		"unavailable":     {cause: errors.New("unavailable"), kind: "unavailable"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := newPostgreSQLConnectionError(test.cause)
			if err.Error() != "connect to PostgreSQL: "+test.kind || !errors.Is(err, test.cause) {
				t.Fatalf("connection error = %q, unwrap=%t", err, errors.Is(err, test.cause))
			}
		})
	}
}

func TestRoleResolutionAndIdentifierQuoting(t *testing.T) {
	resolver := testConnectionResolver(nil)
	role, err := resolvedConnectionRole(Settings{Role: `role"name`}, resolver)
	if err != nil || role != `role"name` {
		t.Fatalf("resolved role = %q, %v", role, err)
	}
	if quoted := quotePostgreSQLIdentifier(role); quoted != `"role""name"` {
		t.Fatalf("quoted role = %q", quoted)
	}
}

type timeoutNetworkError struct{}

func (timeoutNetworkError) Error() string   { return "timeout" }
func (timeoutNetworkError) Timeout() bool   { return true }
func (timeoutNetworkError) Temporary() bool { return false }

var _ net.Error = timeoutNetworkError{}
