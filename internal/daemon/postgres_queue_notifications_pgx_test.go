package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPGXQueueNotificationStatementAdapter(t *testing.T) {
	executed := ""
	execute := queueNotificationStatement(func(_ context.Context, statement string, _ ...any) (pgconn.CommandTag, error) {
		executed = statement
		return pgconn.CommandTag{}, nil
	}, "LISTEN runnerfoundry_queued_jobs")
	if err := execute(context.Background()); err != nil || executed != "LISTEN runnerfoundry_queued_jobs" {
		t.Fatalf("execute notification statement = (%q, %v)", executed, err)
	}
}

func TestPGXQueueNotificationWaitAdapter(t *testing.T) {
	wait := queueNotificationWait(func(context.Context) (*pgconn.Notification, error) {
		return &pgconn.Notification{Payload: "windows"}, nil
	})
	if runnerPool, err := wait(context.Background()); err != nil || runnerPool != "windows" {
		t.Fatalf("wait notification = (%q, %v)", runnerPool, err)
	}
	wantErr := errors.New("wait failed")
	failedWait := queueNotificationWait(func(context.Context) (*pgconn.Notification, error) { return nil, wantErr })
	if _, err := failedWait(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("wait error = %v, want %v", err, wantErr)
	}
	racingWait := queueNotificationWait(func(context.Context) (*pgconn.Notification, error) {
		return &pgconn.Notification{Payload: "linux"}, context.DeadlineExceeded
	})
	if runnerPool, err := racingWait(context.Background()); err != nil || runnerPool != "linux" {
		t.Fatalf("notification racing with deadline = (%q, %v), want linux", runnerPool, err)
	}
}

func TestPGXQueueNotificationDiscardAdapter(t *testing.T) {
	closed, released := false, false
	discard := queueNotificationDiscard(func(context.Context) error { closed = true; return nil }, func() { released = true })
	if err := discard(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !closed || !released {
		t.Fatalf("discarded connection = closed:%t released:%t", closed, released)
	}
	wantCloseErr := errors.New("close failed")
	if err := queueNotificationDiscard(func(context.Context) error { return wantCloseErr }, func() {})(context.Background()); !errors.Is(err, wantCloseErr) {
		t.Fatalf("discard error = %v, want %v", err, wantCloseErr)
	}
}

func TestPGXQueueNotificationAcquireAdaptsSuccessAndFailure(t *testing.T) {
	wantErr := errors.New("acquire failed")
	failed := adaptQueueNotificationAcquire(
		func(context.Context) (*pgxpool.Conn, error) { return nil, wantErr },
		func(acquiredPGXConnection) queueNotificationConnection { return queueNotificationConnection{} },
	)
	if _, err := failed(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("acquire error = %v, want %v", err, wantErr)
	}

	adapted := false
	succeeded := adaptQueueNotificationAcquire(
		func(context.Context) (*pgxpool.Conn, error) { return &pgxpool.Conn{}, nil },
		func(acquiredPGXConnection) queueNotificationConnection {
			adapted = true
			return queueNotificationConnection{}
		},
	)
	if _, err := succeeded(context.Background()); err != nil || !adapted {
		t.Fatalf("adapted acquisition = (%t, %v)", adapted, err)
	}
}

func TestPGXQueueNotificationConnectionBuildsOwnedProtocol(t *testing.T) {
	connection := &acquiredPGXConnectionFake{}
	protocol := adaptPGXQueueNotificationConnection(connection)
	if protocol.listen == nil || protocol.ping == nil || protocol.wait == nil || protocol.unlisten == nil ||
		protocol.release == nil || protocol.discard == nil {
		t.Fatal("PGX notification connection has an incomplete protocol")
	}
	if err := protocol.ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if connection.pingCalls != 1 {
		t.Fatalf("native connection pings = %d, want 1", connection.pingCalls)
	}
}

func TestPGXQueueNotificationPoolUsesConfiguredPrimaryCapacity(t *testing.T) {
	configured, err := pgxpool.ParseConfig("postgres://runnerfoundry@localhost/runnerfoundry")
	if err != nil {
		t.Fatal(err)
	}
	configured.MaxConns = 7
	pool, err := pgxpool.NewWithConfig(context.Background(), configured)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	adapted := newPGXQueueNotificationPool(pool)
	if adapted.maxConnections != 7 || adapted.acquire == nil {
		t.Fatalf("adapted pool = %#v", adapted)
	}
}

type acquiredPGXConnectionFake struct {
	pingCalls int
}

func (*acquiredPGXConnectionFake) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (connection *acquiredPGXConnectionFake) Ping(context.Context) error {
	connection.pingCalls++
	return nil
}
func (*acquiredPGXConnectionFake) Conn() *pgx.Conn { return nil }
func (*acquiredPGXConnectionFake) Release()        {}
