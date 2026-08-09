package daemon

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/meza/RunnerFoundry/internal/core"
)

const listenForQueuedJobsSQL = `LISTEN runnerfoundry_queued_jobs`
const stopListeningForQueuedJobsSQL = `UNLISTEN runnerfoundry_queued_jobs`

type acquiredPGXConnection interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Conn() *pgx.Conn
	Release()
}

func newPGXQueueNotificationPool(pool *pgxpool.Pool) queueNotificationPool {
	return queueNotificationPool{
		maxConnections: pool.Config().MaxConns,
		acquire:        adaptQueueNotificationAcquire(pool.Acquire, adaptPGXQueueNotificationConnection),
	}
}

func adaptQueueNotificationAcquire(
	acquire func(context.Context) (*pgxpool.Conn, error),
	adapt func(acquiredPGXConnection) queueNotificationConnection,
) func(context.Context) (queueNotificationConnection, error) {
	return func(ctx context.Context) (queueNotificationConnection, error) {
		connection, err := acquire(ctx)
		if err != nil {
			return queueNotificationConnection{}, err
		}
		return adapt(connection), nil
	}
}

func adaptPGXQueueNotificationConnection(connection acquiredPGXConnection) queueNotificationConnection {
	rawConnection := connection.Conn()
	return queueNotificationConnection{
		listen:   queueNotificationStatement(connection.Exec, listenForQueuedJobsSQL),
		wait:     queueNotificationWait(rawConnection.WaitForNotification),
		unlisten: queueNotificationStatement(connection.Exec, stopListeningForQueuedJobsSQL),
		release:  connection.Release,
		discard:  queueNotificationDiscard(rawConnection.Close, connection.Release),
	}
}

func queueNotificationStatement(
	execute func(context.Context, string, ...any) (pgconn.CommandTag, error),
	statement string,
) func(context.Context) error {
	return func(ctx context.Context) error {
		_, err := execute(ctx, statement)
		return err
	}
}

func queueNotificationWait(
	wait func(context.Context) (*pgconn.Notification, error),
) func(context.Context) (core.RunnerPool, error) {
	return func(ctx context.Context) (core.RunnerPool, error) {
		notification, err := wait(ctx)
		if err != nil {
			return "", err
		}
		return core.RunnerPool(notification.Payload), nil
	}
}

func queueNotificationDiscard(
	closeConnection func(context.Context) error,
	release func(),
) func(context.Context) error {
	return func(ctx context.Context) error {
		err := closeConnection(ctx)
		release()
		return err
	}
}
