package daemon

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestPostgreSQLQueueNotificationStartupUsesCallerBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		connection := newQueueNotificationConnectionFake()
		protocol := connection.protocol()
		protocol.listen = func(ctx context.Context) error {
			select {
			case <-time.After(2 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
			maxConnections: 2,
			acquire: func(ctx context.Context) (queueNotificationConnection, error) {
				select {
				case <-time.After(2 * time.Second):
					return protocol, nil
				case <-ctx.Done():
					return queueNotificationConnection{}, ctx.Err()
				}
			},
		})
		startupContext, cancelStartup := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStartup()
		runtimeContext, cancelRuntime := context.WithCancel(context.Background())
		defer cancelRuntime()
		if err := listener.Start(startupContext, runtimeContext); err != nil {
			t.Fatalf("start listener within caller budget: %v", err)
		}
		stopQueueNotificationListener(t, listener)
	})
}

func TestPostgreSQLQueueNotificationAcquisitionHonorsStartupCancellation(t *testing.T) {
	startupContext, cancelStartup := context.WithCancel(context.Background())
	defer cancelStartup()
	listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
		maxConnections: 2,
		acquire: func(ctx context.Context) (queueNotificationConnection, error) {
			cancelStartup()
			<-ctx.Done()
			return queueNotificationConnection{}, ctx.Err()
		},
	})
	if err := listener.Start(startupContext, context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("startup acquisition cancellation = %v, want context canceled", err)
	}
	stopQueueNotificationListener(t, listener)
}

func TestPostgreSQLQueueNotificationListenDiscardsConnectionOnStartupCancellation(t *testing.T) {
	connection := newQueueNotificationConnectionFake()
	protocol := connection.protocol()
	startupContext, cancelStartup := context.WithCancel(context.Background())
	defer cancelStartup()
	protocol.listen = func(ctx context.Context) error {
		cancelStartup()
		<-ctx.Done()
		return ctx.Err()
	}
	listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
		maxConnections: 2,
		acquire:        func(context.Context) (queueNotificationConnection, error) { return protocol, nil },
	})
	if err := listener.Start(startupContext, context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("startup LISTEN cancellation = %v, want context canceled", err)
	}
	stopQueueNotificationListener(t, listener)
	if connection.discardCalls != 1 {
		t.Fatalf("canceled listening connection discards = %d, want 1", connection.discardCalls)
	}
}

func TestPostgreSQLQueueNotificationReconnectRemainsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		connection := newQueueNotificationConnectionFake()
		disconnected := errors.New("connection lost")
		connection.notifications <- queueNotificationResult{err: disconnected}
		initialConnection := true
		listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
			maxConnections: 2,
			acquire: func(ctx context.Context) (queueNotificationConnection, error) {
				if initialConnection {
					initialConnection = false
					return connection.protocol(), nil
				}
				<-ctx.Done()
				return queueNotificationConnection{}, ctx.Err()
			},
		})
		retries := 0
		listener.retry = func(context.Context) bool { retries++; return retries == 1 }
		reported := make(chan error, 2)
		listener.report = func(err error) { reported <- err }
		startQueueNotificationListener(context.Background(), t, listener)
		assertReportedQueueNotificationError(t, reported, disconnected)
		assertReportedQueueNotificationError(t, reported, context.DeadlineExceeded)
		stopQueueNotificationListener(t, listener)
	})
}
