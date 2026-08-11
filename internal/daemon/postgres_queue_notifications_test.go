package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestPostgreSQLQueueNotificationsWakeEveryReceivedRunnerPool(t *testing.T) {
	connection := newQueueNotificationConnectionFake()
	connection.notifications <- queueNotificationResult{runnerPool: "windows"}
	listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
		maxConnections: 2,
		acquire:        func(context.Context) (queueNotificationConnection, error) { return connection.protocol(), nil },
	})
	woken := make(chan core.RunnerPool, 1)
	listener.wake = func(runnerPool core.RunnerPool) { woken <- runnerPool }
	listener.retry = func(context.Context) bool { return false }

	ctx, cancel := context.WithCancel(context.Background())
	listener.Start(ctx)
	if runnerPool := <-woken; runnerPool != "windows" {
		t.Fatalf("woken Runner Pool = %q, want windows", runnerPool)
	}
	cancel()
	if err := listener.Stop(context.Background()); err != nil {
		t.Fatalf("stop listener: %v", err)
	}
	if connection.listenCalls != 1 || connection.unlistenCalls != 1 || connection.releaseCalls != 1 {
		t.Fatalf("listener lifecycle = listen:%d unlisten:%d release:%d",
			connection.listenCalls, connection.unlistenCalls, connection.releaseCalls)
	}
}

func TestPostgreSQLQueueNotificationsPingIdleSessionAndContinueWaiting(t *testing.T) {
	connection := newQueueNotificationConnectionFake()
	listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
		maxConnections: 2,
		acquire:        func(context.Context) (queueNotificationConnection, error) { return connection.protocol(), nil },
	})
	listener.keepaliveInterval = time.Millisecond
	reported := make(chan error, 1)
	listener.report = func(err error) { reported <- err }
	woken := make(chan core.RunnerPool, 1)
	listener.wake = func(runnerPool core.RunnerPool) { woken <- runnerPool }

	ctx, cancel := context.WithCancel(context.Background())
	listener.Start(ctx)
	<-connection.pinged
	connection.notifications <- queueNotificationResult{runnerPool: "windows"}
	assertWokenRunnerPool(t, woken, "windows")
	assertNoQueueNotificationReport(t, reported, "idle keepalive")
	cancel()
	stopQueueNotificationListener(t, listener)
}

func TestPostgreSQLQueueNotificationsCancelAndJoinBlockedPing(t *testing.T) {
	connection := newQueueNotificationConnectionFake()
	pingStarted := make(chan context.Context)
	protocol := connection.protocol()
	protocol.ping = func(ctx context.Context) error {
		pingStarted <- ctx
		<-ctx.Done()
		return ctx.Err()
	}
	listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
		maxConnections: 2,
		acquire:        func(context.Context) (queueNotificationConnection, error) { return protocol, nil },
	})
	listener.keepaliveInterval = time.Millisecond
	reported := make(chan error, 1)
	listener.report = func(err error) { reported <- err }

	ctx, cancel := context.WithCancel(context.Background())
	listener.Start(ctx)
	if _, bounded := (<-pingStarted).Deadline(); !bounded {
		t.Fatal("notification ping was not bounded")
	}
	cancel()
	stopQueueNotificationListener(t, listener)
	assertDiscardedQueueNotificationConnection(t, connection, "canceled ping")
	assertNoQueueNotificationReport(t, reported, "canceled ping")
}

func TestPostgreSQLQueueNotificationsRemainBestEffort(t *testing.T) {
	wantErr := errors.New("notifications unavailable")
	reports := make(chan error, 1)
	listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
		maxConnections: 2,
		acquire: func(ctx context.Context) (queueNotificationConnection, error) {
			if _, bounded := ctx.Deadline(); !bounded {
				t.Fatal("notification acquisition was not bounded")
			}
			return queueNotificationConnection{}, wantErr
		},
	})
	listener.report = func(err error) { reports <- err }
	listener.retry = func(context.Context) bool { return false }
	listener.Start(context.Background())
	if err := <-reports; !errors.Is(err, wantErr) {
		t.Fatalf("reported error = %v, want %v", err, wantErr)
	}
	if err := listener.Stop(context.Background()); err != nil {
		t.Fatalf("stop failed listener: %v", err)
	}

	skipped := newPostgreSQLQueueNotificationListener(queueNotificationPool{maxConnections: 1})
	skipped.Start(context.Background())
	if err := skipped.Stop(context.Background()); err != nil {
		t.Fatalf("stop skipped listener: %v", err)
	}
}

func TestPostgreSQLQueueNotificationsReconnectAfterListenerFailure(t *testing.T) {
	wantErr := errors.New("listener disconnected")
	failed := newQueueNotificationConnectionFake()
	failed.notifications <- queueNotificationResult{err: wantErr}
	recovered := newQueueNotificationConnectionFake()
	recovered.notifications <- queueNotificationResult{runnerPool: "windows"}
	listener, reported, woken := newReconnectingQueueNotificationListener(
		failed.protocol(), recovered.protocol(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	listener.Start(ctx)
	assertReportedQueueNotificationError(t, reported, wantErr)
	assertWokenRunnerPool(t, woken, "windows")
	assertQueueNotificationRecovery(t, reported)
	cancel()
	stopQueueNotificationListener(t, listener)
	if failed.discardCalls != 1 || recovered.releaseCalls != 1 {
		t.Fatalf("connection outcomes = discarded:%d released:%d", failed.discardCalls, recovered.releaseCalls)
	}
}

func TestPostgreSQLQueueNotificationsReconnectAfterPingFailure(t *testing.T) {
	wantErr := errors.New("ping failed")
	failed := newQueueNotificationConnectionFake()
	failed.pingErr = wantErr
	recovered := newQueueNotificationConnectionFake()
	recovered.notifications <- queueNotificationResult{runnerPool: "windows"}
	listener, reported, woken := newReconnectingQueueNotificationListener(
		failed.protocol(), recovered.protocol(),
	)
	listener.keepaliveInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	listener.Start(ctx)
	assertReportedQueueNotificationError(t, reported, wantErr)
	assertWokenRunnerPool(t, woken, "windows")
	assertQueueNotificationRecovery(t, reported)
	cancel()
	stopQueueNotificationListener(t, listener)
	if failed.discardCalls != 1 || recovered.releaseCalls != 1 {
		t.Fatalf("connection outcomes = discarded:%d released:%d", failed.discardCalls, recovered.releaseCalls)
	}
}

func TestPostgreSQLQueueNotificationsDiscardListenFailures(t *testing.T) {
	wantErr := errors.New("notification operation failed")
	listenFailure := newQueueNotificationConnectionFake()
	listenProtocol := listenFailure.protocol()
	listenProtocol.listen = func(context.Context) error { return wantErr }
	assertQueueNotificationFailure(t, listenProtocol, wantErr)
	if listenFailure.discardCalls != 1 {
		t.Fatalf("listen failure discards = %d, want 1", listenFailure.discardCalls)
	}
}

func TestPostgreSQLQueueNotificationsDiscardCleanupFailures(t *testing.T) {
	wantErr := errors.New("notification operation failed")
	cleanupFailure := newQueueNotificationConnectionFake()
	cleanupFailure.notifications <- queueNotificationResult{runnerPool: "linux"}
	cleanupProtocol := cleanupFailure.protocol()
	cleanupProtocol.unlisten = func(context.Context) error { return wantErr }
	reported := make(chan error, 1)
	listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
		maxConnections: 2,
		acquire:        func(context.Context) (queueNotificationConnection, error) { return cleanupProtocol, nil },
	})
	listener.report = func(err error) { reported <- err }
	woken := make(chan core.RunnerPool, 1)
	listener.wake = func(runnerPool core.RunnerPool) { woken <- runnerPool }
	ctx, cancel := context.WithCancel(context.Background())
	listener.Start(ctx)
	<-woken
	cancel()
	assertReportedQueueNotificationError(t, reported, wantErr)
	stopQueueNotificationListener(t, listener)
	if cleanupFailure.discardCalls != 1 || cleanupFailure.releaseCalls != 0 {
		t.Fatalf("cleanup failure = discarded:%d released:%d", cleanupFailure.discardCalls, cleanupFailure.releaseCalls)
	}
}

func assertReportedQueueNotificationError(t *testing.T, reported <-chan error, want error) {
	t.Helper()
	if err := <-reported; !errors.Is(err, want) {
		t.Fatalf("reported notification error = %v, want %v", err, want)
	}
}

func assertWokenRunnerPool(t *testing.T, woken <-chan core.RunnerPool, want core.RunnerPool) {
	t.Helper()
	if runnerPool := <-woken; runnerPool != want {
		t.Fatalf("woken Runner Pool = %q, want %q", runnerPool, want)
	}
}

func assertQueueNotificationRecovery(t *testing.T, reported <-chan error) {
	t.Helper()
	if err := <-reported; err != nil {
		t.Fatalf("recovery report = %v, want nil", err)
	}
}

func assertNoQueueNotificationReport(t *testing.T, reported <-chan error, operation string) {
	t.Helper()
	select {
	case err := <-reported:
		t.Fatalf("%s report = %v, want none", operation, err)
	default:
	}
}

func assertDiscardedQueueNotificationConnection(
	t *testing.T,
	connection *queueNotificationConnectionFake,
	operation string,
) {
	t.Helper()
	if connection.discardCalls != 1 || connection.unlistenCalls != 0 || connection.releaseCalls != 0 {
		t.Fatalf("%s = discarded:%d unlistened:%d released:%d", operation,
			connection.discardCalls, connection.unlistenCalls, connection.releaseCalls)
	}
}

func newReconnectingQueueNotificationListener(
	connections ...queueNotificationConnection,
) (*postgreSQLQueueNotificationListener, <-chan error, <-chan core.RunnerPool) {
	listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
		maxConnections: 2,
		acquire: func(context.Context) (queueNotificationConnection, error) {
			connection := connections[0]
			connections = connections[1:]
			return connection, nil
		},
	})
	retries := 0
	listener.retry = func(context.Context) bool { retries++; return retries == 1 }
	reported := make(chan error, 2)
	listener.report = func(err error) { reported <- err }
	woken := make(chan core.RunnerPool, 1)
	listener.wake = func(runnerPool core.RunnerPool) { woken <- runnerPool }
	return listener, reported, woken
}

func stopQueueNotificationListener(t *testing.T, listener *postgreSQLQueueNotificationListener) {
	t.Helper()
	if err := listener.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPostgreSQLQueueNotificationLifecycleIsIdempotentAndBounded(t *testing.T) {
	listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{maxConnections: 1})
	if err := listener.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	listener.Start(context.Background())
	listener.Start(context.Background())
	if err := listener.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	blocked := newPostgreSQLQueueNotificationListener(queueNotificationPool{})
	blocked.cancel = func() {}
	blocked.done = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := blocked.Stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("bounded stop error = %v", err)
	}
}

func TestQueueNotificationRetryHonorsDelayAndCancellation(t *testing.T) {
	if !waitForQueueNotificationRetryAfter(context.Background(), 0) {
		t.Fatal("immediate retry was rejected")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitForQueueNotificationRetry(ctx) {
		t.Fatal("default canceled retry was accepted")
	}
	if waitForQueueNotificationRetryAfter(ctx, time.Hour) {
		t.Fatal("canceled retry was accepted")
	}
}

func TestDiscardQueueNotificationConnectionPreservesEveryFailure(t *testing.T) {
	wantOperationErr := errors.New("listen failed")
	wantDiscardErr := errors.New("close failed")
	connection := queueNotificationConnection{
		discard: func(context.Context) error { return wantDiscardErr },
	}
	err := discardQueueNotificationConnection(context.Background(), connection, wantOperationErr)
	if !errors.Is(err, wantOperationErr) || !errors.Is(err, wantDiscardErr) {
		t.Fatalf("discard error = %v, want operation and close failures", err)
	}
}

func assertQueueNotificationFailure(t *testing.T, connection queueNotificationConnection, wantErr error) {
	t.Helper()
	reported := make(chan error, 1)
	listener := newPostgreSQLQueueNotificationListener(queueNotificationPool{
		maxConnections: 2,
		acquire:        func(context.Context) (queueNotificationConnection, error) { return connection, nil },
	})
	listener.report = func(err error) { reported <- err }
	listener.retry = func(context.Context) bool { return false }
	listener.Start(context.Background())
	if err := <-reported; !errors.Is(err, wantErr) {
		t.Fatalf("reported error = %v, want %v", err, wantErr)
	}
	if err := listener.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type queueNotificationResult struct {
	runnerPool core.RunnerPool
	err        error
}

type queueNotificationConnectionFake struct {
	notifications chan queueNotificationResult
	pinged        chan struct{}
	pingErr       error
	listenCalls   int
	pingCalls     int
	unlistenCalls int
	releaseCalls  int
	discardCalls  int
}

func newQueueNotificationConnectionFake() *queueNotificationConnectionFake {
	return &queueNotificationConnectionFake{
		notifications: make(chan queueNotificationResult, 1),
		pinged:        make(chan struct{}, 1),
	}
}

func (connection *queueNotificationConnectionFake) protocol() queueNotificationConnection {
	return queueNotificationConnection{
		listen: func(context.Context) error { connection.listenCalls++; return nil },
		ping: func(context.Context) error {
			connection.pingCalls++
			select {
			case connection.pinged <- struct{}{}:
			default:
			}
			return connection.pingErr
		},
		wait: func(ctx context.Context) (core.RunnerPool, error) {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case result := <-connection.notifications:
				return result.runnerPool, result.err
			}
		},
		unlisten: func(context.Context) error { connection.unlistenCalls++; return nil },
		release:  func() { connection.releaseCalls++ },
		discard:  func(context.Context) error { connection.discardCalls++; return nil },
	}
}
