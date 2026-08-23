package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

const queueNotificationAcquireTimeout = time.Second
const queueNotificationKeepaliveInterval = time.Minute
const queueNotificationPingTimeout = 5 * time.Second
const queueNotificationRetryDelay = time.Second

type queueNotificationConnection struct {
	listen   func(context.Context) error
	ping     func(context.Context) error
	wait     func(context.Context) (core.RunnerPool, error)
	unlisten func(context.Context) error
	release  func()
	discard  func(context.Context) error
}

type queueNotificationPool struct {
	maxConnections int32
	acquire        func(context.Context) (queueNotificationConnection, error)
}

type queueNotificationAction uint8

const (
	continueQueueNotifications queueNotificationAction = iota
	retryQueueNotifications
	stopQueueNotifications
)

type postgreSQLQueueNotificationListener struct {
	pool              queueNotificationPool
	wake              func(core.RunnerPool)
	report            func(error)
	retry             func(context.Context) bool
	failed            bool
	keepaliveInterval time.Duration

	mutex  sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func newPostgreSQLQueueNotificationListener(pool queueNotificationPool) *postgreSQLQueueNotificationListener {
	return &postgreSQLQueueNotificationListener{
		pool: pool, wake: func(core.RunnerPool) {}, report: func(error) {}, retry: waitForQueueNotificationRetry,
		keepaliveInterval: queueNotificationKeepaliveInterval,
	}
}

func (listener *postgreSQLQueueNotificationListener) Start(ctx context.Context) {
	listener.mutex.Lock()
	defer listener.mutex.Unlock()
	if listener.done != nil {
		return
	}
	runContext, cancel := context.WithCancel(ctx)
	listener.cancel = cancel
	listener.done = make(chan struct{})
	go listener.run(runContext, listener.done)
}

func (listener *postgreSQLQueueNotificationListener) Stop(ctx context.Context) error {
	listener.mutex.Lock()
	cancel, done := listener.cancel, listener.done
	listener.mutex.Unlock()
	if done == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		listener.mutex.Lock()
		listener.cancel = nil
		listener.done = nil
		listener.mutex.Unlock()
		return nil
	case <-ctx.Done():
		return fmt.Errorf("stop PostgreSQL queue notifications: %w", ctx.Err())
	}
}

func (listener *postgreSQLQueueNotificationListener) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	if listener.pool.maxConnections < 2 || listener.pool.acquire == nil {
		return
	}
	for ctx.Err() == nil {
		action := listener.listenOnce(ctx)
		if action == stopQueueNotifications || action == retryQueueNotifications && !listener.retry(ctx) {
			return
		}
	}
}

func (listener *postgreSQLQueueNotificationListener) listenOnce(ctx context.Context) queueNotificationAction {
	acquireContext, cancel := context.WithTimeout(ctx, queueNotificationAcquireTimeout)
	connection, err := listener.pool.acquire(acquireContext)
	cancel()
	if err != nil {
		listener.reportFailure(fmt.Errorf("acquire PostgreSQL queue notification connection: %w", err))
		return retryQueueNotifications
	}
	if err = connection.listen(ctx); err != nil {
		err = discardQueueNotificationConnection(ctx, connection, err)
		listener.reportFailure(fmt.Errorf("listen for PostgreSQL queue notifications: %w", err))
		return retryQueueNotifications
	}
	listener.reportRecovery()
	return listener.receiveNotifications(ctx, connection)
}

func (listener *postgreSQLQueueNotificationListener) receiveNotifications(
	ctx context.Context,
	connection queueNotificationConnection,
) queueNotificationAction {
	for {
		if action := listener.receiveNotification(ctx, connection); action != continueQueueNotifications {
			return action
		}
	}
}

func (listener *postgreSQLQueueNotificationListener) receiveNotification(
	ctx context.Context,
	connection queueNotificationConnection,
) queueNotificationAction {
	waitContext, cancelWait := context.WithTimeout(ctx, listener.keepaliveInterval)
	runnerPool, waitErr := connection.wait(waitContext)
	waitExpired := errors.Is(waitContext.Err(), context.DeadlineExceeded) &&
		errors.Is(waitErr, context.DeadlineExceeded)
	cancelWait()
	if waitErr == nil {
		listener.wake(runnerPool)
		return continueQueueNotifications
	}
	if ctx.Err() != nil {
		listener.releaseHealthyConnection(ctx, connection)
		return stopQueueNotifications
	}
	if waitExpired {
		return listener.pingNotificationConnection(ctx, connection)
	}
	waitErr = discardQueueNotificationConnection(ctx, connection, waitErr)
	listener.reportFailure(fmt.Errorf("wait for PostgreSQL queue notification: %w", waitErr))
	return retryQueueNotifications
}

func (listener *postgreSQLQueueNotificationListener) pingNotificationConnection(
	ctx context.Context,
	connection queueNotificationConnection,
) queueNotificationAction {
	pingContext, cancelPing := context.WithTimeout(ctx, queueNotificationPingTimeout)
	pingErr := connection.ping(pingContext)
	cancelPing()
	if pingErr == nil {
		return continueQueueNotifications
	}
	pingErr = discardQueueNotificationConnection(ctx, connection, pingErr)
	if ctx.Err() != nil {
		return stopQueueNotifications
	}
	listener.reportFailure(fmt.Errorf("ping PostgreSQL queue notification connection: %w", pingErr))
	return retryQueueNotifications
}

func (listener *postgreSQLQueueNotificationListener) releaseHealthyConnection(
	ctx context.Context,
	connection queueNotificationConnection,
) {
	cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), queueNotificationAcquireTimeout)
	defer cancel()
	if err := connection.unlisten(cleanupContext); err != nil {
		err = discardQueueNotificationConnection(cleanupContext, connection, err)
		listener.reportFailure(fmt.Errorf("stop PostgreSQL queue notifications: %w", err))
		return
	}
	connection.release()
}

func discardQueueNotificationConnection(
	ctx context.Context,
	connection queueNotificationConnection,
	cause error,
) error {
	if err := connection.discard(context.WithoutCancel(ctx)); err != nil {
		return errors.Join(cause, fmt.Errorf("discard PostgreSQL queue notification connection: %w", err))
	}
	return cause
}

func (listener *postgreSQLQueueNotificationListener) reportFailure(err error) {
	listener.failed = true
	listener.report(err)
}

func (listener *postgreSQLQueueNotificationListener) reportRecovery() {
	if listener.failed {
		listener.failed = false
		listener.report(nil)
	}
}

func waitForQueueNotificationRetry(ctx context.Context) bool {
	return waitForQueueNotificationRetryAfter(ctx, queueNotificationRetryDelay)
}

func waitForQueueNotificationRetryAfter(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
