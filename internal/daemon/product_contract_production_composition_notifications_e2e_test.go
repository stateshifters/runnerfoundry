//go:build e2e

package daemon

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

func configureProductionCompositionQueueNotificationReadiness(
	dependencies *productionDependencies,
	tickers *productionCompositionTickers,
) {
	ready := make(chan struct{})
	var readyOnce sync.Once
	markReady := func() { readyOnce.Do(func() { close(ready) }) }
	dependencies.newQueueNotificationPool = productionCompositionObservedQueueNotificationPool(
		dependencies.newQueueNotificationPool,
		markReady,
	)
	tickers.notificationReady = ready
}

func productionCompositionObservedQueueNotificationPool(
	build func(*pgxpool.Pool) queueNotificationPool,
	markReady func(),
) func(*pgxpool.Pool) queueNotificationPool {
	return func(pool *pgxpool.Pool) queueNotificationPool {
		configured := build(pool)
		configured.acquire = productionCompositionObservedQueueNotificationAcquire(configured.acquire, markReady)
		return configured
	}
}

func productionCompositionObservedQueueNotificationAcquire(
	acquire func(context.Context) (queueNotificationConnection, error),
	markReady func(),
) func(context.Context) (queueNotificationConnection, error) {
	return func(ctx context.Context) (queueNotificationConnection, error) {
		connection, err := acquire(ctx)
		if err != nil {
			return queueNotificationConnection{}, err
		}
		connection.listen = productionCompositionObservedQueueNotificationListen(connection.listen, markReady)
		return connection, nil
	}
}

func productionCompositionObservedQueueNotificationListen(
	listen func(context.Context) error,
	markReady func(),
) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := listen(ctx); err != nil {
			return err
		}
		markReady()
		return nil
	}
}
