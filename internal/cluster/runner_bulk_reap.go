package cluster

import (
	"context"
	"fmt"
	"time"
)

const requestExcessClusterRunnersSQL = `WITH busy AS (
	SELECT COUNT(*) AS runners FROM scaler_runner_lifecycle
	WHERE scale_set = $1 AND repository = $2 AND phase = 'busy'
), excess AS (
	SELECT node_id, scale_set, repository, runner_name
	FROM scaler_runner_lifecycle, busy
	WHERE scale_set = $1 AND repository = $2 AND request_id = 0 AND phase = 'waiting_for_job'
	ORDER BY waiting_since DESC, runner_name DESC
	OFFSET GREATEST($3 - busy.runners, 0)
)
UPDATE scaler_runner_lifecycle lifecycle
SET phase = 'reap_pending', reap_reason = $4, updated_at = $5
FROM excess
WHERE lifecycle.node_id = excess.node_id AND lifecycle.scale_set = excess.scale_set
	AND lifecycle.repository = excess.repository AND lifecycle.runner_name = excess.runner_name`

func (store *Store) RequestExcessWaitingRunnerReaps(
	ctx context.Context,
	scaleSet, repository string,
	retain int,
	reason string,
	requestedAt time.Time,
) (changed int, resultErr error) {
	if retain < 0 {
		retain = 0
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return 0, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin clustered excess runner reap: %w", err)
	}
	transactionOpen := true
	defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
	changed, err = requestExcessClusterRunnersInTransaction(
		ctx, transaction, scaleSet, repository, retain, reason, requestedAt,
	)
	if err != nil {
		return 0, err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit clustered excess runner reap: %w", err)
	}
	return changed, nil
}

func requestExcessClusterRunnersInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	retain int,
	reason string,
	requestedAt time.Time,
) (int, error) {
	commandTag, err := transaction.Exec(ctx, requestExcessClusterRunnersSQL,
		scaleSet, repository, retain, reason, requestedAt)
	if err != nil {
		return 0, fmt.Errorf("request clustered excess runner reaps: %w", err)
	}
	return int(commandTag.RowsAffected()), nil
}
