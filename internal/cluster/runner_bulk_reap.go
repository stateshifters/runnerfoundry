package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/scaler"
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
	if err = reconcileClusterBulkLibvirtReaps(ctx, transaction, scaleSet, repository, reason); err != nil {
		return 0, err
	}
	return int(commandTag.RowsAffected()), nil
}

func reconcileClusterBulkLibvirtReaps(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository, reason string,
) error {
	_, err := transaction.Exec(ctx, `WITH lifecycle AS MATERIALIZED (
		SELECT lifecycle.node_id, lifecycle.scale_set, lifecycle.repository,
		       lifecycle.request_id, lifecycle.runner_name
		FROM scaler_runner_lifecycle lifecycle
		WHERE lifecycle.scale_set = $1 AND ($2 = '' OR lifecycle.repository = $2)
		  AND lifecycle.provider = 'libvirt' AND lifecycle.phase = 'reap_pending'
		  AND lifecycle.reap_reason = $3
	), locked AS MATERIALIZED (
		SELECT COUNT(*) FROM (SELECT pg_advisory_xact_lock(hashtextextended(concat_ws(E'\x1f', scale_set, repository,
			CASE WHEN request_id > 0 THEN request_id::text ELSE runner_name END), 0))
			FROM lifecycle ORDER BY scale_set, repository, request_id, runner_name) acquired
	)
	INSERT INTO scaler_libvirt_runner_lifecycle AS legacy
		(node_id, scale_set, repository, request_id, runner_name, reap_requested)
		SELECT lifecycle.node_id, lifecycle.scale_set, lifecycle.repository,
		       lifecycle.request_id, lifecycle.runner_name, true FROM lifecycle CROSS JOIN locked
		WHERE true
		  AND NOT EXISTS (SELECT 1 FROM scaler_libvirt_runner_lifecycle owner
		      WHERE owner.scale_set = lifecycle.scale_set AND owner.repository = lifecycle.repository
		        AND owner.request_id = lifecycle.request_id
		        AND owner.request_id > 0 AND owner.runner_name <> lifecycle.runner_name)
		ON CONFLICT (node_id, scale_set, repository, runner_name) DO UPDATE SET
		request_id = EXCLUDED.request_id, reap_requested = true
		WHERE legacy.request_id = 0 OR legacy.request_id = EXCLUDED.request_id`,
		scaleSet, repository, reason)
	if err != nil {
		return fmt.Errorf("reconcile clustered bulk libvirt runner reaps: %w", err)
	}
	return validateClusterBulkLifecycleConsistency(ctx, transaction, scaleSet, repository, reason)
}

func validateClusterBulkLifecycleConsistency(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository, reason string,
) error {
	var conflicts bool
	err := transaction.QueryRow(ctx, `/* cluster_bulk_lifecycle_consistency */ SELECT EXISTS (
		SELECT 1 FROM scaler_runner_lifecycle lifecycle
		WHERE lifecycle.scale_set = $1 AND ($2 = '' OR lifecycle.repository = $2)
		  AND lifecycle.phase = 'reap_pending' AND lifecycle.reap_reason = $3
		  AND (EXISTS (SELECT 1 FROM scaler_libvirt_runner_lifecycle legacy
		        WHERE legacy.node_id = lifecycle.node_id AND legacy.scale_set = lifecycle.scale_set
		          AND legacy.repository = lifecycle.repository AND legacy.runner_name = lifecycle.runner_name
		          AND (lifecycle.provider <> 'libvirt' OR legacy.request_id <> lifecycle.request_id))
		    OR EXISTS (SELECT 1 FROM scaler_libvirt_runner_lifecycle owner
		        WHERE owner.scale_set = lifecycle.scale_set AND owner.repository = lifecycle.repository
		          AND owner.request_id = lifecycle.request_id
		          AND owner.request_id > 0 AND owner.runner_name <> lifecycle.runner_name))
	)`, scaleSet, repository, reason).Scan(&conflicts)
	if err != nil {
		return fmt.Errorf("validate clustered bulk lifecycle consistency: %w", err)
	}
	if conflicts {
		return &scaler.RunnerLifecycleConsistencyError{
			ScaleSet: scaleSet, Repository: repository,
			Reason: "legacy lifecycle conflicts with authoritative bulk reap",
		}
	}
	return nil
}
