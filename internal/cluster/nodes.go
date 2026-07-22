package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Publish atomically updates a node's heartbeat and health and replaces all of
// its scale-set contributions. Omitting a previous contribution withdraws it;
// an empty list withdraws every contribution from the node. After replacement,
// Publish removes demand and associated assignments for scale sets that no node
// declares. No part of the publication is committed if any step fails.
//
// Publish returns ErrInvalidPublication for a blank node ID, a blank scale-set
// or runner-group name, or a negative maximum. Callers should correct the
// publication rather than retrying the same invalid input as a transient error.
func (store *Store) Publish(
	ctx context.Context,
	nodeID NodeID,
	health NodeHealth,
	contributions []Contribution,
	now time.Time,
) (resultErr error) {
	if err := validatePublication(nodeID, contributions); err != nil {
		return err
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer finishConnection(ctx, connection, &resultErr)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin contribution publication: %w", err)
	}
	transactionOpen := true
	defer finishTransaction(ctx, transaction, &transactionOpen, &resultErr)
	if err = publishInTransaction(ctx, transaction, nodeID, health, contributions, now); err != nil {
		return err
	}
	transactionOpen = false
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit contribution publication: %w", err)
	}
	return nil
}

func publishInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	nodeID NodeID,
	health NodeHealth,
	contributions []Contribution,
	now time.Time,
) error {
	if err := publishNode(ctx, transaction, nodeID, health, now); err != nil {
		return err
	}
	if err := replaceContributions(ctx, transaction, nodeID, contributions); err != nil {
		return err
	}
	return removeWithdrawnDemand(ctx, transaction)
}

func validatePublication(nodeID NodeID, contributions []Contribution) error {
	if strings.TrimSpace(string(nodeID)) == "" {
		return ErrInvalidPublication
	}
	for _, contribution := range contributions {
		if strings.TrimSpace(contribution.ScaleSet) == "" || strings.TrimSpace(contribution.RunnerGroup) == "" || contribution.MaxRunners < 0 {
			return ErrInvalidPublication
		}
	}
	return nil
}

func publishNode(
	ctx context.Context,
	transaction *databaseTransaction,
	nodeID NodeID,
	health NodeHealth,
	now time.Time,
) error {
	if _, err := transaction.Exec(ctx, `
		INSERT INTO scaler_nodes (node_id, heartbeat_at, healthy)
		VALUES ($1, $2, $3)
		ON CONFLICT (node_id) DO UPDATE
		SET heartbeat_at = EXCLUDED.heartbeat_at, healthy = EXCLUDED.healthy`, nodeID, now, bool(health)); err != nil {
		return fmt.Errorf("publish node state: %w", err)
	}
	return nil
}

func replaceContributions(
	ctx context.Context,
	transaction *databaseTransaction,
	nodeID NodeID,
	contributions []Contribution,
) error {
	if _, err := transaction.Exec(ctx, `DELETE FROM scaler_contributions WHERE node_id = $1`, nodeID); err != nil {
		return fmt.Errorf("replace node contributions: %w", err)
	}
	for _, contribution := range contributions {
		if _, err := transaction.Exec(ctx, `
			INSERT INTO scaler_contributions (node_id, scale_set, runner_group, max_runners, healthy)
			VALUES ($1, $2, $3, $4, $5)`,
			nodeID, contribution.ScaleSet, contribution.RunnerGroup, contribution.MaxRunners, contribution.Healthy,
		); err != nil {
			return fmt.Errorf("publish scale-set contribution: %w", err)
		}
	}
	return nil
}

func removeWithdrawnDemand(ctx context.Context, transaction *databaseTransaction) error {
	if _, err := transaction.Exec(ctx, `
		DELETE FROM scaler_demand demand
		WHERE NOT EXISTS (
		    SELECT 1 FROM scaler_contributions contribution
		    WHERE contribution.scale_set = demand.scale_set
		)`); err != nil {
		return fmt.Errorf("remove demand for fully withdrawn scale sets: %w", err)
	}
	return nil
}

// ScaleSets returns every declared scale set ordered by name. Effective capacity
// sums contributions that are healthy, belong to healthy nodes whose heartbeat
// is no older than now minus liveFor, and agree on their runner group. A scale
// set with conflicting eligible runner groups is incompatible and has zero
// effective capacity; stale or unhealthy declarations remain listed.
func (store *Store) ScaleSets(ctx context.Context, now time.Time, liveFor time.Duration) (statuses []ScaleSetStatus, resultErr error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer finishConnection(ctx, connection, &resultErr)
	rows, err := connection.Query(ctx, contributorEligibilitySQL+`
		SELECT contribution.scale_set,
		       COALESCE(
		           MIN(live_contribution.runner_group),
		           MIN(contribution.runner_group)
		       ),
		       COALESCE(BOOL_AND(compatibility.compatible), true),
		       COALESCE(SUM(eligible_contribution.max_runners), 0)
		FROM scaler_contributions contribution
		LEFT JOIN live_contributions live_contribution
		  ON live_contribution.node_id = contribution.node_id
		 AND live_contribution.scale_set = contribution.scale_set
		LEFT JOIN scale_set_compatibility compatibility
		  ON compatibility.scale_set = contribution.scale_set
		LEFT JOIN eligible_contributions eligible_contribution
		  ON eligible_contribution.node_id = contribution.node_id
		 AND eligible_contribution.scale_set = contribution.scale_set
		GROUP BY contribution.scale_set
		ORDER BY contribution.scale_set`, now.Add(-liveFor))
	if err != nil {
		return nil, fmt.Errorf("load scale-set contributions: %w", err)
	}
	defer rows.Close()
	return scanScaleSetStatuses(rows)
}

func scanScaleSetStatuses(rows *databaseRows) ([]ScaleSetStatus, error) {
	statuses := make([]ScaleSetStatus, 0)
	for rows.Next() {
		var status ScaleSetStatus
		if err := rows.Scan(&status.Name, &status.RunnerGroup, &status.Compatible, &status.EffectiveCapacity); err != nil {
			return nil, fmt.Errorf("scan scale-set contribution: %w", err)
		}
		if !status.Compatible {
			status.EffectiveCapacity = 0
		}
		statuses = append(statuses, status)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read scale-set contributions: %w", err)
	}
	return statuses, nil
}
