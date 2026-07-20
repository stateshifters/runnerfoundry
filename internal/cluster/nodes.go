package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (store *Store) Publish(
	ctx context.Context,
	nodeID string,
	health NodeHealth,
	contributions []Contribution,
	now time.Time,
) error {
	if err := validatePublication(nodeID, contributions); err != nil {
		return err
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer closeConnection(ctx, connection)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin contribution publication: %w", err)
	}
	defer rollbackTransaction(ctx, transaction)
	if err = publishNode(ctx, transaction, nodeID, health, now); err != nil {
		return err
	}
	if err = replaceContributions(ctx, transaction, nodeID, contributions); err != nil {
		return err
	}
	if err = removeWithdrawnDemand(ctx, transaction); err != nil {
		return err
	}
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit contribution publication: %w", err)
	}
	return nil
}

func validatePublication(nodeID string, contributions []Contribution) error {
	if strings.TrimSpace(nodeID) == "" {
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
	transaction databaseTransaction,
	nodeID string,
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
	transaction databaseTransaction,
	nodeID string,
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

func removeWithdrawnDemand(ctx context.Context, transaction databaseTransaction) error {
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

func (store *Store) ScaleSets(ctx context.Context, now time.Time, liveFor time.Duration) ([]ScaleSetStatus, error) {
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer closeConnection(ctx, connection)
	rows, err := connection.Query(ctx, `
		SELECT contributions.scale_set,
		       COALESCE(
		           MIN(contributions.runner_group) FILTER (
		               WHERE nodes.healthy AND contributions.healthy AND nodes.heartbeat_at >= $1
		           ),
		           MIN(contributions.runner_group)
		       ),
		       COUNT(DISTINCT contributions.runner_group) FILTER (
		           WHERE nodes.healthy AND contributions.healthy AND nodes.heartbeat_at >= $1
		       ) <= 1,
		       COALESCE(SUM(contributions.max_runners) FILTER (
		           WHERE nodes.healthy AND contributions.healthy AND nodes.heartbeat_at >= $1
		       ), 0)
		FROM scaler_contributions contributions
		JOIN scaler_nodes nodes ON nodes.node_id = contributions.node_id
		GROUP BY contributions.scale_set
		ORDER BY contributions.scale_set`, now.Add(-liveFor))
	if err != nil {
		return nil, fmt.Errorf("load scale-set contributions: %w", err)
	}
	defer rows.Close()
	return scanScaleSetStatuses(rows)
}

func scanScaleSetStatuses(rows databaseRows) ([]ScaleSetStatus, error) {
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
