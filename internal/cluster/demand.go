package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (store *Store) Enqueue(ctx context.Context, demand Demand) error {
	if strings.TrimSpace(demand.ScaleSet) == "" || strings.TrimSpace(demand.Repository) == "" || demand.RequestID <= 0 || demand.QueueTime.IsZero() {
		return ErrInvalidDemand
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return err
	}
	defer closeConnection(ctx, connection)
	if _, err = connection.Exec(ctx, `
		INSERT INTO scaler_demand (scale_set, repository, request_id, queue_time)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (scale_set, repository, request_id) DO NOTHING`,
		demand.ScaleSet, demand.Repository, demand.RequestID, demand.QueueTime,
	); err != nil {
		return fmt.Errorf("enqueue scale-set demand: %w", err)
	}
	return nil
}

func (store *Store) AssignOldest(
	ctx context.Context,
	scaleSet string,
	now time.Time,
	liveFor time.Duration,
) (*Assignment, error) {
	if strings.TrimSpace(scaleSet) == "" {
		return nil, ErrInvalidDemand
	}
	connection, err := store.connector(ctx)
	if err != nil {
		return nil, err
	}
	defer closeConnection(ctx, connection)
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin FIFO assignment: %w", err)
	}
	defer rollbackTransaction(ctx, transaction)
	assignment, err := assignOldestInTransaction(ctx, transaction, scaleSet, now, liveFor)
	if err != nil {
		return nil, err
	}
	if err = transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit FIFO assignment: %w", err)
	}
	return assignment, nil
}

func assignOldestInTransaction(
	ctx context.Context,
	transaction databaseTransaction,
	scaleSet string,
	now time.Time,
	liveFor time.Duration,
) (*Assignment, error) {
	if err := releaseUnavailableAssignments(ctx, transaction, now.Add(-liveFor)); err != nil {
		return nil, err
	}
	demandID, assignment, err := loadOldestDemand(ctx, transaction, scaleSet)
	if err != nil {
		return nil, err
	}
	if err = selectContributor(ctx, transaction, scaleSet, now.Add(-liveFor), assignment); err != nil {
		return nil, err
	}
	assignment.ScaleSet = scaleSet
	assignment.State = AssignmentReserved
	if err = transaction.QueryRow(ctx, `
		INSERT INTO scaler_assignments (demand_id, node_id, state)
		VALUES ($1, $2, 'reserved')
		RETURNING assignment_id`, demandID, assignment.NodeID).Scan(&assignment.ID); err != nil {
		return nil, fmt.Errorf("persist FIFO assignment: %w", err)
	}
	return assignment, nil
}

func releaseUnavailableAssignments(ctx context.Context, transaction databaseTransaction, expiredBefore time.Time) error {
	if _, err := transaction.Exec(ctx, `
		UPDATE scaler_assignments assignment
		SET state = 'complete'
		FROM scaler_nodes node
		WHERE assignment.node_id = node.node_id
		  AND assignment.state <> 'complete'
		  AND node.heartbeat_at < $1`, expiredBefore); err != nil {
		return fmt.Errorf("release assignments from unavailable nodes: %w", err)
	}
	return nil
}

func loadOldestDemand(ctx context.Context, transaction databaseTransaction, scaleSet string) (int64, *Assignment, error) {
	var demandID int64
	var assignment Assignment
	err := transaction.QueryRow(ctx, `
		SELECT demand_id, repository, request_id, queue_time
		FROM scaler_demand
		WHERE scale_set = $1
		  AND NOT EXISTS (SELECT 1 FROM scaler_assignments WHERE scaler_assignments.demand_id = scaler_demand.demand_id AND state <> 'complete')
		ORDER BY queue_time, observation_id
		LIMIT 1
		FOR UPDATE`, scaleSet).Scan(&demandID, &assignment.Repository, &assignment.RequestID, &assignment.QueueTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, ErrNoAssignment
	}
	if err != nil {
		return 0, nil, fmt.Errorf("load oldest scale-set demand: %w", err)
	}
	return demandID, &assignment, nil
}

func selectContributor(
	ctx context.Context,
	transaction databaseTransaction,
	scaleSet string,
	liveAfter time.Time,
	assignment *Assignment,
) error {
	err := transaction.QueryRow(ctx, `
		SELECT contribution.node_id
		FROM scaler_contributions contribution
		JOIN scaler_nodes node ON node.node_id = contribution.node_id
		WHERE contribution.scale_set = $1
		  AND contribution.healthy
		  AND node.healthy
		  AND node.heartbeat_at >= $2
		  AND NOT EXISTS (
		      SELECT 1
		      FROM scaler_contributions conflicting
		      JOIN scaler_nodes conflicting_node ON conflicting_node.node_id = conflicting.node_id
		      WHERE conflicting.scale_set = contribution.scale_set
		        AND conflicting.runner_group <> contribution.runner_group
		        AND conflicting.healthy
		        AND conflicting_node.healthy
		        AND conflicting_node.heartbeat_at >= $2
		  )
		  AND contribution.max_runners > (
		      SELECT COUNT(*)
		      FROM scaler_assignments assignment
		      JOIN scaler_demand assignment_demand ON assignment_demand.demand_id = assignment.demand_id
		      WHERE assignment.node_id = contribution.node_id
		        AND assignment_demand.scale_set = contribution.scale_set
		        AND assignment.state <> 'complete'
		  )
		ORDER BY contribution.node_id
		LIMIT 1
		FOR UPDATE OF contribution`, scaleSet, liveAfter).Scan(&assignment.NodeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoAssignment
	}
	if err != nil {
		return fmt.Errorf("select scale-set contributor: %w", err)
	}
	return nil
}
