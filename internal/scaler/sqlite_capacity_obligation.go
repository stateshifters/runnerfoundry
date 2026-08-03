package scaler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// CapacityObligationSource identifies the GitHub signal that requested one
// unit of runner capacity.
type CapacityObligationSource string

const (
	CapacityObligationJobAvailable  CapacityObligationSource = "job_available"
	CapacityObligationAssignedCount CapacityObligationSource = "assigned_count"
)

var (
	ErrInvalidAssignedCapacityObservation = errors.New("invalid assigned capacity observation")
	ErrInvalidCapacityObligation          = errors.New("invalid capacity obligation")
)

// AssignedCapacityObservation is the durable standalone representation of an
// authoritative TotalAssignedJobs statistic.
type AssignedCapacityObservation struct {
	ScaleSet        string
	Repository      string
	ControllerEpoch int64
	AssignedJobs    int
	ObservedAt      time.Time
}

// ObserveAssignedCapacity records the latest authoritative count and adjusts
// only unclaimed identity-free obligations. activeCapacity is supplied by the
// coordinator after inspecting durable lifecycle and provider state.
func (store *SQLiteDemandStore) ObserveAssignedCapacity(
	ctx context.Context,
	observation AssignedCapacityObservation,
	activeCapacity int,
) (decision AssignedObligationDecision, resultErr error) {
	if !validAssignedCapacityObservation(observation, activeCapacity) {
		return decision, ErrInvalidAssignedCapacityObservation
	}
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return decision, fmt.Errorf("begin assigned capacity observation: %w", err)
	}
	transactionOpen := true
	defer finishSQLiteTransaction(transaction, &transactionOpen, "assigned capacity observation", &resultErr)

	decision, err = reconcileAssignedCapacityObservation(ctx, transaction, observation, activeCapacity)
	if err != nil {
		return decision, err
	}
	transactionOpen = false
	if err = transaction.Commit(); err != nil {
		return decision, fmt.Errorf("commit assigned capacity observation: %w", err)
	}
	return decision, nil
}

func validAssignedCapacityObservation(observation AssignedCapacityObservation, activeCapacity int) bool {
	return strings.TrimSpace(observation.ScaleSet) != "" && strings.TrimSpace(observation.Repository) != "" &&
		observation.ControllerEpoch > 0 && observation.AssignedJobs >= 0 && activeCapacity >= 0 && !observation.ObservedAt.IsZero()
}

func reconcileAssignedCapacityObservation(
	ctx context.Context,
	transaction *sql.Tx,
	observation AssignedCapacityObservation,
	activeCapacity int,
) (AssignedObligationDecision, error) {
	stale, err := assignedCapacityObservationIsStale(ctx, transaction, observation)
	if err != nil || stale {
		return AssignedObligationDecision{}, err
	}
	observationID, err := upsertAssignedCapacityObservation(ctx, transaction, observation)
	if err != nil {
		return AssignedObligationDecision{}, err
	}
	if err = removeObsoleteAssignedCapacity(ctx, transaction, observation); err != nil {
		return AssignedObligationDecision{}, err
	}
	queued, err := countQueuedAssignedCapacity(ctx, transaction, observationID)
	if err != nil {
		return AssignedObligationDecision{}, err
	}
	decision := DecideAssignedObligations(AssignedObligationSnapshot{
		AssignedJobs: observation.AssignedJobs, ActiveCapacity: activeCapacity, QueuedObligations: queued,
	})
	return decision, applyAssignedObligationDecision(ctx, transaction, observation, observationID, decision)
}

func applyAssignedObligationDecision(
	ctx context.Context,
	transaction *sql.Tx,
	observation AssignedCapacityObservation,
	observationID int64,
	decision AssignedObligationDecision,
) error {
	if decision.Cancel > 0 {
		if err := cancelQueuedAssignedCapacity(ctx, transaction, observationID, decision.Cancel); err != nil {
			return err
		}
	}
	if decision.Create > 0 {
		if err := createQueuedAssignedCapacity(ctx, transaction, observation, observationID, decision.Create); err != nil {
			return err
		}
	}
	return nil
}

func assignedCapacityObservationIsStale(ctx context.Context, transaction *sql.Tx, observation AssignedCapacityObservation) (bool, error) {
	var epoch, observedAt int64
	err := transaction.QueryRowContext(ctx, `SELECT controller_epoch, observed_at
		FROM assigned_capacity_observations
		WHERE scale_set = ? AND repository = ?
		ORDER BY controller_epoch DESC, observed_at DESC LIMIT 1`, observation.ScaleSet, observation.Repository).Scan(&epoch, &observedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load assigned capacity observation: %w", err)
	}
	return epoch > observation.ControllerEpoch ||
		(epoch == observation.ControllerEpoch && observedAt > observation.ObservedAt.UnixNano()), nil
}

func upsertAssignedCapacityObservation(ctx context.Context, transaction *sql.Tx, observation AssignedCapacityObservation) (int64, error) {
	_, err := transaction.ExecContext(ctx, `INSERT INTO assigned_capacity_observations(
		scale_set, repository, controller_epoch, assigned_jobs, observed_at)
		VALUES(?, ?, ?, ?, ?)
		ON CONFLICT(scale_set, repository, controller_epoch) DO UPDATE SET
		assigned_jobs = excluded.assigned_jobs, observed_at = excluded.observed_at`,
		observation.ScaleSet, observation.Repository, observation.ControllerEpoch,
		observation.AssignedJobs, observation.ObservedAt.UnixNano())
	if err != nil {
		return 0, fmt.Errorf("record assigned capacity observation: %w", err)
	}
	var observationID int64
	if err = transaction.QueryRowContext(ctx, `SELECT assigned_observation_id
		FROM assigned_capacity_observations
		WHERE scale_set = ? AND repository = ? AND controller_epoch = ?`,
		observation.ScaleSet, observation.Repository, observation.ControllerEpoch).Scan(&observationID); err != nil {
		return 0, fmt.Errorf("load recorded assigned capacity observation: %w", err)
	}
	return observationID, nil
}

func removeObsoleteAssignedCapacity(ctx context.Context, transaction *sql.Tx, observation AssignedCapacityObservation) error {
	if _, err := transaction.ExecContext(ctx, `DELETE FROM assigned_capacity_observations
		WHERE scale_set = ? AND repository = ? AND controller_epoch < ?`,
		observation.ScaleSet, observation.Repository, observation.ControllerEpoch); err != nil {
		return fmt.Errorf("remove obsolete assigned capacity observations: %w", err)
	}
	return nil
}

func countQueuedAssignedCapacity(ctx context.Context, transaction *sql.Tx, observationID int64) (int, error) {
	var count int
	err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM demand
		WHERE assigned_observation_id = ? AND source = 'assigned_count'
		AND NOT EXISTS (SELECT 1 FROM runner_lifecycle lifecycle
			WHERE lifecycle.assignment_id = demand.demand_id)`, observationID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count queued assigned capacity obligations: %w", err)
	}
	return count, nil
}

func cancelQueuedAssignedCapacity(ctx context.Context, transaction *sql.Tx, observationID int64, count int) error {
	_, err := transaction.ExecContext(ctx, `DELETE FROM demand WHERE demand_id IN (
		SELECT demand_id FROM demand
		WHERE assigned_observation_id = ? AND source = 'assigned_count'
		AND NOT EXISTS (SELECT 1 FROM runner_lifecycle lifecycle
			WHERE lifecycle.assignment_id = demand.demand_id)
		ORDER BY queue_time DESC, observation_order DESC LIMIT ?
	)`, observationID, count)
	if err != nil {
		return fmt.Errorf("cancel queued assigned capacity obligations: %w", err)
	}
	return nil
}

func createQueuedAssignedCapacity(
	ctx context.Context,
	transaction *sql.Tx,
	observation AssignedCapacityObservation,
	observationID int64,
	count int,
) error {
	for range count {
		_, err := transaction.ExecContext(ctx, `INSERT INTO demand(
			observation_order, scale_set, repository, request_id, queue_time, acquired, source,
			assigned_observation_id, controller_epoch, attempt, next_attempt_at)
			VALUES((SELECT COALESCE(MAX(observation_order), 0) + 1 FROM demand), ?, ?, NULL, ?, 1,
			       'assigned_count', ?, ?, 0, ?)`, observation.ScaleSet, observation.Repository,
			observation.ObservedAt.UnixNano(), observationID, observation.ControllerEpoch, observation.ObservedAt.UnixNano())
		if err != nil {
			return fmt.Errorf("queue assigned capacity obligation: %w", err)
		}
	}
	return nil
}

// RecordObligationFailure preserves retry eligibility across process restart.
func (store *SQLiteDemandStore) RecordObligationFailure(ctx context.Context, obligationID int64, nextAttemptAt time.Time) error {
	if obligationID <= 0 || nextAttemptAt.IsZero() {
		return ErrInvalidCapacityObligation
	}
	result, err := store.database.ExecContext(ctx, `UPDATE demand
		SET attempt = attempt + 1, next_attempt_at = ? WHERE demand_id = ?`, nextAttemptAt.UnixNano(), obligationID)
	if err != nil {
		return fmt.Errorf("record capacity obligation failure: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count deferred capacity obligations: %w", err)
	}
	if changed == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// DeleteObligation removes a capacity obligation after its durable lifecycle
// reservation or positive-demand completion owns the remaining work.
func (store *SQLiteDemandStore) DeleteObligation(ctx context.Context, obligationID int64) error {
	if obligationID <= 0 {
		return ErrInvalidCapacityObligation
	}
	if _, err := store.database.ExecContext(ctx, "DELETE FROM demand WHERE demand_id = ?", obligationID); err != nil {
		return fmt.Errorf("delete capacity obligation: %w", err)
	}
	return nil
}

// DeleteLifecycleOwnedObligations completes the durable ownership handoff for
// assigned obligations whose runner lifecycle was registered successfully.
// Repeating the operation is a successful no-op.
func (store *SQLiteDemandStore) DeleteLifecycleOwnedObligations(ctx context.Context, scaleSet string) error {
	if strings.TrimSpace(scaleSet) == "" {
		return ErrInvalidCapacityObligation
	}
	_, err := store.database.ExecContext(ctx, `DELETE FROM demand
		WHERE scale_set = ? AND source = 'assigned_count'
		AND EXISTS (SELECT 1 FROM runner_lifecycle lifecycle
			WHERE lifecycle.assignment_id = demand.demand_id)`, scaleSet)
	if err != nil {
		return fmt.Errorf("delete lifecycle-owned capacity obligations: %w", err)
	}
	return nil
}
