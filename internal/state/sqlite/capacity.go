package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	statecontract "github.com/stateshifters/runnerfoundry/internal/state"
)

var _ statecontract.MemberRegistry = (*Store)(nil)
var _ statecontract.RunnerPoolCapacityStore = (*Store)(nil)

const heartbeatMemberSQL = `INSERT INTO runnerfoundry_members(member_id, heartbeat_at) VALUES(?, ?)
	ON CONFLICT(member_id) DO UPDATE SET heartbeat_at = excluded.heartbeat_at`
const insertCapacitySQL = `INSERT INTO runnerfoundry_member_runner_pools(member_id, runner_pool, capacity)
	VALUES(?, ?, ?)`
const removeMemberCapacitiesSQL = `DELETE FROM runnerfoundry_member_runner_pools WHERE member_id = ?`
const countLiveMembersSQL = `SELECT COUNT(*) FROM runnerfoundry_members WHERE heartbeat_at >= ?`
const availableCapacitySQL = `SELECT
	COALESCE((
		SELECT SUM(pools.capacity)
		FROM runnerfoundry_member_runner_pools pools
		JOIN runnerfoundry_members members ON members.member_id = pools.member_id
		WHERE pools.runner_pool = ?1 AND members.heartbeat_at >= ?2
	), 0), (
		SELECT COUNT(*) FROM runnerfoundry_queued_jobs
		WHERE runner_pool = ?1
	), (
		SELECT COUNT(*) FROM runnerfoundry_processing_jobs processing
		WHERE processing.runner_pool = ?1
	)`

func (store *Store) PublishMemberState(ctx context.Context, memberID core.MemberID, heartbeatAt time.Time, capacities []core.RunnerPoolCapacity) error {
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		if _, err := transaction.Execute(ctx, heartbeatMemberSQL, string(memberID), heartbeatAt.UnixNano()); err != nil {
			return fmt.Errorf("publish member heartbeat: %w", err)
		}
		if _, err := transaction.Execute(ctx, removeMemberCapacitiesSQL, string(memberID)); err != nil {
			return fmt.Errorf("replace member capacities: %w", err)
		}
		return insertMemberCapacities(ctx, transaction, memberID, capacities)
	})
	if err != nil {
		return fmt.Errorf("publish member state: %w", err)
	}
	return nil
}

func insertMemberCapacities(
	ctx context.Context,
	transaction Transaction,
	memberID core.MemberID,
	capacities []core.RunnerPoolCapacity,
) error {
	for _, capacity := range capacities {
		if capacity.Capacity <= 0 {
			continue
		}
		if _, err := transaction.Execute(ctx, insertCapacitySQL,
			string(memberID), string(capacity.RunnerPool), capacity.Capacity); err != nil {
			return fmt.Errorf("publish Runner Pool %q capacity: %w", capacity.RunnerPool, err)
		}
	}
	return nil
}

func (store *Store) WithdrawMemberCapacity(ctx context.Context, memberID core.MemberID) error {
	return store.executeTransaction(ctx, "withdraw member capacity", removeMemberCapacitiesSQL, string(memberID))
}

func (store *Store) CountLiveMembers(ctx context.Context, cutoff time.Time) (int, error) {
	var count int
	err := store.executor.ReadRow(ctx, countLiveMembersSQL, func(row Scanner) error { return row.Scan(&count) }, cutoff.UnixNano())
	if err != nil {
		return 0, fmt.Errorf("count live members: %w", err)
	}
	return count, nil
}

func (store *Store) AvailableRunnerPoolCapacity(ctx context.Context, runnerPool core.RunnerPool, cutoff time.Time) (int, error) {
	var liveCapacity, queuedJobs, processingJobs int
	err := store.executor.ReadRow(ctx, availableCapacitySQL, func(row Scanner) error { return row.Scan(&liveCapacity, &queuedJobs, &processingJobs) },
		string(runnerPool), cutoff.UnixNano())
	if err != nil {
		return 0, fmt.Errorf("available runner pool capacity: %w", err)
	}
	return statecontract.CalculateAvailableRunnerPoolCapacity(liveCapacity, queuedJobs, processingJobs), nil
}

func (store *Store) executeTransaction(ctx context.Context, description, statement string, arguments ...any) error {
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		_, err := transaction.Execute(ctx, statement, arguments...)
		if err != nil {
			return fmt.Errorf("execute %s: %w", description, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("%s: %w", description, err)
	}
	return nil
}
