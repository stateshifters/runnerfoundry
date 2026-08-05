package sqlite

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
	statecontract "github.com/meza/RunnerFoundry/internal/state"
)

var _ statecontract.MemberRegistry = (*Store)(nil)
var _ statecontract.RunnerPoolCapacityStore = (*Store)(nil)

const heartbeatMemberSQL = `INSERT INTO runnerfoundry_members(member_id, heartbeat_at) VALUES(?, ?)
	ON CONFLICT(member_id) DO UPDATE SET heartbeat_at = excluded.heartbeat_at`
const publishCapacitySQL = `INSERT INTO runnerfoundry_member_runner_pools(member_id, runner_pool, capacity)
	VALUES(?, ?, ?) ON CONFLICT(member_id, runner_pool) DO UPDATE SET capacity = excluded.capacity`
const removeCapacitySQL = `DELETE FROM runnerfoundry_member_runner_pools WHERE member_id = ? AND runner_pool = ?`
const removeMemberCapacitiesSQL = `DELETE FROM runnerfoundry_member_runner_pools WHERE member_id = ?`
const expireMemberSQL = `DELETE FROM runnerfoundry_members WHERE member_id = ?
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_jobs WHERE member_id = ?)`
const listLiveMembersSQL = `SELECT member_id FROM runnerfoundry_members WHERE heartbeat_at >= ? ORDER BY member_id`
const listExpiredMembersSQL = `SELECT member_id FROM runnerfoundry_members WHERE heartbeat_at < ? ORDER BY member_id`
const countLiveMembersSQL = `SELECT COUNT(*) FROM runnerfoundry_members WHERE heartbeat_at >= ?`
const aggregateCapacitySQL = `SELECT COALESCE(SUM(pools.capacity), 0)
	FROM runnerfoundry_member_runner_pools pools
	JOIN runnerfoundry_members members ON members.member_id = pools.member_id
	WHERE pools.runner_pool = ? AND members.heartbeat_at >= ?`

func (store *Store) HeartbeatMember(ctx context.Context, memberID core.MemberID, heartbeatAt time.Time) error {
	return store.executeTransaction(ctx, "heartbeat member", heartbeatMemberSQL, memberID.String(), heartbeatAt.UnixNano())
}

func (store *Store) PublishRunnerPoolCapacity(
	ctx context.Context,
	memberID core.MemberID,
	runnerPool core.RunnerPool,
	capacity int,
) error {
	if capacity <= 0 {
		return store.executeTransaction(ctx, "remove runner pool capacity", removeCapacitySQL,
			memberID.String(), string(runnerPool))
	}
	return store.executeTransaction(ctx, "publish runner pool capacity", publishCapacitySQL,
		memberID.String(), string(runnerPool), capacity)
}

func (store *Store) ExpireMember(ctx context.Context, memberID core.MemberID) error {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin expire member: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open)
	if _, err := transaction.Execute(ctx, removeMemberCapacitiesSQL, memberID.String()); err != nil {
		return fmt.Errorf("remove expired member capacities: %w", err)
	}
	if _, err := transaction.Execute(ctx, expireMemberSQL, memberID.String(), memberID.String()); err != nil {
		return fmt.Errorf("remove unreferenced expired member: %w", err)
	}
	open = false
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit expire member: %w", err)
	}
	return nil
}

func (store *Store) ListLiveMembers(ctx context.Context, cutoff time.Time) (members []core.MemberID, resultErr error) {
	return store.listMembers(ctx, "live", listLiveMembersSQL, cutoff.UnixNano())
}

func (store *Store) ListExpiredMembers(ctx context.Context, cutoff time.Time) ([]core.MemberID, error) {
	return store.listMembers(ctx, "expired", listExpiredMembersSQL, cutoff.UnixNano())
}

func (store *Store) listMembers(ctx context.Context, description, statement string, cutoff any) (members []core.MemberID, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin list %s members: %w", description, err)
	}
	open := true
	defer rollback(ctx, transaction, &open)
	rows, err := transaction.Query(ctx, statement, cutoff)
	if err != nil {
		return nil, fmt.Errorf("query %s members: %w", description, err)
	}
	members, resultErr = scanMemberRows(rows)
	if resultErr != nil {
		return nil, resultErr
	}
	open = false
	if err := transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit list live members: %w", err)
	}
	return members, nil
}

func scanMemberRows(rows Rows) (members []core.MemberID, resultErr error) {
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		var memberID string
		if err := rows.Scan(&memberID); err != nil {
			return nil, fmt.Errorf("scan member: %w", err)
		}
		parsedMemberID, err := uuid.Parse(memberID)
		if err != nil {
			return nil, fmt.Errorf("parse member UUID: %w", err)
		}
		members = append(members, parsedMemberID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate members: %w", err)
	}
	return members, nil
}

func (store *Store) CountLiveMembers(ctx context.Context, cutoff time.Time) (int, error) {
	var count int
	if err := store.executor.QueryRow(ctx, countLiveMembersSQL, cutoff.UnixNano()).Scan(&count); err != nil {
		return 0, fmt.Errorf("count live members: %w", err)
	}
	return count, nil
}

func (store *Store) AggregateRunnerPoolCapacity(
	ctx context.Context,
	runnerPool core.RunnerPool,
	cutoff time.Time,
) (int, error) {
	var capacity int
	if err := store.executor.QueryRow(ctx, aggregateCapacitySQL, string(runnerPool), cutoff.UnixNano()).Scan(&capacity); err != nil {
		return 0, fmt.Errorf("aggregate runner pool capacity: %w", err)
	}
	return capacity, nil
}

func (store *Store) executeTransaction(
	ctx context.Context,
	description, statement string,
	arguments ...any,
) (resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s: %w", description, err)
	}
	open := true
	defer func() {
		if open {
			resultErr = errors.Join(resultErr, transaction.Rollback(context.WithoutCancel(ctx)))
		}
	}()
	if _, err := transaction.Execute(ctx, statement, arguments...); err != nil {
		return fmt.Errorf("%s: %w", description, err)
	}
	open = false
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", description, err)
	}
	return nil
}
