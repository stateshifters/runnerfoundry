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
const insertCapacitySQL = `INSERT INTO runnerfoundry_member_runner_pools(member_id, runner_pool, capacity)
	VALUES(?, ?, ?)`
const removeMemberCapacitiesSQL = `DELETE FROM runnerfoundry_member_runner_pools WHERE member_id = ?`
const expireMemberSQL = `DELETE FROM runnerfoundry_members WHERE member_id = ?
	AND NOT EXISTS (SELECT 1 FROM runnerfoundry_jobs WHERE member_id = ?)`
const listLiveMembersSQL = `SELECT member_id FROM runnerfoundry_members WHERE heartbeat_at >= ? ORDER BY member_id`
const listExpiredMembersSQL = `SELECT member_id FROM runnerfoundry_members WHERE heartbeat_at < ? ORDER BY member_id`
const countLiveMembersSQL = `SELECT COUNT(*) FROM runnerfoundry_members WHERE heartbeat_at >= ?`
const availableCapacitySQL = `SELECT
	COALESCE((
		SELECT SUM(pools.capacity)
		FROM runnerfoundry_member_runner_pools pools
		JOIN runnerfoundry_members members ON members.member_id = pools.member_id
		WHERE pools.runner_pool = ?1 AND members.heartbeat_at >= ?2
	), 0), (
		SELECT COUNT(*) FROM runnerfoundry_jobs
		WHERE runner_pool = ?1 AND state = 'queued'
	), (
		SELECT COUNT(*) FROM runnerfoundry_jobs
		WHERE runner_pool = ?1 AND state = 'attempting' AND member_id IS NOT NULL
	)`

func (store *Store) PublishMemberState(
	ctx context.Context,
	memberID core.MemberID,
	heartbeatAt time.Time,
	capacities []core.RunnerPoolCapacity,
) (resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin publish member state: %w", err)
	}
	open := true
	defer func() {
		if open {
			resultErr = errors.Join(resultErr, transaction.Rollback(context.WithoutCancel(ctx)))
		}
	}()
	if _, err := transaction.Execute(ctx, heartbeatMemberSQL, memberID.String(), heartbeatAt.UnixNano()); err != nil {
		return fmt.Errorf("publish member heartbeat: %w", err)
	}
	if _, err := transaction.Execute(ctx, removeMemberCapacitiesSQL, memberID.String()); err != nil {
		return fmt.Errorf("replace member capacities: %w", err)
	}
	if err := insertMemberCapacities(ctx, transaction, memberID, capacities); err != nil {
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit publish member state: %w", err)
	}
	open = false
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
			memberID.String(), string(capacity.RunnerPool), capacity.Capacity); err != nil {
			return fmt.Errorf("publish Runner Pool %q capacity: %w", capacity.RunnerPool, err)
		}
	}
	return nil
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

func (store *Store) AvailableRunnerPoolCapacity(
	ctx context.Context,
	runnerPool core.RunnerPool,
	cutoff time.Time,
) (int, error) {
	var liveCapacity, queuedJobs, attemptingJobs int
	if err := store.executor.QueryRow(ctx, availableCapacitySQL,
		string(runnerPool), cutoff.UnixNano()).Scan(&liveCapacity, &queuedJobs, &attemptingJobs); err != nil {
		return 0, fmt.Errorf("available runner pool capacity: %w", err)
	}
	return statecontract.CalculateAvailableRunnerPoolCapacity(liveCapacity, queuedJobs, attemptingJobs), nil
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
