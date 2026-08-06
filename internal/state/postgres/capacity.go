package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	statecontract "github.com/meza/RunnerFoundry/internal/state"
)

var _ statecontract.MemberRegistry = (*Store)(nil)
var _ statecontract.RunnerPoolCapacityStore = (*Store)(nil)

const heartbeatMemberSQL = `INSERT INTO public.runnerfoundry_members(member_id, heartbeat_at) VALUES($1, $2)
	ON CONFLICT(member_id) DO UPDATE SET heartbeat_at = excluded.heartbeat_at`
const insertCapacitySQL = `INSERT INTO public.runnerfoundry_member_runner_pools(member_id, runner_pool, capacity)
	VALUES($1, $2, $3)`
const removeMemberCapacitiesSQL = `DELETE FROM public.runnerfoundry_member_runner_pools WHERE member_id = $1`
const countLiveMembersSQL = `SELECT COUNT(*) FROM public.runnerfoundry_members WHERE heartbeat_at >= $1`
const availableCapacitySQL = `SELECT
	COALESCE((
		SELECT SUM(pools.capacity)
		FROM public.runnerfoundry_member_runner_pools pools
		JOIN public.runnerfoundry_members members ON members.member_id = pools.member_id
		WHERE pools.runner_pool = $1 AND members.heartbeat_at >= $2
	), 0), (
		SELECT COUNT(*) FROM public.runnerfoundry_queued_jobs
		WHERE runner_pool = $1
	), (
		SELECT COUNT(*)
		FROM public.runnerfoundry_processing_jobs processing
		WHERE processing.runner_pool = $1
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
	if _, err := transaction.Execute(ctx, heartbeatMemberSQL, memberID, heartbeatAt); err != nil {
		return fmt.Errorf("publish member heartbeat: %w", err)
	}
	if _, err := transaction.Execute(ctx, removeMemberCapacitiesSQL, memberID); err != nil {
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
			memberID, string(capacity.RunnerPool), capacity.Capacity); err != nil {
			return fmt.Errorf("publish Runner Pool %q capacity: %w", capacity.RunnerPool, err)
		}
	}
	return nil
}

func (store *Store) WithdrawMemberCapacity(ctx context.Context, memberID core.MemberID) error {
	return store.executeTransaction(ctx, "withdraw member capacity", removeMemberCapacitiesSQL, memberID)
}

func (store *Store) executeConditionalTransaction(
	ctx context.Context,
	description, statement string,
	arguments ...any,
) (_ bool, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin %s: %w", description, err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	result, err := transaction.Execute(ctx, statement, arguments...)
	if err != nil {
		return false, fmt.Errorf("%s: %w", description, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read %s count: %w", description, err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit %s: %w", description, err)
	}
	open = false
	return rowsAffected == 1, nil
}

func (store *Store) CountLiveMembers(ctx context.Context, cutoff time.Time) (int, error) {
	var count int
	if err := store.executor.QueryRow(ctx, countLiveMembersSQL, cutoff).Scan(&count); err != nil {
		return 0, fmt.Errorf("count live members: %w", err)
	}
	return count, nil
}

func (store *Store) AvailableRunnerPoolCapacity(
	ctx context.Context,
	runnerPool core.RunnerPool,
	cutoff time.Time,
) (int, error) {
	var liveCapacity, queuedJobs, processingJobs int
	if err := store.executor.QueryRow(ctx, availableCapacitySQL, string(runnerPool), cutoff).
		Scan(&liveCapacity, &queuedJobs, &processingJobs); err != nil {
		return 0, fmt.Errorf("available runner pool capacity: %w", err)
	}
	return statecontract.CalculateAvailableRunnerPoolCapacity(liveCapacity, queuedJobs, processingJobs), nil
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
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", description, err)
	}
	open = false
	return nil
}
