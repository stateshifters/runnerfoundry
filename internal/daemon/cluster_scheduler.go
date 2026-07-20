package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/meza/github-scaler/internal/cluster"
)

type clusterScheduler struct {
	store                       clusterSchedulingState
	mutex                       sync.Mutex
	sessions                    map[repositoryScaleSetSessionKey]*repositoryScaleSetSession
	acquisitionConfirmationWait time.Duration
	waitBeforeConfirmationRetry func(context.Context, time.Duration) bool
}

func (scheduler *clusterScheduler) add(session *repositoryScaleSetSession) {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	scheduler.sessions[session.key] = session
}

func (scheduler *clusterScheduler) remove(key repositoryScaleSetSessionKey) {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	delete(scheduler.sessions, key)
}

func (scheduler *clusterScheduler) clear() {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	scheduler.sessions = map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}
}

func (scheduler *clusterScheduler) schedule(ctx context.Context, scaleSet string) error {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	if err := scheduler.repairPendingAcquisitions(ctx, scaleSet); err != nil {
		return err
	}
	for {
		outcome, err := scheduler.scheduleOne(ctx, scaleSet)
		if outcome == scheduleDrained || err != nil {
			return err
		}
	}
}

type scheduleOutcome int

const (
	scheduleProgressed scheduleOutcome = iota
	scheduleDrained
)

func (scheduler *clusterScheduler) repairPendingAcquisitions(ctx context.Context, scaleSet string) error {
	assignments, err := scheduler.store.PendingAcquisitions(ctx)
	if err != nil {
		return fmt.Errorf("load clustered acquisition recovery: %w", err)
	}
	for index := range assignments {
		assignment := &assignments[index]
		if assignment.ScaleSet != scaleSet {
			continue
		}
		if err = scheduler.repairPendingAcquisition(ctx, assignment); err != nil {
			return err
		}
	}
	return nil
}

func (scheduler *clusterScheduler) repairPendingAcquisition(
	ctx context.Context,
	assignment *cluster.Assignment,
) error {
	key := repositoryScaleSetSessionKey{repository: assignment.Repository, scaleSet: assignment.ScaleSet}
	session := scheduler.sessions[key]
	if session != nil {
		return scheduler.acquireAssignment(ctx, assignment, session)
	}
	if err := scheduler.store.CompleteAssignment(
		ctx, assignment.ID, assignment.NodeID, assignment.State,
	); err != nil {
		return fmt.Errorf("complete acquisition without a repository scale-set session: %w", err)
	}
	return nil
}

func (scheduler *clusterScheduler) scheduleOne(ctx context.Context, scaleSet string) (scheduleOutcome, error) {
	assignment, err := scheduler.store.AssignOldest(ctx, scaleSet, time.Now(), clusterNodeLifetime)
	if errors.Is(err, cluster.ErrNoAssignment) {
		return scheduleDrained, nil
	}
	if err != nil {
		return scheduleDrained, fmt.Errorf("assign oldest clustered demand: %w", err)
	}
	session := scheduler.sessions[repositoryScaleSetSessionKey{repository: assignment.Repository, scaleSet: scaleSet}]
	if session == nil {
		if releaseErr := scheduler.store.CompleteAssignment(
			ctx, assignment.ID, assignment.NodeID, cluster.AssignmentReserved,
		); releaseErr != nil {
			return scheduleDrained, fmt.Errorf("release demand without a repository scale-set session: %w", releaseErr)
		}
		return scheduleProgressed, nil
	}
	if err = scheduler.acquireAssignment(ctx, assignment, session); err != nil {
		return scheduleDrained, err
	}
	return scheduleProgressed, nil
}

func (scheduler *clusterScheduler) acquireAssignment(
	ctx context.Context,
	assignment *cluster.Assignment,
	session *repositoryScaleSetSession,
) error {
	if assignment.State == "" {
		assignment.State = cluster.AssignmentReserved
	}
	if assignment.State == cluster.AssignmentReserved {
		if err := scheduler.store.BeginAcquisition(
			ctx, assignment.ID, session.installationID, session.runnerScaleSet.ID,
		); err != nil {
			return fmt.Errorf("begin clustered assignment acquisition: %w", err)
		}
		assignment.State = cluster.AssignmentAcquiring
	}
	if assignment.State != cluster.AssignmentAcquiring {
		return fmt.Errorf("acquire assignment in state %q", assignment.State)
	}
	acquireCtx, cancelAcquire := context.WithTimeout(ctx, clusterGitHubTimeout)
	err := session.messageClient.Acquire(acquireCtx, assignment.RequestID)
	cancelAcquire()
	if err != nil {
		if releaseErr := scheduler.store.CompleteAssignment(
			context.WithoutCancel(ctx), assignment.ID, assignment.NodeID, cluster.AssignmentAcquiring,
		); releaseErr != nil {
			return errors.Join(err, fmt.Errorf("release unacquired clustered demand: %w", releaseErr))
		}
		return err
	}
	return scheduler.confirmAcquisition(ctx, assignment.ID)
}

func (scheduler *clusterScheduler) confirmAcquisition(ctx context.Context, assignmentID int64) error {
	for {
		confirmationCtx, cancelConfirmation := context.WithTimeout(ctx, clusterCoordinationTimeout)
		err := scheduler.store.ConfirmAcquisition(confirmationCtx, assignmentID)
		cancelConfirmation()
		if err == nil {
			return nil
		}
		if !scheduler.waitForConfirmationRetry(ctx) {
			return fmt.Errorf("confirm clustered assignment acquisition: %w", errors.Join(err, ctx.Err()))
		}
	}
}

func (scheduler *clusterScheduler) waitForConfirmationRetry(ctx context.Context) bool {
	retryDelay := scheduler.acquisitionConfirmationWait
	if retryDelay <= 0 {
		retryDelay = clusterLeadershipRetryDelay
	}
	waitForRetry := scheduler.waitBeforeConfirmationRetry
	if waitForRetry == nil {
		waitForRetry = waitForRetryDelay
	}
	return waitForRetry(ctx, retryDelay)
}

func waitForRetryDelay(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
