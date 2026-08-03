package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/meza/RunnerFoundry/internal/cluster"
)

type clusterScheduler struct {
	store                       clusterExactSchedulingState
	workerDone                  <-chan struct{}
	workerWait                  sync.WaitGroup
	wakeMutex                   sync.Mutex
	wakeChannels                map[string]chan struct{}
	reportScheduleError         func(string, error)
	safetyWakeInterval          time.Duration
	sessionsMutex               sync.RWMutex
	sessions                    map[repositoryScaleSetSessionKey]*repositoryScaleSetSession
	scheduleLocksMutex          sync.Mutex
	scheduleLocks               map[string]*sync.Mutex
	acquisitionConfirmationWait time.Duration
	waitBeforeConfirmationRetry func(context.Context, time.Duration) bool
}

func (scheduler *clusterScheduler) wake(ctx context.Context, scaleSet string) {
	scheduler.wakeMutex.Lock()
	if scheduler.wakeChannels == nil {
		scheduler.wakeChannels = make(map[string]chan struct{})
	}
	wakeChannel := scheduler.wakeChannels[scaleSet]
	if wakeChannel == nil {
		wakeChannel = make(chan struct{}, 1)
		scheduler.wakeChannels[scaleSet] = wakeChannel
		scheduler.workerWait.Add(1)
		go scheduler.runWakeLoop(ctx, scaleSet, wakeChannel)
	}
	scheduler.wakeMutex.Unlock()
	select {
	case wakeChannel <- struct{}{}:
	default:
	}
}

func (scheduler *clusterScheduler) runWakeLoop(ctx context.Context, scaleSet string, wakeChannel <-chan struct{}) {
	defer scheduler.workerWait.Done()
	workerCtx, cancelWorker := scheduler.workerLoopContext(ctx)
	defer cancelWorker()
	safetyWake := time.NewTicker(scheduler.wakeInterval())
	defer safetyWake.Stop()
	for {
		select {
		case <-workerCtx.Done():
			return
		case <-wakeChannel:
		case <-safetyWake.C:
		}
		scheduler.scheduleAndReport(workerCtx, scaleSet)
	}
}

func (scheduler *clusterScheduler) workerLoopContext(ctx context.Context) (context.Context, context.CancelFunc) {
	workerCtx, cancelWorker := context.WithCancel(context.WithoutCancel(ctx))
	if scheduler.workerDone == nil {
		return workerCtx, cancelWorker
	}
	go func() {
		select {
		case <-scheduler.workerDone:
			cancelWorker()
		case <-workerCtx.Done():
		}
	}()
	return workerCtx, cancelWorker
}

func (scheduler *clusterScheduler) wakeInterval() time.Duration {
	if scheduler.safetyWakeInterval > 0 {
		return scheduler.safetyWakeInterval
	}
	return assignedJobRecoveryInterval
}

func (scheduler *clusterScheduler) scheduleAndReport(ctx context.Context, scaleSet string) {
	err := scheduler.schedule(ctx, scaleSet)
	if err != nil && scheduler.reportScheduleError != nil {
		scheduler.reportScheduleError(scaleSet, err)
	}
}

func (scheduler *clusterScheduler) wait() {
	scheduler.workerWait.Wait()
}

func (scheduler *clusterScheduler) add(session *repositoryScaleSetSession) {
	scheduler.sessionsMutex.Lock()
	defer scheduler.sessionsMutex.Unlock()
	scheduler.sessions[session.key] = session
}

func (scheduler *clusterScheduler) remove(key repositoryScaleSetSessionKey) {
	scheduler.sessionsMutex.Lock()
	defer scheduler.sessionsMutex.Unlock()
	delete(scheduler.sessions, key)
}

func (scheduler *clusterScheduler) clear() {
	scheduler.sessionsMutex.Lock()
	defer scheduler.sessionsMutex.Unlock()
	scheduler.sessions = map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}
}

func (scheduler *clusterScheduler) schedule(ctx context.Context, scaleSet string) error {
	scaleSetLock := scheduler.scheduleLock(scaleSet)
	scaleSetLock.Lock()
	defer scaleSetLock.Unlock()
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

func (scheduler *clusterScheduler) scheduleLock(scaleSet string) *sync.Mutex {
	scheduler.scheduleLocksMutex.Lock()
	defer scheduler.scheduleLocksMutex.Unlock()
	if scheduler.scheduleLocks == nil {
		scheduler.scheduleLocks = make(map[string]*sync.Mutex)
	}
	lock := scheduler.scheduleLocks[scaleSet]
	if lock == nil {
		lock = &sync.Mutex{}
		scheduler.scheduleLocks[scaleSet] = lock
	}
	return lock
}

func (scheduler *clusterScheduler) session(key repositoryScaleSetSessionKey) *repositoryScaleSetSession {
	scheduler.sessionsMutex.RLock()
	defer scheduler.sessionsMutex.RUnlock()
	return scheduler.sessions[key]
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
	session := scheduler.session(key)
	if session != nil {
		return scheduler.acquireAssignment(ctx, assignment, session)
	}
	if err := scheduler.store.DeferAssignment(
		ctx, assignment.ID, assignment.NodeID, assignment.State, time.Now(),
	); err != nil {
		return fmt.Errorf("defer acquisition without a repository scale-set session: %w", err)
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
	if assignment.IdentityFree {
		return scheduleProgressed, nil
	}
	session := scheduler.session(repositoryScaleSetSessionKey{repository: assignment.Repository, scaleSet: scaleSet})
	if session == nil {
		if releaseErr := scheduler.store.DeferAssignment(
			ctx, assignment.ID, assignment.NodeID, cluster.AssignmentReserved, time.Now(),
		); releaseErr != nil {
			return scheduleDrained, fmt.Errorf("defer demand without a repository scale-set session: %w", releaseErr)
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
		return scheduler.deferUnacquiredAssignment(ctx, assignment, err)
	}
	return scheduler.confirmAcquisition(ctx, assignment.ID)
}

func (scheduler *clusterScheduler) deferUnacquiredAssignment(
	ctx context.Context,
	assignment *cluster.Assignment,
	acquireErr error,
) error {
	deferCtx, cancelDefer := context.WithTimeout(context.WithoutCancel(ctx), clusterCoordinationTimeout)
	deferErr := scheduler.store.DeferAssignment(
		deferCtx, assignment.ID, assignment.NodeID, cluster.AssignmentAcquiring, time.Now(),
	)
	cancelDefer()
	if deferErr != nil {
		return errors.Join(acquireErr, fmt.Errorf("defer unacquired clustered demand: %w", deferErr))
	}
	return acquireErr
}

func (scheduler *clusterScheduler) confirmAcquisition(ctx context.Context, assignmentID cluster.AssignmentID) error {
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
