package daemon

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const assignedJobRecoveryWorkerCount = 4

type assignedJobRecoveryWork struct {
	recovery *clusterAssignedJobRecovery
	trigger  string
}

type assignedJobRecoveryExecutor struct {
	attemptTimeout time.Duration
	cancel         context.CancelFunc
	safetyTicks    <-chan time.Time
	stopSafety     func()
	notify         chan struct{}
	mutex          sync.Mutex
	active         map[repositoryScaleSetSessionKey]*clusterAssignedJobRecovery
	pending        map[repositoryScaleSetSessionKey]assignedJobRecoveryWork
	running        map[repositoryScaleSetSessionKey]struct{}
	failed         map[repositoryScaleSetSessionKey]*clusterAssignedJobRecovery
	workers        sync.WaitGroup
}

func newAssignedJobRecoveryExecutor(ctx context.Context, attemptTimeout time.Duration) *assignedJobRecoveryExecutor {
	safetyTicker := time.NewTicker(assignedJobRecoveryInterval)
	return newAssignedJobRecoveryExecutorWithSafetyTicks(ctx, attemptTimeout, safetyTicker.C, safetyTicker.Stop)
}

func newAssignedJobRecoveryExecutorWithSafetyTicks(
	ctx context.Context,
	attemptTimeout time.Duration,
	safetyTicks <-chan time.Time,
	stopSafety func(),
) *assignedJobRecoveryExecutor {
	workerCtx, cancel := context.WithCancel(ctx)
	executor := &assignedJobRecoveryExecutor{
		attemptTimeout: attemptTimeout, cancel: cancel,
		safetyTicks: safetyTicks, stopSafety: stopSafety,
		notify:  make(chan struct{}, assignedJobRecoveryWorkerCount),
		active:  make(map[repositoryScaleSetSessionKey]*clusterAssignedJobRecovery),
		pending: make(map[repositoryScaleSetSessionKey]assignedJobRecoveryWork),
		running: make(map[repositoryScaleSetSessionKey]struct{}),
		failed:  make(map[repositoryScaleSetSessionKey]*clusterAssignedJobRecovery),
	}
	executor.workers.Add(assignedJobRecoveryWorkerCount + 1)
	for workerIndex := 0; workerIndex < assignedJobRecoveryWorkerCount; workerIndex++ {
		go executor.runWorker(workerCtx)
	}
	go executor.runSafetyLoop(workerCtx)
	return executor
}

func (executor *assignedJobRecoveryExecutor) register(recovery *clusterAssignedJobRecovery) {
	executor.mutex.Lock()
	executor.active[recovery.key()] = recovery
	executor.mutex.Unlock()
}

func (executor *assignedJobRecoveryExecutor) unregister(recovery *clusterAssignedJobRecovery) {
	key := recovery.key()
	executor.mutex.Lock()
	if executor.active[key] == recovery {
		delete(executor.active, key)
		delete(executor.pending, key)
		delete(executor.failed, key)
	}
	executor.mutex.Unlock()
}

func (executor *assignedJobRecoveryExecutor) enqueue(
	ctx context.Context,
	recovery *clusterAssignedJobRecovery,
	trigger string,
) {
	key := recovery.key()
	executor.mutex.Lock()
	if executor.active[key] != recovery {
		executor.mutex.Unlock()
		return
	}
	phase := "queued"
	if _, pending := executor.pending[key]; pending {
		phase = "coalesced"
	}
	if _, running := executor.running[key]; running {
		phase = "coalesced"
	}
	executor.pending[key] = assignedJobRecoveryWork{recovery: recovery, trigger: trigger}
	queueDepth := len(executor.pending)
	activeWorkers := len(executor.running)
	executor.mutex.Unlock()
	logAction(ctx, effectiveLogger(recovery.logger), slog.LevelInfo,
		"assigned_recovery_trigger", phase,
		slog.String("repository", recovery.repository), slog.String("scaleSet", recovery.scaleSet),
		slog.String("trigger", trigger), slog.Int64("epoch", recovery.controllerEpoch),
		slog.Int("queueDepth", queueDepth), slog.Int("activeWorkers", activeWorkers))
	executor.signal()
}

func (executor *assignedJobRecoveryExecutor) runWorker(ctx context.Context) {
	defer executor.workers.Done()
	for {
		work, available := executor.next(ctx)
		if !available {
			return
		}
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, executor.attemptTimeout)
		stopSessionCancellation := work.recovery.cancelAttemptWhenStopped(cancelAttempt)
		err := work.recovery.reconcileLatest(attemptCtx, work.trigger)
		stopSessionCancellation()
		cancelAttempt()
		executor.complete(work, err)
	}
}

func (executor *assignedJobRecoveryExecutor) next(ctx context.Context) (assignedJobRecoveryWork, bool) {
	for {
		executor.mutex.Lock()
		for key, work := range executor.pending {
			if _, running := executor.running[key]; running {
				continue
			}
			delete(executor.pending, key)
			executor.running[key] = struct{}{}
			executor.mutex.Unlock()
			return work, true
		}
		executor.mutex.Unlock()
		select {
		case <-ctx.Done():
			return assignedJobRecoveryWork{}, false
		case <-executor.notify:
		}
	}
}

func (executor *assignedJobRecoveryExecutor) complete(work assignedJobRecoveryWork, err error) {
	key := work.recovery.key()
	executor.mutex.Lock()
	delete(executor.running, key)
	if executor.active[key] == work.recovery {
		if err != nil {
			executor.failed[key] = work.recovery
		} else {
			delete(executor.failed, key)
		}
	}
	pending := len(executor.pending) > 0
	executor.mutex.Unlock()
	if pending {
		executor.signal()
	}
}

func (executor *assignedJobRecoveryExecutor) runSafetyLoop(ctx context.Context) {
	defer executor.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-executor.safetyTicks:
			executor.retryFailed(ctx)
		}
	}
}

func (executor *assignedJobRecoveryExecutor) retryFailed(ctx context.Context) {
	executor.mutex.Lock()
	failed := make([]*clusterAssignedJobRecovery, 0, len(executor.failed))
	for _, recovery := range executor.failed {
		failed = append(failed, recovery)
	}
	pending := len(executor.pending) > 0
	executor.mutex.Unlock()
	for _, recovery := range failed {
		executor.enqueue(ctx, recovery, "safety")
	}
	if pending {
		executor.signal()
	}
}

func (executor *assignedJobRecoveryExecutor) wakeAssigned(ctx context.Context) {
	executor.mutex.Lock()
	active := make([]*clusterAssignedJobRecovery, 0, len(executor.active))
	for _, recovery := range executor.active {
		active = append(active, recovery)
	}
	executor.mutex.Unlock()
	for _, recovery := range active {
		if recovery.hasAssignedJobs() {
			executor.enqueue(ctx, recovery, "heartbeat")
		}
	}
}

func (executor *assignedJobRecoveryExecutor) signal() {
	select {
	case executor.notify <- struct{}{}:
	default:
	}
}

func (executor *assignedJobRecoveryExecutor) stop() {
	executor.stopSafety()
	executor.cancel()
	executor.workers.Wait()
}
