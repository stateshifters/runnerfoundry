package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
)

type queuedJobRequester interface {
	RequestWork(ctx context.Context, runnerPool core.RunnerPool) (runtimeengine.ProcessResult, error)
}

type queuedJobEligibilityGate struct {
	inventoryBarrier sync.RWMutex
	poolsMutex       sync.Mutex
	pools            map[core.RunnerPool]*queuedJobPoolGate
}

type queuedJobPoolGate struct {
	mutex    sync.Mutex
	eligible bool
}

func newQueuedJobEligibilityGate() *queuedJobEligibilityGate {
	return &queuedJobEligibilityGate{pools: make(map[core.RunnerPool]*queuedJobPoolGate)}
}

func (gate *queuedJobEligibilityGate) RefreshInventory(work func() error) error {
	gate.inventoryBarrier.Lock()
	defer gate.inventoryBarrier.Unlock()
	gate.markAllIneligible()
	return work()
}

func (gate *queuedJobEligibilityGate) MaintainPool(
	runnerPool core.RunnerPool,
	work func() (eligible bool, err error),
) error {
	pool := gate.pool(runnerPool)
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	eligible, err := work()
	pool.eligible = eligible
	return err
}

func (gate *queuedJobEligibilityGate) markAllIneligible() {
	gate.poolsMutex.Lock()
	pools := make([]*queuedJobPoolGate, 0, len(gate.pools))
	for _, pool := range gate.pools {
		pools = append(pools, pool)
	}
	gate.poolsMutex.Unlock()
	for _, pool := range pools {
		pool.mutex.Lock()
		pool.eligible = false
		pool.mutex.Unlock()
	}
}

func (gate *queuedJobEligibilityGate) pool(runnerPool core.RunnerPool) *queuedJobPoolGate {
	gate.poolsMutex.Lock()
	defer gate.poolsMutex.Unlock()
	pool := gate.pools[runnerPool]
	if pool == nil {
		pool = &queuedJobPoolGate{}
		gate.pools[runnerPool] = pool
	}
	return pool
}

func (gate *queuedJobEligibilityGate) requestIfEligible(
	ctx context.Context,
	runnerPool core.RunnerPool,
	requester queuedJobRequester,
) (runtimeengine.ProcessResult, bool, error) {
	gate.inventoryBarrier.RLock()
	defer gate.inventoryBarrier.RUnlock()
	pool := gate.pool(runnerPool)
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	if !pool.eligible {
		return runtimeengine.ProcessResult{}, false, nil
	}
	result, err := requester.RequestWork(ctx, runnerPool)
	if err != nil {
		return result, true, fmt.Errorf("request queued work for Runner Pool %q: %w", runnerPool, err)
	}
	return result, true, nil
}

type queuedJobProcessor struct {
	requester queuedJobRequester
	gate      *queuedJobEligibilityGate
	report    func(error)

	lifecycleMutex sync.Mutex
	stopMutex      sync.Mutex
	cancel         context.CancelFunc
	workers        map[core.RunnerPool]*queuedJobPoolWorker
}

type queuedJobPoolWorker struct {
	runnerPool core.RunnerPool
	wake       chan struct{}
	done       chan struct{}
	failed     bool
}

func newQueuedJobProcessorForPools(
	requester queuedJobRequester,
	gate *queuedJobEligibilityGate,
	report func(error),
	runnerPools []core.RunnerPool,
) *queuedJobProcessor {
	processor := &queuedJobProcessor{
		requester: requester,
		gate:      gate,
		report:    report,
		workers:   make(map[core.RunnerPool]*queuedJobPoolWorker, len(runnerPools)),
	}
	for _, runnerPool := range runnerPools {
		processor.workers[runnerPool] = newQueuedJobPoolWorker(runnerPool)
		gate.pool(runnerPool)
	}
	return processor
}

func newQueuedJobPoolWorker(runnerPool core.RunnerPool) *queuedJobPoolWorker {
	return &queuedJobPoolWorker{runnerPool: runnerPool, wake: make(chan struct{}, 1)}
}

func (processor *queuedJobProcessor) Start(ctx context.Context) error {
	processor.lifecycleMutex.Lock()
	defer processor.lifecycleMutex.Unlock()
	if processor.cancel != nil {
		return errors.New("queued-job processor is already started")
	}
	runContext, cancel := context.WithCancel(ctx)
	processor.cancel = cancel
	for _, worker := range processor.workers {
		processor.startWorker(runContext, worker)
	}
	return nil
}

func (processor *queuedJobProcessor) Stop(ctx context.Context) error {
	processor.stopMutex.Lock()
	defer processor.stopMutex.Unlock()
	processor.lifecycleMutex.Lock()
	cancel := processor.cancel
	if cancel == nil {
		processor.lifecycleMutex.Unlock()
		return nil
	}
	done := make([]<-chan struct{}, 0, len(processor.workers))
	for _, worker := range processor.workers {
		if worker.done != nil {
			done = append(done, worker.done)
		}
	}
	processor.lifecycleMutex.Unlock()
	cancel()
	for _, workerDone := range done {
		select {
		case <-workerDone:
		case <-ctx.Done():
			return fmt.Errorf("stop queued-job processor: %w", ctx.Err())
		}
	}
	processor.lifecycleMutex.Lock()
	processor.cancel = nil
	for _, worker := range processor.workers {
		worker.done = nil
	}
	processor.lifecycleMutex.Unlock()
	return nil
}

func (processor *queuedJobProcessor) startWorker(ctx context.Context, worker *queuedJobPoolWorker) {
	worker.done = make(chan struct{})
	go processor.runWorker(ctx, worker)
}

func (processor *queuedJobProcessor) runWorker(ctx context.Context, worker *queuedJobPoolWorker) {
	defer close(worker.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-worker.wake:
			processor.processRunnerPool(ctx, worker)
		}
	}
}

func (processor *queuedJobProcessor) processRunnerPool(ctx context.Context, worker *queuedJobPoolWorker) {
	result, eligible, err := processor.gate.requestIfEligible(ctx, worker.runnerPool, processor.requester)
	if !eligible {
		return
	}
	if err != nil {
		processor.reportProcessingFailure(ctx, worker, result, err)
		return
	}
	if worker.failed {
		reportOperational(processor.report, operationalEvent{
			operation: "job_processing", outcome: "recovered", runnerPool: worker.runnerPool,
		})
		worker.failed = false
	}
	if shouldContinueQueuedJobPass(result) {
		processor.wakeWorker(worker)
	}
}

func (processor *queuedJobProcessor) reportProcessingFailure(
	ctx context.Context,
	worker *queuedJobPoolWorker,
	result runtimeengine.ProcessResult,
	err error,
) {
	if ctx.Err() != nil {
		return
	}
	if !operationalErrorWasReported(err) {
		reportOperational(processor.report, operationalEvent{
			operation: "job_processing", outcome: "failed", runnerPool: worker.runnerPool, err: err,
		})
	}
	worker.failed = true
	if shouldContinueQueuedJobPass(result) {
		processor.wakeWorker(worker)
	}
}

func shouldContinueQueuedJobPass(result runtimeengine.ProcessResult) bool {
	return result.Outcome == runtimeengine.ProcessRunnerStarted ||
		result.Outcome == runtimeengine.ProcessProvisioningFailureReported ||
		(result.Outcome == runtimeengine.ProcessRetry && result.RetryOutcome == core.RetryRequeued)
}

func (processor *queuedJobProcessor) Wake(runnerPool core.RunnerPool) {
	processor.lifecycleMutex.Lock()
	worker := processor.workers[runnerPool]
	if worker != nil {
		processor.wakeWorker(worker)
	}
	processor.lifecycleMutex.Unlock()
}

func (*queuedJobProcessor) wakeWorker(worker *queuedJobPoolWorker) {
	select {
	case worker.wake <- struct{}{}:
	default:
	}
}
