package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

type queuedJobRequester interface {
	RequestWork(ctx context.Context, runnerPool core.RunnerPool) (runtimeengine.ProcessResult, error)
}

type queuedJobEligibilityGate struct {
	mutex    sync.Mutex
	eligible map[core.RunnerPool]struct{}
}

func newQueuedJobEligibilityGate() *queuedJobEligibilityGate {
	return &queuedJobEligibilityGate{eligible: make(map[core.RunnerPool]struct{})}
}

func (gate *queuedJobEligibilityGate) Maintain(work func(*queuedJobMaintenance)) {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	clear(gate.eligible)
	work(&queuedJobMaintenance{eligible: gate.eligible})
}

type queuedJobMaintenance struct {
	eligible map[core.RunnerPool]struct{}
}

func (maintenance *queuedJobMaintenance) MarkEligible(runnerPool core.RunnerPool) {
	maintenance.eligible[runnerPool] = struct{}{}
}

func (gate *queuedJobEligibilityGate) requestIfEligible(
	ctx context.Context,
	runnerPool core.RunnerPool,
	requester queuedJobRequester,
) (runtimeengine.ProcessResult, bool, error) {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	if _, eligible := gate.eligible[runnerPool]; !eligible {
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

	queueMutex sync.Mutex
	queue      []core.RunnerPool
	pending    map[core.RunnerPool]struct{}
	wake       chan struct{}

	lifecycleMutex sync.Mutex
	cancel         context.CancelFunc
	done           chan struct{}
	failed         bool
}

func newQueuedJobProcessor(
	requester queuedJobRequester,
	gate *queuedJobEligibilityGate,
	report func(error),
) *queuedJobProcessor {
	return &queuedJobProcessor{
		requester: requester,
		gate:      gate,
		report:    report,
		pending:   make(map[core.RunnerPool]struct{}),
		wake:      make(chan struct{}, 1),
	}
}

func (processor *queuedJobProcessor) Start(ctx context.Context) error {
	processor.lifecycleMutex.Lock()
	defer processor.lifecycleMutex.Unlock()
	if processor.done != nil {
		return errors.New("queued-job processor is already started")
	}
	runContext, cancel := context.WithCancel(ctx)
	processor.cancel = cancel
	processor.done = make(chan struct{})
	go processor.run(runContext, processor.done)
	return nil
}

func (processor *queuedJobProcessor) Stop(ctx context.Context) error {
	processor.lifecycleMutex.Lock()
	cancel, done := processor.cancel, processor.done
	processor.lifecycleMutex.Unlock()
	if done == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		processor.lifecycleMutex.Lock()
		if processor.done == done {
			processor.cancel = nil
			processor.done = nil
		}
		processor.lifecycleMutex.Unlock()
		return nil
	case <-ctx.Done():
		return fmt.Errorf("stop queued-job processor: %w", ctx.Err())
	}
}

func (processor *queuedJobProcessor) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	for {
		if ctx.Err() != nil {
			return
		}
		runnerPool, available := processor.dequeue()
		if available {
			processor.processRunnerPool(ctx, runnerPool)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-processor.wake:
		}
	}
}

func (processor *queuedJobProcessor) processRunnerPool(ctx context.Context, runnerPool core.RunnerPool) {
	result, eligible, err := processor.gate.requestIfEligible(ctx, runnerPool, processor.requester)
	if !eligible {
		return
	}
	if err != nil {
		processor.reportProcessingFailure(ctx, runnerPool, result, err)
		return
	}
	if processor.failed {
		reportOperational(processor.report, operationalEvent{
			operation: "job_processing", outcome: "recovered", runnerPool: runnerPool,
		})
		processor.failed = false
	}
	if shouldContinueQueuedJobPass(result) {
		processor.enqueue(runnerPool)
	}
}

func (processor *queuedJobProcessor) reportProcessingFailure(
	ctx context.Context,
	runnerPool core.RunnerPool,
	result runtimeengine.ProcessResult,
	err error,
) {
	if ctx.Err() != nil {
		return
	}
	reportOperational(processor.report, operationalEvent{
		operation: "job_processing", outcome: "failed", runnerPool: runnerPool, err: err,
	})
	processor.failed = true
	if shouldContinueQueuedJobPass(result) {
		processor.enqueue(runnerPool)
	}
}

func shouldContinueQueuedJobPass(result runtimeengine.ProcessResult) bool {
	return result.Outcome == runtimeengine.ProcessRunnerStarted ||
		(result.Outcome == runtimeengine.ProcessRetry && result.RetryOutcome == core.RetryRequeued)
}

func (processor *queuedJobProcessor) Wake(runnerPool core.RunnerPool) {
	processor.enqueue(runnerPool)
}

func (processor *queuedJobProcessor) enqueue(runnerPool core.RunnerPool) {
	processor.queueMutex.Lock()
	if _, exists := processor.pending[runnerPool]; !exists {
		processor.queue = append(processor.queue, runnerPool)
		processor.pending[runnerPool] = struct{}{}
	}
	processor.queueMutex.Unlock()
	select {
	case processor.wake <- struct{}{}:
	default:
	}
}

func (processor *queuedJobProcessor) dequeue() (core.RunnerPool, bool) {
	processor.queueMutex.Lock()
	defer processor.queueMutex.Unlock()
	if len(processor.queue) == 0 {
		return "", false
	}
	runnerPool := processor.queue[0]
	processor.queue = processor.queue[1:]
	delete(processor.pending, runnerPool)
	return runnerPool, true
}
