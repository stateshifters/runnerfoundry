package runtime

import (
	"context"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

// QueueHandler translates durable source assignments and cancellations into
// the source-neutral queue without owning runner execution.
type QueueHandler struct {
	store        core.JobQueueStore
	wakeUp       func(core.RunnerPool)
	stateBackend string
}

// DurableStateError identifies a failed state operation without exposing the
// backend driver's error text to observability code.
type DurableStateError struct {
	Backend   string
	Operation string
	cause     error
}

func (stateError DurableStateError) Error() string {
	return fmt.Sprintf("%s durable state %s failed", stateError.Backend, stateError.Operation)
}

func (stateError DurableStateError) Unwrap() error          { return stateError.cause }
func (stateError DurableStateError) StateBackend() string   { return stateError.Backend }
func (stateError DurableStateError) StateOperation() string { return stateError.Operation }

func NewQueueHandler(store core.JobQueueStore) *QueueHandler { return &QueueHandler{store: store} }

func NewQueueHandlerWithWake(store core.JobQueueStore, wakeUp func(core.RunnerPool)) *QueueHandler {
	return &QueueHandler{store: store, wakeUp: wakeUp}
}

func NewQueueHandlerWithWakeAndBackend(
	store core.JobQueueStore,
	wakeUp func(core.RunnerPool),
	stateBackend string,
) *QueueHandler {
	return &QueueHandler{store: store, wakeUp: wakeUp, stateBackend: stateBackend}
}

func (handler *QueueHandler) Request(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	result, err := handler.store.Queue(ctx, request)
	if err != nil {
		if handler.stateBackend != "" {
			return result, DurableStateError{Backend: handler.stateBackend, Operation: "enqueue_job", cause: err}
		}
		return result, fmt.Errorf("enqueue source request %q: %w", request.Source.ID, err)
	}
	if handler.wakeUp != nil && (result.Outcome == core.QueueQueued || result.Outcome == core.QueueAlreadyQueued) {
		handler.wakeUp(request.RunnerPool)
	}
	return result, nil
}

// RunnerStarted durably records that the source assigned the job to a runner.
// The store atomically resolves delayed start observations against queued work
// so callers can safely redeliver the same source event.
func (handler *QueueHandler) RunnerStarted(ctx context.Context, source core.SourceReference) error {
	if err := handler.store.MarkRunnerStarted(ctx, source); err != nil {
		return fmt.Errorf("mark source request %q runner started: %w", source.ID, err)
	}
	return nil
}

// Terminalize durably ends source demand. Provider cleanup remains
// owned by reconciliation, which retains processing until cleanup succeeds.
func (handler *QueueHandler) Terminalize(ctx context.Context, source core.SourceReference) error {
	if err := handler.store.TerminalizeSource(ctx, source); err != nil {
		return fmt.Errorf("terminalize source request %q: %w", source.ID, err)
	}
	return nil
}
