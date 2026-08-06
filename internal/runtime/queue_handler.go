package runtime

import (
	"context"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

// QueueHandler translates durable source assignments and cancellations into
// the source-neutral queue without owning runner execution.
type QueueHandler struct{ store core.JobQueueStore }

func NewQueueHandler(store core.JobQueueStore) *QueueHandler { return &QueueHandler{store: store} }

func (handler *QueueHandler) Request(ctx context.Context, request core.JobRequest) error {
	if _, _, err := handler.store.Enqueue(ctx, request); err != nil {
		return fmt.Errorf("enqueue source request %q: %w", request.Source.ID, err)
	}
	return nil
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
