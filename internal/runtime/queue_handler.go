package runtime

import (
	"context"
	"fmt"

	"github.com/meza/RunnerFoundry/internal/core"
)

// QueueHandler translates durable source assignments and cancellations into
// the source-neutral queue without owning runner execution.
type QueueHandler struct{ store core.JobStore }

func NewQueueHandler(store core.JobStore) *QueueHandler { return &QueueHandler{store: store} }

func (handler *QueueHandler) Request(ctx context.Context, request core.JobRequest) error {
	if _, _, err := handler.store.Enqueue(ctx, request); err != nil {
		return fmt.Errorf("enqueue source request %q: %w", request.Source.ID, err)
	}
	return nil
}

func (handler *QueueHandler) Cancel(ctx context.Context, source core.SourceReference) error {
	if err := handler.store.CancelQueued(ctx, source); err != nil {
		return fmt.Errorf("cancel queued source request %q: %w", source.ID, err)
	}
	return nil
}
