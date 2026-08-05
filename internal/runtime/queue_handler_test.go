package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestQueueHandlerPersistsAssignmentsAndCancellations(t *testing.T) {
	request := core.JobRequest{
		Source:     core.SourceReference{Adapter: "github", ID: "request-1"},
		RunnerPool: "linux", EnqueuedAt: time.Unix(1, 0),
	}
	store := &jobStoreFake{
		enqueue: func(_ context.Context, actual core.JobRequest) (core.Job, bool, error) {
			if actual != request {
				t.Fatalf("enqueue request = %#v", actual)
			}
			return core.Job{}, true, nil
		},
		cancel: func(_ context.Context, source core.SourceReference) error {
			if source != request.Source {
				t.Fatalf("cancel source = %#v", source)
			}
			return nil
		},
	}
	handler := runtimeengine.NewQueueHandler(store)
	if err := handler.Request(context.Background(), request); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if err := handler.Cancel(context.Background(), request.Source); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
}

func TestQueueHandlerReportsStoreFailures(t *testing.T) {
	wantErr := errors.New("state unavailable")
	store := &jobStoreFake{
		enqueue: func(context.Context, core.JobRequest) (core.Job, bool, error) { return core.Job{}, false, wantErr },
		cancel:  func(context.Context, core.SourceReference) error { return wantErr },
	}
	handler := runtimeengine.NewQueueHandler(store)
	if err := handler.Request(context.Background(), core.JobRequest{}); !errors.Is(err, wantErr) {
		t.Fatalf("Request() error = %v", err)
	}
	if err := handler.Cancel(context.Background(), core.SourceReference{}); !errors.Is(err, wantErr) {
		t.Fatalf("Cancel() error = %v", err)
	}
}
