package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestQueueHandlerPersistsAssignment(t *testing.T) {
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
	}
	handler := runtimeengine.NewQueueHandler(store)
	if err := handler.Request(context.Background(), request); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
}

func TestQueueHandlerPersistsRunnerStart(t *testing.T) {
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	store := &jobStoreFake{started: func(_ context.Context, actual core.SourceReference) error {
		if actual != source {
			t.Fatalf("started source = %#v", actual)
		}
		return nil
	}}
	handler := runtimeengine.NewQueueHandler(store)
	if err := handler.RunnerStarted(context.Background(), source); err != nil {
		t.Fatalf("RunnerStarted() error = %v", err)
	}
}

func TestQueueHandlerPersistsTerminalEvent(t *testing.T) {
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	store := &jobStoreFake{terminalize: func(_ context.Context, actual core.SourceReference) error {
		if actual != source {
			t.Fatalf("terminal source = %#v", actual)
		}
		return nil
	}}
	handler := runtimeengine.NewQueueHandler(store)
	if err := handler.Terminalize(context.Background(), source); err != nil {
		t.Fatalf("Terminalize() error = %v", err)
	}
}

func TestQueueHandlerReportsStoreFailures(t *testing.T) {
	wantErr := errors.New("state unavailable")
	store := &jobStoreFake{
		enqueue:     func(context.Context, core.JobRequest) (core.Job, bool, error) { return core.Job{}, false, wantErr },
		started:     func(context.Context, core.SourceReference) error { return wantErr },
		terminalize: func(context.Context, core.SourceReference) error { return wantErr },
	}
	handler := runtimeengine.NewQueueHandler(store)
	if err := handler.Request(context.Background(), core.JobRequest{}); !errors.Is(err, wantErr) {
		t.Fatalf("Request() error = %v", err)
	}
	if err := handler.RunnerStarted(context.Background(), core.SourceReference{}); !errors.Is(err, wantErr) {
		t.Fatalf("RunnerStarted() error = %v", err)
	}
	if err := handler.Terminalize(context.Background(), core.SourceReference{}); !errors.Is(err, wantErr) {
		t.Fatalf("Terminalize() error = %v", err)
	}
}
