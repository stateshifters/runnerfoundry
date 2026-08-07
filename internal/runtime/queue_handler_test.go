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

func TestQueueHandlerWakesProcessingOnlyAfterDurableEnqueue(t *testing.T) {
	queued := make(chan core.RunnerPool, 1)
	queueErr := error(nil)
	store := &jobStoreFake{enqueue: func(context.Context, core.JobRequest) (core.Job, bool, error) {
		return core.Job{}, true, queueErr
	}}
	handler := runtimeengine.NewQueueHandlerWithWake(store, func(runnerPool core.RunnerPool) { queued <- runnerPool })
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-1"}, RunnerPool: "linux"}

	if err := handler.Request(context.Background(), request); err != nil {
		t.Fatalf("queue request: %v", err)
	}
	select {
	case runnerPool := <-queued:
		if runnerPool != request.RunnerPool {
			t.Fatalf("wake Runner Pool = %q, want %q", runnerPool, request.RunnerPool)
		}
	default:
		t.Fatal("durable enqueue did not wake processing")
	}

	queueErr = errors.New("queue unavailable")
	if err := handler.Request(context.Background(), request); err == nil {
		t.Fatal("failed enqueue unexpectedly succeeded")
	}
	select {
	case runnerPool := <-queued:
		t.Fatalf("failed enqueue woke Runner Pool %q", runnerPool)
	default:
	}
}

func TestQueueHandlerDoesNotWakeForDurableNonQueuedOutcome(t *testing.T) {
	woken := false
	handler := runtimeengine.NewQueueHandlerWithWake(
		queueOutcomeStoreFake{outcome: core.QueueProcessing},
		func(core.RunnerPool) { woken = true },
	)
	if err := handler.Request(context.Background(), core.JobRequest{RunnerPool: "linux"}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if woken {
		t.Fatal("processing source outcome woke queued-job processing")
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

type queueOutcomeStoreFake struct {
	outcome core.QueueOutcome
}

func (fake queueOutcomeStoreFake) Queue(context.Context, core.JobRequest) (core.EnqueueResult, error) {
	return core.EnqueueResult{Outcome: fake.outcome}, nil
}
func (queueOutcomeStoreFake) MarkRunnerStarted(context.Context, core.SourceReference) error {
	return nil
}
func (queueOutcomeStoreFake) TerminalizeSource(context.Context, core.SourceReference) error {
	return nil
}
