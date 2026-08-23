package runtime_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
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
	if _, err := handler.Request(context.Background(), request); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
}

func TestDurableStateErrorPreservesCauseAndSafeContext(t *testing.T) {
	cause := errors.New("driver detail")
	err := runtimeengine.NewDurableStateError("postgresql", "reconcile_assigned_demand", cause)
	var stateErr runtimeengine.DurableStateError
	if !errors.As(err, &stateErr) || !errors.Is(err, cause) ||
		stateErr.StateBackend() != "postgresql" || stateErr.StateOperation() != "reconcile_assigned_demand" {
		t.Fatalf("durable state error = %v", err)
	}
	if strings.Contains(err.Error(), cause.Error()) {
		t.Fatalf("durable state error disclosed driver detail: %v", err)
	}
}

func TestQueueHandlerPersistsLifecycleEventsBySource(t *testing.T) {
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	want := core.JobIdentity{Source: source}
	var started, terminal core.JobIdentity
	store := &jobStoreFake{
		started: func(_ context.Context, actual core.JobIdentity) (core.ObservationResult, error) {
			started = actual
			return core.ObservationResult{Outcome: core.ObservationApplied}, nil
		},
		terminalize: func(_ context.Context, actual core.JobIdentity) (core.ObservationResult, error) {
			terminal = actual
			return core.ObservationResult{Outcome: core.ObservationApplied}, nil
		},
	}
	handler := runtimeengine.NewQueueHandler(store)
	if _, err := handler.RunnerStarted(context.Background(), core.RunnerObservation{Source: source}); err != nil {
		t.Fatalf("RunnerStarted() error = %v", err)
	}
	if _, err := handler.Terminalize(context.Background(), core.RunnerObservation{Source: source}); err != nil {
		t.Fatalf("Terminalize() error = %v", err)
	}
	if started != want || terminal != want {
		t.Fatalf("lifecycle identities: started=%#v terminal=%#v, want %#v", started, terminal, want)
	}
}

func TestQueueHandlerResolvesCanonicalRunnerNameToJobID(t *testing.T) {
	jobID := uuid.MustParse("a73851b8-7df5-4d4c-bc8e-2381926746d8")
	store := &jobStoreFake{started: func(_ context.Context, actual core.JobIdentity) (core.ObservationResult, error) {
		if actual != (core.JobIdentity{JobID: jobID}) {
			t.Fatalf("started identity = %#v", actual)
		}
		return core.ObservationResult{Outcome: core.ObservationApplied}, nil
	}}
	handler := runtimeengine.NewQueueHandler(store)

	for _, name := range []core.RunnerName{
		core.RunnerName("runnerfoundry-" + jobID.String()),
		core.AttemptRunnerName(jobID, uuid.New()),
	} {
		result, err := handler.RunnerStarted(context.Background(), core.RunnerObservation{RunnerName: name})
		if err != nil || result.Outcome != core.ObservationApplied {
			t.Fatalf("RunnerStarted(%q) = %#v, %v", name, result, err)
		}
	}
}

func TestQueueHandlerPreservesMatchingSourceAndRunnerName(t *testing.T) {
	jobID := uuid.MustParse("a73851b8-7df5-4d4c-bc8e-2381926746d8")
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	store := &jobStoreFake{terminalize: func(_ context.Context, actual core.JobIdentity) (core.ObservationResult, error) {
		want := core.JobIdentity{Source: source, JobID: jobID}
		if actual != want {
			t.Fatalf("terminal identity = %#v, want %#v", actual, want)
		}
		return core.ObservationResult{Outcome: core.ObservationApplied}, nil
	}}
	handler := runtimeengine.NewQueueHandler(store)

	_, err := handler.Terminalize(context.Background(), core.RunnerObservation{
		Source: source, RunnerName: core.RunnerName("runnerfoundry-" + jobID.String()),
	})
	if err != nil {
		t.Fatalf("Terminalize() error = %v", err)
	}
}

func TestQueueHandlerAcknowledgesLegacyRunnerNameAsUnmatched(t *testing.T) {
	handler := runtimeengine.NewQueueHandler(&jobStoreFake{})
	result, err := handler.Terminalize(context.Background(), core.RunnerObservation{RunnerName: "legacy-runner-name"})
	if err != nil || result.Outcome != core.ObservationUnmatched {
		t.Fatalf("Terminalize() = %#v, %v", result, err)
	}
	result, err = handler.RunnerStarted(context.Background(), core.RunnerObservation{RunnerName: "legacy-runner-name"})
	if err != nil || result.Outcome != core.ObservationUnmatched {
		t.Fatalf("RunnerStarted() = %#v, %v", result, err)
	}
}

func TestQueueHandlerRejectsMalformedCanonicalRunnerName(t *testing.T) {
	handler := runtimeengine.NewQueueHandler(&jobStoreFake{})
	for _, name := range []core.RunnerName{
		"runnerfoundry-not-a-uuid",
		"runnerfoundry-v2-short",
		"runnerfoundry-v2-!!!!!!!!!!!!!!!!!!!!!!-AAAAAAAAAAAAAAAAAAAAAA",
		"runnerfoundry-v2-AAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAA",
		"runnerfoundry-v2-AAAAAAAAAAAAAAAAAAAAAQ-!!!!!!!!!!!!!!!!!!!!!!",
	} {
		_, err := handler.Terminalize(context.Background(), core.RunnerObservation{RunnerName: name})
		if !errors.Is(err, core.ErrInvalidJobIdentity) {
			t.Fatalf("Terminalize(%q) error = %v", name, err)
		}
	}
}

func TestQueueHandlerRejectsPartialSourceIdentity(t *testing.T) {
	handler := runtimeengine.NewQueueHandler(&jobStoreFake{})
	_, err := handler.Terminalize(context.Background(), core.RunnerObservation{
		Source: core.SourceReference{Adapter: "github"},
	})
	if !errors.Is(err, core.ErrInvalidJobIdentity) {
		t.Fatalf("Terminalize() error = %v", err)
	}
	if _, err = handler.RunnerStarted(context.Background(), core.RunnerObservation{}); !errors.Is(
		err, core.ErrInvalidJobIdentity,
	) {
		t.Fatalf("RunnerStarted() error = %v", err)
	}
}

func TestQueueHandlerUsesSourceWithLegacyRunnerName(t *testing.T) {
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	store := &jobStoreFake{started: func(_ context.Context, identity core.JobIdentity) (core.ObservationResult, error) {
		if identity != (core.JobIdentity{Source: source}) {
			t.Fatalf("job identity = %#v", identity)
		}
		return core.ObservationResult{Outcome: core.ObservationApplied}, nil
	}}
	result, err := runtimeengine.NewQueueHandler(store).RunnerStarted(context.Background(), core.RunnerObservation{
		Source: source, RunnerName: "legacy-runner-name",
	})
	if err != nil || result.Outcome != core.ObservationApplied {
		t.Fatalf("RunnerStarted() = %#v, %v", result, err)
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

	if _, err := handler.Request(context.Background(), request); err != nil {
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
	if _, err := handler.Request(context.Background(), request); err == nil {
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
	if _, err := handler.Request(context.Background(), core.JobRequest{RunnerPool: "linux"}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if woken {
		t.Fatal("processing source outcome woke queued-job processing")
	}
}

func TestQueueHandlerReportsStoreFailures(t *testing.T) {
	wantErr := errors.New("state unavailable")
	store := &jobStoreFake{
		enqueue: func(context.Context, core.JobRequest) (core.Job, bool, error) { return core.Job{}, false, wantErr },
		started: func(context.Context, core.JobIdentity) (core.ObservationResult, error) {
			return core.ObservationResult{}, wantErr
		},
		terminalize: func(context.Context, core.JobIdentity) (core.ObservationResult, error) {
			return core.ObservationResult{}, wantErr
		},
	}
	handler := runtimeengine.NewQueueHandler(store)
	if _, err := handler.Request(context.Background(), core.JobRequest{}); !errors.Is(err, wantErr) {
		t.Fatalf("Request() error = %v", err)
	}
	if _, err := handler.RunnerStarted(context.Background(), core.RunnerObservation{
		Source: core.SourceReference{Adapter: "github", ID: "request-1"},
	}); !errors.Is(err, wantErr) {
		t.Fatalf("RunnerStarted() error = %v", err)
	}
	if _, err := handler.Terminalize(context.Background(), core.RunnerObservation{
		Source: core.SourceReference{Adapter: "github", ID: "request-1"},
	}); !errors.Is(err, wantErr) {
		t.Fatalf("Terminalize() error = %v", err)
	}
}

func TestQueueHandlerClassifiesBackendFailureWithoutExposingDriverDetail(t *testing.T) {
	wantErr := errors.New("driver included a PostgreSQL DSN")
	store := &jobStoreFake{enqueue: func(context.Context, core.JobRequest) (core.Job, bool, error) {
		return core.Job{}, false, wantErr
	}}
	handler := runtimeengine.NewQueueHandlerWithWakeAndBackend(store, nil, "postgresql")

	_, err := handler.Request(context.Background(), core.JobRequest{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Request() error = %v", err)
	}
	var stateFailure interface {
		StateBackend() string
		StateOperation() string
	}
	if !errors.As(err, &stateFailure) || stateFailure.StateBackend() != "postgresql" ||
		stateFailure.StateOperation() != "enqueue_job" {
		t.Fatalf("state failure context = %#v", stateFailure)
	}
	if strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("classified state error exposed driver detail: %q", err)
	}
}

func TestQueueHandlerClassifiesLifecycleBackendFailure(t *testing.T) {
	wantErr := errors.New("driver detail")
	store := &jobStoreFake{terminalize: func(context.Context, core.JobIdentity) (core.ObservationResult, error) {
		return core.ObservationResult{}, wantErr
	}}
	handler := runtimeengine.NewQueueHandlerWithWakeAndBackend(store, nil, "sqlite")

	_, err := handler.Terminalize(context.Background(), core.RunnerObservation{
		Source: core.SourceReference{Adapter: "github", ID: "request-1"},
	})
	var stateFailure interface {
		StateBackend() string
		StateOperation() string
	}
	if !errors.As(err, &stateFailure) || stateFailure.StateBackend() != "sqlite" ||
		stateFailure.StateOperation() != "terminalize_job" {
		t.Fatalf("state failure context = %#v", stateFailure)
	}
}

type queueOutcomeStoreFake struct {
	outcome core.QueueOutcome
}

func (fake queueOutcomeStoreFake) Queue(context.Context, core.JobRequest) (core.EnqueueResult, error) {
	return core.EnqueueResult{Outcome: fake.outcome}, nil
}
func (queueOutcomeStoreFake) MarkRunnerStarted(context.Context, core.JobIdentity) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}
func (queueOutcomeStoreFake) Terminalize(context.Context, core.JobIdentity) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}
