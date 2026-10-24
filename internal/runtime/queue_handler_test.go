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
		Source:     core.SourceReference{Adapter: "test-source", ID: "request-1"},
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
	if _, err := handler.IngestDemand(context.Background(), request); err != nil {
		t.Fatalf("IngestDemand() error = %v", err)
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
	source := core.SourceReference{Adapter: "test-source", ID: "request-1"}
	want := core.JobIdentity{Source: source}
	var accepted, terminal core.JobIdentity
	store := &jobStoreFake{
		accepted: func(_ context.Context, actual core.JobIdentity) (core.ObservationResult, error) {
			accepted = actual
			return core.ObservationResult{Outcome: core.ObservationApplied}, nil
		},
		terminalize: func(_ context.Context, actual core.JobIdentity) (core.ObservationResult, error) {
			terminal = actual
			return core.ObservationResult{Outcome: core.ObservationApplied}, nil
		},
	}
	handler := runtimeengine.NewQueueHandler(store)
	if _, err := handler.ObserveRunnerAcceptance(context.Background(), core.RunnerObservation{Source: source}); err != nil {
		t.Fatalf("ObserveRunnerAcceptance() error = %v", err)
	}
	if _, err := handler.ObserveTerminal(context.Background(), core.RunnerObservation{Source: source}); err != nil {
		t.Fatalf("ObserveTerminal() error = %v", err)
	}
	if accepted != want || terminal != want {
		t.Fatalf("lifecycle identities: accepted=%#v terminal=%#v, want %#v", accepted, terminal, want)
	}
}

func TestQueueHandlerConditionallyTerminalizesExactUnacceptedJob(t *testing.T) {
	want := core.JobIdentity{
		JobID:  uuid.MustParse("a73851b8-7df5-4d4c-bc8e-2381926746d8"),
		Source: core.SourceReference{Adapter: "github", ID: "aggregate-request"},
	}
	store := &jobStoreFake{terminalizeIfUnaccepted: func(
		_ context.Context,
		actual core.JobIdentity,
	) (core.ConditionalTerminalResult, error) {
		if actual != want {
			t.Fatalf("conditional terminal identity = %#v, want %#v", actual, want)
		}
		return core.ConditionalTerminalResult{Outcome: core.ConditionalTerminalApplied}, nil
	}}

	result, err := runtimeengine.NewQueueHandler(store).ObserveTerminalIfUnaccepted(context.Background(), want)

	if err != nil || result.Outcome != core.ConditionalTerminalApplied {
		t.Fatalf("conditional terminal result = (%#v, %v)", result, err)
	}
}

func TestQueueHandlerRejectsIncompleteConditionalTerminalIdentity(t *testing.T) {
	handler := runtimeengine.NewQueueHandler(&jobStoreFake{})
	for _, identity := range []core.JobIdentity{
		{Source: core.SourceReference{Adapter: "github", ID: "aggregate"}},
		{JobID: uuid.MustParse("a73851b8-7df5-4d4c-bc8e-2381926746d8")},
		{JobID: uuid.MustParse("a73851b8-7df5-4d4c-bc8e-2381926746d8"),
			Source: core.SourceReference{Adapter: "github"}},
	} {
		if _, err := handler.ObserveTerminalIfUnaccepted(context.Background(), identity); !errors.Is(
			err, core.ErrInvalidJobIdentity,
		) {
			t.Fatalf("ObserveTerminalIfUnaccepted(%#v) error = %v", identity, err)
		}
	}
}

func TestQueueHandlerClassifiesConditionalTerminalBackendFailure(t *testing.T) {
	wantErr := errors.New("driver detail")
	store := &jobStoreFake{terminalizeIfUnaccepted: func(
		context.Context,
		core.JobIdentity,
	) (core.ConditionalTerminalResult, error) {
		return core.ConditionalTerminalResult{}, wantErr
	}}
	handler := runtimeengine.NewQueueHandlerWithWakeAndBackend(store, nil, "postgresql")
	_, err := handler.ObserveTerminalIfUnaccepted(context.Background(), core.JobIdentity{
		JobID:  uuid.MustParse("a73851b8-7df5-4d4c-bc8e-2381926746d8"),
		Source: core.SourceReference{Adapter: "github", ID: "aggregate"},
	})
	var stateFailure interface {
		StateBackend() string
		StateOperation() string
	}
	if !errors.As(err, &stateFailure) || stateFailure.StateBackend() != "postgresql" ||
		stateFailure.StateOperation() != "terminalize_unaccepted_job" || !errors.Is(err, wantErr) {
		t.Fatalf("conditional terminal error = %v", err)
	}
}

func TestQueueHandlerResolvesCanonicalRunnerNameToJobID(t *testing.T) {
	jobID := uuid.MustParse("a73851b8-7df5-4d4c-bc8e-2381926746d8")
	attemptID := core.AttemptID(uuid.MustParse("b73851b8-7df5-4d4c-bc8e-2381926746d8"))
	store := &jobStoreFake{accepted: func(_ context.Context, actual core.JobIdentity) (core.ObservationResult, error) {
		if actual != (core.JobIdentity{JobID: jobID, AttemptID: attemptID}) {
			t.Fatalf("accepted identity = %#v", actual)
		}
		return core.ObservationResult{Outcome: core.ObservationApplied}, nil
	}}
	handler := runtimeengine.NewQueueHandler(store)

	for _, name := range []core.RunnerName{core.AttemptRunnerName(jobID, attemptID)} {
		result, err := handler.ObserveRunnerAcceptance(context.Background(), core.RunnerObservation{RunnerName: name})
		if err != nil || result.Outcome != core.ObservationApplied {
			t.Fatalf("ObserveRunnerAcceptance(%q) = %#v, %v", name, result, err)
		}
	}
}

func TestQueueHandlerPreservesMatchingSourceAndRunnerName(t *testing.T) {
	jobID := uuid.MustParse("a73851b8-7df5-4d4c-bc8e-2381926746d8")
	attemptID := core.AttemptID(uuid.MustParse("b73851b8-7df5-4d4c-bc8e-2381926746d8"))
	source := core.SourceReference{Adapter: "test-source", ID: "request-1"}
	store := &jobStoreFake{terminalize: func(_ context.Context, actual core.JobIdentity) (core.ObservationResult, error) {
		want := core.JobIdentity{Source: source, JobID: jobID, AttemptID: attemptID}
		if actual != want {
			t.Fatalf("terminal identity = %#v, want %#v", actual, want)
		}
		return core.ObservationResult{Outcome: core.ObservationApplied}, nil
	}}
	handler := runtimeengine.NewQueueHandler(store)

	_, err := handler.ObserveTerminal(context.Background(), core.RunnerObservation{
		Source: source, RunnerName: core.AttemptRunnerName(jobID, attemptID),
	})
	if err != nil {
		t.Fatalf("ObserveTerminal() error = %v", err)
	}
}

func TestQueueHandlerAcknowledgesUnownedRunnerNameAsUnmatched(t *testing.T) {
	handler := runtimeengine.NewQueueHandler(&jobStoreFake{})
	result, err := handler.ObserveTerminal(context.Background(), core.RunnerObservation{RunnerName: "external-runner-name"})
	if err != nil || result.Outcome != core.ObservationUnmatched {
		t.Fatalf("ObserveTerminal() = %#v, %v", result, err)
	}
	result, err = handler.ObserveRunnerAcceptance(context.Background(), core.RunnerObservation{RunnerName: "external-runner-name"})
	if err != nil || result.Outcome != core.ObservationUnmatched {
		t.Fatalf("ObserveRunnerAcceptance() = %#v, %v", result, err)
	}
}

func TestQueueHandlerRejectsMalformedCanonicalRunnerName(t *testing.T) {
	handler := runtimeengine.NewQueueHandler(&jobStoreFake{})
	for _, name := range []core.RunnerName{
		"runnerfoundry-v2-short",
		"runnerfoundry-v2-!!!!!!!!!!!!!!!!!!!!!!-AAAAAAAAAAAAAAAAAAAAAA",
		"runnerfoundry-v2-AAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAA",
		"runnerfoundry-v2-AAAAAAAAAAAAAAAAAAAAAQ-!!!!!!!!!!!!!!!!!!!!!!",
	} {
		_, err := handler.ObserveTerminal(context.Background(), core.RunnerObservation{RunnerName: name})
		if !errors.Is(err, core.ErrInvalidRunnerName) {
			t.Fatalf("ObserveTerminal(%q) error = %v", name, err)
		}
	}
}

func TestQueueHandlerRejectsPartialSourceIdentity(t *testing.T) {
	handler := runtimeengine.NewQueueHandler(&jobStoreFake{})
	_, err := handler.ObserveTerminal(context.Background(), core.RunnerObservation{
		Source: core.SourceReference{Adapter: "test-source"},
	})
	if !errors.Is(err, core.ErrInvalidJobIdentity) {
		t.Fatalf("ObserveTerminal() error = %v", err)
	}
	if _, err = handler.ObserveRunnerAcceptance(context.Background(), core.RunnerObservation{}); !errors.Is(
		err, core.ErrInvalidJobIdentity,
	) {
		t.Fatalf("ObserveRunnerAcceptance() error = %v", err)
	}
}

func TestQueueHandlerUsesSourceWithUnownedRunnerName(t *testing.T) {
	source := core.SourceReference{Adapter: "test-source", ID: "request-1"}
	store := &jobStoreFake{accepted: func(_ context.Context, identity core.JobIdentity) (core.ObservationResult, error) {
		if identity != (core.JobIdentity{Source: source}) {
			t.Fatalf("job identity = %#v", identity)
		}
		return core.ObservationResult{Outcome: core.ObservationApplied}, nil
	}}
	result, err := runtimeengine.NewQueueHandler(store).ObserveRunnerAcceptance(context.Background(), core.RunnerObservation{
		Source: source, RunnerName: "external-runner-name",
	})
	if err != nil || result.Outcome != core.ObservationApplied {
		t.Fatalf("ObserveRunnerAcceptance() = %#v, %v", result, err)
	}
}

func TestQueueHandlerWakesProcessingOnlyAfterDurableEnqueue(t *testing.T) {
	queued := make(chan core.RunnerPool, 1)
	queueErr := error(nil)
	store := &jobStoreFake{enqueue: func(context.Context, core.JobRequest) (core.Job, bool, error) {
		return core.Job{}, true, queueErr
	}}
	handler := runtimeengine.NewQueueHandlerWithWake(store, func(runnerPool core.RunnerPool) { queued <- runnerPool })
	request := core.JobRequest{Source: core.SourceReference{Adapter: "test-source", ID: "request-1"}, RunnerPool: "linux"}

	if _, err := handler.IngestDemand(context.Background(), request); err != nil {
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
	if _, err := handler.IngestDemand(context.Background(), request); err == nil {
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
	if _, err := handler.IngestDemand(context.Background(), core.JobRequest{RunnerPool: "linux"}); err != nil {
		t.Fatalf("IngestDemand() error = %v", err)
	}
	if woken {
		t.Fatal("processing source outcome woke queued-job processing")
	}
}

func TestQueueHandlerReportsStoreFailures(t *testing.T) {
	wantErr := errors.New("state unavailable")
	store := &jobStoreFake{
		enqueue: func(context.Context, core.JobRequest) (core.Job, bool, error) { return core.Job{}, false, wantErr },
		accepted: func(context.Context, core.JobIdentity) (core.ObservationResult, error) {
			return core.ObservationResult{}, wantErr
		},
		terminalize: func(context.Context, core.JobIdentity) (core.ObservationResult, error) {
			return core.ObservationResult{}, wantErr
		},
	}
	handler := runtimeengine.NewQueueHandler(store)
	if _, err := handler.IngestDemand(context.Background(), core.JobRequest{}); !errors.Is(err, wantErr) {
		t.Fatalf("IngestDemand() error = %v", err)
	}
	if _, err := handler.ObserveRunnerAcceptance(context.Background(), core.RunnerObservation{
		Source: core.SourceReference{Adapter: "test-source", ID: "request-1"},
	}); !errors.Is(err, wantErr) {
		t.Fatalf("ObserveRunnerAcceptance() error = %v", err)
	}
	if _, err := handler.ObserveTerminal(context.Background(), core.RunnerObservation{
		Source: core.SourceReference{Adapter: "test-source", ID: "request-1"},
	}); !errors.Is(err, wantErr) {
		t.Fatalf("ObserveTerminal() error = %v", err)
	}
}

func TestQueueHandlerReportsConditionalTerminalStoreFailure(t *testing.T) {
	wantErr := errors.New("state unavailable")
	store := &jobStoreFake{terminalizeIfUnaccepted: func(
		context.Context,
		core.JobIdentity,
	) (core.ConditionalTerminalResult, error) {
		return core.ConditionalTerminalResult{}, wantErr
	}}
	handler := runtimeengine.NewQueueHandler(store)
	if _, err := handler.ObserveTerminalIfUnaccepted(context.Background(), core.JobIdentity{
		JobID:  uuid.MustParse("a73851b8-7df5-4d4c-bc8e-2381926746d8"),
		Source: core.SourceReference{Adapter: "test-source", ID: "request-1"},
	}); !errors.Is(err, wantErr) {
		t.Fatalf("ObserveTerminalIfUnaccepted() error = %v", err)
	}
}

func TestQueueHandlerClassifiesBackendFailureWithoutExposingDriverDetail(t *testing.T) {
	wantErr := errors.New("driver included a PostgreSQL DSN")
	store := &jobStoreFake{enqueue: func(context.Context, core.JobRequest) (core.Job, bool, error) {
		return core.Job{}, false, wantErr
	}}
	handler := runtimeengine.NewQueueHandlerWithWakeAndBackend(store, nil, "postgresql")

	_, err := handler.IngestDemand(context.Background(), core.JobRequest{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("IngestDemand() error = %v", err)
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

	_, err := handler.ObserveTerminal(context.Background(), core.RunnerObservation{
		Source: core.SourceReference{Adapter: "test-source", ID: "request-1"},
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
func (queueOutcomeStoreFake) ObserveRunnerAcceptance(context.Context, core.JobIdentity) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}
func (queueOutcomeStoreFake) Terminalize(context.Context, core.JobIdentity) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}
func (queueOutcomeStoreFake) ObserveTerminalIfUnaccepted(context.Context, core.JobIdentity) (core.ConditionalTerminalResult, error) {
	return core.ConditionalTerminalResult{}, nil
}
