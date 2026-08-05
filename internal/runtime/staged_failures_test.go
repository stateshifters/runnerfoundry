package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestFinalizedRequestRedeliveryIsAcknowledged(t *testing.T) {
	t.Parallel()

	store := &runtimeStoreFake{
		enqueue: func(context.Context, core.JobRequest) (core.JobRecord, bool, error) {
			return core.JobRecord{}, false, nil
		},
		lookupReceipt: func(context.Context, core.SourceReference) (core.Receipt, bool, error) {
			return core.Receipt{}, true, nil
		},
	}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), nil, runtimeTestID(1), fixedClock)
	if err := orchestrator.Request(context.Background(), core.JobRequest{}); err != nil {
		t.Fatalf("finalized redelivery: %v", err)
	}

	store.lookupReceipt = func(context.Context, core.SourceReference) (core.Receipt, bool, error) {
		return core.Receipt{}, false, nil
	}
	if err := orchestrator.Request(context.Background(), core.JobRequest{}); err == nil {
		t.Fatal("request without a job or receipt was acknowledged")
	}
	store.lookupReceipt = func(context.Context, core.SourceReference) (core.Receipt, bool, error) {
		return core.Receipt{}, false, errors.New("receipt unavailable")
	}
	if err := orchestrator.Request(context.Background(), core.JobRequest{}); err == nil {
		t.Fatal("receipt lookup failure was hidden")
	}
}

func TestRequestReportsDurableAcquisitionBeginFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("stage unavailable")
	record := core.JobRecord{Job: core.Job{ID: runtimeTestID(100)}, State: core.JobStateAttempting}
	store := &runtimeStoreFake{
		enqueue: func(context.Context, core.JobRequest) (core.JobRecord, bool, error) { return record, false, nil },
		beginAcquisition: func(context.Context, core.JobID, time.Time) (core.JobRecord, error) {
			return core.JobRecord{}, wantErr
		},
	}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), nil, runtimeTestID(1), fixedClock)
	requireErrorIs(t, "begin acquisition", orchestrator.Request(context.Background(), core.JobRequest{}), wantErr)
}

func TestSourceAcquisitionFailureIsTerminalAndAckable(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("source unavailable")
	record := core.JobRecord{Job: core.Job{ID: runtimeTestID(100)}, State: core.JobStateAttempting}
	store := &runtimeStoreFake{
		enqueue:          func(context.Context, core.JobRequest) (core.JobRecord, bool, error) { return record, false, nil },
		beginAcquisition: func(context.Context, core.JobID, time.Time) (core.JobRecord, error) { return record, nil },
		setResultPending: func(_ context.Context, _ core.JobID, result core.Result) (core.JobRecord, error) {
			return core.JobRecord{Job: record.Job, State: core.JobStateResultPending, Result: &result}, nil
		},
		finalize: func(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error) {
			return core.Receipt{}, nil
		},
	}
	reporter := resultReporterFake(func(context.Context, core.Job, core.Result) (core.ResultDelivery, error) {
		return core.ResultDeliveryReported, nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(reporter, sourceAcquirerFake(func(context.Context, core.Job) error {
		return wantErr
	}), nil), nil, runtimeTestID(1), fixedClock)

	requireNoError(t, "terminal acquisition failure", orchestrator.Request(context.Background(), core.JobRequest{}))
}

func TestRequestReportsAcquisitionConfirmationFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("confirmation unavailable")
	record := core.JobRecord{Job: core.Job{ID: runtimeTestID(100)}, State: core.JobStateAttempting}
	store := &runtimeStoreFake{
		enqueue:          func(context.Context, core.JobRequest) (core.JobRecord, bool, error) { return record, false, nil },
		beginAcquisition: func(context.Context, core.JobID, time.Time) (core.JobRecord, error) { return record, nil },
		confirmAcquisition: func(context.Context, core.JobID, time.Time) (core.JobRecord, error) {
			return core.JobRecord{}, wantErr
		},
	}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, sourceAcquirerFake(func(context.Context, core.Job) error { return nil }), nil), nil, runtimeTestID(1), fixedClock)
	requireErrorIs(t, "confirm acquisition", orchestrator.Request(context.Background(), core.JobRequest{}), wantErr)
}

func TestInterruptedAcquisitionRecoveryContinuesAcrossTransitionFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("transition unavailable")
	jobs := []core.Job{{ID: runtimeTestID(101)}, {ID: runtimeTestID(102)}}
	store := &runtimeStoreFake{
		pendingAcquisitions: func(context.Context, int) ([]core.JobRecord, error) {
			return []core.JobRecord{{Job: jobs[0]}, {Job: jobs[1]}}, nil
		},
		setResultPending: func(_ context.Context, jobID core.JobID, result core.Result) (core.JobRecord, error) {
			if jobID == jobs[0].ID {
				return core.JobRecord{}, wantErr
			}
			return core.JobRecord{Job: jobs[1], State: core.JobStateResultPending, Result: &result}, nil
		},
		finalize: func(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error) {
			return core.Receipt{}, nil
		},
	}
	reporter := resultReporterFake(func(context.Context, core.Job, core.Result) (core.ResultDelivery, error) {
		return core.ResultDeliveryReported, nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(reporter, nil, nil), nil, runtimeTestID(1), fixedClock)

	recovered, err := orchestrator.RecoverInterruptedAcquisitions(context.Background(), 10)
	requireEqual(t, "recovered acquisitions", recovered, 1)
	requireErrorIs(t, "recovery", err, wantErr)
	store.pendingAcquisitions = func(context.Context, int) ([]core.JobRecord, error) { return nil, wantErr }
	_, err = orchestrator.RecoverInterruptedAcquisitions(context.Background(), 10)
	requireErrorIs(t, "recovery lookup", err, wantErr)
}

func TestStartAcquiredTerminalizesBootstrapFailure(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("bootstrap unavailable")
	assertStartFailure(t, bootstrapGeneratorFake(func(context.Context, core.Job, string) (execution.Bootstrap, error) {
		return execution.Bootstrap{}, wantErr
	}), func(context.Context, execution.StartRequest) error { return nil }, wantErr)
}

func TestStartAcquiredTerminalizesProviderFailure(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("provider unavailable")
	assertStartFailure(t, bootstrapGeneratorFake(func(context.Context, core.Job, string) (execution.Bootstrap, error) {
		return execution.NewBootstrap([]byte("opaque")), nil
	}), func(context.Context, execution.StartRequest) error { return wantErr }, wantErr)
}

func assertStartFailure(
	t *testing.T,
	generator bootstrapGeneratorFake,
	start func(context.Context, execution.StartRequest) error,
	wantErr error,
) {
	t.Helper()
	job := core.Job{ID: runtimeTestID(103), RunnerPool: "linux"}
	store := &runtimeStoreFake{
		attemptsByMember: func(context.Context, core.MemberID, core.RunnerPool) ([]core.JobRecord, error) {
			return []core.JobRecord{{Job: job, AcquiredAt: time.Unix(1, 0)}}, nil
		},
		setResultPending: func(_ context.Context, _ core.JobID, result core.Result) (core.JobRecord, error) {
			return core.JobRecord{Job: job, State: core.JobStateResultPending, Result: &result}, nil
		},
		finalize: func(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error) {
			return core.Receipt{}, nil
		},
	}
	provider := &providerFake{
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) { return nil, nil },
		start:   start,
	}
	reporter := resultReporterFake(func(context.Context, core.Job, core.Result) (core.ResultDelivery, error) {
		return core.ResultDeliveryReported, nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(reporter, nil, generator), provider, runtimeTestID(1), fixedClock)

	started, err := orchestrator.StartAcquired(context.Background(), "linux")
	requireEqual(t, "started attempts", started, 0)
	requireErrorIs(t, "start failures", err, wantErr)
}

func TestStartAcquiredReportsAttemptAndProviderInventoryFailures(t *testing.T) {
	t.Parallel()
	assertInventoryFailures(t, func(orchestrator *runtimeengine.Orchestrator) error {
		_, err := orchestrator.StartAcquired(context.Background(), "linux")
		return fmt.Errorf("start acquired: %w", err)
	})
}
