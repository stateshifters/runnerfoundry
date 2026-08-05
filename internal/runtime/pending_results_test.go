package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestPendingResultRecoveryDeliversEachJobIndependently(t *testing.T) {
	t.Parallel()

	deliveryErr := errors.New("first source unavailable")
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService}
	jobs := []core.Job{{ID: runtimeTestID(70)}, {ID: runtimeTestID(71)}}
	store := &runtimeStoreFake{
		pendingResults: func(context.Context, int) ([]core.JobRecord, error) {
			return []core.JobRecord{
				{Job: jobs[0], State: core.JobStateResultPending, Result: &result},
				{Job: jobs[1], State: core.JobStateResultPending, Result: &result},
			}, nil
		},
		finalize: func(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error) {
			return core.Receipt{}, nil
		},
	}
	reporter := resultReporterFake(func(_ context.Context, job core.Job, _ core.Result) (core.ResultDelivery, error) {
		if job.ID == jobs[0].ID {
			return "", deliveryErr
		}
		return core.ResultDeliveryReported, nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(reporter, nil, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })

	delivered, err := orchestrator.RetryPendingResults(context.Background(), 10)
	requireEqual(t, "delivered results", delivered, 1)
	requireErrorIs(t, "pending recovery", err, deliveryErr)
}

func TestPendingResultRecoveryReportsLookupAndInvalidRecordFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("store unavailable")
	store := &runtimeStoreFake{pendingResults: func(context.Context, int) ([]core.JobRecord, error) {
		return nil, wantErr
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })
	if delivered, err := orchestrator.RetryPendingResults(context.Background(), 1); delivered != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("lookup failure = (%d, %v), want (0, %v)", delivered, err, wantErr)
	}

	store.pendingResults = func(context.Context, int) ([]core.JobRecord, error) {
		return []core.JobRecord{{Job: core.Job{ID: runtimeTestID(72)}, State: core.JobStateResultPending}}, nil
	}
	if delivered, err := orchestrator.RetryPendingResults(context.Background(), 1); delivered != 0 || err == nil {
		t.Fatalf("invalid record = (%d, %v), want validation failure", delivered, err)
	}
}

func TestRetrySinglePendingResultDeliversAndFinalizes(t *testing.T) {
	t.Parallel()

	result := core.Result{Outcome: core.OutcomeSuccess, Origin: core.ResultOriginRunner}
	job := core.Job{ID: runtimeTestID(73)}
	store := &runtimeStoreFake{
		lookup: func(context.Context, core.JobID) (core.JobRecord, bool, error) {
			return core.JobRecord{Job: job, State: core.JobStateResultPending, Result: &result}, true, nil
		},
		finalize: func(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error) {
			return core.Receipt{}, nil
		},
	}
	reporter := resultReporterFake(func(context.Context, core.Job, core.Result) (core.ResultDelivery, error) {
		return core.ResultDeliveryReported, nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(reporter, nil, nil), nil, runtimeTestID(1), fixedClock)
	if err := orchestrator.RetryResult(context.Background(), job.ID); err != nil {
		t.Fatalf("retry result: %v", err)
	}
}
