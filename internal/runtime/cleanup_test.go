package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestUnstartedAttemptsExpireThroughTheFailureResultPath(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 5, 15, 0, 0, 0, time.UTC)
	job := core.Job{ID: runtimeTestID(35), Source: core.SourceReference{Adapter: "source", ID: "request-35"}}
	store := &runtimeStoreFake{
		setUnstartedAttemptsResultPending: func(_ context.Context, runnerPool core.RunnerPool, before time.Time, result core.Result) ([]core.JobRecord, error) {
			if runnerPool != "linux" || !before.Equal(now.Add(-10*time.Minute)) || result.Origin != core.ResultOriginService {
				t.Fatalf("unexpected timeout transition: pool=%s before=%s result=%+v", runnerPool, before, result)
			}
			return []core.JobRecord{{Job: job, State: core.JobStateResultPending, Result: &result}}, nil
		},
		finalize: func(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error) {
			return core.Receipt{}, nil
		},
	}
	reporter := resultReporterFake(func(context.Context, core.Job, core.Result) (core.ResultDelivery, error) {
		return core.ResultDeliveryReported, nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(reporter, nil, nil), nil, runtimeTestID(1), func() time.Time { return now })

	expired, err := orchestrator.FailUnstartedAttempts(context.Background(), "linux", 10*time.Minute)
	if err != nil || expired != 1 {
		t.Fatalf("fail unstarted attempts = (%d, %v), want (1, nil)", expired, err)
	}
}

func TestQueuedUnserviceableJobUsesTheFailureResultPath(t *testing.T) {
	t.Parallel()

	job := core.Job{ID: runtimeTestID(36), Source: core.SourceReference{Adapter: "source", ID: "request-36"}}
	finalized := false
	store := &runtimeStoreFake{
		setResultPending: func(_ context.Context, _ core.JobID, result core.Result) (core.JobRecord, error) {
			return core.JobRecord{Job: job, State: core.JobStateResultPending, Result: &result}, nil
		},
		finalize: func(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error) {
			finalized = true
			return core.Receipt{}, nil
		},
	}
	reporter := resultReporterFake(func(context.Context, core.Job, core.Result) (core.ResultDelivery, error) {
		return core.ResultDeliveryReported, nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(reporter, nil, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })

	if err := orchestrator.CleanupUnserviceable(context.Background(), job.ID, "no local provider"); err != nil {
		t.Fatalf("cleanup unserviceable job: %v", err)
	}
	if !finalized {
		t.Fatal("unserviceable job was not finalized after reporting")
	}
}
