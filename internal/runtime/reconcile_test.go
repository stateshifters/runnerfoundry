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

func TestPoolReconciliationFailsLostAttemptsAndDestroysTerminalOrOrphanRunners(t *testing.T) {
	t.Parallel()

	orchestrator := newReconciliationOrchestrator()
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	requireNoError(t, "reconcile pool", err)
	requireEqual(t, "failed attempts", result.FailedAttempts, 1)
	requireEqual(t, "removed runners", result.RemovedRunners, 2)
}

func newReconciliationOrchestrator() *runtimeengine.Orchestrator {
	active := core.Job{ID: runtimeTestID(60), RunnerPool: "linux"}
	lost := core.Job{ID: runtimeTestID(61), RunnerPool: "linux"}
	store := reconciliationStore(active, lost)
	provider := &providerFake{
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
			return reconciliationInstances(), nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error { return nil },
	}
	reporter := resultReporterFake(func(context.Context, core.Job, core.Result) (core.ResultDelivery, error) {
		return core.ResultDeliveryReported, nil
	})
	return runtimeengine.NewOrchestrator(store, combineSource(reporter, nil, nil), provider, runtimeTestID(1), func() time.Time {
		return time.Unix(1, 0)
	})
}

func reconciliationStore(active, lost core.Job) *runtimeStoreFake {
	return &runtimeStoreFake{
		attemptsByMember: func(context.Context, core.MemberID, core.RunnerPool) ([]core.JobRecord, error) {
			return []core.JobRecord{{Job: active}, {Job: lost}}, nil
		},
		setResultPending: func(_ context.Context, _ core.JobID, result core.Result) (core.JobRecord, error) {
			return core.JobRecord{Job: lost, State: core.JobStateResultPending, Result: &result}, nil
		},
		finalize: func(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error) {
			return core.Receipt{}, nil
		},
	}
}

func reconciliationInstances() []execution.RunnerInstance {
	return []execution.RunnerInstance{
		{ID: "active", RunnerName: "runnerfoundry-" + runtimeTestID(60).String(), State: execution.RunnerStateRunning},
		{ID: "terminal", RunnerName: "runnerfoundry-" + runtimeTestID(61).String(), State: execution.RunnerStateUnknown},
		{ID: "orphan", RunnerName: "runnerfoundry-" + runtimeTestID(62).String(), State: execution.RunnerStateRunning},
		{ID: "foreign", RunnerName: "foreign", State: execution.RunnerStateRunning},
		{ID: "invalid", RunnerName: "runnerfoundry-invalid", State: execution.RunnerStateRunning},
		{ID: "nil", RunnerName: "runnerfoundry-00000000-0000-0000-0000-000000000000", State: execution.RunnerStateRunning},
	}
}

func TestPoolReconciliationReportsInventoryFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("inventory unavailable")
	provider := &providerFake{inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
		return nil, wantErr
	}}
	orchestrator := runtimeengine.NewOrchestrator(&runtimeStoreFake{}, combineSource(nil, nil, nil), provider, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })
	if _, err := orchestrator.ReconcilePool(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("provider inventory error = %v, want %v", err, wantErr)
	}

	provider.inspect = func(context.Context, string) ([]execution.RunnerInstance, error) { return nil, nil }
	store := &runtimeStoreFake{attemptsByMember: func(context.Context, core.MemberID, core.RunnerPool) ([]core.JobRecord, error) {
		return nil, wantErr
	}}
	orchestrator = runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), provider, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })
	if _, err := orchestrator.ReconcilePool(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("attempt inventory error = %v, want %v", err, wantErr)
	}
}

func TestPoolReconciliationContinuesAfterAttemptAndCleanupFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("operation unavailable")
	job := core.Job{ID: runtimeTestID(80), RunnerPool: "linux"}
	store := &runtimeStoreFake{
		attemptsByMember: func(context.Context, core.MemberID, core.RunnerPool) ([]core.JobRecord, error) {
			return []core.JobRecord{{Job: job}}, nil
		},
		setResultPending: func(context.Context, core.JobID, core.Result) (core.JobRecord, error) {
			return core.JobRecord{}, wantErr
		},
	}
	provider := &providerFake{
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{ID: "terminal", RunnerName: "runnerfoundry-" + runtimeTestID(80).String(), State: execution.RunnerStateUnknown}}, nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error { return wantErr },
	}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), provider, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })

	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if result.FailedAttempts != 1 || result.RemovedRunners != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("reconciliation = (%+v, %v), want joined operation failures", result, err)
	}
}

func TestPoolReconciliationLeavesAbsentAttemptsForTimeoutOrSourceCompletion(t *testing.T) {
	t.Parallel()

	store := &runtimeStoreFake{attemptsByMember: func(context.Context, core.MemberID, core.RunnerPool) ([]core.JobRecord, error) {
		return []core.JobRecord{
			{Job: core.Job{ID: runtimeTestID(81)}},
			{Job: core.Job{ID: runtimeTestID(82)}, StartedAt: time.Unix(1, 0)},
		}, nil
	}}
	provider := &providerFake{inspect: func(context.Context, string) ([]execution.RunnerInstance, error) { return nil, nil }}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), provider, runtimeTestID(1), fixedClock)

	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if err != nil || result.FailedAttempts != 0 {
		t.Fatalf("absent attempts reconciliation = (%+v, %v), want no eager failure", result, err)
	}
}

func TestOccupiedSlotsUnionsAttemptsAndProviderInstancesByRunnerName(t *testing.T) {
	t.Parallel()

	store := &runtimeStoreFake{attemptsByMember: func(context.Context, core.MemberID, core.RunnerPool) ([]core.JobRecord, error) {
		return []core.JobRecord{{Job: core.Job{ID: runtimeTestID(83)}}, {Job: core.Job{ID: runtimeTestID(84)}}}, nil
	}}
	provider := &providerFake{inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
		return []execution.RunnerInstance{
			{ID: "same", RunnerName: "runnerfoundry-" + runtimeTestID(83).String()},
			{ID: "orphan", RunnerName: "runnerfoundry-" + runtimeTestID(85).String()},
			{ID: "unnamed"},
		}, nil
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), provider, runtimeTestID(1), fixedClock)

	occupied, err := orchestrator.OccupiedSlots(context.Background(), "linux")
	if err != nil || occupied != 4 {
		t.Fatalf("occupied slots = (%d, %v), want (4, nil)", occupied, err)
	}
}

func TestOccupiedSlotsReportsStoreAndProviderFailures(t *testing.T) {
	t.Parallel()
	assertInventoryFailures(t, func(orchestrator *runtimeengine.Orchestrator) error {
		_, err := orchestrator.OccupiedSlots(context.Background(), "linux")
		return fmt.Errorf("occupied slots: %w", err)
	})
}
