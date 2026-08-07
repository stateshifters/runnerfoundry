package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestForeignRecoveryRecoversExpiredUnstartedProcessing(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	processing := core.ProcessingJob{
		Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, MemberID: "member-a",
		ClaimedAt: now.Add(-5 * time.Minute), InventoryEndpointID: "test:endpoint",
	}
	recovered := false
	result, err := runForeignRecovery(t, now, processing, &providerFake{}, func(_ context.Context, actual core.ProcessingJob) (core.RetryResult, error) {
		recovered = actual.Job.ID == processing.Job.ID
		return core.RetryResult{Outcome: core.RetryRequeued}, nil
	})
	if err != nil || !recovered || result.RemovedProcessing != 1 || result.RemainingProcessing != 0 {
		t.Fatalf("RecoverForeignPool() = (%#v, %v), recovered=%t", result, err, recovered)
	}
}

func TestForeignRecoveryRetainsStartedAndUninspectableProcessing(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	started := core.ProcessingJob{
		Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, MemberID: "member-a", RunnerStarted: true,
		ClaimedAt: now.Add(-time.Hour), InventoryEndpointID: "test:endpoint",
	}
	assertForeignRecoveryRetained(t, now, started, &providerFake{})
}

func TestForeignRecoveryRetainsUninspectableProcessing(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	processing := core.ProcessingJob{
		Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, MemberID: "member-a",
		ClaimedAt: now.Add(-time.Hour), InventoryEndpointID: "test:endpoint",
	}
	provider := &providerFake{canInspect: func(execution.InventoryEndpointID, core.MemberID) bool { return false }}
	assertForeignRecoveryRetained(t, now, processing, provider)
}

func assertForeignRecoveryRetained(t *testing.T, now time.Time, processing core.ProcessingJob, provider *providerFake) {
	t.Helper()
	mutated := false
	result, err := runForeignRecovery(t, now, processing, provider, func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
		mutated = true
		return core.RetryResult{Outcome: core.RetryRequeued}, nil
	})
	if err != nil || mutated || result.RemainingProcessing != 1 {
		t.Fatalf("RecoverForeignPool() = (%#v, %v), mutated=%t", result, err, mutated)
	}
}

func TestForeignRecoveryCleansTerminalProcessingImmediately(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	processing := core.ProcessingJob{
		Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, MemberID: "member-a", SourceTerminal: true,
		ClaimedAt: now, InventoryEndpointID: "test:endpoint",
	}
	removed := false
	store := &jobStoreFake{
		poolList: func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{processing}, nil
		},
		removeTerminal: func(context.Context, core.JobID) (bool, error) { removed = true; return true, nil },
	}
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestrator(
		store, &bootstrapGeneratorFake{}, provider, provider, "member-b", func() time.Time { return now },
	)
	result, err := orchestrator.RecoverForeignPool(context.Background(), "linux")
	if err != nil || !removed || result.RemovedProcessing != 1 || result.RemainingProcessing != 0 {
		t.Fatalf("RecoverForeignPool() = (%#v, %v), removed=%t", result, err, removed)
	}
}

func TestForeignRecoveryRetainsProcessingWhenCleanupFails(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	processing := core.ProcessingJob{
		Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, MemberID: "member-a",
		ClaimedAt: now.Add(-time.Hour), InventoryEndpointID: "test:endpoint",
	}
	provider := &providerFake{inspect: func(context.Context) ([]execution.RunnerInstance, error) {
		return []execution.RunnerInstance{{
			ID: "foreign", RunnerName: core.RunnerName("runnerfoundry-" + processing.Job.ID.String()),
			RunnerPool: "linux", EndpointID: "test:endpoint", State: execution.RunnerStateCompleted,
		}}, nil
	}, destroy: func(context.Context, execution.RunnerInstance) error { return errors.New("cleanup failed") }}
	result, err := runForeignRecovery(t, now, processing, provider, nil)
	if err == nil || result.RemainingProcessing != 1 {
		t.Fatalf("RecoverForeignPool() = (%#v, %v), want cleanup failure", result, err)
	}
}

func TestForeignRecoveryRetainsProcessingWhenJobStartedWinsGuardedRace(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	processing := core.ProcessingJob{
		Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, MemberID: "member-a",
		ClaimedAt: now.Add(-time.Hour), InventoryEndpointID: "test:endpoint",
	}
	result, err := runForeignRecovery(t, now, processing, &providerFake{}, func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
		return core.RetryResult{Outcome: core.RetryClaimChanged}, nil
	})
	if err != nil || result.RemainingProcessing != 1 {
		t.Fatalf("RecoverForeignPool() = (%#v, %v), want guarded retention", result, err)
	}
}

func TestForeignRecoveryReportsPoolStateFailure(t *testing.T) {
	wantErr := errors.New("state unavailable")
	store := &jobStoreFake{poolList: func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
		return nil, wantErr
	}}
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member-b", testCurrentTime)
	if _, err := orchestrator.RecoverForeignPool(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("RecoverForeignPool() error = %v", err)
	}
}

func TestForeignRecoveryReportsInventoryFailure(t *testing.T) {
	wantErr := errors.New("inventory unavailable")
	store := &jobStoreFake{poolList: func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
		return nil, nil
	}}
	provider := &providerFake{inspect: func(context.Context) ([]execution.RunnerInstance, error) { return nil, wantErr }}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member-b", testCurrentTime)
	if _, err := orchestrator.RecoverForeignPool(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("RecoverForeignPool() error = %v", err)
	}
}

func TestForeignRecoveryIgnoresLeaderOwnedProcessing(t *testing.T) {
	processing := core.ProcessingJob{Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, MemberID: "member-b"}
	result, err := runForeignRecovery(t, testCurrentTime(), processing, &providerFake{}, nil)
	if err != nil || result != (runtimeengine.PoolReconciliation{}) {
		t.Fatalf("RecoverForeignPool() = (%#v, %v), want no local mutation", result, err)
	}
}

func runForeignRecovery(
	t *testing.T,
	now time.Time,
	processing core.ProcessingJob,
	provider *providerFake,
	recoverProcessing func(context.Context, core.ProcessingJob) (core.RetryResult, error),
) (runtimeengine.PoolReconciliation, error) {
	t.Helper()
	store := &jobStoreFake{
		poolList: func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{processing}, nil
		},
		recover: recoverProcessing,
	}
	orchestrator := runtimeengine.NewOrchestrator(
		store, &bootstrapGeneratorFake{}, provider, provider, "member-b", func() time.Time { return now },
	)
	result, err := orchestrator.RecoverForeignPool(context.Background(), "linux")
	if err != nil {
		return result, fmt.Errorf("recover foreign pool: %w", err)
	}
	return result, nil
}
