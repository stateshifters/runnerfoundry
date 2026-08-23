package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
)

func TestOccupiedSlotsCountsOnlyMemberOwnedProcessing(t *testing.T) {
	localJob := core.ProcessingJob{Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, MemberID: "member-b"}
	foreignJob := core.ProcessingJob{Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, MemberID: "member-a"}
	store := &jobStoreFake{
		list: func(_ context.Context, member core.MemberID, _ core.RunnerPool) ([]core.ProcessingJob, error) {
			if member != "member-b" {
				t.Fatalf("member lookup = %q, want member-b", member)
			}
			return []core.ProcessingJob{localJob}, nil
		},
		poolList: func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{localJob, foreignJob}, nil
		},
	}
	provider := &providerFake{inspect: func(context.Context) ([]execution.RunnerInstance, error) {
		return []execution.RunnerInstance{{
			ID: "foreign", RunnerName: core.RunnerName("runnerfoundry-" + foreignJob.Job.ID.String()), RunnerPool: "linux",
		}}, nil
	}}
	orchestrator := runtimeengine.NewOrchestrator(
		store, &bootstrapGeneratorFake{}, provider, provider, "member-b", testCurrentTime,
	)
	occupied, err := orchestrator.OccupiedSlots(context.Background(), "linux")
	if err != nil || occupied != 1 {
		t.Fatalf("OccupiedSlots() = (%d, %v), want one member-owned slot", occupied, err)
	}
}

func TestOrdinaryReconciliationCannotMutateForeignProcessing(t *testing.T) {
	foreignJob := core.ProcessingJob{
		Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, MemberID: "member-a", SourceTerminal: true,
		InventoryEndpointID: "test:endpoint",
	}
	store := &jobStoreFake{
		list: emptyProcessing,
		poolList: func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{foreignJob}, nil
		},
	}
	provider := &providerFake{inspect: func(context.Context) ([]execution.RunnerInstance, error) {
		return []execution.RunnerInstance{{
			ID: "foreign", RunnerName: core.RunnerName("runnerfoundry-" + foreignJob.Job.ID.String()), RunnerPool: "linux",
		}}, nil
	}}
	orchestrator := runtimeengine.NewOrchestrator(
		store, &bootstrapGeneratorFake{}, provider, provider, "member-b", testCurrentTime,
	)
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if err != nil {
		t.Fatalf("ReconcilePool() error = %v", err)
	}
	if result.RemovedRunners != 0 || result.RemovedProcessing != 0 {
		t.Fatalf("foreign processing mutated: %#v", result)
	}
}

func TestTerminatePoolDoesNotSweepUnownedRunners(t *testing.T) {
	orphanID := uuid.New()
	destroyed := make(map[string]bool)
	store := &jobStoreFake{list: emptyProcessing}
	provider := &providerFake{
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{
				{ID: "orphan", RunnerName: core.RunnerName("runnerfoundry-" + orphanID.String())},
				{ID: "external", RunnerName: "external"},
				{ID: "malformed", RunnerName: "runnerfoundry-not-a-uuid"},
				{ID: "zero", RunnerName: "runnerfoundry-00000000-0000-0000-0000-000000000000"},
			}, nil
		},
		destroy: func(_ context.Context, instance execution.RunnerInstance) error {
			destroyed[instance.ID] = true
			return nil
		},
	}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	result, err := orchestrator.TerminatePool(context.Background(), "linux")
	assertReconciliation(t, result, err, 0, 0)
	assertNotRecorded(t, "orphan destroyed", destroyed, "orphan")
	assertNotRecorded(t, "external destroyed", destroyed, "external")
	assertNotRecorded(t, "malformed destroyed", destroyed, "malformed")
	assertNotRecorded(t, "zero UUID destroyed", destroyed, "zero")
}

func TestOccupiedSlotsCountsOnlyDurableMemberProcessing(t *testing.T) {
	jobID := uuid.New()
	store := &jobStoreFake{list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{{Job: core.Job{ID: jobID}}}, nil
	}}
	provider := &providerFake{inspect: func(context.Context) ([]execution.RunnerInstance, error) {
		return []execution.RunnerInstance{
			{ID: "same", RunnerName: core.RunnerName("runnerfoundry-" + jobID.String())},
			{ID: "unnamed"},
		}, nil
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	occupied, err := orchestrator.OccupiedSlots(context.Background(), "linux")
	if err != nil || occupied != 1 {
		t.Fatalf("OccupiedSlots() = (%d, %v)", occupied, err)
	}
}

func TestOccupiedSlotsAndReconcileReportInventoryFailures(t *testing.T) {
	wantErr := errors.New("inventory failed")
	store := &jobStoreFake{list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return nil, wantErr
	}}
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	if _, err := orchestrator.OccupiedSlots(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("OccupiedSlots() error = %v", err)
	}
	if _, err := orchestrator.ReconcilePool(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("ReconcilePool() error = %v", err)
	}
}

func TestOccupiedSlotsDoesNotDependOnProviderInventory(t *testing.T) {
	wantErr := errors.New("inventory failed")
	store := &jobStoreFake{list: emptyProcessing}
	provider := &providerFake{inspect: func(context.Context) ([]execution.RunnerInstance, error) {
		return nil, wantErr
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	if _, err := orchestrator.OccupiedSlots(context.Background(), "linux"); err != nil {
		t.Fatalf("OccupiedSlots() should not inspect provider inventory: %v", err)
	}
	if _, err := orchestrator.ReconcilePool(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("ReconcilePool() error = %v", err)
	}
}

func TestReconcilePoolReportsPoolWideOwnershipFailure(t *testing.T) {
	wantErr := errors.New("pool state unavailable")
	store := &jobStoreFake{
		list: emptyProcessing,
		poolList: func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
			return nil, wantErr
		},
	}
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	if _, err := orchestrator.ReconcilePool(context.Background(), "linux"); !errors.Is(err, wantErr) {
		t.Fatalf("ReconcilePool() error = %v", err)
	}
}
