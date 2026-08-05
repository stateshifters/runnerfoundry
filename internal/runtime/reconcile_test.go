package runtime_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestCleanupPoolDestroysRunningOwnedResourcesBeforeRemovingProcessing(t *testing.T) {
	jobID := uuid.New()
	memberID := uuid.New()
	processing := core.ProcessingJob{Job: core.Job{ID: jobID, RunnerPool: "linux"}, MemberID: memberID}
	events := make([]string, 0, 2)
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{processing}, nil
		},
		remove: func(context.Context, core.JobID) error {
			events = append(events, "remove")
			return nil
		},
	}
	provider := &providerFake{
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{
				ID: "runner", RunnerName: "runnerfoundry-" + jobID.String(), State: execution.RunnerStateRunning,
			}}, nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error {
			events = append(events, "destroy")
			return nil
		},
	}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, memberID)

	result, err := orchestrator.CleanupPool(context.Background(), "linux")
	assertReconciliation(t, result, err, 1, 1)
	if strings.Join(events, ",") != "destroy,remove" {
		t.Fatalf("cleanup events = %v", events)
	}
}

func TestCleanupPoolRetainsProcessingWhenDestroyFails(t *testing.T) {
	jobID := uuid.New()
	wantErr := errors.New("destroy failed")
	removed := false
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: jobID}}}, nil
		},
		remove: func(context.Context, core.JobID) error { removed = true; return nil },
	}
	provider := &providerFake{
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{ID: "runner", RunnerName: "runnerfoundry-" + jobID.String()}}, nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error { return wantErr },
	}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, uuid.New())
	_, err := orchestrator.CleanupPool(context.Background(), "linux")
	if !errors.Is(err, wantErr) || removed {
		t.Fatalf("CleanupPool() error = %v, removed=%v", err, removed)
	}
}

func TestCleanupPoolReportsInventoryFailures(t *testing.T) {
	wantErr := errors.New("inventory failed")
	tests := map[string]struct {
		store    *jobStoreFake
		provider *providerFake
	}{
		"processing": {
			store: &jobStoreFake{list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
				return nil, wantErr
			}},
			provider: &providerFake{},
		},
		"provider": {
			store: &jobStoreFake{list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
				return nil, nil
			}},
			provider: &providerFake{inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
				return nil, wantErr
			}},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			orchestrator := runtimeengine.NewOrchestrator(test.store, &bootstrapGeneratorFake{}, test.provider, uuid.New())
			if _, err := orchestrator.CleanupPool(context.Background(), "linux"); !errors.Is(err, wantErr) {
				t.Fatalf("CleanupPool() error = %v", err)
			}
		})
	}
}

func TestCleanupPoolCleansOrphansAndIgnoresInvalidNames(t *testing.T) {
	orphanID := uuid.New()
	destroyed := make(map[string]bool)
	store := &jobStoreFake{list: emptyProcessing}
	provider := &providerFake{
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{
				{ID: "orphan", RunnerName: "runnerfoundry-" + orphanID.String()},
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
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, uuid.New())
	result, err := orchestrator.CleanupPool(context.Background(), "linux")
	assertReconciliation(t, result, err, 1, 0)
	assertRecorded(t, "orphan destroyed", destroyed, "orphan")
	assertNotRecorded(t, "external destroyed", destroyed, "external")
	assertNotRecorded(t, "malformed destroyed", destroyed, "malformed")
	assertNotRecorded(t, "zero UUID destroyed", destroyed, "zero")
}

func TestCleanupPoolReportsMissingProcessingRemovalFailure(t *testing.T) {
	missingID := uuid.New()
	wantErr := errors.New("remove failed")
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: missingID}}}, nil
		},
		remove: func(context.Context, core.JobID) error { return wantErr },
	}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, &providerFake{}, uuid.New())
	result, err := orchestrator.CleanupPool(context.Background(), "linux")
	assertReconciliationError(t, result, err, wantErr, 0, 0)
}

func TestCleanupPoolContinuesAfterOwnedRemovalFailure(t *testing.T) {
	resourceID := uuid.New()
	missingID := uuid.New()
	wantErr := errors.New("remove failed")
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: resourceID}}, {Job: core.Job{ID: missingID}}}, nil
		},
		remove: func(_ context.Context, jobID core.JobID) error {
			if jobID == resourceID {
				return wantErr
			}
			return nil
		},
	}
	provider := &providerFake{inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
		return []execution.RunnerInstance{{ID: "resource", RunnerName: "runnerfoundry-" + resourceID.String()}}, nil
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, uuid.New())
	result, err := orchestrator.CleanupPool(context.Background(), "linux")
	if !errors.Is(err, wantErr) || result.RemovedRunners != 1 || result.RemovedProcessing != 1 {
		t.Fatalf("CleanupPool() = (%#v, %v)", result, err)
	}
}

func TestOccupiedSlotsCountsProcessingAndProviderInventoryOnce(t *testing.T) {
	jobID := uuid.New()
	store := &jobStoreFake{list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{{Job: core.Job{ID: jobID}}}, nil
	}}
	provider := &providerFake{inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
		return []execution.RunnerInstance{
			{ID: "same", RunnerName: "runnerfoundry-" + jobID.String()},
			{ID: "unnamed"},
		}, nil
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, uuid.New())
	occupied, err := orchestrator.OccupiedSlots(context.Background(), "linux")
	if err != nil || occupied != 2 {
		t.Fatalf("OccupiedSlots() = (%d, %v)", occupied, err)
	}
}

func TestOccupiedSlotsAndReconcileReportInventoryFailures(t *testing.T) {
	wantErr := errors.New("inventory failed")
	for name, store := range map[string]*jobStoreFake{
		"processing": {list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return nil, wantErr
		}},
		"provider": {list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return nil, nil
		}},
	} {
		t.Run(name, func(t *testing.T) {
			provider := &providerFake{inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
				return nil, wantErr
			}}
			orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, uuid.New())
			if _, err := orchestrator.OccupiedSlots(context.Background(), "linux"); !errors.Is(err, wantErr) {
				t.Fatalf("OccupiedSlots() error = %v", err)
			}
			if _, err := orchestrator.ReconcilePool(context.Background(), "linux"); !errors.Is(err, wantErr) {
				t.Fatalf("ReconcilePool() error = %v", err)
			}
		})
	}
}

func TestReconcilePoolPreservesRunningWork(t *testing.T) {
	fixture := newOwnedReconciliationFixture(execution.RunnerStateRunning)
	result, err := fixture.orchestrator.ReconcilePool(context.Background(), "linux")
	assertReconciliation(t, result, err, 0, 0)
	assertNotRecorded(t, "running processing removed", fixture.removed, fixture.jobID)
	assertNotRecorded(t, "running resource destroyed", fixture.destroyed, "runner")
}

func TestReconcilePoolCleansFinishedWork(t *testing.T) {
	fixture := newOwnedReconciliationFixture(execution.RunnerStateCompleted)
	result, err := fixture.orchestrator.ReconcilePool(context.Background(), "linux")
	assertReconciliation(t, result, err, 1, 1)
	assertRecorded(t, "finished processing removed", fixture.removed, fixture.jobID)
	assertRecorded(t, "finished resource destroyed", fixture.destroyed, "runner")
}

type ownedReconciliationFixture struct {
	jobID        core.JobID
	removed      map[core.JobID]bool
	destroyed    map[string]bool
	orchestrator *runtimeengine.Orchestrator
}

func newOwnedReconciliationFixture(state execution.RunnerState) ownedReconciliationFixture {
	jobID := uuid.New()
	removed := make(map[core.JobID]bool)
	destroyed := make(map[string]bool)
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: jobID}}}, nil
		},
		remove: func(_ context.Context, jobID core.JobID) error { removed[jobID] = true; return nil },
	}
	provider := &providerFake{
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{
				ID: "runner", RunnerName: "runnerfoundry-" + jobID.String(), State: state,
			}}, nil
		},
		destroy: func(_ context.Context, instance execution.RunnerInstance) error {
			destroyed[instance.ID] = true
			return nil
		},
	}
	return ownedReconciliationFixture{
		jobID: jobID, removed: removed, destroyed: destroyed,
		orchestrator: runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, uuid.New()),
	}
}

func TestReconcilePoolCleansMissingAndOrphanedWork(t *testing.T) {
	missingID := uuid.New()
	orphanID := uuid.New()
	removed := false
	destroyed := false
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: missingID}}}, nil
		},
		remove: func(context.Context, core.JobID) error { removed = true; return nil },
	}
	provider := &providerFake{
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{
				{ID: "orphan", RunnerName: "runnerfoundry-" + orphanID.String()},
				{ID: "external", RunnerName: "external"},
			}, nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error { destroyed = true; return nil },
	}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, uuid.New())
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	assertReconciliation(t, result, err, 1, 1)
	if !removed || !destroyed {
		t.Fatalf("removed=%t destroyed=%t", removed, destroyed)
	}
}

func TestReconcilePoolRetainsProcessingAcrossCleanupFailures(t *testing.T) {
	destroyID := uuid.New()
	removeID := uuid.New()
	destroyErr := errors.New("destroy failed")
	removeErr := errors.New("remove failed")
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: destroyID}}, {Job: core.Job{ID: removeID}}}, nil
		},
		remove: func(_ context.Context, jobID core.JobID) error {
			if jobID == removeID {
				return removeErr
			}
			return nil
		},
	}
	provider := &providerFake{
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{
				ID: "destroy", RunnerName: "runnerfoundry-" + destroyID.String(), State: execution.RunnerStateUnknown,
			}}, nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error { return destroyErr },
	}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, uuid.New())
	_, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if !errors.Is(err, destroyErr) || !errors.Is(err, removeErr) {
		t.Fatalf("ReconcilePool() error = %v", err)
	}
}

func TestReconcilePoolReportsRemovalFailureAfterDestroy(t *testing.T) {
	jobID := uuid.New()
	wantErr := errors.New("remove failed")
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: jobID}}}, nil
		},
		remove: func(context.Context, core.JobID) error { return wantErr },
	}
	provider := &providerFake{
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{ID: "finished", RunnerName: "runnerfoundry-" + jobID.String(), State: execution.RunnerStateCompleted}}, nil
		},
	}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, uuid.New())
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if !errors.Is(err, wantErr) || result.RemovedRunners != 1 || result.RemovedProcessing != 0 {
		t.Fatalf("ReconcilePool() = (%#v, %v)", result, err)
	}
}

func assertReconciliation(
	t *testing.T,
	actual runtimeengine.PoolReconciliation,
	err error,
	wantRunners, wantProcessing int,
) {
	t.Helper()
	if err != nil {
		t.Fatalf("reconciliation error = %v", err)
	}
	assertReconciliationCounts(t, actual, wantRunners, wantProcessing)
}

func assertReconciliationError(
	t *testing.T,
	actual runtimeengine.PoolReconciliation,
	err, wantErr error,
	wantRunners, wantProcessing int,
) {
	t.Helper()
	if !errors.Is(err, wantErr) {
		t.Fatalf("reconciliation error = %v, want %v", err, wantErr)
	}
	assertReconciliationCounts(t, actual, wantRunners, wantProcessing)
}

func assertReconciliationCounts(t *testing.T, actual runtimeengine.PoolReconciliation, wantRunners, wantProcessing int) {
	t.Helper()
	if actual.RemovedRunners != wantRunners {
		t.Fatalf("removed runners = %d, want %d", actual.RemovedRunners, wantRunners)
	}
	if actual.RemovedProcessing != wantProcessing {
		t.Fatalf("removed processing = %d, want %d", actual.RemovedProcessing, wantProcessing)
	}
}

func assertRecorded[K comparable](t *testing.T, description string, records map[K]bool, key K) {
	t.Helper()
	if !records[key] {
		t.Fatalf("%s = false, want true", description)
	}
}

func assertNotRecorded[K comparable](t *testing.T, description string, records map[K]bool, key K) {
	t.Helper()
	if records[key] {
		t.Fatalf("%s = true, want false", description)
	}
}

func emptyProcessing(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
	return nil, nil
}
