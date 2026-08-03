package scaler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCoordinatorAssignedBackendReportsRunningCountFailureDuringPreparation(t *testing.T) {
	coordinator := NewDurableCoordinator("linux", 1, &failingDemandStore{})
	coordinator.AddRepository("repo", failingWorker{runningErr: errors.New("running")})
	backend := coordinatorAssignedJobBackend{coordinator: coordinator, repository: "repo"}

	_, err := backend.Prepare(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "running") {
		t.Fatalf("preparation error = %v", err)
	}
}

func TestCoordinatorAssignedBackendReportsPendingCapacityFailure(t *testing.T) {
	store := &coordinatorLifecycleFaultStore{
		MemoryDemandStore: createMemoryDemandStore(),
		pendingErr:        errors.New("pending"),
	}
	coordinator := NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", &recordingWorker{})
	backend := coordinatorAssignedJobBackend{coordinator: coordinator, repository: "repo"}

	_, err := backend.currentCapacity(context.Background())
	if err == nil || !strings.Contains(err.Error(), "pending") {
		t.Fatalf("capacity error = %v", err)
	}
}

func TestCoordinatorAssignedBackendReportsPendingRunnerInspectionFailure(t *testing.T) {
	ctx := context.Background()
	store := createMemoryDemandStore()
	if err := store.RegisterRunner(ctx, RunnerLifecycle{
		ScaleSet: "linux", Repository: "repo", RunnerName: "runner", Phase: RunnerReapPending,
		WaitingSince: time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatal(err)
	}
	coordinator := NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", lifecycleIdentityWorker{presentErr: errors.New("inspect")})
	backend := coordinatorAssignedJobBackend{coordinator: coordinator, repository: "repo"}

	_, err := backend.currentCapacity(ctx)
	if err == nil || !strings.Contains(err.Error(), "inspect") {
		t.Fatalf("capacity error = %v", err)
	}
}

func TestCoordinatorAssignedBackendStopsWhenRepositoryWorkerIsAbsent(t *testing.T) {
	backend := coordinatorAssignedJobBackend{coordinator: NewCoordinator(1), repository: "missing"}

	started, err := backend.StartAssigned(context.Background())
	if err != nil || started {
		t.Fatalf("started = %t, error = %v", started, err)
	}
}
