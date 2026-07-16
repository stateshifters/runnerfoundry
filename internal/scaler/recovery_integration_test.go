package scaler

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestReopenedCoordinatorPreservesFIFOWhileCapacityIsFullAndResumesAfterRelease(t *testing.T) { //nolint:gocyclo // One end-to-end scenario asserts each recovery phase.
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	queueTime := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	if err := store.Apply(context.Background(), "linux", []JobDemand{
		{Repository: "newest", RequestID: 3, QueueTime: queueTime.Add(2 * time.Minute), Acquired: true},
		{Repository: "oldest", RequestID: 1, QueueTime: queueTime, Acquired: true},
		{Repository: "middle", RequestID: 2, QueueTime: queueTime.Add(time.Minute), Acquired: true},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, reopenErr := OpenSQLiteDemandStore(databasePath)
	if reopenErr != nil {
		t.Fatal(reopenErr)
	}
	defer reopened.Close()
	startOrder := []string{}
	workers := map[string]*recordingWorker{
		"oldest": {repository: "oldest", startOrder: &startOrder},
		"middle": {repository: "middle", startOrder: &startOrder},
		"newest": {repository: "newest", startOrder: &startOrder},
	}
	coordinator := NewDurableCoordinator("linux", 2, reopened)
	for repository, worker := range workers {
		coordinator.AddRepository(repository, worker)
	}

	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(startOrder) != 2 || startOrder[0] != "oldest" || startOrder[1] != "middle" {
		t.Fatalf("expected oldest recovered demand to consume capacity, got %#v", startOrder)
	}
	remaining, orderedErr := reopened.Ordered(context.Background(), "linux")
	if orderedErr != nil || len(remaining) != 3 {
		t.Fatalf("expected provisioned and waiting demand to remain durable, got %#v, %v", remaining, orderedErr)
	}
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(startOrder) != 2 {
		t.Fatalf("expected full capacity to preserve queued demand, got %#v", startOrder)
	}
	if err := reopened.Apply(context.Background(), "linux", nil, []DemandRemoval{
		{Repository: "oldest", RequestID: 1},
		{Repository: "middle", RequestID: 2},
	}); err != nil {
		t.Fatal(err)
	}
	workers["oldest"].running = 0
	workers["middle"].running = 0
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(startOrder) != 3 || startOrder[2] != "newest" {
		t.Fatalf("expected queued demand to resume after capacity release, got %#v", startOrder)
	}
}

func TestReopenedCoordinatorRetainsAcquiredDemandWhenRunnerStartFails(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	if err := store.Apply(context.Background(), "linux", []JobDemand{{
		Repository: "repo", RequestID: 9, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	coordinator := NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", failingWorker{startErr: errors.New("start failed")})
	if err := coordinator.Schedule(context.Background()); err == nil {
		t.Fatal("expected runner start failure")
	}
	remaining, orderedErr := store.Ordered(context.Background(), "linux")
	if orderedErr != nil || len(remaining) != 1 || !remaining[0].Acquired {
		t.Fatalf("expected acquired demand to remain durable, got %#v, %v", remaining, orderedErr)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, reopenErr := OpenSQLiteDemandStore(databasePath)
	if reopenErr != nil {
		t.Fatal(reopenErr)
	}
	defer reopened.Close()
	startOrder := []string{}
	retryCoordinator := NewDurableCoordinator("linux", 1, reopened)
	retryWorker := &recordingWorker{repository: "repo", startOrder: &startOrder}
	retryCoordinator.AddRepository("repo", retryWorker)
	if err := retryCoordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(retryWorker.acquired) != 0 || len(startOrder) != 1 {
		t.Fatalf("expected acquired demand to retry without reacquisition, acquired=%#v started=%#v", retryWorker.acquired, startOrder)
	}
}

func TestReopenedCoordinatorRetainsProvisionedDemandUntilJobStarts(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer store.Close()
	if err := store.Apply(context.Background(), "linux", []JobDemand{{
		Repository: "repo", RequestID: 10, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}

	startOrder := []string{}
	coordinator := NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", &recordingWorker{repository: "repo", startOrder: &startOrder})
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	remaining, orderedErr := store.Ordered(context.Background(), "linux")
	if orderedErr != nil || len(remaining) != 1 || remaining[0].RequestID != 10 || !remaining[0].Acquired {
		t.Fatalf("expected provisioned demand to remain durable until JobStarted, got %#v, %v", remaining, orderedErr)
	}
}

func TestReopenedCoordinatorReprovisionsMissingRunnerBeforeJobStarts(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer store.Close()
	if err := store.Apply(context.Background(), "linux", []JobDemand{{
		Repository: "repo", RequestID: 11, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}

	startOrder := []string{}
	worker := &recordingWorker{repository: "repo", startOrder: &startOrder, existingRequestID: 11, running: 1}
	coordinator := NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(startOrder) != 0 {
		t.Fatalf("expected provider-visible runner not to be duplicated, got %#v", startOrder)
	}

	worker.existingRequestID = 0
	worker.running = 0
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.acquired) != 0 || len(startOrder) != 1 {
		t.Fatalf("expected missing acquired runner to be reprovisioned without reacquisition, acquired=%#v starts=%#v", worker.acquired, startOrder)
	}
	remaining, orderedErr := store.Ordered(context.Background(), "linux")
	if orderedErr != nil || len(remaining) != 1 || remaining[0].RequestID != 11 {
		t.Fatalf("expected reprovisioned demand to remain durable, got %#v, %v", remaining, orderedErr)
	}
}

func TestReopenedCoordinatorRetainsProviderVisibleDemandWithoutDuplication(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer store.Close()
	if err := store.Apply(context.Background(), "linux", []JobDemand{{
		Repository: "repo", RequestID: 10, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	recoveryStarts := []string{}
	recoveryWorker := &recordingWorker{repository: "repo", startOrder: &recoveryStarts, existingRequestID: 10, running: 1}
	recoveryCoordinator := NewDurableCoordinator("linux", 1, store)
	recoveryCoordinator.AddRepository("repo", recoveryWorker)
	if err := recoveryCoordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(recoveryWorker.acquired) != 0 || len(recoveryStarts) != 0 {
		t.Fatalf("expected provider-visible recovery without duplication, acquired=%#v started=%#v", recoveryWorker.acquired, recoveryStarts)
	}
	remaining, orderedErr := store.Ordered(context.Background(), "linux")
	if orderedErr != nil || len(remaining) != 1 || remaining[0].RequestID != 10 {
		t.Fatalf("expected provider-visible demand to remain durable, got %#v, %v", remaining, orderedErr)
	}
}

func TestReopenedCoordinatorRetainsUnavailableRepositoryUntilWorkerReturns(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	if err := store.Apply(context.Background(), "linux", []JobDemand{{
		Repository: "unavailable", RequestID: 4, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, reopenErr := OpenSQLiteDemandStore(databasePath)
	if reopenErr != nil {
		t.Fatal(reopenErr)
	}
	defer reopened.Close()
	coordinator := NewDurableCoordinator("linux", 1, reopened)
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	remaining, orderedErr := reopened.Ordered(context.Background(), "linux")
	if orderedErr != nil || len(remaining) != 1 || remaining[0].RequestID != 4 {
		t.Fatalf("expected unavailable repository demand to remain durable, got %#v, %v", remaining, orderedErr)
	}
	startOrder := []string{}
	coordinator.AddRepository("unavailable", &recordingWorker{repository: "unavailable", startOrder: &startOrder})
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(startOrder) != 1 || startOrder[0] != "unavailable" {
		t.Fatalf("expected retained demand to resume when the worker returns, got %#v", startOrder)
	}
}

func TestReopenedCoordinatorsKeepScaleSetDemandIsolated(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	queueTime := time.Now()
	if err := store.Apply(context.Background(), "linux", []JobDemand{{
		Repository: "repo", RequestID: 5, QueueTime: queueTime, Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(context.Background(), "gpu", []JobDemand{{
		Repository: "repo", RequestID: 6, QueueTime: queueTime, Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, reopenErr := OpenSQLiteDemandStore(databasePath)
	if reopenErr != nil {
		t.Fatal(reopenErr)
	}
	defer reopened.Close()
	linuxStarts := []string{}
	linuxCoordinator := NewDurableCoordinator("linux", 1, reopened)
	linuxCoordinator.AddRepository("repo", &recordingWorker{repository: "linux", startOrder: &linuxStarts})
	if err := linuxCoordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	gpuDemand, orderedErr := reopened.Ordered(context.Background(), "gpu")
	if orderedErr != nil || len(gpuDemand) != 1 || gpuDemand[0].RequestID != 6 {
		t.Fatalf("expected GPU demand to survive Linux recovery, got %#v, %v", gpuDemand, orderedErr)
	}
	gpuStarts := []string{}
	gpuCoordinator := NewDurableCoordinator("gpu", 1, reopened)
	gpuCoordinator.AddRepository("repo", &recordingWorker{repository: "gpu", startOrder: &gpuStarts})
	if err := gpuCoordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(linuxStarts) != 1 || len(gpuStarts) != 1 {
		t.Fatalf("expected isolated recovery for both scale sets, linux=%#v gpu=%#v", linuxStarts, gpuStarts)
	}
}
