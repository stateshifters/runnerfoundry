//go:build integration

package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestReopenedCoordinatorPreservesFIFOWhileCapacityIsFullAndResumesAfterRelease(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store := openRecoveryStore(t, databasePath)
	seedRecoveredFIFO(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openRecoveryStore(t, databasePath)
	closeSQLiteDemandStore(t, reopened)
	startOrder := []string{}
	coordinator, workers := recoveredFIFOCoordinator(reopened, &startOrder)

	assertRecoveredCapacityFillsInFIFOOrder(t, coordinator, reopened, &startOrder)
	assertQueuedDemandWaitsAtCapacity(t, coordinator, &startOrder)
	releaseRecoveredCapacity(t, reopened, workers)
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(startOrder) != 3 || startOrder[2] != "newest" {
		t.Fatalf("expected queued demand to resume after capacity release, got %#v", startOrder)
	}
}

func openRecoveryStore(t *testing.T, path string) *SQLiteDemandStore {
	t.Helper()
	store, err := OpenSQLiteDemandStore(path)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func seedRecoveredFIFO(t *testing.T, store *SQLiteDemandStore) {
	t.Helper()
	queueTime := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	if err := store.Apply(context.Background(), "linux", []JobDemand{
		{Repository: "newest", RequestID: 3, QueueTime: queueTime.Add(2 * time.Minute), Acquired: true},
		{Repository: "oldest", RequestID: 1, QueueTime: queueTime, Acquired: true},
		{Repository: "middle", RequestID: 2, QueueTime: queueTime.Add(time.Minute), Acquired: true},
	}, nil); err != nil {
		t.Fatal(err)
	}
}

func recoveredFIFOCoordinator(store DemandStore, startOrder *[]string) (*Coordinator, map[string]*recordingWorker) {
	workers := map[string]*recordingWorker{
		"oldest": {repository: "oldest", startOrder: startOrder},
		"middle": {repository: "middle", startOrder: startOrder},
		"newest": {repository: "newest", startOrder: startOrder},
	}
	coordinator := NewDurableCoordinator("linux", 2, store)
	for repository, worker := range workers {
		coordinator.AddRepository(repository, worker)
	}
	return coordinator, workers
}

func assertRecoveredCapacityFillsInFIFOOrder(
	t *testing.T,
	coordinator *Coordinator,
	store DemandStore,
	startOrder *[]string,
) {
	t.Helper()
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(*startOrder) != 2 || (*startOrder)[0] != "oldest" || (*startOrder)[1] != "middle" {
		t.Fatalf("expected oldest recovered demand to consume capacity, got %#v", *startOrder)
	}
	remaining, orderedErr := store.Ordered(context.Background(), "linux")
	if orderedErr != nil || len(remaining) != 3 {
		t.Fatalf("expected provisioned and waiting demand to remain durable, got %#v, %v", remaining, orderedErr)
	}
}

func assertQueuedDemandWaitsAtCapacity(t *testing.T, coordinator *Coordinator, startOrder *[]string) {
	t.Helper()
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(*startOrder) != 2 {
		t.Fatalf("expected full capacity to preserve queued demand, got %#v", *startOrder)
	}
}

func releaseRecoveredCapacity(t *testing.T, store DemandStore, workers map[string]*recordingWorker) {
	t.Helper()
	if err := store.Apply(context.Background(), "linux", nil, []DemandRemoval{
		{Repository: "oldest", RequestID: 1},
		{Repository: "middle", RequestID: 2},
	}); err != nil {
		t.Fatal(err)
	}
	workers["oldest"].running = 0
	workers["middle"].running = 0
}
