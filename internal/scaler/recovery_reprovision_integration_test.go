package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

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
