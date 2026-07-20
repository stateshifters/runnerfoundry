package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

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
