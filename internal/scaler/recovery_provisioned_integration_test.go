//go:build integration

package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

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
