//go:build integration

package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestReopenedCoordinatorRetainsProviderVisibleDemandWithoutDuplication(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	closeSQLiteDemandStore(t, store)
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
