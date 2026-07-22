//go:build integration

package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestReopenedCoordinatorRetainsUnavailableRepositoryUntilWorkerReturns(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	seedRecoveryDemandStore(t, databasePath, map[string][]JobDemand{"linux": {{
		Repository: "unavailable", RequestID: 4, QueueTime: time.Now(), Acquired: true,
	}}})
	reopened := reopenedRecoveryStore(t, databasePath)
	coordinator := NewDurableCoordinator("linux", 1, reopened)
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertSingleRecoveryDemand(t, reopened, "linux", 4)
	startOrder := []string{}
	coordinator.AddRepository("unavailable", &recordingWorker{repository: "unavailable", startOrder: &startOrder})
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(startOrder) != 1 || startOrder[0] != "unavailable" {
		t.Fatalf("expected retained demand to resume when the worker returns, got %#v", startOrder)
	}
}
