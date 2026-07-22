//go:build integration

package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestDurableCoordinatorDoesNotDuplicateProviderVisibleRunner(t *testing.T) {
	store := seededReprovisionStore(t)
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
}

func TestDurableCoordinatorReprovisionsMissingRunnerWithoutReacquisition(t *testing.T) {
	store := seededReprovisionStore(t)
	startOrder := []string{}
	worker := &recordingWorker{repository: "repo", startOrder: &startOrder}
	coordinator := NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.acquired) != 0 || len(startOrder) != 1 {
		t.Fatalf("expected missing acquired runner to be reprovisioned without reacquisition, acquired=%#v starts=%#v", worker.acquired, startOrder)
	}
	assertSingleRecoveryDemand(t, store, "linux", 11)
}

func seededReprovisionStore(t *testing.T) *SQLiteDemandStore {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "state.db")
	seedRecoveryDemandStore(t, databasePath, map[string][]JobDemand{"linux": {{
		Repository: "repo", RequestID: 11, QueueTime: time.Now(), Acquired: true,
	}}})
	return reopenedRecoveryStore(t, databasePath)
}
