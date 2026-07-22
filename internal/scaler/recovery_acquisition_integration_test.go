//go:build integration

package scaler

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestReopenedCoordinatorRetainsAcquiredDemandWhenRunnerStartFails(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	seedRecoveryDemandStore(t, databasePath, map[string][]JobDemand{"linux": {{
		Repository: "repo", RequestID: 9, QueueTime: time.Now(), Acquired: true,
	}}})
	assertFailedStartRetainsAcquiredDemand(t, databasePath)
	assertReopenedDemandStartsWithoutReacquisition(t, databasePath)
}

func assertFailedStartRetainsAcquiredDemand(t *testing.T, databasePath string) {
	t.Helper()
	store := openRecoveryStore(t, databasePath)
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close failed-start recovery store: %v", err)
		}
	}()
	coordinator := NewDurableCoordinator("linux", 1, store)
	coordinator.AddRepository("repo", failingWorker{startErr: errors.New("start failed")})
	if err := coordinator.Schedule(context.Background()); err == nil {
		t.Fatal("expected runner start failure")
	}
	if remaining := assertSingleRecoveryDemand(t, store, "linux", 9); !remaining.Acquired {
		t.Fatalf("expected acquired demand to remain acquired, got %#v", remaining)
	}
}

func assertReopenedDemandStartsWithoutReacquisition(t *testing.T, databasePath string) {
	t.Helper()
	reopened := reopenedRecoveryStore(t, databasePath)
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
