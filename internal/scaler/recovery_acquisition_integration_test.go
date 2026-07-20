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
