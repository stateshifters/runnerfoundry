//go:build integration

package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

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
