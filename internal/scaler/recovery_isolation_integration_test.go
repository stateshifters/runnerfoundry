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
	queueTime := time.Now()
	seedRecoveryDemandStore(t, databasePath, map[string][]JobDemand{
		"linux": {{Repository: "repo", RequestID: 5, QueueTime: queueTime, Acquired: true}},
		"gpu":   {{Repository: "repo", RequestID: 6, QueueTime: queueTime, Acquired: true}},
	})
	reopened := reopenedRecoveryStore(t, databasePath)
	linuxStarts := []string{}
	scheduleRecoveredScaleSet(t, reopened, "linux", &linuxStarts)
	assertSingleRecoveryDemand(t, reopened, "gpu", 6)
	gpuStarts := []string{}
	scheduleRecoveredScaleSet(t, reopened, "gpu", &gpuStarts)
	if len(linuxStarts) != 1 || len(gpuStarts) != 1 {
		t.Fatalf("expected isolated recovery for both scale sets, linux=%#v gpu=%#v", linuxStarts, gpuStarts)
	}
}

func scheduleRecoveredScaleSet(t *testing.T, store *SQLiteDemandStore, scaleSet string, starts *[]string) {
	t.Helper()
	coordinator := NewDurableCoordinator(scaleSet, 1, store)
	coordinator.AddRepository("repo", &recordingWorker{repository: scaleSet, startOrder: starts})
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
}
