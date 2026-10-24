package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
)

func TestCapacitySnapshotServiceSuppliesSourceNeutralCapacity(t *testing.T) {
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	store := &capacityStoreFake{available: 4}
	service := runtimeengine.NewCapacitySnapshotService(store, func() time.Time { return now }, 45*time.Second)

	snapshot, err := service.CapacitySnapshot(context.Background(), "linux")
	if err != nil {
		t.Fatalf("CapacitySnapshot() error = %v", err)
	}
	if snapshot != (core.CapacitySnapshot{RunnerPool: "linux", Available: 4}) {
		t.Fatalf("CapacitySnapshot() = %#v", snapshot)
	}
	if want := now.Add(-45 * time.Second); !store.cutoff.Equal(want) {
		t.Fatalf("capacity cutoff = %v, want %v", store.cutoff, want)
	}
}

func TestCapacitySnapshotServicePropagatesStoreFailure(t *testing.T) {
	wantErr := errors.New("capacity unavailable")
	service := runtimeengine.NewCapacitySnapshotService(
		&capacityStoreFake{err: wantErr}, time.Now, time.Minute,
	)

	_, err := service.CapacitySnapshot(context.Background(), "linux")
	if !errors.Is(err, wantErr) {
		t.Fatalf("CapacitySnapshot() error = %v, want %v", err, wantErr)
	}
}

type capacityStoreFake struct {
	available int
	err       error
	cutoff    time.Time
}

func (store *capacityStoreFake) AvailableRunnerPoolCapacity(
	_ context.Context,
	_ core.RunnerPool,
	cutoff time.Time,
) (int, error) {
	store.cutoff = cutoff
	return store.available, store.err
}
