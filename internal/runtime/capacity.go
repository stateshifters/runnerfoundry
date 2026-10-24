package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

// CapacitySnapshotService supplies source adapters with source-neutral
// availability while retaining liveness and persistence mechanics in runtime.
type CapacitySnapshotService struct {
	store       state.RunnerPoolCapacityStore
	currentTime func() time.Time
	memberTTL   time.Duration
}

// NewCapacitySnapshotService derives the live-member cutoff from the supplied
// clock and membership TTL, keeping that policy out of source adapters.
func NewCapacitySnapshotService(
	store state.RunnerPoolCapacityStore,
	currentTime func() time.Time,
	memberTTL time.Duration,
) *CapacitySnapshotService {
	return &CapacitySnapshotService{store: store, currentTime: currentTime, memberTTL: memberTTL}
}

func (service *CapacitySnapshotService) CapacitySnapshot(
	ctx context.Context,
	runnerPool core.RunnerPool,
) (core.CapacitySnapshot, error) {
	available, err := service.store.AvailableRunnerPoolCapacity(
		ctx,
		runnerPool,
		service.currentTime().Add(-service.memberTTL),
	)
	if err != nil {
		return core.CapacitySnapshot{}, fmt.Errorf("read Runner Pool %q available capacity: %w", runnerPool, err)
	}
	return core.CapacitySnapshot{RunnerPool: runnerPool, Available: available}, nil
}

var _ core.CapacitySnapshotProvider = (*CapacitySnapshotService)(nil)
