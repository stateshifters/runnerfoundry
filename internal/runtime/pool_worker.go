package runtime

import (
	"context"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

type WorkCoordinator interface {
	Process(ctx context.Context, runnerPool core.RunnerPool) (ProcessResult, error)
	OccupiedSlots(ctx context.Context, runnerPool core.RunnerPool) (int, error)
}

// PoolWorker gates job claims on provider-observed local capacity. Capacity
// must be positive and is local to this member and Runner Pool.
type PoolWorker struct {
	coordinator   WorkCoordinator
	runnerPool    core.RunnerPool
	localCapacity int
}

func NewPoolWorker(
	coordinator WorkCoordinator,
	runnerPool core.RunnerPool,
	localCapacity int,
) *PoolWorker {
	return &PoolWorker{coordinator: coordinator, runnerPool: runnerPool, localCapacity: localCapacity}
}

// RequestWork claims at most one job only when the provider reports an
// available local slot. Existing instances occupy slots until cleanup removes them.
func (worker *PoolWorker) RequestWork(ctx context.Context) (ProcessResult, error) {
	occupied, err := worker.coordinator.OccupiedSlots(ctx, worker.runnerPool)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("inspect local capacity for Runner Pool %q: %w", worker.runnerPool, err)
	}
	if occupied >= worker.localCapacity {
		return ProcessResult{Outcome: ProcessNoWork}, nil
	}

	result, err := worker.coordinator.Process(ctx, worker.runnerPool)
	if err != nil {
		return result, fmt.Errorf("request work for Runner Pool %q: %w", worker.runnerPool, err)
	}

	return result, nil
}
