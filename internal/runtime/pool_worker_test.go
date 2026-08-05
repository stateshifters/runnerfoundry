package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestPoolWorkerCountsClaimedAttemptBeforeAProviderInstanceExists(t *testing.T) {
	t.Parallel()

	occupied := 1
	claims := 0
	coordinator := &workCoordinatorFake{
		occupied: func(context.Context, core.RunnerPool) (int, error) { return occupied, nil },
		claim: func(context.Context, core.RunnerPool) (bool, error) {
			claims++
			occupied++
			return true, nil
		},
	}
	worker := runtimeengine.NewPoolWorker(coordinator, "linux", 1)
	if claimed, err := worker.RequestWork(context.Background()); err != nil || claimed {
		t.Fatalf("full worker request = (%v, %v), want (false, nil)", claimed, err)
	}

	occupied = 0
	if claimed, err := worker.RequestWork(context.Background()); err != nil || !claimed {
		t.Fatalf("available worker request = (%v, %v), want (true, nil)", claimed, err)
	}
	if claimed, err := worker.RequestWork(context.Background()); err != nil || claimed || claims != 1 {
		t.Fatalf("second request = (%v, %v), claims=%d; claimed attempt overfilled capacity", claimed, err, claims)
	}
}

func TestPoolWorkerReportsOccupancyAndClaimFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("unavailable")
	coordinator := &workCoordinatorFake{occupied: func(context.Context, core.RunnerPool) (int, error) { return 0, wantErr }}
	worker := runtimeengine.NewPoolWorker(coordinator, "linux", 1)
	if _, err := worker.RequestWork(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("occupancy error = %v, want %v", err, wantErr)
	}

	coordinator.occupied = func(context.Context, core.RunnerPool) (int, error) { return 0, nil }
	coordinator.claim = func(context.Context, core.RunnerPool) (bool, error) { return false, wantErr }
	if _, err := worker.RequestWork(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("claim error = %v, want %v", err, wantErr)
	}
}

type workCoordinatorFake struct {
	occupied func(context.Context, core.RunnerPool) (int, error)
	claim    func(context.Context, core.RunnerPool) (bool, error)
}

func (coordinator *workCoordinatorFake) OccupiedSlots(ctx context.Context, pool core.RunnerPool) (int, error) {
	return coordinator.occupied(ctx, pool)
}

func (coordinator *workCoordinatorFake) ClaimWork(ctx context.Context, pool core.RunnerPool) (bool, error) {
	return coordinator.claim(ctx, pool)
}
