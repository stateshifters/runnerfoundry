package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestPoolWorkerProcessesOnlyWhenLocalCapacityIsFree(t *testing.T) {
	for name, occupied := range map[string]int{"free": 1, "full": 2} {
		t.Run(name, func(t *testing.T) {
			processedCalls := 0
			coordinator := &workCoordinatorFake{
				occupied: func(context.Context, core.RunnerPool) (int, error) { return occupied, nil },
				process:  func(context.Context, core.RunnerPool) (bool, error) { processedCalls++; return true, nil },
			}
			worker := runtimeengine.NewPoolWorker(coordinator, "linux", 2)
			processed, err := worker.RequestWork(context.Background())
			if err != nil || processed != (occupied < 2) || processedCalls != map[bool]int{true: 1, false: 0}[occupied < 2] {
				t.Fatalf("RequestWork() = (%v, %v), calls=%d", processed, err, processedCalls)
			}
		})
	}
}

func TestPoolWorkerReportsInspectionAndProcessingFailures(t *testing.T) {
	wantErr := errors.New("failed")
	for name, coordinator := range map[string]*workCoordinatorFake{
		"inspect": {occupied: func(context.Context, core.RunnerPool) (int, error) { return 0, wantErr }},
		"process": {
			occupied: func(context.Context, core.RunnerPool) (int, error) { return 0, nil },
			process:  func(context.Context, core.RunnerPool) (bool, error) { return false, wantErr },
		},
	} {
		t.Run(name, func(t *testing.T) {
			worker := runtimeengine.NewPoolWorker(coordinator, "linux", 1)
			if _, err := worker.RequestWork(context.Background()); !errors.Is(err, wantErr) {
				t.Fatalf("RequestWork() error = %v", err)
			}
		})
	}
}

type workCoordinatorFake struct {
	occupied func(context.Context, core.RunnerPool) (int, error)
	process  func(context.Context, core.RunnerPool) (bool, error)
}

func (coordinator *workCoordinatorFake) OccupiedSlots(ctx context.Context, pool core.RunnerPool) (int, error) {
	return coordinator.occupied(ctx, pool)
}
func (coordinator *workCoordinatorFake) ProcessOne(ctx context.Context, pool core.RunnerPool) (bool, error) {
	return coordinator.process(ctx, pool)
}
