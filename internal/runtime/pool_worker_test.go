package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestPoolWorkerProcessesOnlyWhenLocalCapacityIsFree(t *testing.T) {
	for name, test := range map[string]struct {
		occupied int
		outcome  runtimeengine.ProcessOutcome
		calls    int
	}{"free": {occupied: 1, outcome: runtimeengine.ProcessRunnerStarted, calls: 1},
		"full": {occupied: 2, outcome: runtimeengine.ProcessNoWork}} {
		t.Run(name, func(t *testing.T) {
			processedCalls := 0
			coordinator := &workCoordinatorFake{
				occupied: func(context.Context, core.RunnerPool) (int, error) { return test.occupied, nil },
				process: func(context.Context, core.RunnerPool) (runtimeengine.ProcessResult, error) {
					processedCalls++
					return runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessRunnerStarted}, nil
				},
			}
			worker := runtimeengine.NewPoolWorker(coordinator, "linux", 2)
			processed, err := worker.RequestWork(context.Background())
			if err != nil || processed.Outcome != test.outcome || processedCalls != test.calls {
				t.Fatalf("RequestWork() = (%v, %v), calls=%d", processed, err, processedCalls)
			}
		})
	}
}

func TestPoolWorkerPreservesOutcomesWhileReportingFailures(t *testing.T) {
	wantErr := errors.New("failed")
	for name, test := range map[string]struct {
		coordinator *workCoordinatorFake
		want        runtimeengine.ProcessResult
	}{
		"inspect": {coordinator: &workCoordinatorFake{occupied: func(context.Context, core.RunnerPool) (int, error) { return 0, wantErr }}},
		"process": {
			coordinator: &workCoordinatorFake{
				occupied: func(context.Context, core.RunnerPool) (int, error) { return 0, nil },
				process: func(context.Context, core.RunnerPool) (runtimeengine.ProcessResult, error) {
					return runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessRetry, RetryOutcome: core.RetryRunnerStarted}, wantErr
				},
			},
			want: runtimeengine.ProcessResult{Outcome: runtimeengine.ProcessRetry, RetryOutcome: core.RetryRunnerStarted},
		},
	} {
		t.Run(name, func(t *testing.T) {
			worker := runtimeengine.NewPoolWorker(test.coordinator, "linux", 1)
			result, err := worker.RequestWork(context.Background())
			if !errors.Is(err, wantErr) || result != test.want {
				t.Fatalf("RequestWork() = (%#v, %v), want (%#v, %v)", result, err, test.want, wantErr)
			}
		})
	}
}

type workCoordinatorFake struct {
	occupied func(context.Context, core.RunnerPool) (int, error)
	process  func(context.Context, core.RunnerPool) (runtimeengine.ProcessResult, error)
}

func (coordinator *workCoordinatorFake) OccupiedSlots(ctx context.Context, pool core.RunnerPool) (int, error) {
	return coordinator.occupied(ctx, pool)
}
func (coordinator *workCoordinatorFake) Process(ctx context.Context, pool core.RunnerPool) (runtimeengine.ProcessResult, error) {
	return coordinator.process(ctx, pool)
}
