package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestProcessOneReportsEmptyQueueAndTakeFailure(t *testing.T) {
	wantErr := errors.New("take failed")
	for name, fixture := range map[string]struct {
		found bool
		err   error
	}{"empty": {}, "failed": {err: wantErr}} {
		t.Run(name, func(t *testing.T) {
			store := &jobStoreFake{take: func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error) {
				return core.ProcessingJob{}, fixture.found, fixture.err
			}}
			orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, &providerFake{}, uuid.New())
			processed, err := orchestrator.ProcessOne(context.Background(), "linux")
			if processed || !errors.Is(err, fixture.err) {
				t.Fatalf("ProcessOne() = (%v, %v)", processed, err)
			}
		})
	}
}

func TestProcessOneRemovesFailedBootstrapWithoutRequeue(t *testing.T) {
	jobID := uuid.New()
	wantErr := errors.New("bootstrap failed")
	removed := false
	store := processingStore(jobID, func(_ context.Context, actual core.JobID) error {
		removed = actual == jobID
		return nil
	})
	bootstrapper := &bootstrapGeneratorFake{generate: func(context.Context, core.Job, string) (execution.Bootstrap, error) {
		return execution.Bootstrap{}, wantErr
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, bootstrapper, &providerFake{}, uuid.New())
	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if !processed || !errors.Is(err, wantErr) || !removed {
		t.Fatalf("ProcessOne() = (%v, %v), removed=%v", processed, err, removed)
	}
}

func TestProcessOneReportsBootstrapAndRemovalFailures(t *testing.T) {
	jobID := uuid.New()
	bootstrapErr := errors.New("bootstrap failed")
	removeErr := errors.New("remove failed")
	store := processingStore(jobID, func(context.Context, core.JobID) error { return removeErr })
	bootstrapper := &bootstrapGeneratorFake{generate: func(context.Context, core.Job, string) (execution.Bootstrap, error) {
		return execution.Bootstrap{}, bootstrapErr
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, bootstrapper, &providerFake{}, uuid.New())
	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if !processed || !errors.Is(err, bootstrapErr) || !errors.Is(err, removeErr) {
		t.Fatalf("ProcessOne() = (%v, %v)", processed, err)
	}
}

func TestProcessOneRetainsProcessingWhenPartialStartCannotBeCleaned(t *testing.T) {
	jobID := uuid.New()
	startErr := errors.New("start failed")
	inspectErr := errors.New("inspect failed")
	destroyErr := errors.New("destroy failed")
	for name, provider := range map[string]*providerFake{
		"inspect": {
			start:   func(context.Context, execution.StartRequest) error { return startErr },
			inspect: func(context.Context, string) ([]execution.RunnerInstance, error) { return nil, inspectErr },
		},
		"destroy": {
			start: func(context.Context, execution.StartRequest) error { return startErr },
			inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
				return []execution.RunnerInstance{{ID: "partial", RunnerName: "runnerfoundry-" + jobID.String()}}, nil
			},
			destroy: func(context.Context, execution.RunnerInstance) error { return destroyErr },
		},
	} {
		t.Run(name, func(t *testing.T) {
			removed := false
			store := processingStore(jobID, func(context.Context, core.JobID) error { removed = true; return nil })
			orchestrator := runtimeengine.NewOrchestrator(store, successfulBootstrapper(), provider, uuid.New())
			processed, err := orchestrator.ProcessOne(context.Background(), "linux")
			if !processed || !errors.Is(err, startErr) || removed {
				t.Fatalf("ProcessOne() = (%v, %v), removed=%v", processed, err, removed)
			}
		})
	}
}

func TestProcessOneRemovesProcessingAfterAbsentPartialStart(t *testing.T) {
	assertFailedStartRemoved(t, nil)
}

func TestProcessOneRemovesProcessingAfterDestroyingPartialStart(t *testing.T) {
	jobID := uuid.New()
	assertFailedStartRemovedForJob(t, jobID, []execution.RunnerInstance{{
		ID: "partial", RunnerName: "runnerfoundry-" + jobID.String(),
	}})
}

func assertFailedStartRemoved(t *testing.T, instances []execution.RunnerInstance) {
	t.Helper()
	assertFailedStartRemovedForJob(t, uuid.New(), instances)
}

func assertFailedStartRemovedForJob(t *testing.T, jobID core.JobID, instances []execution.RunnerInstance) {
	t.Helper()
	removed := false
	provider := &providerFake{
		start:   func(context.Context, execution.StartRequest) error { return errors.New("start failed") },
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) { return instances, nil },
	}
	store := processingStore(jobID, func(context.Context, core.JobID) error { removed = true; return nil })
	orchestrator := runtimeengine.NewOrchestrator(store, successfulBootstrapper(), provider, uuid.New())
	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if !processed || err == nil || !removed {
		t.Fatalf("ProcessOne() = (%v, %v), removed=%v", processed, err, removed)
	}
}

func TestProcessOneReportsRemovalFailureAfterDestroyingPartialStart(t *testing.T) {
	jobID := uuid.New()
	startErr := errors.New("start failed")
	removeErr := errors.New("remove failed")
	provider := &providerFake{
		start: func(context.Context, execution.StartRequest) error { return startErr },
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{ID: "partial", RunnerName: "runnerfoundry-" + jobID.String()}}, nil
		},
	}
	store := processingStore(jobID, func(context.Context, core.JobID) error { return removeErr })
	orchestrator := runtimeengine.NewOrchestrator(store, successfulBootstrapper(), provider, uuid.New())
	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if !processed || !errors.Is(err, startErr) || !errors.Is(err, removeErr) {
		t.Fatalf("ProcessOne() = (%v, %v)", processed, err)
	}
}

func TestProcessOneIgnoresUnrelatedPartialRunners(t *testing.T) {
	jobID := uuid.New()
	removed := false
	provider := &providerFake{
		start: func(context.Context, execution.StartRequest) error { return errors.New("start failed") },
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{ID: "other", RunnerName: "runnerfoundry-" + uuid.NewString()}}, nil
		},
	}
	store := processingStore(jobID, func(context.Context, core.JobID) error { removed = true; return nil })
	orchestrator := runtimeengine.NewOrchestrator(store, successfulBootstrapper(), provider, uuid.New())
	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if !processed || err == nil || !removed {
		t.Fatalf("ProcessOne() = (%v, %v), removed=%v", processed, err, removed)
	}
}

func processingStore(jobID core.JobID, remove func(context.Context, core.JobID) error) *jobStoreFake {
	return &jobStoreFake{
		take: func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error) {
			return core.ProcessingJob{Job: core.Job{ID: jobID, RunnerPool: "linux"}}, true, nil
		},
		remove: remove,
	}
}

func successfulBootstrapper() *bootstrapGeneratorFake {
	return &bootstrapGeneratorFake{generate: func(context.Context, core.Job, string) (execution.Bootstrap, error) {
		return execution.NewBootstrap([]byte("opaque")), nil
	}}
}
