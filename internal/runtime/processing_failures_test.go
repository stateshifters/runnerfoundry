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
			provider := &providerFake{}
			orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
			processed, err := orchestrator.ProcessOne(context.Background(), "linux")
			if processed || !errors.Is(err, fixture.err) {
				t.Fatalf("ProcessOne() = (%v, %v)", processed, err)
			}
		})
	}
}

func TestProcessOneRetriesFailedBootstrapAtQueueTail(t *testing.T) {
	jobID := uuid.New()
	wantErr := errors.New("bootstrap failed")
	retried := false
	store := processingStore(jobID, func(_ context.Context, actual core.JobID) (bool, error) {
		retried = actual == jobID
		return true, nil
	})
	bootstrapper := &bootstrapGeneratorFake{generate: func(context.Context, core.Job, core.RunnerName) (execution.Bootstrap, error) {
		return execution.Bootstrap{}, wantErr
	}}
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestrator(store, bootstrapper, provider, provider, "member", testCurrentTime)
	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if !processed || !errors.Is(err, wantErr) || !retried {
		t.Fatalf("ProcessOne() = (%v, %v), retried=%v", processed, err, retried)
	}
}

func TestProcessOneReportsBootstrapAndRemovalFailures(t *testing.T) {
	jobID := uuid.New()
	bootstrapErr := errors.New("bootstrap failed")
	removeErr := errors.New("remove failed")
	store := processingStore(jobID, func(context.Context, core.JobID) (bool, error) { return false, removeErr })
	bootstrapper := &bootstrapGeneratorFake{generate: func(context.Context, core.Job, core.RunnerName) (execution.Bootstrap, error) {
		return execution.Bootstrap{}, bootstrapErr
	}}
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestrator(store, bootstrapper, provider, provider, "member", testCurrentTime)
	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if !processed || !errors.Is(err, bootstrapErr) || !errors.Is(err, removeErr) {
		t.Fatalf("ProcessOne() = (%v, %v)", processed, err)
	}
}

func TestProcessOneRetainsProcessingWhenPartialStartCannotBeInspected(t *testing.T) {
	jobID := uuid.New()
	startErr := errors.New("start failed")
	provider := &providerFake{
		start: func(context.Context, execution.StartRequest) error { return startErr },
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return nil, errors.New("inspect failed")
		},
	}
	assertFailedStartRetainsProcessing(t, jobID, provider, startErr)
}

func TestProcessOneRetainsProcessingWhenPartialStartCannotBeDestroyed(t *testing.T) {
	jobID := uuid.New()
	startErr := errors.New("start failed")
	provider := &providerFake{
		start: func(context.Context, execution.StartRequest) error { return startErr },
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{ID: "partial", RunnerName: core.RunnerName("runnerfoundry-" + jobID.String())}}, nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error { return errors.New("destroy failed") },
	}
	assertFailedStartRetainsProcessing(t, jobID, provider, startErr)
}

func assertFailedStartRetainsProcessing(t *testing.T, jobID core.JobID, provider *providerFake, startErr error) {
	t.Helper()
	retried := false
	store := processingStore(jobID, func(context.Context, core.JobID) (bool, error) {
		retried = true
		return true, nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, successfulBootstrapper(), provider, provider, "member", testCurrentTime)
	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if !processed || !errors.Is(err, startErr) || retried {
		t.Fatalf("ProcessOne() = (%v, %v), retried=%v", processed, err, retried)
	}
}

func TestProcessOneRemovesProcessingAfterAbsentPartialStart(t *testing.T) {
	assertFailedStartRemoved(t, nil)
}

func TestProcessOneRemovesProcessingAfterDestroyingPartialStart(t *testing.T) {
	jobID := uuid.New()
	assertFailedStartRemovedForJob(t, jobID, []execution.RunnerInstance{{
		ID: "partial", RunnerName: core.RunnerName("runnerfoundry-" + jobID.String()),
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
		inspect: func(context.Context) ([]execution.RunnerInstance, error) { return instances, nil },
	}
	store := processingStore(jobID, func(context.Context, core.JobID) (bool, error) {
		removed = true
		return true, nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, successfulBootstrapper(), provider, provider, "member", testCurrentTime)
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
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{ID: "partial", RunnerName: core.RunnerName("runnerfoundry-" + jobID.String())}}, nil
		},
	}
	store := processingStore(jobID, func(context.Context, core.JobID) (bool, error) { return false, removeErr })
	orchestrator := runtimeengine.NewOrchestrator(store, successfulBootstrapper(), provider, provider, "member", testCurrentTime)
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
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{ID: "other", RunnerName: core.RunnerName("runnerfoundry-" + uuid.NewString())}}, nil
		},
	}
	store := processingStore(jobID, func(context.Context, core.JobID) (bool, error) {
		removed = true
		return true, nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, successfulBootstrapper(), provider, provider, "member", testCurrentTime)
	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if !processed || err == nil || !removed {
		t.Fatalf("ProcessOne() = (%v, %v), removed=%v", processed, err, removed)
	}
}

func processingStore(jobID core.JobID, retry func(context.Context, core.JobID) (bool, error)) *jobStoreFake {
	return &jobStoreFake{
		take: func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error) {
			return core.ProcessingJob{Job: core.Job{ID: jobID, RunnerPool: "linux"}}, true, nil
		},
		retry: retry,
	}
}

func successfulBootstrapper() *bootstrapGeneratorFake {
	return &bootstrapGeneratorFake{generate: func(context.Context, core.Job, core.RunnerName) (execution.Bootstrap, error) {
		return execution.NewBootstrap([]byte("opaque")), nil
	}}
}
