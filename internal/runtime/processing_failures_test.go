package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
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
			orchestrator := newTestOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
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
	cleaned := false
	store := processingStore(jobID, func(_ context.Context, actual core.JobID) (bool, error) {
		retried = actual == jobID
		return true, nil
	})
	bootstrapper := &bootstrapGeneratorFake{generate: func(context.Context, core.Job, core.RunnerName) (core.Bootstrap, error) {
		return core.Bootstrap{}, wantErr
	}, cleanup: func(context.Context, core.Job, core.RunnerName) error {
		cleaned = true
		return nil
	}}
	provider := &providerFake{}
	orchestrator := newTestOrchestrator(store, bootstrapper, provider, provider, "member", testCurrentTime)
	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if !processed || !errors.Is(err, wantErr) || !cleaned || !retried {
		t.Fatalf("ProcessOne() = (%v, %v), cleaned=%v retried=%v", processed, err, cleaned, retried)
	}
}

func TestProcessOneRetainsFailedBootstrapUntilJITCleanupSucceeds(t *testing.T) {
	jobID := uuid.New()
	cleanupErr := errors.New("cleanup failed")
	retried := false
	store := processingStore(jobID, func(context.Context, core.JobID) (bool, error) {
		retried = true
		return true, nil
	})
	bootstrapper := &bootstrapGeneratorFake{
		generate: func(context.Context, core.Job, core.RunnerName) (core.Bootstrap, error) {
			return core.Bootstrap{}, errors.New("bootstrap failed")
		},
		cleanup: func(context.Context, core.Job, core.RunnerName) error { return cleanupErr },
	}
	provider := &providerFake{}
	result, err := newTestOrchestrator(store, bootstrapper, provider, provider,
		"member", testCurrentTime).Process(context.Background(), "linux")
	if !errors.Is(err, cleanupErr) || retried || result.RetryOutcome != core.RetryCleanupPending {
		t.Fatalf("Process() = (%#v, %v), retried=%v", result, err, retried)
	}
}

func TestProcessOneRetainsFailedStartUntilJITCleanupSucceeds(t *testing.T) {
	t.Run("absent instance", func(t *testing.T) { assertFailedStartCleanupPending(t, nil) })
	t.Run("destroyed instance", func(t *testing.T) {
		assertFailedStartCleanupPending(t, func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{}}, nil
		})
	})
}

func assertFailedStartCleanupPending(
	t *testing.T,
	inspect func(context.Context) ([]execution.RunnerInstance, error),
) {
	t.Helper()
	jobID := uuid.New()
	cleanupErr := errors.New("cleanup failed")
	retried := false
	store := processingStore(jobID, func(context.Context, core.JobID) (bool, error) {
		retried = true
		return true, nil
	})
	provider := &providerFake{start: func(context.Context, execution.StartRequest) error {
		return errors.New("start failed")
	}}
	if inspect != nil {
		provider.inspect = func(ctx context.Context) ([]execution.RunnerInstance, error) {
			instances, err := inspect(ctx)
			instances[0].RunnerName = testRunnerName(jobID)
			return instances, err
		}
	}
	bootstrapper := successfulBootstrapper()
	bootstrapper.cleanup = func(context.Context, core.Job, core.RunnerName) error { return cleanupErr }
	result, err := newTestOrchestrator(store, bootstrapper, provider, provider,
		"member", testCurrentTime).Process(context.Background(), "linux")
	if !errors.Is(err, cleanupErr) || retried || result.RetryOutcome != core.RetryCleanupPending {
		t.Fatalf("Process() = (%#v, %v), retried=%v", result, err, retried)
	}
}

func TestProcessCarriesAConcurrentSourceOutcomeThroughProvisioningFailure(t *testing.T) {
	jobID := uuid.New()
	store := processingStore(jobID, nil)
	store.retryOutcome = core.RetrySourceTerminal
	bootstrapper := &bootstrapGeneratorFake{generate: func(context.Context, core.Job, core.RunnerName) (core.Bootstrap, error) {
		return core.Bootstrap{}, errors.New("bootstrap failed")
	}}
	provider := &providerFake{}
	orchestrator := newTestOrchestrator(store, bootstrapper, provider, provider, "member", testCurrentTime)
	result, err := orchestrator.Process(context.Background(), "linux")
	if err == nil || result.Outcome != runtimeengine.ProcessRetry || result.RetryOutcome != core.RetrySourceTerminal {
		t.Fatalf("Process() = (%#v, %v)", result, err)
	}
}

func TestProcessOneReportsBootstrapAndRemovalFailures(t *testing.T) {
	jobID := uuid.New()
	bootstrapErr := errors.New("bootstrap failed")
	removeErr := errors.New("remove failed")
	store := processingStore(jobID, func(context.Context, core.JobID) (bool, error) { return false, removeErr })
	bootstrapper := &bootstrapGeneratorFake{generate: func(context.Context, core.Job, core.RunnerName) (core.Bootstrap, error) {
		return core.Bootstrap{}, bootstrapErr
	}}
	provider := &providerFake{}
	orchestrator := newTestOrchestrator(store, bootstrapper, provider, provider, "member", testCurrentTime)
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

func TestProcessOneRetainsTimedOutProvisioningWhenOutcomeCannotBeInspected(t *testing.T) {
	jobID := uuid.New()
	provider := &providerFake{
		start: func(context.Context, execution.StartRequest) error { return context.DeadlineExceeded },
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return nil, context.DeadlineExceeded
		},
	}
	assertFailedStartRetainsProcessing(t, jobID, provider, context.DeadlineExceeded)
}

func TestProcessOneRequeuesTimedOutProvisioningOnlyAfterConfirmedAbsence(t *testing.T) {
	jobID := uuid.New()
	retried := false
	provider := &providerFake{
		start:   func(context.Context, execution.StartRequest) error { return context.DeadlineExceeded },
		inspect: func(context.Context) ([]execution.RunnerInstance, error) { return nil, nil },
	}
	store := processingStore(jobID, func(context.Context, core.JobID) (bool, error) {
		retried = true
		return true, nil
	})
	orchestrator := newTestOrchestrator(store, successfulBootstrapper(), provider, provider, "member", testCurrentTime)
	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if !processed || !errors.Is(err, context.DeadlineExceeded) || !retried {
		t.Fatalf("ProcessOne() = (%v, %v), retried=%t", processed, err, retried)
	}
}

func TestProcessOneRetainsProcessingWhenPartialStartCannotBeDestroyed(t *testing.T) {
	jobID := uuid.New()
	startErr := errors.New("start failed")
	provider := &providerFake{
		start: func(context.Context, execution.StartRequest) error { return startErr },
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{ID: "partial", RunnerName: testRunnerName(jobID)}}, nil
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
	orchestrator := newTestOrchestrator(store, successfulBootstrapper(), provider, provider, "member", testCurrentTime)
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
		ID: "partial", RunnerName: testRunnerName(jobID),
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
	orchestrator := newTestOrchestrator(store, successfulBootstrapper(), provider, provider, "member", testCurrentTime)
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
			return []execution.RunnerInstance{{ID: "partial", RunnerName: testRunnerName(jobID)}}, nil
		},
	}
	store := processingStore(jobID, func(context.Context, core.JobID) (bool, error) { return false, removeErr })
	orchestrator := newTestOrchestrator(store, successfulBootstrapper(), provider, provider, "member", testCurrentTime)
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
			return []execution.RunnerInstance{{ID: "other", RunnerName: testRunnerName(uuid.New())}}, nil
		},
	}
	store := processingStore(jobID, func(context.Context, core.JobID) (bool, error) {
		removed = true
		return true, nil
	})
	orchestrator := newTestOrchestrator(store, successfulBootstrapper(), provider, provider, "member", testCurrentTime)
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
	return &bootstrapGeneratorFake{generate: func(context.Context, core.Job, core.RunnerName) (core.Bootstrap, error) {
		return core.NewBootstrap([]byte("opaque")), nil
	}}
}
