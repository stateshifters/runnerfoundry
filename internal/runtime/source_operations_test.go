package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
)

func TestProvisioningFailureRequeuesWhenSourceCannotReportIt(t *testing.T) {
	job := sourceOwnedJob()
	retried := false
	store := provisioningFailureStore(job, &retried)
	source := failingBootstrapSource(errors.New("bootstrap failed"))
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestratorWithSourceOperations(
		store, store, store,
		sourceOperationsResolverFake{operations: source},
		provider,
		provider,
		"member",
		testCurrentTime,
	)

	result, err := orchestrator.Process(context.Background(), job.RunnerPool)
	if err == nil || result.RetryOutcome != core.RetryRequeued || !retried {
		t.Fatalf("Process() = (%#v, %v), retried=%t", result, err, retried)
	}
}

func TestProvisioningFailureTerminalizesWhenSourceReportsIt(t *testing.T) {
	fixture := newSuccessfulReportingFixture()

	result, err := fixture.orchestrator.Process(context.Background(), fixture.job.RunnerPool)
	if err == nil {
		t.Fatal("Process() unexpectedly succeeded after a provisioning failure")
	}
	if result.Outcome != runtimeengine.ProcessProvisioningFailureReported {
		t.Fatalf("Process() outcome = %q", result.Outcome)
	}
	if !fixture.reported || !fixture.finalized || fixture.retried {
		t.Fatalf("reported=%t finalized=%t retried=%t",
			fixture.reported, fixture.finalized, fixture.retried)
	}
}

func TestProvisioningFailureIsReservedBeforeSourceReporting(t *testing.T) {
	job := sourceOwnedJob()
	store := provisioningFailureStore(job, new(bool))
	reserved := false
	store.reserve = func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
		reserved = true
		return core.RetryResult{Outcome: core.RetryFailureReportingPending}, nil
	}
	source := &reportingBootstrapSource{
		bootstrapGeneratorFake: failingBootstrapSource(errors.New("bootstrap failed")),
		report: func(context.Context, core.Job, core.RunnerName) error {
			if !reserved {
				t.Fatal("source report ran before durable reservation")
			}
			return nil
		},
	}
	result, err := newSourceOperationsOrchestrator(store, source).Process(context.Background(), job.RunnerPool)
	if err == nil || result.Outcome != runtimeengine.ProcessProvisioningFailureReported {
		t.Fatalf("Process() = (%#v, %v)", result, err)
	}
}

func TestAcceptedAttemptWinsBeforeProvisioningFailureReservation(t *testing.T) {
	job := sourceOwnedJob()
	store := provisioningFailureStore(job, new(bool))
	store.reserve = func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
		return core.RetryResult{Outcome: core.RetryRunnerAccepted}, nil
	}
	reports := 0
	source := &reportingBootstrapSource{
		bootstrapGeneratorFake: failingBootstrapSource(errors.New("bootstrap failed")),
		report: func(context.Context, core.Job, core.RunnerName) error {
			reports++
			return nil
		},
	}
	result, err := newSourceOperationsOrchestrator(store, source).Process(context.Background(), job.RunnerPool)
	if err == nil || result.RetryOutcome != core.RetryRunnerAccepted || reports != 0 {
		t.Fatalf("Process() = (%#v, %v), reports=%d", result, err, reports)
	}
}

func TestProvisioningFailureReservationFailuresRetainProcessing(t *testing.T) {
	stateErr := errors.New("state failed")
	for name, testCase := range map[string]struct {
		result core.RetryResult
		err    error
	}{
		"state failure":      {err: stateErr},
		"unexpected outcome": {result: core.RetryResult{Outcome: core.RetryRequeued}},
	} {
		t.Run(name, func(t *testing.T) {
			job := sourceOwnedJob()
			store := provisioningFailureStore(job, new(bool))
			store.reserve = func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
				return testCase.result, testCase.err
			}
			source := &reportingBootstrapSource{
				bootstrapGeneratorFake: failingBootstrapSource(errors.New("bootstrap failed")),
				report:                 func(context.Context, core.Job, core.RunnerName) error { return nil },
			}
			result, err := newSourceOperationsOrchestrator(store, source).Process(context.Background(), job.RunnerPool)
			if err == nil || result.RetryOutcome != core.RetryFailureReportingPending {
				t.Fatalf("Process() = (%#v, %v)", result, err)
			}
		})
	}
}

func TestProvisioningFailureAlreadyReportedFinalizesWithoutSourceReplay(t *testing.T) {
	job := sourceOwnedJob()
	store := provisioningFailureStore(job, new(bool))
	store.reserve = func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
		return core.RetryResult{Outcome: core.RetryFailureFinalizationPending}, nil
	}
	finalized, reports := false, 0
	store.finalizeFailure = func(context.Context, core.ProcessingJob) (core.TerminalRemovalResult, error) {
		finalized = true
		return core.TerminalRemovalResult{Outcome: core.TerminalRemoved}, nil
	}
	source := &reportingBootstrapSource{
		bootstrapGeneratorFake: failingBootstrapSource(errors.New("bootstrap failed")),
		report: func(context.Context, core.Job, core.RunnerName) error {
			reports++
			return nil
		},
	}
	result, err := newSourceOperationsOrchestrator(store, source).Process(context.Background(), job.RunnerPool)
	if err == nil || !finalized || reports != 0 || result.RetryOutcome != core.RetrySourceTerminal {
		t.Fatalf("Process() = (%#v, %v), finalized=%t reports=%d", result, err, finalized, reports)
	}
}

func TestUnexpectedReportRecordingOutcomeRetainsFinalization(t *testing.T) {
	job := sourceOwnedJob()
	store := provisioningFailureStore(job, new(bool))
	store.recordReported = func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
		return core.RetryResult{Outcome: core.RetryClaimChanged}, nil
	}
	source := &reportingBootstrapSource{
		bootstrapGeneratorFake: failingBootstrapSource(errors.New("bootstrap failed")),
		report:                 func(context.Context, core.Job, core.RunnerName) error { return nil },
	}
	result, err := newSourceOperationsOrchestrator(store, source).Process(context.Background(), job.RunnerPool)
	if err == nil || result.RetryOutcome != core.RetryFailureFinalizationPending {
		t.Fatalf("Process() = (%#v, %v)", result, err)
	}
}

func TestProvisioningFailureAcceptsMissingRecordAfterConcurrentFinalization(t *testing.T) {
	job := sourceOwnedJob()
	store := provisioningFailureStore(job, new(bool))
	store.recordReported = func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
		return core.RetryResult{Outcome: core.RetryMissing}, nil
	}
	source := &reportingBootstrapSource{
		bootstrapGeneratorFake: failingBootstrapSource(errors.New("bootstrap failed")),
		report:                 func(context.Context, core.Job, core.RunnerName) error { return nil },
	}
	result, err := newSourceOperationsOrchestrator(store, source).Process(context.Background(), job.RunnerPool)
	if err == nil || result.Outcome != runtimeengine.ProcessProvisioningFailureReported ||
		result.RetryOutcome != core.RetryMissing {
		t.Fatalf("Process() = (%#v, %v)", result, err)
	}
}

func TestUnknownSourceRetainsProcessingWithoutBootstrap(t *testing.T) {
	job := sourceOwnedJob()
	retried := false
	store := provisioningFailureStore(job, &retried)
	wantErr := errors.New("unknown source")
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestratorWithSourceOperations(
		store, store, store,
		sourceOperationsResolverFake{err: wantErr},
		provider,
		provider,
		"member",
		testCurrentTime,
	)

	result, err := orchestrator.Process(context.Background(), job.RunnerPool)
	if !errors.Is(err, wantErr) || result.Outcome != runtimeengine.ProcessClaimed || retried {
		t.Fatalf("Process() = (%#v, %v), retried=%t", result, err, retried)
	}
}

func TestReconciliationRetainsTerminalProcessingWhenSourceResolutionFails(t *testing.T) {
	assertReconciliationRetainsUnknownSource(t, core.ProcessingJob{
		Job:            core.Job{ID: uuid.New(), Source: core.SourceReference{Adapter: "unknown"}, RunnerPool: "linux"},
		SourceTerminal: true,
	})
}

func TestReconciliationRetainsExpiredProcessingWhenSourceResolutionFails(t *testing.T) {
	assertReconciliationRetainsUnknownSource(t, core.ProcessingJob{
		Job:       core.Job{ID: uuid.New(), Source: core.SourceReference{Adapter: "unknown"}, RunnerPool: "linux"},
		ClaimedAt: testCurrentTime().Add(-5 * time.Minute), InventoryEndpointID: "test:endpoint",
	})
}

func TestReconciliationRetainsUnstartedProcessingWhenSourceResolutionFails(t *testing.T) {
	assertReconciliationRetainsUnknownSource(t, core.ProcessingJob{
		Job: core.Job{ID: uuid.New(), Source: core.SourceReference{Adapter: "unknown"}, RunnerPool: "linux"},
	})
}

func assertReconciliationRetainsUnknownSource(t *testing.T, processing core.ProcessingJob) {
	t.Helper()
	wantErr := errors.New("unknown source")
	store := &jobStoreFake{list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}}
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestratorWithSourceOperations(
		store, store, store, sourceOperationsResolverFake{err: wantErr}, provider, provider, "member", testCurrentTime,
	)
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if !errors.Is(err, wantErr) || result.RemainingProcessing != 1 || result.RemovedProcessing != 0 {
		t.Fatalf("ReconcilePool() = (%#v, %v)", result, err)
	}
}

func TestMissingSourceOperationsRetainProcessing(t *testing.T) {
	job := sourceOwnedJob()
	var typedNil *bootstrapGeneratorFake
	for name, resolver := range map[string]core.SourceOperationsResolver{
		"missing resolver":     nil,
		"missing operations":   sourceOperationsResolverFake{},
		"typed nil operations": sourceOperationsResolverFake{operations: typedNil},
	} {
		t.Run(name, func(t *testing.T) {
			retried := false
			provider := &providerFake{}
			store := provisioningFailureStore(job, &retried)
			orchestrator := runtimeengine.NewOrchestratorWithSourceOperations(
				store, store, store, resolver, provider, provider, "member", testCurrentTime,
			)
			result, err := orchestrator.Process(context.Background(), job.RunnerPool)
			if err == nil || result.Outcome != runtimeengine.ProcessClaimed || retried {
				t.Fatalf("Process() = (%#v, %v), retried=%t", result, err, retried)
			}
		})
	}
}

func TestReportedProvisioningFailureRetainsProcessingUntilFinalizationCompletes(t *testing.T) {
	wantErr := errors.New("state failed")
	for name, configure := range map[string]func(*jobStoreFake){
		"report recording failure": func(store *jobStoreFake) {
			store.recordReported = func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
				return core.RetryResult{}, wantErr
			}
		},
		"finalization failure": func(store *jobStoreFake) {
			store.finalizeFailure = func(context.Context, core.ProcessingJob) (core.TerminalRemovalResult, error) {
				return core.TerminalRemovalResult{}, wantErr
			}
		},
		"guarded finalization refusal": func(store *jobStoreFake) {
			store.finalizeFailure = func(context.Context, core.ProcessingJob) (core.TerminalRemovalResult, error) {
				return core.TerminalRemovalResult{Outcome: core.TerminalNotTerminal}, nil
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := processReportedProvisioningFailure(configure)
			if err == nil || result.RetryOutcome != core.RetryFailureFinalizationPending {
				t.Fatalf("Process() = (%#v, %v)", result, err)
			}
		})
	}
}

func TestReportedProvisioningFailureAcceptsAlreadyRemovedProcessing(t *testing.T) {
	result, err := processReportedProvisioningFailure(func(store *jobStoreFake) {
		store.finalizeFailure = func(context.Context, core.ProcessingJob) (core.TerminalRemovalResult, error) {
			return core.TerminalRemovalResult{Outcome: core.TerminalMissing}, nil
		}
	})
	if err == nil || result.Outcome != runtimeengine.ProcessProvisioningFailureReported {
		t.Fatalf("Process() = (%#v, %v)", result, err)
	}
}

func processReportedProvisioningFailure(configure func(*jobStoreFake)) (runtimeengine.ProcessResult, error) {
	job := sourceOwnedJob()
	retried := false
	store := provisioningFailureStore(job, &retried)
	configure(store)
	source := &reportingBootstrapSource{
		bootstrapGeneratorFake: failingBootstrapSource(errors.New("bootstrap failed")),
		report: func(context.Context, core.Job, core.RunnerName) error {
			return nil
		},
	}
	result, err := newSourceOperationsOrchestrator(store, source).Process(context.Background(), job.RunnerPool)
	if err != nil {
		return result, fmt.Errorf("process reported provisioning failure: %w", err)
	}
	return result, nil
}

func TestFailedProvisioningReportIsReplayedDuringReconciliation(t *testing.T) {
	fixture := newReplayReportingFixture()

	if _, err := fixture.orchestrator.Process(
		context.Background(), fixture.job.RunnerPool,
	); !errors.Is(err, fixture.reportError) {
		t.Fatalf("Process() error = %v", err)
	}
	result, err := fixture.orchestrator.ReconcilePool(context.Background(), fixture.job.RunnerPool)
	if err != nil {
		t.Fatalf("ReconcilePool() error = %v", err)
	}
	if fixture.reports != 2 || result.RemovedProcessing != 1 || len(result.ReportedProvisioningFailures) != 1 ||
		result.ReportedProvisioningFailures[0] != fixture.job || fixture.retried {
		t.Fatalf("ReconcilePool() = %#v, reports=%d retried=%t", result, fixture.reports, fixture.retried)
	}
}

func TestReconciliationFinalizesDurableProvisioningFailureWithoutAnotherReport(t *testing.T) {
	assertReconciliationFinalizesWithoutReporting(t, core.ProcessingJob{
		Job: sourceOwnedJob(), ProvisioningFailure: core.ProvisioningFailureReported,
	})
}

func TestReconciliationRetainsReportingReservationWithoutCapability(t *testing.T) {
	assertReportingReservationRetained(t, func(*jobStoreFake, *reportingBootstrapSource) core.RunnerBootstrapProvider {
		return &bootstrapGeneratorFake{}
	})
}

func TestReconciliationRetainsReportingReservationAfterSourceFailure(t *testing.T) {
	assertReportingReservationRetained(t, func(_ *jobStoreFake, source *reportingBootstrapSource) core.RunnerBootstrapProvider {
		source.report = func(context.Context, core.Job, core.RunnerName) error { return errors.New("report failed") }
		return source
	})
}

func TestReconciliationRetainsReportingReservationAfterRecordingFailure(t *testing.T) {
	assertReportingReservationRetained(t, func(store *jobStoreFake, source *reportingBootstrapSource) core.RunnerBootstrapProvider {
		store.recordReported = func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
			return core.RetryResult{}, errors.New("record failed")
		}
		return source
	})
}

func TestReconciliationRetainsReportingReservationAfterUnexpectedRecordingOutcome(t *testing.T) {
	assertReportingReservationRetained(t, func(store *jobStoreFake, source *reportingBootstrapSource) core.RunnerBootstrapProvider {
		store.recordReported = func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
			return core.RetryResult{Outcome: core.RetryClaimChanged}, nil
		}
		return source
	})
}

func TestReconciliationRetainsReportingReservationAfterFinalizationFailure(t *testing.T) {
	assertReportingReservationRetained(t, func(store *jobStoreFake, source *reportingBootstrapSource) core.RunnerBootstrapProvider {
		store.finalizeFailure = func(context.Context, core.ProcessingJob) (core.TerminalRemovalResult, error) {
			return core.TerminalRemovalResult{}, errors.New("finalization failed")
		}
		return source
	})
}

func TestReconciliationRetainsReservedFailureWhenSourceResolutionFails(t *testing.T) {
	processing := core.ProcessingJob{Job: sourceOwnedJob(), ProvisioningFailure: core.ProvisioningFailureReporting}
	processing.RunnerName = core.AttemptRunnerName(processing.Job.ID, core.AttemptID(uuid.New()))
	store := provisioningFailureStore(processing.Job, new(bool))
	store.list = func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	store.poolList = func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	wantErr := errors.New("unknown source")
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestratorWithSourceOperations(
		store, store, store, sourceOperationsResolverFake{err: wantErr}, provider, provider, "member", testCurrentTime,
	)
	result, err := orchestrator.ReconcilePool(context.Background(), processing.Job.RunnerPool)
	if !errors.Is(err, wantErr) || result.RemainingProcessing != 1 || result.RemovedProcessing != 0 {
		t.Fatalf("ReconcilePool() = (%#v, %v)", result, err)
	}
}

func TestReconciliationRetainsReportedFailureWhenFinalizationFails(t *testing.T) {
	processing := core.ProcessingJob{Job: sourceOwnedJob(), ProvisioningFailure: core.ProvisioningFailureReported}
	processing.RunnerName = core.AttemptRunnerName(processing.Job.ID, core.AttemptID(uuid.New()))
	store := provisioningFailureStore(processing.Job, new(bool))
	store.list = func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	store.poolList = func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	wantErr := errors.New("state failed")
	store.finalizeFailure = func(context.Context, core.ProcessingJob) (core.TerminalRemovalResult, error) {
		return core.TerminalRemovalResult{}, wantErr
	}
	result, err := newSourceOperationsOrchestrator(store, &reportingBootstrapSource{
		bootstrapGeneratorFake: &bootstrapGeneratorFake{},
		report:                 func(context.Context, core.Job, core.RunnerName) error { return nil },
	}).ReconcilePool(context.Background(), processing.Job.RunnerPool)
	if !errors.Is(err, wantErr) || result.RemainingProcessing != 1 || result.RemovedProcessing != 0 {
		t.Fatalf("ReconcilePool() = (%#v, %v)", result, err)
	}
}

type reportingFixture struct {
	job          core.Job
	orchestrator *runtimeengine.Orchestrator
	retried      bool
	reported     bool
	finalized    bool
	reports      int
	reportError  error
}

func newSuccessfulReportingFixture() *reportingFixture {
	fixture := &reportingFixture{job: sourceOwnedJob()}
	store := fixture.reportingStore()
	source := &reportingBootstrapSource{
		bootstrapGeneratorFake: failingBootstrapSource(errors.New("bootstrap failed")),
		report: func(_ context.Context, actual core.Job, _ core.RunnerName) error {
			fixture.reported = actual == fixture.job
			return nil
		},
	}
	fixture.orchestrator = newSourceOperationsOrchestrator(store, source)
	return fixture
}

func newReplayReportingFixture() *reportingFixture {
	fixture := &reportingFixture{job: sourceOwnedJob(), reportError: errors.New("report failed")}
	store := fixture.reportingStore()
	processing := core.ProcessingJob{Job: fixture.job,
		RunnerName: core.AttemptRunnerName(fixture.job.ID, core.AttemptID(uuid.New()))}
	store.reserve = func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
		processing.ProvisioningFailure = core.ProvisioningFailureReporting
		return core.RetryResult{Outcome: core.RetryFailureReportingPending}, nil
	}
	store.list = func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	store.poolList = func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	source := &reportingBootstrapSource{
		bootstrapGeneratorFake: failingBootstrapSource(errors.New("bootstrap failed")),
		report:                 fixture.failFirstReport,
	}
	fixture.orchestrator = newSourceOperationsOrchestrator(store, source)
	return fixture
}

func (fixture *reportingFixture) reportingStore() *jobStoreFake {
	store := provisioningFailureStore(fixture.job, &fixture.retried)
	store.finalizeFailure = func(_ context.Context, processing core.ProcessingJob) (core.TerminalRemovalResult, error) {
		fixture.finalized = processing.Job.ID == fixture.job.ID && processing.Job.Source == fixture.job.Source
		return core.TerminalRemovalResult{Outcome: core.TerminalRemoved}, nil
	}
	return store
}

func (fixture *reportingFixture) failFirstReport(context.Context, core.Job, core.RunnerName) error {
	fixture.reports++
	if fixture.reports == 1 {
		return fixture.reportError
	}
	return nil
}

func sourceOwnedJob() core.Job {
	return core.Job{ID: uuid.New(), Source: core.SourceReference{Adapter: "test", ID: "request"}, RunnerPool: "linux"}
}

func provisioningFailureStore(job core.Job, retried *bool) *jobStoreFake {
	return &jobStoreFake{
		take: func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error) {
			return core.ProcessingJob{Job: job}, true, nil
		},
		retry: func(context.Context, core.JobID) (bool, error) {
			*retried = true
			return true, nil
		},
	}
}

func newSourceOperationsOrchestrator(
	store *jobStoreFake,
	source core.RunnerBootstrapProvider,
) *runtimeengine.Orchestrator {
	provider := &providerFake{}
	return runtimeengine.NewOrchestratorWithSourceOperations(
		store, store, store,
		sourceOperationsResolverFake{operations: source},
		provider,
		provider,
		"member",
		testCurrentTime,
	)
}

func failingBootstrapSource(cause error) *bootstrapGeneratorFake {
	return &bootstrapGeneratorFake{
		generate: func(context.Context, core.Job, core.RunnerName) (core.Bootstrap, error) {
			return core.Bootstrap{}, cause
		},
	}
}

type sourceOperationsResolverFake struct {
	operations core.RunnerBootstrapProvider
	err        error
}

func (resolver sourceOperationsResolverFake) ResolveSourceOperations(
	core.SourceAdapter,
) (core.SourceOperations, error) {
	return core.SourceOperations{RunnerBootstrapProvider: resolver.operations}, resolver.err
}

type reportingBootstrapSource struct {
	*bootstrapGeneratorFake
	report func(context.Context, core.Job, core.RunnerName) error
}

func (source *reportingBootstrapSource) ReportProvisioningFailure(
	ctx context.Context,
	job core.Job,
	runnerName core.RunnerName,
) error {
	return source.report(ctx, job, runnerName)
}
