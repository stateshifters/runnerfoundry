package runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestReconciliationReportsReservedFailureWhenInventoryIsUnavailable(t *testing.T) {
	processing := reservedFailureProcessing("member")
	store := reservedFailureStore(processing)
	reports := 0
	wantErr := errors.New("inventory unavailable")
	provider := &providerFake{inspect: func(context.Context) ([]execution.RunnerInstance, error) {
		return nil, wantErr
	}}
	orchestrator := newTestOrchestrator(store, &reportingBootstrapSource{
		bootstrapGeneratorFake: &bootstrapGeneratorFake{},
		report: func(context.Context, core.Job, core.RunnerName) error {
			reports++
			return nil
		},
	}, provider, provider, "member", testCurrentTime)

	result, err := orchestrator.ReconcilePool(context.Background(), processing.Job.RunnerPool)
	if !errors.Is(err, wantErr) || reports != 1 || result.RemovedProcessing != 1 {
		t.Fatalf("ReconcilePool() = (%#v, %v), reports=%d", result, err, reports)
	}
}

func TestForeignRecoveryReportsReservedFailureWithoutEndpointAccess(t *testing.T) {
	processing := reservedFailureProcessing("member-a")
	store := reservedFailureStore(processing)
	reports := 0
	provider := &providerFake{canInspect: func(execution.InventoryEndpointID, core.MemberID) bool {
		return false
	}}
	orchestrator := newTestOrchestrator(store, &reportingBootstrapSource{
		bootstrapGeneratorFake: &bootstrapGeneratorFake{},
		report: func(context.Context, core.Job, core.RunnerName) error {
			reports++
			return nil
		},
	}, provider, provider, "member-b", testCurrentTime)

	result, err := orchestrator.RecoverForeignPool(context.Background(), processing.Job.RunnerPool)
	if err != nil || reports != 1 || result.RemovedProcessing != 1 || result.RemainingProcessing != 0 {
		t.Fatalf("RecoverForeignPool() = (%#v, %v), reports=%d", result, err, reports)
	}
}

func reservedFailureProcessing(member core.MemberID) core.ProcessingJob {
	job := sourceOwnedJob()
	return core.ProcessingJob{
		Job: job, MemberID: member, RunnerName: core.AttemptRunnerName(job.ID, core.AttemptID(uuid.New())),
		InventoryEndpointID: "foreign:endpoint", ProvisioningFailure: core.ProvisioningFailureReporting,
	}
}

func reservedFailureStore(processing core.ProcessingJob) *jobStoreFake {
	store := provisioningFailureStore(processing.Job, new(bool))
	store.list = func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	store.poolList = func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	return store
}

func TestReconciliationCompletesReservedReportAfterTerminalObservation(t *testing.T) {
	fixture := newReplayReportingFixture()
	processing := core.ProcessingJob{Job: fixture.job, ProvisioningFailure: core.ProvisioningFailureReporting,
		SourceTerminal: true, RunnerName: core.AttemptRunnerName(fixture.job.ID, core.AttemptID(uuid.New()))}
	store := fixture.reportingStore()
	store.list = func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	store.poolList = func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	reports := 0
	source := &reportingBootstrapSource{bootstrapGeneratorFake: &bootstrapGeneratorFake{},
		report: func(context.Context, core.Job, core.RunnerName) error { reports++; return nil }}
	result, err := newSourceOperationsOrchestrator(store, source).ReconcilePool(context.Background(), processing.Job.RunnerPool)
	if err != nil || reports != 1 || result.RemovedProcessing != 1 {
		t.Fatalf("ReconcilePool() = (%#v, %v), reports=%d", result, err, reports)
	}
}

func TestReconciliationAcceptsMissingRecordAfterConcurrentFinalization(t *testing.T) {
	processing := core.ProcessingJob{Job: sourceOwnedJob(), ProvisioningFailure: core.ProvisioningFailureReporting}
	processing.RunnerName = core.AttemptRunnerName(processing.Job.ID, core.AttemptID(uuid.New()))
	store := provisioningFailureStore(processing.Job, new(bool))
	store.list = func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	store.poolList = func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	store.recordReported = func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
		return core.RetryResult{Outcome: core.RetryMissing}, nil
	}
	source := &reportingBootstrapSource{bootstrapGeneratorFake: &bootstrapGeneratorFake{},
		report: func(context.Context, core.Job, core.RunnerName) error { return nil }}
	result, err := newSourceOperationsOrchestrator(store, source).ReconcilePool(context.Background(), processing.Job.RunnerPool)
	if err != nil || result.RemainingProcessing != 0 || result.RemovedProcessing != 1 {
		t.Fatalf("ReconcilePool() = (%#v, %v)", result, err)
	}
}

func assertReconciliationFinalizesWithoutReporting(t *testing.T, processing core.ProcessingJob) {
	t.Helper()
	processing.RunnerName = core.AttemptRunnerName(processing.Job.ID, core.AttemptID(uuid.New()))
	store := provisioningFailureStore(processing.Job, new(bool))
	store.list = func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	store.poolList = func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	finalized := false
	store.finalizeFailure = func(context.Context, core.ProcessingJob) (core.TerminalRemovalResult, error) {
		finalized = true
		return core.TerminalRemovalResult{Outcome: core.TerminalRemoved}, nil
	}
	reports := 0
	source := &reportingBootstrapSource{bootstrapGeneratorFake: failingBootstrapSource(nil),
		report: func(context.Context, core.Job, core.RunnerName) error { reports++; return nil }}
	result, err := newSourceOperationsOrchestrator(store, source).ReconcilePool(
		context.Background(), processing.Job.RunnerPool,
	)
	if err != nil || !finalized || reports != 0 || result.RemovedProcessing != 1 {
		t.Fatalf("ReconcilePool() = (%#v, %v), finalized=%t reports=%d", result, err, finalized, reports)
	}
}

func assertReportingReservationRetained(
	t *testing.T,
	configure func(*jobStoreFake, *reportingBootstrapSource) core.RunnerBootstrapProvider,
) {
	t.Helper()
	processing := core.ProcessingJob{Job: sourceOwnedJob(), ProvisioningFailure: core.ProvisioningFailureReporting}
	processing.RunnerName = core.AttemptRunnerName(processing.Job.ID, core.AttemptID(uuid.New()))
	store := provisioningFailureStore(processing.Job, new(bool))
	store.list = func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	store.poolList = func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	source := &reportingBootstrapSource{bootstrapGeneratorFake: &bootstrapGeneratorFake{},
		report: func(context.Context, core.Job, core.RunnerName) error { return nil }}
	result, err := newSourceOperationsOrchestrator(store, configure(store, source)).ReconcilePool(
		context.Background(), processing.Job.RunnerPool,
	)
	if err == nil || result.RemainingProcessing != 1 || result.RemovedProcessing != 0 {
		t.Fatalf("ReconcilePool() = (%#v, %v)", result, err)
	}
}
