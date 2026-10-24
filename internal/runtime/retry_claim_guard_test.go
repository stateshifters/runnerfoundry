package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
)

func TestProvisioningFailureRetryUsesClaimSnapshot(t *testing.T) {
	processing := sourceOwnedProcessing()
	store := provisioningFailureStore(processing.Job, new(bool))
	store.take = func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error) {
		return processing, true, nil
	}
	store.retry = nil
	store.retryOutcome = core.RetryClaimChanged
	orchestrator := newSourceOperationsOrchestrator(
		store,
		failingBootstrapSource(errors.New("bootstrap failed")),
	)

	result, err := orchestrator.Process(context.Background(), processing.Job.RunnerPool)
	if err == nil || result.RetryOutcome != core.RetryClaimChanged || len(store.retried) != 1 ||
		store.retried[0] != processing {
		t.Fatalf("Process() = (%#v, %v), retried=%#v, want=%#v", result, err, store.retried, processing)
	}
}

func TestReconciliationRetryUsesClaimSnapshot(t *testing.T) {
	processing := sourceOwnedProcessing()
	processing.InventoryEndpointID = ""
	store := provisioningFailureStore(processing.Job, new(bool))
	store.list = func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	store.retry = nil
	store.retryOutcome = core.RetryClaimChanged
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestratorWithSourceOperations(
		store, store, store,
		sourceOperationsResolverFake{operations: &bootstrapGeneratorFake{}},
		provider,
		provider,
		runtimeengine.OrchestratorConfig{MemberID: "member", CurrentTime: testCurrentTime, StopIfNotStartedWithin: 5 * time.Minute},
	)

	result, err := orchestrator.ReconcilePool(context.Background(), processing.Job.RunnerPool)
	if err != nil || result.RemainingProcessing != 1 || len(store.retried) != 1 ||
		store.retried[0] != processing {
		t.Fatalf("ReconcilePool() = (%#v, %v), retried=%#v", result, err, store.retried)
	}
}

func sourceOwnedProcessing() core.ProcessingJob {
	job := sourceOwnedJob()
	return core.ProcessingJob{
		Job:                 job,
		MemberID:            "member",
		RunnerName:          core.AttemptRunnerName(job.ID, core.AttemptID(uuid.New())),
		ClaimedAt:           testCurrentTime(),
		InventoryEndpointID: "test:endpoint",
	}
}
