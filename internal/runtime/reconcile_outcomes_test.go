package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestReconcilePoolCorrectsOwnershipCountsWhenSnapshotIsStale(t *testing.T) {
	now := time.Unix(600, 0).UTC()
	tests := map[string]struct {
		processing core.ProcessingJob
		store      *jobStoreFake
	}{
		"recovery already removed": {
			processing: core.ProcessingJob{Job: core.Job{ID: uuid.New(), RunnerPool: "linux"},
				ClaimedAt: now.Add(-5 * time.Minute), InventoryEndpointID: "test:endpoint"},
			store: &jobStoreFake{recover: func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
				return core.RetryResult{Outcome: core.RetryMissing}, nil
			}},
		},
		"terminal already removed": {
			processing: core.ProcessingJob{Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, SourceTerminal: true},
			store:      &jobStoreFake{terminalOutcome: core.TerminalMissing},
		},
		"retry ownership already removed": {
			processing: core.ProcessingJob{Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}},
			store:      &jobStoreFake{retryOutcome: core.RetryMissing},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) { assertStaleOwnershipCorrected(t, now, test.processing, test.store) })
	}
}

func assertStaleOwnershipCorrected(t *testing.T, now time.Time, processing core.ProcessingJob, store *jobStoreFake) {
	t.Helper()
	store.list = func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", func() time.Time { return now })
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if err != nil || result.RemovedProcessing != 0 || result.RemainingProcessing != 0 {
		t.Fatalf("reconcile = (%#v, %v)", result, err)
	}
}
