package runtime_test

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func newExpiredRecoveryOrchestrator(
	now time.Time,
	events *[]string,
	destroyErr error,
	recoverErr error,
	recovered *bool,
) *runtimeengine.Orchestrator {
	jobID := uuid.New()
	processing := core.ProcessingJob{Job: core.Job{ID: jobID, RunnerPool: "linux"},
		ClaimedAt: now.Add(-5 * time.Minute), InventoryEndpointID: "test:endpoint"}
	store := &jobStoreFake{list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}, recover: func(context.Context, core.ProcessingJob) (core.TransitionResult, error) {
		if recovered != nil {
			*recovered = true
		}
		appendRecoveryEvent(events, "recover")
		return core.TransitionResult{Applied: recoverErr == nil}, recoverErr
	}}
	provider := &providerFake{inspect: func(context.Context) ([]execution.RunnerInstance, error) {
		return []execution.RunnerInstance{{ID: "runner", RunnerName: core.RunnerName("runnerfoundry-" + jobID.String()),
			RunnerPool: "linux", EndpointID: "test:endpoint"}}, nil
	}, destroy: func(context.Context, execution.RunnerInstance) error {
		appendRecoveryEvent(events, "destroy")
		return destroyErr
	}}
	return runtimeengine.NewOrchestrator(
		store, &bootstrapGeneratorFake{}, provider, provider, "leader", func() time.Time { return now },
	)
}

func appendRecoveryEvent(events *[]string, event string) {
	if events != nil {
		*events = append(*events, event)
	}
}
