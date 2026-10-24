package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
)

func TestReconcilePoolRecoversOnlyExpiredClaimsWithEndpointProof(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	endpointID := core.InventoryEndpointID("test:endpoint")
	tests := []struct {
		name        string
		claimedAt   time.Time
		endpointID  core.InventoryEndpointID
		accepted    bool
		wantRecover bool
	}{
		{name: "claim younger than lease is retained", claimedAt: now.Add(-5*time.Minute + time.Nanosecond), endpointID: endpointID},
		{name: "claim at lease boundary is recovered", claimedAt: now.Add(-5 * time.Minute), endpointID: endpointID, wantRecover: true},
		{name: "unknown endpoint is retained", claimedAt: now.Add(-6 * time.Minute), endpointID: "unknown"},
		{name: "delayed acceptance is never recovered", claimedAt: now.Add(-6 * time.Minute), endpointID: endpointID, accepted: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, recovered, err := recoveryDecision(now, test.claimedAt, test.endpointID, test.accepted, 5*time.Minute)
			if err != nil || recovered != test.wantRecover {
				t.Fatalf("ReconcilePool() = (%#v, %v), recovered=%t", result, err, recovered)
			}
		})
	}
}

func recoveryDecision(
	now time.Time,
	claimedAt time.Time,
	endpointID core.InventoryEndpointID,
	accepted bool,
	stopIfNotStartedWithin time.Duration,
) (runtimeengine.PoolReconciliation, bool, error) {
	recovered := false
	processing := core.ProcessingJob{Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, ClaimedAt: claimedAt,
		InventoryEndpointID: endpointID, RunnerAccepted: accepted}
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{processing}, nil
		},
		recover: func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
			recovered = true
			return core.RetryResult{Outcome: core.RetryRequeued}, nil
		},
	}
	provider := &providerFake{}
	orchestrator := runtimeengine.NewOrchestratorWithSourceOperations(
		store, store, store, sourceOperationsResolverFake{operations: &bootstrapGeneratorFake{}},
		provider, provider, runtimeengine.OrchestratorConfig{MemberID: "leader", CurrentTime: func() time.Time { return now }, StopIfNotStartedWithin: stopIfNotStartedWithin},
	)
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if err != nil {
		return result, recovered, fmt.Errorf("reconcile recovery decision: %w", err)
	}
	return result, recovered, nil
}

func TestReconcilePoolDestroysExpiredRunnerBeforeRecovery(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	events := make([]string, 0, 2)
	orchestrator := newExpiredRecoveryOrchestrator(now, &events, nil, nil, nil)
	if _, err := orchestrator.ReconcilePool(context.Background(), "linux"); err != nil {
		t.Fatalf("ReconcilePool() error = %v", err)
	}
	if strings.Join(events, ",") != "destroy,recover" {
		t.Fatalf("events = %v, want destroy then recover", events)
	}
}

func TestReconcilePoolRetainsExpiredProcessingWhenRunnerCleanupFails(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	wantErr := errors.New("destroy failed")
	recovered := false
	orchestrator := newExpiredRecoveryOrchestrator(now, nil, wantErr, nil, &recovered)

	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	assertReconciliationError(t, result, err, wantErr, 0, 0)
	if result.RemainingProcessing != 1 || recovered {
		t.Fatalf("ReconcilePool() = (%#v, %v), recovered=%t", result, err, recovered)
	}
}

func TestReconcilePoolRetainsUnstartedProcessingUntilJITCleanupSucceeds(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	t.Run("expired", func(t *testing.T) {
		assertJITCleanupRetainsProcessing(t, now, core.ProcessingJob{
			Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, ClaimedAt: now.Add(-5 * time.Minute),
			InventoryEndpointID: "test:endpoint",
		})
	})
	t.Run("terminal", func(t *testing.T) {
		assertJITCleanupRetainsProcessing(t, now, core.ProcessingJob{
			Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}, SourceTerminal: true,
		})
	})
	t.Run("ordinary", func(t *testing.T) {
		assertJITCleanupRetainsProcessing(t, now, core.ProcessingJob{Job: core.Job{ID: uuid.New(), RunnerPool: "linux"}})
	})
}

func assertJITCleanupRetainsProcessing(t *testing.T, now time.Time, processing core.ProcessingJob) {
	t.Helper()
	cleanupErr := errors.New("JIT cleanup failed")
	transitioned := false
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{processing}, nil
		},
		retry: func(context.Context, core.JobID) (bool, error) { transitioned = true; return true, nil },
		recover: func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
			transitioned = true
			return core.RetryResult{Outcome: core.RetryRequeued}, nil
		},
		remove: func(context.Context, core.JobID) error { transitioned = true; return nil },
	}
	bootstrapper := &bootstrapGeneratorFake{cleanup: func(context.Context, core.Job, core.RunnerName) error {
		return cleanupErr
	}}
	provider := &providerFake{}
	result, err := newTestOrchestrator(store, bootstrapper, provider, provider, "member",
		func() time.Time { return now }).ReconcilePool(context.Background(), "linux")
	if !errors.Is(err, cleanupErr) || transitioned || result.RemainingProcessing != 1 {
		t.Fatalf("ReconcilePool() = (%#v, %v), transitioned=%v", result, err, transitioned)
	}
}

func TestReconcilePoolReportsExpiredProcessingRecoveryFailureAfterCleanup(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	jobID := uuid.New()
	wantErr := errors.New("recovery failed")
	processing := core.ProcessingJob{
		Job: core.Job{ID: jobID, RunnerPool: "linux"}, ClaimedAt: now.Add(-5 * time.Minute),
		InventoryEndpointID: "test:endpoint",
	}
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{processing}, nil
		},
		recover: func(context.Context, core.ProcessingJob) (core.RetryResult, error) {
			return core.RetryResult{}, wantErr
		},
	}
	provider := &providerFake{
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{
				ID: "runner", RunnerName: testRunnerName(jobID), RunnerPool: "linux",
			}}, nil
		},
	}
	orchestrator := newTestOrchestrator(
		store, &bootstrapGeneratorFake{}, provider, provider, "leader", func() time.Time { return now },
	)

	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	assertReconciliationError(t, result, err, wantErr, 1, 0)
	if result.RemainingProcessing != 1 {
		t.Fatalf("remaining processing = %d, want 1", result.RemainingProcessing)
	}
}

func TestReconcilePoolRetainsExpiredProcessingWhenRecoveryTransitionDoesNotApply(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	jobID := uuid.New()
	processing := core.ProcessingJob{
		Job:       core.Job{ID: jobID, RunnerPool: "linux", EnqueuedAt: now.Add(-time.Hour)},
		ClaimedAt: now.Add(-5 * time.Minute), InventoryEndpointID: "test:endpoint",
	}
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{processing}, nil
		},
		recover: func(_ context.Context, recovered core.ProcessingJob) (core.RetryResult, error) {
			if recovered.Job.EnqueuedAt != now {
				t.Fatalf("recovered enqueue time = %v, want %v", recovered.Job.EnqueuedAt, now)
			}
			return core.RetryResult{Outcome: core.RetryClaimChanged}, nil
		},
	}
	provider := &providerFake{}
	orchestrator := newTestOrchestrator(
		store, &bootstrapGeneratorFake{}, provider, provider, "leader", func() time.Time { return now },
	)

	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	assertReconciliation(t, result, err, 0, 0)
	if result.RemainingProcessing != 1 {
		t.Fatalf("remaining processing = %d, want 1", result.RemainingProcessing)
	}
}

func TestReconciliationUsesMemberInventoryInsteadOfTheLaunchProvider(t *testing.T) {
	jobID := uuid.New()
	retried := false
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: jobID, RunnerPool: "linux"}}}, nil
		},
		retry: func(context.Context, core.JobID) (bool, error) {
			retried = true
			return true, nil
		},
	}
	inventory := &providerFake{inspect: func(context.Context) ([]execution.RunnerInstance, error) {
		return []execution.RunnerInstance{{
			ID: "existing", RunnerName: testRunnerName(jobID), State: execution.RunnerStateRunning,
		}}, nil
	}}
	orchestrator := newTestOrchestrator(
		store, &bootstrapGeneratorFake{}, launcherFake{}, inventory, "member", testCurrentTime,
	)
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if err != nil || result.RemainingProcessing != 1 || retried {
		t.Fatalf("reconcile = (%#v, %v), retried=%t", result, err, retried)
	}
}

func TestTerminatePoolDestroysRunningOwnedResourcesAndRetainsAcceptedProcessing(t *testing.T) {
	jobID := uuid.New()
	memberID := core.MemberID("member")
	processing := core.ProcessingJob{Job: core.Job{ID: jobID, RunnerPool: "linux"}, MemberID: memberID}
	processing.RunnerAccepted = true
	events := make([]string, 0, 2)
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{processing}, nil
		},
	}
	provider := &providerFake{
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{
				ID: "runner", RunnerName: testRunnerName(jobID), State: execution.RunnerStateRunning,
			}}, nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error {
			events = append(events, "destroy")
			return nil
		},
	}
	orchestrator := newTestOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, memberID, testCurrentTime)

	result, err := orchestrator.TerminatePool(context.Background(), "linux")
	assertReconciliation(t, result, err, 1, 0)
	if result.RemainingProcessing != 1 || strings.Join(events, ",") != "destroy" {
		t.Fatalf("cleanup events = %v", events)
	}
}

func TestTerminatePoolRetainsProcessingWhenDestroyFails(t *testing.T) {
	jobID := uuid.New()
	wantErr := errors.New("destroy failed")
	removed := false
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: jobID}}}, nil
		},
		remove: func(context.Context, core.JobID) error { removed = true; return nil },
	}
	provider := &providerFake{
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{ID: "runner", RunnerName: testRunnerName(jobID)}}, nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error { return wantErr },
	}
	orchestrator := newTestOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	_, err := orchestrator.TerminatePool(context.Background(), "linux")
	if !errors.Is(err, wantErr) || removed {
		t.Fatalf("TerminatePool() error = %v, removed=%v", err, removed)
	}
}

func TestTerminatePoolReportsInventoryFailures(t *testing.T) {
	wantErr := errors.New("inventory failed")
	tests := map[string]struct {
		store    *jobStoreFake
		provider *providerFake
	}{
		"processing": {
			store: &jobStoreFake{list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
				return nil, wantErr
			}},
			provider: &providerFake{},
		},
		"provider": {
			store: &jobStoreFake{list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
				return nil, nil
			}},
			provider: &providerFake{inspect: func(context.Context) ([]execution.RunnerInstance, error) {
				return nil, wantErr
			}},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			orchestrator := newTestOrchestrator(
				test.store, &bootstrapGeneratorFake{}, test.provider, test.provider, "member", testCurrentTime,
			)
			if _, err := orchestrator.TerminatePool(context.Background(), "linux"); !errors.Is(err, wantErr) {
				t.Fatalf("TerminatePool() error = %v", err)
			}
		})
	}
}

func TestTerminatePoolReportsMissingProcessingRemovalFailure(t *testing.T) {
	missingID := uuid.New()
	wantErr := errors.New("remove failed")
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: missingID}, SourceTerminal: true}}, nil
		},
		remove: func(context.Context, core.JobID) error { return wantErr },
	}
	provider := &providerFake{}
	orchestrator := newTestOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	result, err := orchestrator.TerminatePool(context.Background(), "linux")
	assertReconciliationError(t, result, err, wantErr, 0, 0)
}

func TestTerminatePoolContinuesAfterOwnedRemovalFailure(t *testing.T) {
	resourceID := uuid.New()
	missingID := uuid.New()
	wantErr := errors.New("remove failed")
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{
				{Job: core.Job{ID: resourceID}, SourceTerminal: true},
				{Job: core.Job{ID: missingID}, SourceTerminal: true},
			}, nil
		},
		remove: func(_ context.Context, jobID core.JobID) error {
			if jobID == resourceID {
				return wantErr
			}
			return nil
		},
	}
	provider := &providerFake{inspect: func(context.Context) ([]execution.RunnerInstance, error) {
		return []execution.RunnerInstance{{ID: "resource", RunnerName: testRunnerName(resourceID)}}, nil
	}}
	orchestrator := newTestOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	result, err := orchestrator.TerminatePool(context.Background(), "linux")
	if !errors.Is(err, wantErr) || result.RemovedRunners != 1 || result.RemovedProcessing != 1 {
		t.Fatalf("TerminatePool() = (%#v, %v)", result, err)
	}
}

func TestReconcilePoolPreservesRunningWork(t *testing.T) {
	fixture := newOwnedReconciliationFixture(execution.RunnerStateRunning)
	result, err := fixture.orchestrator.ReconcilePool(context.Background(), "linux")
	assertReconciliation(t, result, err, 0, 0)
	assertNotRecorded(t, "running processing removed", fixture.removed, fixture.jobID)
	assertNotRecorded(t, "running resource destroyed", fixture.destroyed, "runner")
}

type lifecycleReconciliationCase struct {
	name            string
	processing      core.ProcessingJob
	instanceState   execution.RunnerState
	instancePresent bool
	wantDestroyed   bool
	wantRetried     bool
	wantRemoved     bool
	wantRemaining   int
}

func TestReconcilePoolHonoursDurableLifecycleBoundaries(t *testing.T) {
	for _, test := range lifecycleReconciliationCases() {
		t.Run(test.name, func(t *testing.T) { assertLifecycleReconciliation(t, test) })
	}
}

func lifecycleReconciliationCases() []lifecycleReconciliationCase {
	return []lifecycleReconciliationCase{
		{name: "unstarted missing retries", wantRetried: true},
		{name: "accepted missing awaits completion", processing: core.ProcessingJob{RunnerAccepted: true}, wantRemaining: 1},
		{name: "terminal missing removes", processing: core.ProcessingJob{SourceTerminal: true}, wantRemoved: true},
		{name: "unaccepted running is preserved while acceptance may be delayed", instancePresent: true,
			instanceState: execution.RunnerStateRunning, wantRemaining: 1},
		{name: "accepted running is preserved", processing: core.ProcessingJob{RunnerAccepted: true},
			instancePresent: true, instanceState: execution.RunnerStateRunning, wantRemaining: 1},
		{name: "unstarted completed retries after cleanup", instancePresent: true,
			instanceState: execution.RunnerStateCompleted, wantDestroyed: true, wantRetried: true},
		{name: "accepted completed cleans up and awaits completion", processing: core.ProcessingJob{RunnerAccepted: true},
			instancePresent: true, instanceState: execution.RunnerStateCompleted, wantDestroyed: true, wantRemaining: 1},
		{name: "accepted unknown cleans up and awaits completion", processing: core.ProcessingJob{RunnerAccepted: true},
			instancePresent: true, instanceState: execution.RunnerStateUnknown, wantDestroyed: true, wantRemaining: 1},
		{name: "terminal running cleans up and removes",
			processing: core.ProcessingJob{RunnerAccepted: true, SourceTerminal: true}, instancePresent: true,
			instanceState: execution.RunnerStateRunning, wantDestroyed: true, wantRemoved: true},
	}
}

func assertLifecycleReconciliation(t *testing.T, test lifecycleReconciliationCase) {
	t.Helper()
	jobID := uuid.New()
	test.processing.Job = core.Job{ID: jobID, RunnerPool: "linux"}
	effects := &reconciliationEffects{}
	store := lifecycleStore(test.processing, effects)
	provider := lifecycleProvider(jobID, test, effects)
	orchestrator := newTestOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if err != nil {
		t.Fatalf("ReconcilePool() error = %v", err)
	}
	if effects.destroyed != test.wantDestroyed || effects.retried != test.wantRetried || effects.removed != test.wantRemoved {
		t.Fatalf("effects = destroy:%t retry:%t remove:%t", effects.destroyed, effects.retried, effects.removed)
	}
	if result.RemainingProcessing != test.wantRemaining {
		t.Fatalf("remaining processing = %d, want %d", result.RemainingProcessing, test.wantRemaining)
	}
}

type reconciliationEffects struct{ destroyed, retried, removed bool }

func lifecycleStore(processing core.ProcessingJob, effects *reconciliationEffects) *jobStoreFake {
	return &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{processing}, nil
		},
		retry:  func(context.Context, core.JobID) (bool, error) { effects.retried = true; return true, nil },
		remove: func(context.Context, core.JobID) error { effects.removed = true; return nil },
	}
}

func lifecycleProvider(jobID core.JobID, test lifecycleReconciliationCase, effects *reconciliationEffects) *providerFake {
	instances := make([]execution.RunnerInstance, 0, 1)
	if test.instancePresent {
		instances = append(instances, execution.RunnerInstance{
			ID: "runner", RunnerName: testRunnerName(jobID), State: test.instanceState,
		})
	}
	return &providerFake{
		inspect: func(context.Context) ([]execution.RunnerInstance, error) { return instances, nil },
		destroy: func(context.Context, execution.RunnerInstance) error { effects.destroyed = true; return nil },
	}
}

func TestTerminatePoolRequeuesOnlyUnstartedWorkAfterCleanup(t *testing.T) {
	fixture := newTerminatePoolFixture()
	result, err := fixture.orchestrator.TerminatePool(context.Background(), "linux")
	if err != nil || result.RemainingProcessing != 1 {
		t.Fatalf("TerminatePool() = (%#v, %v)", result, err)
	}
	if !fixture.destroyed["unaccepted"] || !fixture.destroyed["accepted"] ||
		!fixture.retried[fixture.unacceptedID] || fixture.retried[fixture.acceptedID] {
		t.Fatalf("destroyed=%v retried=%v", fixture.destroyed, fixture.retried)
	}
}

type terminatePoolFixture struct {
	unacceptedID core.JobID
	acceptedID   core.JobID
	retried      map[core.JobID]bool
	destroyed    map[string]bool
	orchestrator *runtimeengine.Orchestrator
}

func newTerminatePoolFixture() terminatePoolFixture {
	fixture := terminatePoolFixture{
		unacceptedID: uuid.New(), acceptedID: uuid.New(),
		retried: make(map[core.JobID]bool), destroyed: make(map[string]bool),
	}
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{
				{Job: core.Job{ID: fixture.unacceptedID, RunnerPool: "linux"}},
				{Job: core.Job{ID: fixture.acceptedID, RunnerPool: "linux"}, RunnerAccepted: true},
			}, nil
		},
		retry: func(_ context.Context, jobID core.JobID) (bool, error) {
			fixture.retried[jobID] = true
			return true, nil
		},
	}
	provider := newTerminateProvider(&fixture)
	fixture.orchestrator = newTestOrchestrator(
		store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime,
	)
	return fixture
}

func newTerminateProvider(fixture *terminatePoolFixture) *providerFake {
	return &providerFake{
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{
				{ID: "unaccepted", RunnerName: testRunnerName(fixture.unacceptedID), State: execution.RunnerStateRunning},
				{ID: "accepted", RunnerName: testRunnerName(fixture.acceptedID), State: execution.RunnerStateRunning},
			}, nil
		},
		destroy: func(_ context.Context, instance execution.RunnerInstance) error {
			fixture.destroyed[instance.ID] = true
			return nil
		},
	}
}

func TestReconcilePoolRetainsProcessingWhenAtomicTransitionDoesNotApply(t *testing.T) {
	jobID := uuid.New()
	t.Run("retry raced with acceptance or terminal event", func(t *testing.T) {
		assertAtomicTransitionRetained(t, core.ProcessingJob{Job: core.Job{ID: jobID, RunnerPool: "linux"}})
	})
	t.Run("terminal row already changed", func(t *testing.T) {
		assertAtomicTransitionRetained(t, core.ProcessingJob{
			Job: core.Job{ID: jobID, RunnerPool: "linux"}, SourceTerminal: true,
		})
	})
}

func assertAtomicTransitionRetained(t *testing.T, processing core.ProcessingJob) {
	t.Helper()
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{processing}, nil
		},
		retry:          func(context.Context, core.JobID) (bool, error) { return false, nil },
		removeTerminal: func(context.Context, core.JobID) (bool, error) { return false, nil },
	}
	provider := &providerFake{}
	orchestrator := newTestOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if err != nil || result.RemainingProcessing != 1 || result.RemovedProcessing != 0 {
		t.Fatalf("ReconcilePool() = (%#v, %v)", result, err)
	}
}

func TestReconcilePoolReportsOrphanCleanupFailure(t *testing.T) {
	wantErr := errors.New("destroy failed")
	orphanID := uuid.New()
	store := &jobStoreFake{list: emptyProcessing}
	provider := &providerFake{
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{ID: "orphan", RunnerName: testRunnerName(orphanID)}}, nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error { return wantErr },
	}
	orchestrator := newTestOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if !errors.Is(err, wantErr) || result.RemovedRunners != 0 {
		t.Fatalf("ReconcilePool() = (%#v, %v)", result, err)
	}
}

func TestReconcilePoolRetainsTerminalProcessingWhenRunnerCleanupFails(t *testing.T) {
	jobID := uuid.New()
	wantErr := errors.New("destroy failed")
	removed := false
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{
				Job: core.Job{ID: jobID, RunnerPool: "linux"}, SourceTerminal: true,
			}}, nil
		},
		remove: func(context.Context, core.JobID) error { removed = true; return nil },
	}
	provider := &providerFake{
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{
				ID: "runner", RunnerName: testRunnerName(jobID), State: execution.RunnerStateRunning,
			}}, nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error { return wantErr },
	}
	orchestrator := newTestOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if !errors.Is(err, wantErr) || result.RemainingProcessing != 1 || removed {
		t.Fatalf("ReconcilePool() = (%#v, %v), removed=%t", result, err, removed)
	}
}

func TestReconcilePoolCleansFinishedWork(t *testing.T) {
	fixture := newOwnedReconciliationFixture(execution.RunnerStateCompleted)
	result, err := fixture.orchestrator.ReconcilePool(context.Background(), "linux")
	assertReconciliation(t, result, err, 1, 1)
	assertRecorded(t, "finished processing removed", fixture.removed, fixture.jobID)
	assertRecorded(t, "finished resource destroyed", fixture.destroyed, "runner")
}

type ownedReconciliationFixture struct {
	jobID        core.JobID
	removed      map[core.JobID]bool
	destroyed    map[string]bool
	orchestrator *runtimeengine.Orchestrator
}

func newOwnedReconciliationFixture(state execution.RunnerState) ownedReconciliationFixture {
	jobID := uuid.New()
	removed := make(map[core.JobID]bool)
	destroyed := make(map[string]bool)
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: jobID}}}, nil
		},
		retry: func(_ context.Context, jobID core.JobID) (bool, error) {
			removed[jobID] = true
			return true, nil
		},
	}
	provider := &providerFake{
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{
				ID: "runner", RunnerName: testRunnerName(jobID), State: state,
			}}, nil
		},
		destroy: func(_ context.Context, instance execution.RunnerInstance) error {
			destroyed[instance.ID] = true
			return nil
		},
	}
	return ownedReconciliationFixture{
		jobID: jobID, removed: removed, destroyed: destroyed,
		orchestrator: newTestOrchestrator(
			store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime,
		),
	}
}

func TestReconcilePoolCleansMissingAndOrphanedWork(t *testing.T) {
	missingID := uuid.New()
	orphanID := uuid.New()
	removed := false
	destroyed := false
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: missingID}}}, nil
		},
		retry: func(context.Context, core.JobID) (bool, error) { removed = true; return true, nil },
	}
	provider := &providerFake{
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{
				{ID: "orphan", RunnerName: testRunnerName(orphanID)},
				{ID: "external", RunnerName: "external"},
			}, nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error { destroyed = true; return nil },
	}
	orchestrator := newTestOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	assertReconciliation(t, result, err, 1, 1)
	if !removed || !destroyed {
		t.Fatalf("removed=%t destroyed=%t", removed, destroyed)
	}
}

func TestReconcilePoolRetainsProcessingAcrossCleanupFailures(t *testing.T) {
	destroyID := uuid.New()
	removeID := uuid.New()
	destroyErr := errors.New("destroy failed")
	removeErr := errors.New("remove failed")
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: destroyID}}, {Job: core.Job{ID: removeID}}}, nil
		},
		retry: func(_ context.Context, jobID core.JobID) (bool, error) {
			if jobID == removeID {
				return false, removeErr
			}
			return true, nil
		},
	}
	provider := &providerFake{
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{
				ID: "destroy", RunnerName: testRunnerName(destroyID), State: execution.RunnerStateUnknown,
			}}, nil
		},
		destroy: func(context.Context, execution.RunnerInstance) error { return destroyErr },
	}
	orchestrator := newTestOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	_, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if !errors.Is(err, destroyErr) || !errors.Is(err, removeErr) {
		t.Fatalf("ReconcilePool() error = %v", err)
	}
}

func TestReconcilePoolReportsRemovalFailureAfterDestroy(t *testing.T) {
	jobID := uuid.New()
	wantErr := errors.New("remove failed")
	store := &jobStoreFake{
		list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
			return []core.ProcessingJob{{Job: core.Job{ID: jobID}, SourceTerminal: true}}, nil
		},
		remove: func(context.Context, core.JobID) error { return wantErr },
	}
	provider := &providerFake{
		inspect: func(context.Context) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{ID: "finished", RunnerName: testRunnerName(jobID), State: execution.RunnerStateCompleted}}, nil
		},
	}
	orchestrator := newTestOrchestrator(store, &bootstrapGeneratorFake{}, provider, provider, "member", testCurrentTime)
	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if !errors.Is(err, wantErr) || result.RemovedRunners != 1 || result.RemovedProcessing != 0 {
		t.Fatalf("ReconcilePool() = (%#v, %v)", result, err)
	}
}

func assertReconciliation(
	t *testing.T,
	actual runtimeengine.PoolReconciliation,
	err error,
	wantRunners, wantProcessing int,
) {
	t.Helper()
	if err != nil {
		t.Fatalf("reconciliation error = %v", err)
	}
	assertReconciliationCounts(t, actual, wantRunners, wantProcessing)
}

func assertReconciliationError(
	t *testing.T,
	actual runtimeengine.PoolReconciliation,
	err, wantErr error,
	wantRunners, wantProcessing int,
) {
	t.Helper()
	if !errors.Is(err, wantErr) {
		t.Fatalf("reconciliation error = %v, want %v", err, wantErr)
	}
	assertReconciliationCounts(t, actual, wantRunners, wantProcessing)
}

func assertReconciliationCounts(t *testing.T, actual runtimeengine.PoolReconciliation, wantRunners, wantProcessing int) {
	t.Helper()
	if actual.RemovedRunners != wantRunners {
		t.Fatalf("removed runners = %d, want %d", actual.RemovedRunners, wantRunners)
	}
	if actual.RemovedProcessing != wantProcessing {
		t.Fatalf("removed processing = %d, want %d", actual.RemovedProcessing, wantProcessing)
	}
}

func assertRecorded[K comparable](t *testing.T, description string, records map[K]bool, key K) {
	t.Helper()
	if !records[key] {
		t.Fatalf("%s = false, want true", description)
	}
}

func assertNotRecorded[K comparable](t *testing.T, description string, records map[K]bool, key K) {
	t.Helper()
	if records[key] {
		t.Fatalf("%s = true, want false", description)
	}
}

func emptyProcessing(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
	return nil, nil
}
