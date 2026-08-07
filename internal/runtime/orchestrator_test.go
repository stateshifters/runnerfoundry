package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestProcessOneStartsExactlyOneDequeuedJob(t *testing.T) {
	evidence := newProcessOneEvidence(t)
	processed, err := evidence.orchestrator.ProcessOne(context.Background(), "linux")
	if err != nil {
		t.Fatalf("ProcessOne() error = %v", err)
	}
	if !processed {
		t.Fatal("ProcessOne() processed = false")
	}
	if evidence.claimCalls != 1 || evidence.startCalls != 1 {
		t.Fatalf("claim calls = %d, start calls = %d; want exactly one each", evidence.claimCalls, evidence.startCalls)
	}
}

type processOneEvidence struct {
	orchestrator *runtimeengine.Orchestrator
	claimCalls   int
	startCalls   int
}

func newProcessOneEvidence(t *testing.T) *processOneEvidence {
	t.Helper()
	jobID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	memberID := core.MemberID("member-2")
	job := core.Job{ID: jobID, RunnerPool: "linux", EnqueuedAt: time.Unix(1, 0)}
	evidence := &processOneEvidence{}
	store := &jobStoreFake{take: func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error) {
		evidence.claimCalls++
		return core.ProcessingJob{Job: job, MemberID: memberID}, true, nil
	}}
	bootstrapper := &bootstrapGeneratorFake{generate: func(_ context.Context, actual core.Job, runnerName core.RunnerName) (execution.Bootstrap, error) {
		if actual.ID != jobID || runnerName != core.RunnerName("runnerfoundry-"+jobID.String()) {
			t.Fatalf("bootstrap job = %#v, runner = %q", actual, runnerName)
		}
		return execution.NewBootstrap([]byte("opaque")), nil
	}}
	provider := &providerFake{start: func(_ context.Context, request execution.StartRequest) error {
		evidence.startCalls++
		if request.RunnerPool != "linux" || request.RunnerName != core.RunnerName("runnerfoundry-"+jobID.String()) {
			t.Fatalf("start request = %#v", request)
		}
		return nil
	}}
	evidence.orchestrator = runtimeengine.NewOrchestrator(store, bootstrapper, provider, provider, memberID, testCurrentTime)
	return evidence
}

type jobStoreFake struct {
	enqueue         func(context.Context, core.JobRequest) (core.Job, bool, error)
	take            func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error)
	terminalize     func(context.Context, core.SourceReference) error
	started         func(context.Context, core.SourceReference) error
	list            func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error)
	poolList        func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error)
	retry           func(context.Context, core.JobID) (bool, error)
	retryOutcome    core.RetryOutcome
	recover         func(context.Context, core.ProcessingJob) (core.RetryResult, error)
	remove          func(context.Context, core.JobID) error
	removeTerminal  func(context.Context, core.JobID) (bool, error)
	terminalOutcome core.TerminalRemovalOutcome
}

func (store *jobStoreFake) ClaimOldest(ctx context.Context, pool core.RunnerPool, claim core.ProcessingClaim) (core.ClaimResult, error) {
	job, taken, err := store.take(ctx, pool, claim.MemberID)
	job.ClaimedAt = claim.ClaimedAt
	job.InventoryEndpointID = claim.InventoryEndpointID
	outcome := core.ClaimEmpty
	if taken {
		outcome = core.ClaimClaimed
	}
	return core.ClaimResult{Processing: job, Outcome: outcome}, err
}

func (store *jobStoreFake) Queue(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	job, enqueued, err := store.enqueue(ctx, request)
	outcome := core.QueueAlreadyQueued
	if enqueued {
		outcome = core.QueueQueued
	}
	return core.EnqueueResult{Job: job, Outcome: outcome}, err
}
func (store *jobStoreFake) TerminalizeSource(ctx context.Context, source core.SourceReference) error {
	return store.terminalize(ctx, source)
}
func (store *jobStoreFake) MarkRunnerStarted(ctx context.Context, source core.SourceReference) error {
	return store.started(ctx, source)
}
func (store *jobStoreFake) ProcessingByMember(ctx context.Context, member core.MemberID, pool core.RunnerPool) ([]core.ProcessingJob, error) {
	if store.list == nil {
		return nil, nil
	}
	return store.list(ctx, member, pool)
}
func (store *jobStoreFake) ProcessingByPool(ctx context.Context, pool core.RunnerPool) ([]core.ProcessingJob, error) {
	if store.poolList != nil {
		return store.poolList(ctx, pool)
	}
	return store.ProcessingByMember(ctx, "", pool)
}
func (store *jobStoreFake) RetryUnstarted(ctx context.Context, jobID core.JobID) (core.RetryResult, error) {
	if store.retry == nil {
		if store.retryOutcome != "" {
			return core.RetryResult{Outcome: store.retryOutcome}, nil
		}
		return core.RetryResult{Outcome: core.RetryRequeued}, nil
	}
	applied, err := store.retry(ctx, jobID)
	outcome := core.RetryClaimChanged
	if applied {
		outcome = core.RetryRequeued
	}
	return core.RetryResult{Outcome: outcome}, err
}
func (store *jobStoreFake) RecoverUnstartedProcessing(ctx context.Context, job core.ProcessingJob) (core.RetryResult, error) {
	if store.recover != nil {
		return store.recover(ctx, job)
	}
	return core.RetryResult{Outcome: core.RetryRequeued}, nil
}
func (store *jobStoreFake) RemoveTerminal(ctx context.Context, jobID core.JobID) (core.TerminalRemovalResult, error) {
	if store.terminalOutcome != "" {
		return core.TerminalRemovalResult{Outcome: store.terminalOutcome}, nil
	}
	if store.removeTerminal != nil {
		applied, err := store.removeTerminal(ctx, jobID)
		outcome := core.TerminalNotTerminal
		if applied {
			outcome = core.TerminalRemoved
		}
		return core.TerminalRemovalResult{Outcome: outcome}, err
	}
	if store.remove == nil {
		return core.TerminalRemovalResult{Outcome: core.TerminalRemoved}, nil
	}
	return core.TerminalRemovalResult{Outcome: core.TerminalRemoved}, store.remove(ctx, jobID)
}

type bootstrapGeneratorFake struct {
	generate func(context.Context, core.Job, core.RunnerName) (execution.Bootstrap, error)
}

type launcherFake struct{}

func (launcherFake) EndpointID() execution.InventoryEndpointID { return "test:launcher" }
func (launcherFake) Validate(context.Context) error            { return nil }
func (launcherFake) Start(context.Context, execution.StartRequest) error {
	return nil
}

func (generator *bootstrapGeneratorFake) GenerateBootstrap(ctx context.Context, job core.Job, name core.RunnerName) (execution.Bootstrap, error) {
	return generator.generate(ctx, job, name)
}

func testCurrentTime() time.Time { return time.Unix(1, 0).UTC() }

type providerFake struct {
	start      func(context.Context, execution.StartRequest) error
	inspect    func(context.Context) ([]execution.RunnerInstance, error)
	destroy    func(context.Context, execution.RunnerInstance) error
	canInspect func(execution.InventoryEndpointID, core.MemberID) bool
}

func (*providerFake) Validate(context.Context) error { return nil }
func (provider *providerFake) Start(ctx context.Context, request execution.StartRequest) error {
	if provider.start == nil {
		return nil
	}
	return provider.start(ctx, request)
}
func (provider *providerFake) EndpointID() execution.InventoryEndpointID { return "test:endpoint" }
func (provider *providerFake) KnowsEndpoint(endpointID execution.InventoryEndpointID) bool {
	return provider.EndpointID() == endpointID
}
func (provider *providerFake) CanInspectEndpointForMember(
	endpointID execution.InventoryEndpointID,
	memberID core.MemberID,
) bool {
	if provider.canInspect != nil {
		return provider.canInspect(endpointID, memberID)
	}
	return provider.KnowsEndpoint(endpointID)
}
func (provider *providerFake) Refresh(ctx context.Context) error {
	_, err := provider.Inspect(ctx)
	return err
}
func (provider *providerFake) InspectEndpoint(ctx context.Context, _ execution.InventoryEndpointID) ([]execution.RunnerInstance, error) {
	return provider.Inspect(ctx)
}
func (provider *providerFake) Inspect(ctx context.Context) ([]execution.RunnerInstance, error) {
	if provider.inspect == nil {
		return nil, nil
	}
	return provider.inspect(ctx)
}
func (provider *providerFake) Destroy(ctx context.Context, instance execution.RunnerInstance) error {
	if provider.destroy == nil {
		return nil
	}
	return provider.destroy(ctx, instance)
}
