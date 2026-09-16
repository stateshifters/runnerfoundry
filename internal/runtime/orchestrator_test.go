package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	runtimeengine "github.com/stateshifters/runnerfoundry/internal/runtime"
)

func newTestOrchestrator(
	store *jobStoreFake,
	bootstrapper core.RunnerBootstrapProvider,
	launcher execution.RunnerLauncher,
	inventory execution.RunnerInventory,
	member core.MemberID,
	currentTime func() time.Time,
) *runtimeengine.Orchestrator {
	return runtimeengine.NewOrchestratorWithSourceOperations(
		store, store, store, sourceOperationsResolverFake{operations: bootstrapper}, launcher, inventory, member, currentTime,
	)
}

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

func TestOrchestratorUsesResolvedBootstrapProviderForReconciliation(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	processing := core.ProcessingJob{
		Job: core.Job{
			ID: uuid.New(), Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux",
		},
		ClaimedAt: now.Add(-5 * time.Minute), InventoryEndpointID: "test:endpoint",
	}
	store := &jobStoreFake{list: func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error) {
		return []core.ProcessingJob{processing}, nil
	}}
	cleaned := false
	bootstrapper := &bootstrapGeneratorFake{cleanup: func(context.Context, core.Job, core.RunnerName) error {
		cleaned = true
		return nil
	}}
	provider := &providerFake{}
	orchestrator := newTestOrchestrator(
		store, bootstrapper, provider, provider, "member", func() time.Time { return now },
	)

	result, err := orchestrator.ReconcilePool(context.Background(), "linux")
	if err != nil || !cleaned || result.RemovedProcessing != 1 || result.RemainingProcessing != 0 {
		t.Fatalf("ReconcilePool() = (%#v, %v), bootstrap cleaned=%t", result, err, cleaned)
	}
}

func TestProvisioningAttemptsForSameJobUseDifferentRunnerNames(t *testing.T) {
	job := core.Job{ID: uuid.New(), RunnerPool: "linux"}
	names := make([]core.RunnerName, 0, 2)
	store := &jobStoreFake{persistAttempt: true, take: func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error) {
		return core.ProcessingJob{Job: job}, true, nil
	}}
	bootstrapper := &bootstrapGeneratorFake{generate: func(_ context.Context, _ core.Job, name core.RunnerName) (core.Bootstrap, error) {
		names = append(names, name)
		return core.NewBootstrap([]byte("opaque")), nil
	}}
	provider := &providerFake{}
	orchestrator := newTestOrchestrator(store, bootstrapper, provider, provider, "member", testCurrentTime)
	for range 2 {
		if _, err := orchestrator.Process(context.Background(), "linux"); err != nil {
			t.Fatalf("Process() error = %v", err)
		}
	}
	if len(names) != 2 || names[0] == names[1] {
		t.Fatalf("attempt runner names = %#v", names)
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
	var claimedRunnerName core.RunnerName
	store := &jobStoreFake{persistAttempt: true, take: func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error) {
		evidence.claimCalls++
		return core.ProcessingJob{Job: job, MemberID: memberID}, true, nil
	}}
	bootstrapper := &bootstrapGeneratorFake{generate: func(_ context.Context, actual core.Job, runnerName core.RunnerName) (core.Bootstrap, error) {
		claimedRunnerName = core.AttemptRunnerName(jobID, store.lastClaim.AttemptID)
		if actual.ID != jobID || runnerName == "" || runnerName != claimedRunnerName {
			t.Fatalf("bootstrap job = %#v, runner = %q", actual, runnerName)
		}
		return core.NewBootstrap([]byte("opaque")), nil
	}}
	provider := &providerFake{start: func(_ context.Context, request execution.StartRequest) error {
		evidence.startCalls++
		if request.RunnerPool != "linux" || request.RunnerName != claimedRunnerName {
			t.Fatalf("start request = %#v", request)
		}
		return nil
	}}
	evidence.orchestrator = newTestOrchestrator(store, bootstrapper, provider, provider, memberID, testCurrentTime)
	return evidence
}

type jobStoreFake struct {
	lastClaim       core.ProcessingClaim
	persistAttempt  bool
	enqueue         func(context.Context, core.JobRequest) (core.Job, bool, error)
	take            func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error)
	terminalize     func(context.Context, core.JobIdentity) (core.ObservationResult, error)
	accepted        func(context.Context, core.JobIdentity) (core.ObservationResult, error)
	list            func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error)
	poolList        func(context.Context, core.RunnerPool) ([]core.ProcessingJob, error)
	retry           func(context.Context, core.JobID) (bool, error)
	retried         []core.ProcessingJob
	retryOutcome    core.RetryOutcome
	recover         func(context.Context, core.ProcessingJob) (core.RetryResult, error)
	remove          func(context.Context, core.JobID) error
	removeTerminal  func(context.Context, core.JobID) (bool, error)
	terminalOutcome core.TerminalRemovalOutcome
	reserve         func(context.Context, core.ProcessingJob) (core.RetryResult, error)
	recordReported  func(context.Context, core.ProcessingJob) (core.RetryResult, error)
	finalizeFailure func(context.Context, core.ProcessingJob) (core.TerminalRemovalResult, error)
}

func (store *jobStoreFake) ClaimOldest(ctx context.Context, pool core.RunnerPool, claim core.ProcessingClaim) (core.ClaimResult, error) {
	store.lastClaim = claim
	job, taken, err := store.take(ctx, pool, claim.MemberID)
	job.ClaimedAt = claim.ClaimedAt
	job.InventoryEndpointID = claim.InventoryEndpointID
	if store.persistAttempt || job.RunnerName == "" {
		job.RunnerName = core.AttemptRunnerName(job.Job.ID, claim.AttemptID)
	}
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
func (store *jobStoreFake) Terminalize(ctx context.Context, identity core.JobIdentity) (core.ObservationResult, error) {
	return store.terminalize(ctx, identity)
}
func (store *jobStoreFake) ObserveRunnerAcceptance(ctx context.Context, identity core.JobIdentity) (core.ObservationResult, error) {
	return store.accepted(ctx, identity)
}
func (store *jobStoreFake) ProcessingByMember(ctx context.Context, member core.MemberID, pool core.RunnerPool) ([]core.ProcessingJob, error) {
	if store.list == nil {
		return nil, nil
	}
	processing, err := store.list(ctx, member, pool)
	return normalizedTestProcessing(processing), err
}
func (store *jobStoreFake) ProcessingByPool(ctx context.Context, pool core.RunnerPool) ([]core.ProcessingJob, error) {
	if store.poolList != nil {
		processing, err := store.poolList(ctx, pool)
		return normalizedTestProcessing(processing), err
	}
	return store.ProcessingByMember(ctx, "", pool)
}

func normalizedTestProcessing(processing []core.ProcessingJob) []core.ProcessingJob {
	for index := range processing {
		if processing[index].RunnerName == "" {
			processing[index].RunnerName = testRunnerName(processing[index].Job.ID)
		}
	}
	return processing
}

func testRunnerName(jobID core.JobID) core.RunnerName {
	return core.AttemptRunnerName(jobID, core.AttemptID{1})
}
func (store *jobStoreFake) RetryUnstarted(
	ctx context.Context,
	processing core.ProcessingJob,
) (core.RetryResult, error) {
	store.retried = append(store.retried, processing)
	if store.retry == nil {
		if store.retryOutcome != "" {
			return core.RetryResult{Outcome: store.retryOutcome}, nil
		}
		return core.RetryResult{Outcome: core.RetryRequeued}, nil
	}
	applied, err := store.retry(ctx, processing.Job.ID)
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

func (store *jobStoreFake) ReserveProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
) (core.RetryResult, error) {
	if store.reserve != nil {
		return store.reserve(ctx, processing)
	}
	return core.RetryResult{Outcome: core.RetryFailureReportingPending}, nil
}

func (store *jobStoreFake) RecordProvisioningFailureReported(
	ctx context.Context,
	processing core.ProcessingJob,
) (core.RetryResult, error) {
	if store.recordReported != nil {
		return store.recordReported(ctx, processing)
	}
	return core.RetryResult{Outcome: core.RetryFailureFinalizationPending}, nil
}

func (store *jobStoreFake) FinalizeProvisioningFailure(
	ctx context.Context,
	processing core.ProcessingJob,
) (core.TerminalRemovalResult, error) {
	if store.finalizeFailure != nil {
		return store.finalizeFailure(ctx, processing)
	}
	return core.TerminalRemovalResult{Outcome: core.TerminalRemoved}, nil
}

type bootstrapGeneratorFake struct {
	generate func(context.Context, core.Job, core.RunnerName) (core.Bootstrap, error)
	cleanup  func(context.Context, core.Job, core.RunnerName) error
}

type launcherFake struct{}

func (launcherFake) EndpointID() execution.InventoryEndpointID { return "test:launcher" }
func (launcherFake) Validate(context.Context) error            { return nil }
func (launcherFake) Start(context.Context, execution.StartRequest) error {
	return nil
}

func (generator *bootstrapGeneratorFake) GenerateBootstrap(ctx context.Context, job core.Job, name core.RunnerName) (core.Bootstrap, error) {
	return generator.generate(ctx, job, name)
}

func (generator *bootstrapGeneratorFake) CleanupBootstrap(ctx context.Context, job core.Job, name core.RunnerName) error {
	if generator.cleanup == nil {
		return nil
	}
	return generator.cleanup(ctx, job, name)
}

func testCurrentTime() time.Time { return time.Unix(1, 0).UTC() }

type providerFake struct {
	start      func(context.Context, execution.StartRequest) error
	inspect    func(context.Context) ([]execution.RunnerInstance, error)
	destroy    func(context.Context, execution.RunnerInstance) error
	canInspect func(execution.InventoryEndpointID, core.MemberID) bool
	lastStart  execution.StartRequest
}

func (*providerFake) Validate(context.Context) error { return nil }
func (provider *providerFake) Start(ctx context.Context, request execution.StartRequest) error {
	provider.lastStart = request
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
	instances, err := provider.Inspect(ctx)
	for index := range instances {
		if instances[index].RunnerName == testRunnerName(provider.lastStart.JobID) {
			instances[index].RunnerName = provider.lastStart.RunnerName
		}
	}
	return instances, err
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
