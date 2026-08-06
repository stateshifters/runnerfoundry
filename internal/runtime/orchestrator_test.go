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
	jobID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	memberID := core.MemberID("member-2")
	job := core.Job{ID: jobID, RunnerPool: "linux", EnqueuedAt: time.Unix(1, 0)}
	store := &jobStoreFake{take: func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error) {
		return core.ProcessingJob{Job: job, MemberID: memberID}, true, nil
	}}
	bootstrapper := &bootstrapGeneratorFake{generate: func(_ context.Context, actual core.Job, runnerName string) (execution.Bootstrap, error) {
		if actual.ID != jobID || runnerName != "runnerfoundry-"+jobID.String() {
			t.Fatalf("bootstrap job = %#v, runner = %q", actual, runnerName)
		}
		return execution.NewBootstrap([]byte("opaque")), nil
	}}
	provider := &providerFake{start: func(_ context.Context, request execution.StartRequest) error {
		if request.RunnerPool != "linux" || request.RunnerName != "runnerfoundry-"+jobID.String() {
			t.Fatalf("start request = %#v", request)
		}
		return nil
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, bootstrapper, provider, provider, memberID)

	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if err != nil {
		t.Fatalf("ProcessOne() error = %v", err)
	}
	if !processed {
		t.Fatal("ProcessOne() processed = false")
	}
}

type jobStoreFake struct {
	enqueue        func(context.Context, core.JobRequest) (core.Job, bool, error)
	take           func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error)
	terminalize    func(context.Context, core.SourceReference) error
	started        func(context.Context, core.SourceReference) error
	list           func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error)
	retry          func(context.Context, core.JobID) (bool, error)
	recover        func(context.Context, core.ProcessingJob) (core.TransitionResult, error)
	remove         func(context.Context, core.JobID) error
	removeTerminal func(context.Context, core.JobID) (bool, error)
}

func (store *jobStoreFake) ClaimOldest(ctx context.Context, pool core.RunnerPool, claim core.ProcessingClaim) (core.TakeResult, error) {
	job, taken, err := store.take(ctx, pool, claim.MemberID)
	job.ClaimedAt = claim.ClaimedAt
	job.InventoryEndpointID = claim.InventoryEndpointID
	return core.TakeResult{Processing: job, Taken: taken}, err
}

func (store *jobStoreFake) Enqueue(ctx context.Context, request core.JobRequest) (core.Job, bool, error) {
	return store.enqueue(ctx, request)
}
func (store *jobStoreFake) Queue(ctx context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	job, enqueued, err := store.Enqueue(ctx, request)
	return core.EnqueueResult{Job: job, Enqueued: enqueued}, err
}
func (store *jobStoreFake) TakeOldest(ctx context.Context, pool core.RunnerPool, member core.MemberID) (core.ProcessingJob, bool, error) {
	return store.take(ctx, pool, member)
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
	return store.ProcessingByMember(ctx, "", pool)
}
func (store *jobStoreFake) RetryUnstartedProcessing(ctx context.Context, jobID core.JobID) (bool, error) {
	if store.retry == nil {
		return true, nil
	}
	return store.retry(ctx, jobID)
}
func (store *jobStoreFake) RetryUnstarted(ctx context.Context, jobID core.JobID) (core.TransitionResult, error) {
	applied, err := store.RetryUnstartedProcessing(ctx, jobID)
	return core.TransitionResult{Applied: applied}, err
}
func (store *jobStoreFake) RecoverUnstartedProcessing(ctx context.Context, job core.ProcessingJob) (core.TransitionResult, error) {
	if store.recover != nil {
		return store.recover(ctx, job)
	}
	return core.TransitionResult{Applied: true}, nil
}
func (store *jobStoreFake) RemoveTerminalProcessing(ctx context.Context, jobID core.JobID) (bool, error) {
	if store.removeTerminal != nil {
		return store.removeTerminal(ctx, jobID)
	}
	if store.remove == nil {
		return true, nil
	}
	return true, store.remove(ctx, jobID)
}
func (store *jobStoreFake) RemoveTerminal(ctx context.Context, jobID core.JobID) (core.TransitionResult, error) {
	applied, err := store.RemoveTerminalProcessing(ctx, jobID)
	return core.TransitionResult{Applied: applied}, err
}

type bootstrapGeneratorFake struct {
	generate func(context.Context, core.Job, string) (execution.Bootstrap, error)
}

type launcherFake struct{}

func (launcherFake) EndpointID() execution.InventoryEndpointID { return "test:launcher" }
func (launcherFake) Validate(context.Context) error            { return nil }
func (launcherFake) Start(context.Context, execution.StartRequest) error {
	return nil
}

func (generator *bootstrapGeneratorFake) GenerateBootstrap(ctx context.Context, job core.Job, name string) (execution.Bootstrap, error) {
	return generator.generate(ctx, job, name)
}

type providerFake struct {
	start   func(context.Context, execution.StartRequest) error
	inspect func(context.Context, string) ([]execution.RunnerInstance, error)
	destroy func(context.Context, execution.RunnerInstance) error
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
func (provider *providerFake) Refresh(ctx context.Context) error {
	_, err := provider.Inspect(ctx)
	return err
}
func (provider *providerFake) InspectEndpoint(ctx context.Context, _ execution.InventoryEndpointID) ([]execution.RunnerInstance, error) {
	return provider.Inspect(ctx)
}
func (provider *providerFake) Inspect(ctx context.Context, pools ...string) ([]execution.RunnerInstance, error) {
	pool := ""
	if len(pools) != 0 {
		pool = pools[0]
	}
	if provider.inspect == nil {
		return nil, nil
	}
	return provider.inspect(ctx, pool)
}
func (provider *providerFake) Destroy(ctx context.Context, instance execution.RunnerInstance) error {
	if provider.destroy == nil {
		return nil
	}
	return provider.destroy(ctx, instance)
}
