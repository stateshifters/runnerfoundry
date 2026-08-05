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
	memberID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
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
	orchestrator := runtimeengine.NewOrchestrator(store, bootstrapper, provider, memberID)

	processed, err := orchestrator.ProcessOne(context.Background(), "linux")
	if err != nil {
		t.Fatalf("ProcessOne() error = %v", err)
	}
	if !processed {
		t.Fatal("ProcessOne() processed = false")
	}
}

type jobStoreFake struct {
	enqueue func(context.Context, core.JobRequest) (core.Job, bool, error)
	take    func(context.Context, core.RunnerPool, core.MemberID) (core.ProcessingJob, bool, error)
	cancel  func(context.Context, core.SourceReference) error
	list    func(context.Context, core.MemberID, core.RunnerPool) ([]core.ProcessingJob, error)
	remove  func(context.Context, core.JobID) error
}

func (store *jobStoreFake) Enqueue(ctx context.Context, request core.JobRequest) (core.Job, bool, error) {
	return store.enqueue(ctx, request)
}
func (store *jobStoreFake) TakeOldest(ctx context.Context, pool core.RunnerPool, member core.MemberID) (core.ProcessingJob, bool, error) {
	return store.take(ctx, pool, member)
}
func (store *jobStoreFake) CancelQueued(ctx context.Context, source core.SourceReference) error {
	return store.cancel(ctx, source)
}
func (store *jobStoreFake) ProcessingByMember(ctx context.Context, member core.MemberID, pool core.RunnerPool) ([]core.ProcessingJob, error) {
	if store.list == nil {
		return nil, nil
	}
	return store.list(ctx, member, pool)
}
func (store *jobStoreFake) RemoveProcessing(ctx context.Context, jobID core.JobID) error {
	if store.remove == nil {
		return nil
	}
	return store.remove(ctx, jobID)
}

type bootstrapGeneratorFake struct {
	generate func(context.Context, core.Job, string) (execution.Bootstrap, error)
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
func (provider *providerFake) Inspect(ctx context.Context, pool string) ([]execution.RunnerInstance, error) {
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
