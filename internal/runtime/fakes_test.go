package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func runtimeTestID(seed byte) core.JobID {
	var identifier core.JobID
	identifier[15] = seed
	return identifier
}

type runtimeStoreFake struct {
	core.JobStore
	enqueue                           func(context.Context, core.JobRequest) (core.JobRecord, bool, error)
	claimOldest                       func(context.Context, core.RunnerPool, core.MemberID, time.Time) (core.JobRecord, bool, error)
	markStartedBySource               func(context.Context, core.SourceReference, time.Time) (core.JobRecord, error)
	beginAcquisition                  func(context.Context, core.JobID, time.Time) (core.JobRecord, error)
	confirmAcquisition                func(context.Context, core.JobID, time.Time) (core.JobRecord, error)
	setResultPending                  func(context.Context, core.JobID, core.Result) (core.JobRecord, error)
	setResultPendingBySource          func(context.Context, core.SourceReference, core.Result) (core.JobRecord, error)
	finalize                          func(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error)
	lookup                            func(context.Context, core.JobID) (core.JobRecord, bool, error)
	lookupReceipt                     func(context.Context, core.SourceReference) (core.Receipt, bool, error)
	attemptsByMember                  func(context.Context, core.MemberID, core.RunnerPool) ([]core.JobRecord, error)
	pendingResults                    func(context.Context, int) ([]core.JobRecord, error)
	pendingAcquisitions               func(context.Context, int) ([]core.JobRecord, error)
	setMemberAttemptsResultPending    func(context.Context, core.MemberID, core.Result) ([]core.JobRecord, error)
	setUnstartedAttemptsResultPending func(context.Context, core.RunnerPool, time.Time, core.Result) ([]core.JobRecord, error)
}

func (store *runtimeStoreFake) Enqueue(ctx context.Context, request core.JobRequest) (core.JobRecord, bool, error) {
	return store.enqueue(ctx, request)
}

func (store *runtimeStoreFake) ClaimOldest(ctx context.Context, pool core.RunnerPool, member core.MemberID, at time.Time) (core.JobRecord, bool, error) {
	return store.claimOldest(ctx, pool, member, at)
}

func (store *runtimeStoreFake) MarkStartedBySource(ctx context.Context, source core.SourceReference, at time.Time) (core.JobRecord, error) {
	return store.markStartedBySource(ctx, source, at)
}

func (store *runtimeStoreFake) BeginAcquisition(ctx context.Context, jobID core.JobID, at time.Time) (core.JobRecord, error) {
	return store.beginAcquisition(ctx, jobID, at)
}

func (store *runtimeStoreFake) ConfirmAcquisition(ctx context.Context, jobID core.JobID, at time.Time) (core.JobRecord, error) {
	return store.confirmAcquisition(ctx, jobID, at)
}

func (store *runtimeStoreFake) SetResultPending(ctx context.Context, jobID core.JobID, result core.Result) (core.JobRecord, error) {
	return store.setResultPending(ctx, jobID, result)
}

func (store *runtimeStoreFake) SetResultPendingBySource(ctx context.Context, source core.SourceReference, result core.Result) (core.JobRecord, error) {
	return store.setResultPendingBySource(ctx, source, result)
}

func (store *runtimeStoreFake) SetMemberAttemptsResultPending(ctx context.Context, member core.MemberID, result core.Result) ([]core.JobRecord, error) {
	return store.setMemberAttemptsResultPending(ctx, member, result)
}

func (store *runtimeStoreFake) SetUnstartedAttemptsResultPending(ctx context.Context, runnerPool core.RunnerPool, before time.Time, result core.Result) ([]core.JobRecord, error) {
	return store.setUnstartedAttemptsResultPending(ctx, runnerPool, before, result)
}

func (store *runtimeStoreFake) Finalize(ctx context.Context, jobID core.JobID, delivery core.ResultDelivery) (core.Receipt, error) {
	return store.finalize(ctx, jobID, delivery)
}

func (store *runtimeStoreFake) Lookup(ctx context.Context, jobID core.JobID) (core.JobRecord, bool, error) {
	return store.lookup(ctx, jobID)
}

func (store *runtimeStoreFake) LookupReceipt(ctx context.Context, source core.SourceReference) (core.Receipt, bool, error) {
	return store.lookupReceipt(ctx, source)
}

func (store *runtimeStoreFake) AttemptsByMember(ctx context.Context, member core.MemberID, pool core.RunnerPool) ([]core.JobRecord, error) {
	return store.attemptsByMember(ctx, member, pool)
}

func (store *runtimeStoreFake) PendingResults(ctx context.Context, limit int) ([]core.JobRecord, error) {
	return store.pendingResults(ctx, limit)
}

func (store *runtimeStoreFake) PendingAcquisitions(ctx context.Context, limit int) ([]core.JobRecord, error) {
	return store.pendingAcquisitions(ctx, limit)
}

type sourceAcquirerFake func(context.Context, core.Job) error

func (acquirer sourceAcquirerFake) Acquire(ctx context.Context, job core.Job) error {
	return acquirer(ctx, job)
}

type bootstrapGeneratorFake func(context.Context, core.Job, string) (execution.Bootstrap, error)

func (generator bootstrapGeneratorFake) GenerateBootstrap(ctx context.Context, job core.Job, name string) (execution.Bootstrap, error) {
	return generator(ctx, job, name)
}

type providerFake struct {
	execution.ExecutionProvider
	start   func(context.Context, execution.StartRequest) error
	inspect func(context.Context, string) ([]execution.RunnerInstance, error)
	destroy func(context.Context, execution.RunnerInstance) error
}

func (provider *providerFake) Start(ctx context.Context, request execution.StartRequest) error {
	return provider.start(ctx, request)
}

func (provider *providerFake) Inspect(ctx context.Context, pool string) ([]execution.RunnerInstance, error) {
	return provider.inspect(ctx, pool)
}

func (provider *providerFake) Destroy(ctx context.Context, instance execution.RunnerInstance) error {
	return provider.destroy(ctx, instance)
}

type resultReporterFake func(context.Context, core.Job, core.Result) (core.ResultDelivery, error)

func (reporter resultReporterFake) ReportResult(ctx context.Context, job core.Job, result core.Result) (core.ResultDelivery, error) {
	return reporter(ctx, job, result)
}

type sourceAdapterFake struct {
	reporter  core.ResultReporter
	acquirer  runtimeengine.SourceAcquirer
	generator runtimeengine.BootstrapGenerator
}

func combineSource(
	reporter core.ResultReporter,
	acquirer runtimeengine.SourceAcquirer,
	generator runtimeengine.BootstrapGenerator,
) *sourceAdapterFake {
	return &sourceAdapterFake{reporter: reporter, acquirer: acquirer, generator: generator}
}

func (source *sourceAdapterFake) Acquire(ctx context.Context, job core.Job) error {
	if err := source.acquirer.Acquire(ctx, job); err != nil {
		return fmt.Errorf("fake acquire: %w", err)
	}
	return nil
}

func (source *sourceAdapterFake) GenerateBootstrap(
	ctx context.Context,
	job core.Job,
	runnerName string,
) (execution.Bootstrap, error) {
	bootstrap, err := source.generator.GenerateBootstrap(ctx, job, runnerName)
	if err != nil {
		return execution.Bootstrap{}, fmt.Errorf("fake bootstrap: %w", err)
	}
	return bootstrap, nil
}

func (source *sourceAdapterFake) ReportResult(ctx context.Context, job core.Job, result core.Result) (core.ResultDelivery, error) {
	delivery, err := source.reporter.ReportResult(ctx, job, result)
	if err != nil {
		return "", fmt.Errorf("fake result report: %w", err)
	}
	return delivery, nil
}

func assertEvents(t *testing.T, actual, expected []string) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("events = %v, want %v", actual, expected)
	}
	for index := range expected {
		if actual[index] != expected[index] {
			t.Fatalf("events = %v, want %v", actual, expected)
		}
	}
}

func requireNoError(t *testing.T, operation string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", operation, err)
	}
}

func requireErrorIs(t *testing.T, operation string, err, expected error) {
	t.Helper()
	if !errors.Is(err, expected) {
		t.Fatalf("%s error = %v, want %v", operation, err, expected)
	}
}

func requireEqual[T comparable](t *testing.T, operation string, actual, expected T) {
	t.Helper()
	if actual != expected {
		t.Fatalf("%s = %v, want %v", operation, actual, expected)
	}
}

func fixedClock() time.Time {
	return time.Unix(1, 0).UTC()
}

func assertInventoryFailures(t *testing.T, operation func(*runtimeengine.Orchestrator) error) {
	t.Helper()
	wantErr := errors.New("inventory unavailable")
	store := &runtimeStoreFake{attemptsByMember: func(context.Context, core.MemberID, core.RunnerPool) ([]core.JobRecord, error) {
		return nil, wantErr
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), nil, runtimeTestID(1), fixedClock)
	if err := operation(orchestrator); !errors.Is(err, wantErr) {
		t.Fatalf("attempt inventory error = %v, want %v", err, wantErr)
	}

	store.attemptsByMember = func(context.Context, core.MemberID, core.RunnerPool) ([]core.JobRecord, error) { return nil, nil }
	provider := &providerFake{inspect: func(context.Context, string) ([]execution.RunnerInstance, error) { return nil, wantErr }}
	orchestrator = runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), provider, runtimeTestID(1), fixedClock)
	if err := operation(orchestrator); !errors.Is(err, wantErr) {
		t.Fatalf("provider inventory error = %v, want %v", err, wantErr)
	}
}
