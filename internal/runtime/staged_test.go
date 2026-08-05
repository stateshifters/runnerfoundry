package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/execution"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestRequestAwaitsLocalClaim(t *testing.T) {
	t.Parallel()

	request := core.JobRequest{Source: core.SourceReference{Adapter: "source", ID: "request-90"}, RunnerPool: "linux"}
	record := core.JobRecord{Job: core.Job{ID: runtimeTestID(90), Source: request.Source, RunnerPool: request.RunnerPool}, State: core.JobStateQueued}
	store := &runtimeStoreFake{enqueue: func(context.Context, core.JobRequest) (core.JobRecord, bool, error) {
		return record, false, nil
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), nil, runtimeTestID(1), fixedClock)

	requireErrorIs(t, "unclaimed request", orchestrator.Request(context.Background(), request), runtimeengine.ErrAwaitingClaim)
}

func TestRedeliveredAcquiredOrResultPendingRequestNeedsNoFurtherWork(t *testing.T) {
	t.Parallel()

	for _, record := range []core.JobRecord{
		{Job: core.Job{ID: runtimeTestID(1)}, State: core.JobStateAttempting, AcquiredAt: time.Unix(1, 0)},
		{Job: core.Job{ID: runtimeTestID(2)}, State: core.JobStateResultPending},
	} {
		record := record
		store := &runtimeStoreFake{enqueue: func(context.Context, core.JobRequest) (core.JobRecord, bool, error) {
			return record, false, nil
		}}
		orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), nil, runtimeTestID(1), fixedClock)
		requireNoError(t, "redelivered durable request", orchestrator.Request(context.Background(), core.JobRequest{}))
	}
}

func TestRedeliveredClaimedRequestAcquiresInOrder(t *testing.T) {
	t.Parallel()

	now := time.Unix(10, 0)
	request := core.JobRequest{Source: core.SourceReference{Adapter: "source", ID: "request-90"}, RunnerPool: "linux"}
	record := core.JobRecord{Job: core.Job{ID: runtimeTestID(90), Source: request.Source, RunnerPool: request.RunnerPool}, State: core.JobStateAttempting}
	events := make([]string, 0, 4)
	store := &runtimeStoreFake{
		enqueue: func(context.Context, core.JobRequest) (core.JobRecord, bool, error) { return record, false, nil },
		beginAcquisition: func(context.Context, core.JobID, time.Time) (core.JobRecord, error) {
			events = append(events, "begin")
			record.AcquisitionStartedAt = now
			return record, nil
		},
		confirmAcquisition: func(context.Context, core.JobID, time.Time) (core.JobRecord, error) {
			events = append(events, "confirm")
			record.AcquiredAt = now
			return record, nil
		},
	}
	acquirer := sourceAcquirerFake(func(context.Context, core.Job) error {
		events = append(events, "acquire")
		return nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, acquirer, nil), nil, runtimeTestID(1), func() time.Time { return now })

	requireNoError(t, "redelivered request", orchestrator.Request(context.Background(), request))
	assertEvents(t, events, []string{"begin", "acquire", "confirm"})
}

func TestInterruptedAcquisitionIsTerminalizedWithoutReacquiring(t *testing.T) {
	t.Parallel()

	result := core.Result{}
	record := core.JobRecord{
		Job: core.Job{ID: runtimeTestID(91), Source: core.SourceReference{ID: "request-91"}}, State: core.JobStateAttempting,
		AcquisitionStartedAt: time.Unix(1, 0),
	}
	store := &runtimeStoreFake{
		enqueue: func(context.Context, core.JobRequest) (core.JobRecord, bool, error) { return record, false, nil },
		setResultPending: func(_ context.Context, _ core.JobID, actual core.Result) (core.JobRecord, error) {
			result = actual
			return core.JobRecord{Job: record.Job, State: core.JobStateResultPending, Result: &actual}, nil
		},
		finalize: func(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error) {
			return core.Receipt{}, nil
		},
	}
	reporter := resultReporterFake(func(context.Context, core.Job, core.Result) (core.ResultDelivery, error) {
		return core.ResultDeliveryReported, nil
	})
	acquirer := sourceAcquirerFake(func(context.Context, core.Job) error {
		t.Fatal("interrupted acquisition was repeated")
		return nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(reporter, acquirer, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(2, 0) })

	requireNoError(t, "recover interrupted request", orchestrator.Request(context.Background(), core.JobRequest{}))
	requireEqual(t, "interrupted outcome", result.Outcome, core.OutcomeFailure)
	requireEqual(t, "interrupted origin", result.Origin, core.ResultOriginService)
}

func TestMemberStartsOnlyAcquiredAttemptsWithoutAnInstance(t *testing.T) {
	t.Parallel()

	acquiredAt := time.Unix(2, 0)
	jobs := []core.Job{{ID: runtimeTestID(92), RunnerPool: "linux"}, {ID: runtimeTestID(93), RunnerPool: "linux"}, {ID: runtimeTestID(94), RunnerPool: "linux"}}
	store := &runtimeStoreFake{attemptsByMember: func(context.Context, core.MemberID, core.RunnerPool) ([]core.JobRecord, error) {
		return []core.JobRecord{{Job: jobs[0], AcquiredAt: acquiredAt}, {Job: jobs[1], AcquiredAt: acquiredAt}, {Job: jobs[2]}}, nil
	}}
	provider := &providerFake{
		inspect: func(context.Context, string) ([]execution.RunnerInstance, error) {
			return []execution.RunnerInstance{{RunnerName: "runnerfoundry-" + runtimeTestID(93).String(), State: execution.RunnerStateRunning}}, nil
		},
		start: func(_ context.Context, request execution.StartRequest) error {
			if request.RunnerName != "runnerfoundry-"+runtimeTestID(92).String() {
				t.Fatalf("started runner %q", request.RunnerName)
			}
			return nil
		},
	}
	generator := bootstrapGeneratorFake(func(context.Context, core.Job, string) (execution.Bootstrap, error) {
		return execution.NewBootstrap([]byte("opaque")), nil
	})
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, generator), provider, runtimeTestID(1), func() time.Time { return time.Unix(3, 0) })

	started, err := orchestrator.StartAcquired(context.Background(), "linux")
	if err != nil || started != 1 {
		t.Fatalf("start acquired = (%d, %v), want (1, nil)", started, err)
	}
}
