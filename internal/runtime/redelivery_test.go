package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestRedeliveredStartedAndCompletedEventsRetryOnlyPendingDelivery(t *testing.T) {
	t.Parallel()

	fixture := newRedeliveryFixture()
	if err := fixture.deliverBatch(); err == nil {
		t.Fatal("first batch unexpectedly delivered")
	}
	requireEqual(t, "pending after first batch", fixture.pending, true)
	requireEqual(t, "finalized after first batch", fixture.finalized, false)
	requireNoError(t, "redelivered batch", fixture.deliverBatch())
	requireEqual(t, "finalized after redelivery", fixture.finalized, true)
	requireEqual(t, "report attempts", fixture.reports, 2)
}

type redeliveryFixture struct {
	handler   *runtimeengine.Orchestrator
	source    core.SourceReference
	job       core.Job
	result    core.Result
	pending   bool
	finalized bool
	reports   int
}

func newRedeliveryFixture() *redeliveryFixture {
	fixture := &redeliveryFixture{
		source: core.SourceReference{Adapter: "source", ID: "request-50"},
		result: core.Result{Outcome: core.OutcomeSuccess, Origin: core.ResultOriginRunner},
	}
	fixture.job = core.Job{ID: runtimeTestID(50), Source: fixture.source}
	store := &runtimeStoreFake{
		lookupReceipt: fixture.lookupReceipt, markStartedBySource: fixture.markStarted,
		setResultPendingBySource: fixture.setPending, finalize: fixture.finalize,
	}
	source := combineSource(resultReporterFake(fixture.report), nil, nil)
	fixture.handler = runtimeengine.NewOrchestrator(store, source, nil, runtimeTestID(1), fixedClock)
	return fixture
}

func (fixture *redeliveryFixture) lookupReceipt(context.Context, core.SourceReference) (core.Receipt, bool, error) {
	return core.Receipt{}, fixture.finalized, nil
}

func (fixture *redeliveryFixture) markStarted(context.Context, core.SourceReference, time.Time) (core.JobRecord, error) {
	return core.JobRecord{Job: fixture.job, State: core.JobStateAttempting}, nil
}

func (fixture *redeliveryFixture) setPending(
	_ context.Context,
	_ core.SourceReference,
	_ core.Result,
) (core.JobRecord, error) {
	fixture.pending = true
	return core.JobRecord{Job: fixture.job, State: core.JobStateResultPending, Result: &fixture.result}, nil
}

func (fixture *redeliveryFixture) finalize(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error) {
	fixture.finalized = true
	return core.Receipt{}, nil
}

func (fixture *redeliveryFixture) report(context.Context, core.Job, core.Result) (core.ResultDelivery, error) {
	fixture.reports++
	if fixture.reports == 1 {
		return "", errors.New("transient delivery failure")
	}
	return core.ResultDeliveryReported, nil
}

func (fixture *redeliveryFixture) deliverBatch() error {
	if err := fixture.handler.Started(context.Background(), fixture.source, time.Unix(2, 0)); err != nil {
		return fmt.Errorf("redeliver started event: %w", err)
	}
	if err := fixture.handler.Completed(context.Background(), fixture.source, fixture.result); err != nil {
		return fmt.Errorf("redeliver completed event: %w", err)
	}
	return nil
}
