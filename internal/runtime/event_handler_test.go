package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestSourceEventsReportRepositoryFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("repository unavailable")
	requestStore := &runtimeStoreFake{enqueue: func(context.Context, core.JobRequest) (core.JobRecord, bool, error) {
		return core.JobRecord{}, false, wantErr
	}}
	handler := runtimeengine.NewOrchestrator(requestStore, combineSource(nil, nil, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })
	requireErrorIs(t, "request", handler.Request(context.Background(), core.JobRequest{}), wantErr)

	receiptStore := &runtimeStoreFake{lookupReceipt: func(context.Context, core.SourceReference) (core.Receipt, bool, error) {
		return core.Receipt{}, false, wantErr
	}}
	handler = runtimeengine.NewOrchestrator(receiptStore, combineSource(nil, nil, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })
	requireErrorIs(t, "receipt lookup", handler.Started(context.Background(), core.SourceReference{}, time.Unix(2, 0)), wantErr)

	startedStore := &runtimeStoreFake{
		lookupReceipt: func(context.Context, core.SourceReference) (core.Receipt, bool, error) {
			return core.Receipt{}, false, nil
		},
		markStartedBySource: func(context.Context, core.SourceReference, time.Time) (core.JobRecord, error) {
			return core.JobRecord{}, wantErr
		},
	}
	handler = runtimeengine.NewOrchestrator(startedStore, combineSource(nil, nil, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })
	requireErrorIs(t, "started transition", handler.Started(context.Background(), core.SourceReference{}, time.Unix(2, 0)), wantErr)
}

type sourceEventHandler interface {
	Request(ctx context.Context, request core.JobRequest) error
	Started(ctx context.Context, source core.SourceReference, startedAt time.Time) error
	Completed(ctx context.Context, source core.SourceReference, result core.Result) error
}

var _ sourceEventHandler = (*runtimeengine.Orchestrator)(nil)

func TestFinalizedSourceEventsAreIdempotent(t *testing.T) {
	t.Parallel()

	source := core.SourceReference{Adapter: "source", ID: "request-32"}
	store := &runtimeStoreFake{lookupReceipt: func(_ context.Context, actual core.SourceReference) (core.Receipt, bool, error) {
		if actual != source {
			t.Fatalf("receipt source = %+v, want %+v", actual, source)
		}
		return core.Receipt{Source: source}, true, nil
	}}
	handler := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(2, 0) })

	if err := handler.Started(context.Background(), source, time.Unix(3, 0)); err != nil {
		t.Fatalf("duplicate started event: %v", err)
	}
	if err := handler.Completed(context.Background(), source, core.Result{}); err != nil {
		t.Fatalf("duplicate completed event: %v", err)
	}
}
