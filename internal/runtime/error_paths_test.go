package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	runtimeengine "github.com/meza/RunnerFoundry/internal/runtime"
)

func TestClaimWorkReportsEmptyAndFailedClaims(t *testing.T) {
	t.Parallel()

	claimErr := errors.New("claim failed")
	tests := []struct {
		name  string
		found bool
		err   error
	}{
		{name: "empty", found: false},
		{name: "failure", err: claimErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := &runtimeStoreFake{claimOldest: func(context.Context, core.RunnerPool, core.MemberID, time.Time) (core.JobRecord, bool, error) {
				return core.JobRecord{}, test.found, test.err
			}}
			orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })

			started, err := orchestrator.ClaimWork(context.Background(), "linux")
			if started || !errors.Is(err, test.err) {
				t.Fatalf("request work = (%v, %v), want (false, %v)", started, err, test.err)
			}
		})
	}
}

func TestClaimWorkReportsSuccessfulClaim(t *testing.T) {
	t.Parallel()

	store := &runtimeStoreFake{claimOldest: func(context.Context, core.RunnerPool, core.MemberID, time.Time) (core.JobRecord, bool, error) {
		return core.JobRecord{Job: core.Job{ID: runtimeTestID(1)}}, true, nil
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), nil, runtimeTestID(1), fixedClock)
	claimed, err := orchestrator.ClaimWork(context.Background(), "linux")
	if err != nil || !claimed {
		t.Fatalf("claim work = (%v, %v), want (true, nil)", claimed, err)
	}
}

func TestCompletionReportsPersistenceFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("persist failed")
	store := &runtimeStoreFake{setResultPendingBySource: func(context.Context, core.SourceReference, core.Result) (core.JobRecord, error) {
		return core.JobRecord{}, wantErr
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })

	err := orchestrator.Complete(context.Background(), core.SourceReference{ID: "request"}, core.Result{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("complete error = %v, want %v", err, wantErr)
	}
}

func TestRetryResultRejectsUnavailableJobs(t *testing.T) {
	t.Parallel()

	lookupErr := errors.New("lookup failed")
	tests := []struct {
		name   string
		record core.JobRecord
		found  bool
		err    error
	}{
		{name: "lookup failure", err: lookupErr},
		{name: "missing"},
		{name: "not pending", found: true, record: core.JobRecord{State: core.JobStateAttempting}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := &runtimeStoreFake{lookup: func(context.Context, core.JobID) (core.JobRecord, bool, error) {
				return test.record, test.found, test.err
			}}
			orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })

			if err := orchestrator.RetryResult(context.Background(), runtimeTestID(1)); err == nil {
				t.Fatal("retry unexpectedly succeeded")
			}
		})
	}
}

func TestExpiredMemberReportsTransitionAndDeliveryFailures(t *testing.T) {
	t.Parallel()

	transitionErr := errors.New("transition failed")
	transitionStore := &runtimeStoreFake{setMemberAttemptsResultPending: func(context.Context, core.MemberID, core.Result) ([]core.JobRecord, error) {
		return nil, transitionErr
	}}
	orchestrator := runtimeengine.NewOrchestrator(transitionStore, combineSource(nil, nil, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })
	if failed, err := orchestrator.FailExpiredMember(context.Background(), runtimeTestID(2)); failed != 0 || !errors.Is(err, transitionErr) {
		t.Fatalf("transition failure = (%d, %v), want (0, %v)", failed, err, transitionErr)
	}

	deliveryErr := errors.New("delivery failed")
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService}
	job := core.Job{ID: runtimeTestID(21)}
	deliveryStore := &runtimeStoreFake{setMemberAttemptsResultPending: func(context.Context, core.MemberID, core.Result) ([]core.JobRecord, error) {
		return []core.JobRecord{{Job: job, State: core.JobStateResultPending, Result: &result}}, nil
	}}
	orchestrator = runtimeengine.NewOrchestrator(deliveryStore, combineSource(resultReporterFake(func(context.Context, core.Job, core.Result) (core.ResultDelivery, error) {
		return "", deliveryErr
	}), nil,

		nil),

		nil, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })

	if failed, err := orchestrator.FailExpiredMember(context.Background(), runtimeTestID(2)); failed != 1 || !errors.Is(err, deliveryErr) {
		t.Fatalf("delivery failure = (%d, %v), want (1, %v)", failed, err, deliveryErr)
	}
}

func TestUnstartedTimeoutReportsTransitionFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("transition failed")
	store := &runtimeStoreFake{setUnstartedAttemptsResultPending: func(context.Context, core.RunnerPool, time.Time, core.Result) ([]core.JobRecord, error) {
		return nil, wantErr
	}}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(nil, nil, nil), nil, runtimeTestID(1), func() time.Time { return time.Unix(10, 0) })
	if failed, err := orchestrator.FailUnstartedAttempts(context.Background(), "linux", time.Second); failed != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("timeout transition = (%d, %v), want (0, %v)", failed, err, wantErr)
	}
}

func TestFinalizationPersistenceFailureIsReturned(t *testing.T) {
	t.Parallel()

	job := core.Job{ID: runtimeTestID(22), RunnerPool: "linux"}
	finalizeErr := errors.New("finalize failed")
	result := core.Result{Outcome: core.OutcomeSuccess, Origin: core.ResultOriginRunner}
	store := &runtimeStoreFake{
		setResultPendingBySource: func(context.Context, core.SourceReference, core.Result) (core.JobRecord, error) {
			return core.JobRecord{Job: job, State: core.JobStateResultPending, Result: &result}, nil
		},
		finalize: func(context.Context, core.JobID, core.ResultDelivery) (core.Receipt, error) {
			return core.Receipt{}, finalizeErr
		},
	}
	orchestrator := runtimeengine.NewOrchestrator(store, combineSource(resultReporterFake(func(context.Context, core.Job, core.Result) (core.ResultDelivery, error) {
		return core.ResultDeliveryReported, nil
	}), nil,

		nil),

		nil, runtimeTestID(1), func() time.Time { return time.Unix(1, 0) })

	if err := orchestrator.Complete(context.Background(), core.SourceReference{}, result); !errors.Is(err, finalizeErr) {
		t.Fatalf("completion error = %v, want finalization failure", err)
	}
}
