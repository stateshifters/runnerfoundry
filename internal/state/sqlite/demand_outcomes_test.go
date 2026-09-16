package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

func TestDemandTransactionReturnsTypedActiveJobs(t *testing.T) {
	t.Parallel()
	transaction := transactionStub{query: func(_ context.Context, _ string, arguments ...any) (rowsStub, error) {
		if len(arguments) != 4 || arguments[0] != "source" || arguments[1] != "linux" || arguments[2] != "source" || arguments[3] != "linux" {
			t.Fatalf("active demand query escaped key: %#v", arguments)
		}
		return rowsFromValues([][]any{{testJobIDText, "source", "request", "linux", fixedTime().UnixNano(), "queued"}}), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	err := withTestDemandTransaction(transaction, func(demand state.DemandTransaction) error {
		jobs, err := demand.ActiveJobs(context.Background())
		if err != nil || len(jobs) != 1 || jobs[0].ID != testJobID || jobs[0].Source.ID != "request" ||
			jobs[0].Phase != state.ActiveJobQueued || jobs[0].RunnerPool != "linux" || !jobs[0].StateChangedAt.Equal(fixedTime()) {
			t.Fatalf("active demand = %+v, %v", jobs, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDemandTransactionQueuesValidatedDemand(t *testing.T) {
	t.Parallel()
	transaction := transactionStub{execute: successfulExecute, queryRow: constantRow(rowFromValues([]any{testJobIDText, "source", "request", "linux", fixedTime().UnixNano()})),
		commit: successfulCompletion, rollback: successfulCompletion}
	err := withTestDemandTransaction(transaction, func(demand state.DemandTransaction) error {
		result, err := demand.Queue(context.Background(), core.JobRequest{
			Source: core.SourceReference{Adapter: "source", ID: "request"}, Scope: core.DemandScope{Adapter: "source", ID: "scope"}, RunnerPool: "linux", EnqueuedAt: fixedTime(),
		})
		if err != nil || result.Outcome != core.QueueQueued || result.Job.ID != testJobID {
			t.Fatalf("queue demand = %+v, %v", result, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = withTestDemandTransaction(transaction, func(demand state.DemandTransaction) error {
		_, queueErr := demand.Queue(context.Background(), core.JobRequest{})
		return fmt.Errorf("queue invalid demand: %w", queueErr)
	})
	if !errors.Is(err, core.ErrInvalidJobRequest) {
		t.Fatalf("invalid transactional demand = %v", err)
	}
}

func TestDemandRemovalPreservesProcessingAndReportsDatabaseFailure(t *testing.T) {
	t.Parallel()
	failure := errors.New("remove demand failure")
	for name, testCase := range map[string]struct {
		phase     state.ActiveJobPhase
		execute   func(context.Context, string, ...any) (resultStub, error)
		want      state.QueuedRemovalOutcome
		wantError error
	}{
		"processing":      {phase: state.ActiveJobProcessing, want: state.QueuedJobMissing},
		"removed":         {phase: state.ActiveJobQueued, execute: successfulExecute, want: state.QueuedJobRemoved},
		"execute failure": {phase: state.ActiveJobQueued, execute: failingExecute(failure), wantError: failure},
		"count failure": {phase: state.ActiveJobQueued, execute: func(context.Context, string, ...any) (resultStub, error) {
			return resultStub{rowsAffected: func() (int64, error) { return 0, failure }}, nil
		}, wantError: failure},
	} {
		t.Run(name, func(t *testing.T) {
			assertDemandRemovalOutcome(t, testCase.phase, testCase.execute, testCase.want, testCase.wantError)
		})
	}
}

func withTestDemandTransaction(transaction transactionStub, reconcile func(state.DemandTransaction) error) error {
	return newStore(executorStub{begin: transactionBeginning(transaction)}).WithDemandTransaction(
		context.Background(), state.DemandTransactionKey{Scope: core.DemandScope{Adapter: "source", ID: "scope"}, RunnerPool: "linux"}, reconcile)
}

func assertDemandRemovalOutcome(t *testing.T, phase state.ActiveJobPhase, execute func(context.Context, string, ...any) (resultStub, error), want state.QueuedRemovalOutcome, wantError error) {
	t.Helper()
	transaction := transactionStub{execute: execute, commit: successfulCompletion, rollback: successfulCompletion}
	var outcome state.QueuedRemovalOutcome
	err := withTestDemandTransaction(transaction, func(demand state.DemandTransaction) error {
		var removeErr error
		outcome, removeErr = demand.RemoveQueued(context.Background(), state.ActiveJob{ID: testJobID, Phase: phase,
			Source: core.SourceReference{Adapter: "source", ID: "request"}, RunnerPool: "linux"})
		if removeErr != nil {
			return fmt.Errorf("remove demand: %w", removeErr)
		}
		return nil
	})
	if outcome != want || !errors.Is(err, wantError) {
		t.Fatalf("removal = %q, %v", outcome, err)
	}
}
