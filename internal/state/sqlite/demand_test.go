package sqlite

import (
	"context"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

func TestDemandRemovalReportsMissingQueuedJob(t *testing.T) {
	t.Parallel()
	transaction := transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
		return resultStub{rowsAffected: func() (int64, error) { return 0, nil }}, nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	store := newStore(executorStub{begin: transactionBeginning(transaction)})
	err := store.WithDemandTransaction(context.Background(), state.DemandTransactionKey{Scope: core.DemandScope{Adapter: "source", ID: "scope"}, RunnerPool: "linux"},
		func(transaction state.DemandTransaction) error {
			outcome, err := transaction.RemoveQueued(context.Background(), state.ActiveJob{
				ID: testJobID, Source: core.SourceReference{Adapter: "source", ID: "request"},
				RunnerPool: "linux", Phase: state.ActiveJobQueued, StateChangedAt: fixedTime(),
			})
			if err != nil || outcome != state.QueuedJobMissing {
				t.Fatalf("missing queued removal = %q, %v", outcome, err)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
}
