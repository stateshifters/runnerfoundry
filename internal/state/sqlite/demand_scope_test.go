package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

func TestDemandTransactionRejectsQueueOutsideItsKey(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*core.JobRequest){
		"scope":    func(request *core.JobRequest) { request.Scope.ID = "other" },
		"unscoped": func(request *core.JobRequest) { request.Scope = core.DemandScope{} },
		"adapter":  func(request *core.JobRequest) { request.Source.Adapter = "other"; request.Scope.Adapter = "other" },
		"pool":     func(request *core.JobRequest) { request.RunnerPool = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			request := core.JobRequest{Source: core.SourceReference{Adapter: "source", ID: "request"},
				Scope: core.DemandScope{Adapter: "source", ID: "scope"}, RunnerPool: "linux", EnqueuedAt: fixedTime()}
			change(&request)
			assertDemandMismatchWithoutSQL(t, func(demand state.DemandTransaction) error {
				_, err := demand.Queue(context.Background(), request)
				return fmt.Errorf("queue outside transaction key: %w", err)
			})
		})
	}
}

func TestDemandTransactionRejectsRemovalOutsideItsDomain(t *testing.T) {
	t.Parallel()
	for name, job := range map[string]state.ActiveJob{
		"adapter": {Source: core.SourceReference{Adapter: "other", ID: "request"}, RunnerPool: "linux", Phase: state.ActiveJobQueued},
		"pool":    {Source: core.SourceReference{Adapter: "source", ID: "request"}, RunnerPool: "other", Phase: state.ActiveJobQueued},
	} {
		t.Run(name, func(t *testing.T) {
			assertDemandMismatchWithoutSQL(t, func(demand state.DemandTransaction) error {
				_, err := demand.RemoveQueued(context.Background(), job)
				return fmt.Errorf("remove outside transaction domain: %w", err)
			})
		})
	}
}

func assertDemandMismatchWithoutSQL(t *testing.T, operation func(state.DemandTransaction) error) {
	t.Helper()
	statementCount := 0
	transaction := transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
		statementCount++
		return resultStub{}, errors.New("unexpected SQL")
	}, rollback: successfulCompletion, commit: successfulCompletion}
	err := withTestDemandTransaction(transaction, operation)
	if !errors.Is(err, state.ErrDemandTransactionMismatch) || statementCount != 0 {
		t.Fatalf("outside-key operation = %v, statements = %d", err, statementCount)
	}
}

func TestDemandTransactionRequiresPoolBeforeOpeningTransaction(t *testing.T) {
	t.Parallel()
	opened := errors.New("transaction opened")
	store := newStore(executorStub{begin: failingBegin(opened)})
	err := store.WithDemandTransaction(context.Background(), state.DemandTransactionKey{
		Scope: core.DemandScope{Adapter: "source", ID: "scope"},
	}, func(state.DemandTransaction) error { t.Fatal("invalid key reached callback"); return nil })
	if err == nil || errors.Is(err, opened) {
		t.Fatal("missing pool accepted")
	}
}
