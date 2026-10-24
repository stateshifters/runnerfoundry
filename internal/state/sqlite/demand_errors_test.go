package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

func TestDemandTransactionPropagatesAtomicityFailures(t *testing.T) {
	t.Parallel()
	failure := errors.New("demand transaction failure")
	rollbackFailure := errors.New("rollback failure")
	callback := func(state.DemandTransaction) error { return nil }
	store := newStore(executorStub{begin: failingBegin(failure)})
	if err := store.WithDemandTransaction(context.Background(), state.DemandTransactionKey{}, callback); err == nil || errors.Is(err, failure) {
		t.Fatalf("invalid scope = %v", err)
	}
	if err := store.WithDemandTransaction(context.Background(), state.DemandTransactionKey{Scope: core.DemandScope{Adapter: "source", ID: "scope"}, RunnerPool: "linux"}, callback); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	transaction := transactionStub{commit: failingCompletion(failure), rollback: failingCompletion(rollbackFailure)}
	err := withTestDemandTransaction(transaction, callback)
	if !errors.Is(err, failure) || !errors.Is(err, rollbackFailure) {
		t.Fatalf("commit uncertainty = %v", err)
	}
	err = withTestDemandTransaction(transaction, func(state.DemandTransaction) error { return failure })
	if !errors.Is(err, failure) || !errors.Is(err, rollbackFailure) {
		t.Fatalf("callback failure = %v", err)
	}
}

func TestActiveDemandDoesNotReturnAnIncompleteSnapshot(t *testing.T) {
	t.Parallel()
	failure := errors.New("demand read failure")
	for name, fixture := range map[string]struct {
		rows rowsStub
		err  error
	}{
		"query":                    {err: failure},
		"scan":                     {rows: rowsWithFailure(failure, nil, nil)},
		"iteration":                {rows: rowsWithFailure(nil, failure, nil)},
		"close":                    {rows: rowsWithFailure(nil, nil, failure)},
		"invalid durable identity": {rows: rowsFromValues([][]any{{"invalid", "source", "request", "linux", fixedTime().UnixNano(), "queued"}})},
	} {
		t.Run(name, func(t *testing.T) {
			transaction := transactionStub{query: func(context.Context, string, ...any) (rowsStub, error) { return fixture.rows, fixture.err }, rollback: successfulCompletion}
			assertIncompleteDemandSnapshotRejected(t, transaction)
		})
	}
}
func assertIncompleteDemandSnapshotRejected(t *testing.T, transaction transactionStub) {
	t.Helper()
	err := withTestDemandTransaction(transaction, func(demand state.DemandTransaction) error {
		jobs, readErr := demand.ActiveJobs(context.Background())
		if jobs != nil || readErr == nil {
			t.Fatalf("incomplete snapshot = %+v, %v", jobs, readErr)
		}
		return fmt.Errorf("read demand snapshot: %w", readErr)
	})
	if err == nil {
		t.Fatal("incomplete snapshot committed")
	}
}
