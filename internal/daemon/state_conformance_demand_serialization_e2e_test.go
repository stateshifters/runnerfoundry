//go:build e2e

package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

func TestPostgresDemandSerializesItsReadDomainAndOpaqueScope(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	store := openPostgresStateConformanceStore(t)
	key := state.DemandTransactionKey{Scope: core.DemandScope{Adapter: "state-conformance", ID: "held-scope"}, RunnerPool: "held-pool"}
	release := holdDemandTransaction(t, store, key)
	otherScope := state.DemandTransactionKey{Scope: core.DemandScope{Adapter: key.Scope.Adapter, ID: "other-scope"}, RunnerPool: key.RunnerPool}
	assertDemandTransactionBlocked(t, store, otherScope)
	assertDemandTransactionBlocked(t, store, state.DemandTransactionKey{Scope: key.Scope, RunnerPool: "other-pool"})
	assertDemandTransactionAvailable(t, store, state.DemandTransactionKey{Scope: otherScope.Scope, RunnerPool: "other-pool"})
	assertDemandTransactionAvailable(t, store, state.DemandTransactionKey{Scope: core.DemandScope{Adapter: "other-adapter", ID: "held-scope"}, RunnerPool: key.RunnerPool})
	requests := []core.JobRequest{
		stateConformanceRequest("domain-scoped-enqueue", key.RunnerPool, stateConformanceEpoch),
		stateConformanceRequest("domain-unscoped-enqueue", key.RunnerPool, stateConformanceEpoch),
	}
	requests[1].Scope = core.DemandScope{}
	for _, request := range requests {
		assertDemandEnqueueBlocked(t, store, request)
	}
	release()
	err := store.WithDemandTransaction(context.Background(), otherScope, func(transaction state.DemandTransaction) error {
		jobs, err := transaction.ActiveJobs(context.Background())
		if err != nil {
			return err
		}
		if len(jobs) != 0 {
			t.Errorf("cancelled enqueue persisted demand: %#v", jobs)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("released demand domain unavailable: %v", err)
	}
	for _, request := range requests {
		requireQueued(t, store, request, core.QueueQueued)
	}
	assertDemandTransactionAvailable(t, store, state.DemandTransactionKey{Scope: key.Scope, RunnerPool: "other-pool"})
}

func holdDemandTransaction(t *testing.T, store state.DemandTransactionStore, key state.DemandTransactionKey) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	entered := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan error, 1)
	go func() {
		completed <- store.WithDemandTransaction(ctx, key, func(state.DemandTransaction) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-entered:
	case err := <-completed:
		t.Fatalf("hold demand transaction: %v", err)
	case <-ctx.Done():
		t.Fatal("demand transaction did not enter callback")
	}
	return func() {
		close(release)
		if err := <-completed; err != nil {
			t.Fatalf("release demand transaction: %v", err)
		}
	}
}

func assertDemandTransactionBlocked(t *testing.T, store state.DemandTransactionStore, key state.DemandTransactionKey) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	called := false
	err := store.WithDemandTransaction(ctx, key, func(state.DemandTransaction) error { called = true; return nil })
	if !errors.Is(err, context.DeadlineExceeded) || called {
		t.Fatalf("competing demand transaction = %v, callback entered = %v, key = %#v", err, called, key)
	}
}

func assertDemandTransactionAvailable(t *testing.T, store state.DemandTransactionStore, key state.DemandTransactionKey) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := store.WithDemandTransaction(ctx, key, func(transaction state.DemandTransaction) error {
		_, err := transaction.ActiveJobs(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("independent or released demand transaction: %v", err)
	}
}

func assertDemandEnqueueBlocked(t *testing.T, store state.JobQueueStore, request core.JobRequest) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	result, err := store.Queue(ctx, request)
	if !errors.Is(err, context.DeadlineExceeded) || result != (core.EnqueueResult{}) {
		t.Fatalf("enqueue escaped held demand domain: %+v, %v", result, err)
	}
}
