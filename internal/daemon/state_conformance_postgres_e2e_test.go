//go:build e2e

package daemon

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	statecontract "github.com/stateshifters/runnerfoundry/internal/state"
)

func TestStateStorePostgresConcurrentClaimsAndDemandTransactions(t *testing.T) {
	if !e2eBackendEnabled("postgres") {
		return
	}
	store := openPostgresStateConformanceStore(t)
	exerciseConcurrentPostgresClaims(t, store)
	exerciseConcurrentPostgresDemand(t, store)
}

func exerciseConcurrentPostgresClaims(t *testing.T, store stateConformanceStore) {
	t.Helper()
	runnerPool := core.RunnerPool("state-conformance-concurrent-claims")
	first := requireQueued(t, store,
		stateConformanceRequest("concurrent-claim-first", runnerPool, stateConformanceEpoch.Add(4*time.Hour)),
		core.QueueQueued)
	second := requireQueued(t, store,
		stateConformanceRequest("concurrent-claim-second", runnerPool, stateConformanceEpoch.Add(4*time.Hour+time.Second)),
		core.QueueQueued)

	const claimantCount = 8
	for claimant := 0; claimant < claimantCount; claimant++ {
		requireStateConformanceMember(
			t, store, core.MemberID("concurrent-claimant-"+strconv.Itoa(claimant)),
			stateConformanceEpoch.Add(4*time.Hour+2*time.Second),
		)
	}
	start := make(chan struct{})
	results := make(chan concurrentClaimResult, claimantCount)
	var claimants sync.WaitGroup
	for claimant := 0; claimant < claimantCount; claimant++ {
		claimants.Add(1)
		go func(claimant int) {
			defer claimants.Done()
			<-start
			result, err := store.ClaimOldest(context.Background(), runnerPool,
				stateConformanceClaim(core.MemberID("concurrent-claimant-"+strconv.Itoa(claimant)),
					stateConformanceEpoch.Add(4*time.Hour+2*time.Second)))
			results <- concurrentClaimResult{result: result, err: err}
		}(claimant)
	}
	close(start)
	claimants.Wait()
	close(results)
	assertConcurrentClaims(t, results, first.ID, second.ID)
}

type concurrentClaimResult struct {
	result core.ClaimResult
	err    error
}

func assertConcurrentClaims(
	t *testing.T,
	results <-chan concurrentClaimResult,
	firstJobID core.JobID,
	secondJobID core.JobID,
) {
	t.Helper()
	claimed := make(map[core.JobID]int)
	empty := 0
	for claim := range results {
		if claim.err != nil {
			t.Fatalf("concurrent PostgreSQL claim: %v", claim.err)
		}
		switch claim.result.Outcome {
		case core.ClaimClaimed:
			claimed[claim.result.Processing.Job.ID]++
		case core.ClaimEmpty:
			empty++
		default:
			t.Fatalf("concurrent PostgreSQL claim outcome = %q", claim.result.Outcome)
		}
	}
	if claimed[firstJobID] != 1 || claimed[secondJobID] != 1 || len(claimed) != 2 || empty != 6 {
		t.Fatalf("concurrent PostgreSQL claims = %#v with %d empty, want each queued job once", claimed, empty)
	}
}

func exerciseConcurrentPostgresDemand(t *testing.T, store stateConformanceStore) {
	t.Helper()
	request := stateConformanceRequest(
		"concurrent-demand", "state-conformance-concurrent-demand", stateConformanceEpoch.Add(5*time.Hour),
	)
	scope := core.DemandScope{Adapter: "state-conformance", ID: "concurrent-demand-scope"}
	request.Scope = scope
	const reconcilerCount = 8
	start := make(chan struct{})
	results := make(chan concurrentDemandResult, reconcilerCount)
	var reconcilers sync.WaitGroup
	for reconciler := 0; reconciler < reconcilerCount; reconciler++ {
		reconcilers.Add(1)
		go func() {
			defer reconcilers.Done()
			<-start
			var enqueue core.EnqueueResult
			err := store.WithDemandTransaction(context.Background(), statecontract.DemandTransactionKey{Scope: scope, RunnerPool: request.RunnerPool},
				func(transaction statecontract.DemandTransaction) error {
					var queueErr error
					enqueue, queueErr = transaction.Queue(context.Background(), request)
					return queueErr
				})
			results <- concurrentDemandResult{result: enqueue, err: err}
		}()
	}
	close(start)
	reconcilers.Wait()
	close(results)
	assertConcurrentDemand(t, results)
}

type concurrentDemandResult struct {
	result core.EnqueueResult
	err    error
}

func assertConcurrentDemand(t *testing.T, results <-chan concurrentDemandResult) {
	t.Helper()
	queued := 0
	replayed := 0
	var jobID core.JobID
	for demand := range results {
		if demand.err != nil {
			t.Fatalf("concurrent PostgreSQL demand transaction: %v", demand.err)
		}
		if jobID != (core.JobID{}) && demand.result.Job.ID != jobID {
			t.Fatalf("concurrent demand job ID = %s, want stable ID %s", demand.result.Job.ID, jobID)
		}
		jobID = demand.result.Job.ID
		switch demand.result.Outcome {
		case core.QueueQueued:
			queued++
		case core.QueueAlreadyQueued:
			replayed++
		default:
			t.Fatalf("concurrent PostgreSQL demand outcome = %q", demand.result.Outcome)
		}
	}
	if queued != 1 || replayed != 7 {
		t.Fatalf("concurrent demand outcomes = queued %d replayed %d, want 1 and 7", queued, replayed)
	}
}
