package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestSourceObservationsUpdateExistingJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repository := core.NewMemoryJobStore()
	record := enqueue(ctx, t, repository, "observed", "linux")
	claim(ctx, t, repository, "linux", "member-1", fixedTime)
	started, err := repository.MarkStartedBySource(ctx, record.Job.Source, fixedTime.Add(1))
	requireNoError(t, err)
	if !started.StartedAt.Equal(fixedTime.Add(1)) {
		t.Fatalf("started at = %v", started.StartedAt)
	}
	result := core.Result{Outcome: core.OutcomeSuccess, Origin: core.ResultOriginRunner}
	pending, err := repository.SetResultPendingBySource(ctx, record.Job.Source, result)
	requireNoError(t, err)
	requireState(t, pending, core.JobStateResultPending)
}

func TestRedeliveredStartedObservationDoesNotOverridePendingResult(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repository := core.NewMemoryJobStore()
	record := enqueue(ctx, t, repository, "redelivered-started", "linux")
	result := core.Result{Outcome: core.OutcomeSuccess, Origin: core.ResultOriginRunner}
	pending, err := repository.SetResultPendingBySource(ctx, record.Job.Source, result)
	requireNoError(t, err)

	redelivered, err := repository.MarkStartedBySource(ctx, record.Job.Source, fixedTime.Add(2))
	requireNoError(t, err)
	requireState(t, redelivered, core.JobStateResultPending)
	if redelivered.Result == nil || *redelivered.Result != *pending.Result {
		t.Fatalf("redelivered result = %#v, want %#v", redelivered.Result, pending.Result)
	}
}

func TestInvalidSourceAndResultTransitionsFail(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repository := core.NewMemoryJobStore()
	record := enqueue(ctx, t, repository, "invalid", "linux")
	missing := core.SourceReference{Adapter: "git", ID: "missing"}
	_, err := repository.MarkStartedBySource(ctx, missing, fixedTime)
	requireErrorIs(t, err, core.ErrJobNotFound)
	_, err = repository.MarkStartedBySource(ctx, record.Job.Source, fixedTime)
	requireErrorIs(t, err, core.ErrInvalidTransition)
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService, Reason: "first"}
	_, err = repository.SetResultPendingBySource(ctx, missing, result)
	requireErrorIs(t, err, core.ErrJobNotFound)
	_, err = repository.SetResultPending(ctx, jobID("missing"), result)
	requireErrorIs(t, err, core.ErrJobNotFound)
	_, err = repository.SetResultPending(ctx, record.Job.ID, result)
	requireNoError(t, err)
	result.Reason = "conflicting"
	_, err = repository.SetResultPending(ctx, record.Job.ID, result)
	requireErrorIs(t, err, core.ErrConflictingResult)
}

func TestFinalizeRemovesJobAndRetainsLeanTombstone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repository := core.NewMemoryJobStore()
	record := enqueue(ctx, t, repository, "finalized", "linux")
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService, Reason: "diagnostic only"}
	_, err := repository.SetResultPending(ctx, record.Job.ID, result)
	requireNoError(t, err)
	receipt, err := repository.Finalize(ctx, record.Job.ID, core.ResultDeliveryUnsupported)
	requireNoError(t, err)
	if receipt.Source != record.Job.Source || receipt.Outcome != result.Outcome || receipt.Origin != result.Origin {
		t.Fatalf("receipt = %#v", receipt)
	}
	_, found, err := repository.Lookup(ctx, record.Job.ID)
	requireNoError(t, err)
	if found {
		t.Fatal("finalized job remains active")
	}
	stored, found, err := repository.LookupReceipt(ctx, record.Job.Source)
	requireNoError(t, err)
	if !found || stored != receipt {
		t.Fatalf("stored receipt = (%#v, %t)", stored, found)
	}
}

func TestTombstonePreventsReplacementDemand(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repository := core.NewMemoryJobStore()
	record := enqueue(ctx, t, repository, "tombstone", "linux")
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService}
	_, err := repository.SetResultPending(ctx, record.Job.ID, result)
	requireNoError(t, err)
	_, err = repository.Finalize(ctx, record.Job.ID, core.ResultDeliveryUnsupported)
	requireNoError(t, err)
	_, created, err := repository.Enqueue(ctx, core.JobRequest{Source: record.Job.Source, RunnerPool: "linux"})
	requireNoError(t, err)
	if created {
		t.Fatal("finalized source request created replacement demand")
	}
	_, err = repository.Finalize(ctx, record.Job.ID, core.ResultDeliveryUnsupported)
	requireErrorIs(t, err, core.ErrJobNotFound)
}

func TestLookupBySourceReturnsOnlyAnActiveJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := core.NewMemoryJobStore()
	record := enqueue(ctx, t, store, "lookup-source", "linux")
	loaded, found, err := store.LookupBySource(ctx, record.Job.Source)
	requireNoError(t, err)
	if !found || loaded.Job.ID != record.Job.ID {
		t.Fatalf("source lookup = (%#v, %t)", loaded, found)
	}
	result := core.Result{Outcome: core.OutcomeSuccess, Origin: core.ResultOriginRunner}
	_, err = store.SetResultPending(ctx, record.Job.ID, result)
	requireNoError(t, err)
	_, err = store.Finalize(ctx, record.Job.ID, core.ResultDeliveryRunnerManaged)
	requireNoError(t, err)
	_, found, err = store.LookupBySource(ctx, record.Job.Source)
	requireNoError(t, err)
	if found {
		t.Fatal("finalized job remains discoverable as active")
	}
}

func TestPendingResultsAreReadOnlyAndRespectLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := core.NewMemoryJobStore()
	first := enqueue(ctx, t, store, "pending-first", "linux")
	second := enqueueAt(ctx, t, store, "pending-second", "linux", fixedTime.Add(time.Minute))
	third := enqueue(ctx, t, store, "pending-third", "linux")
	enqueue(ctx, t, store, "still-queued", "linux")
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService}
	_, err := store.SetResultPending(ctx, first.Job.ID, result)
	requireNoError(t, err)
	_, err = store.SetResultPending(ctx, second.Job.ID, result)
	requireNoError(t, err)
	_, err = store.SetResultPending(ctx, third.Job.ID, result)
	requireNoError(t, err)

	pending, err := store.PendingResults(ctx, 1)
	requireNoError(t, err)
	requireJobIDs(t, pending, first.Job.ID)
	pending, err = store.PendingResults(ctx, 10)
	requireNoError(t, err)
	requireJobIDsUnordered(t, pending, first.Job.ID, second.Job.ID, third.Job.ID)
	pending, err = store.PendingResults(ctx, 0)
	requireNoError(t, err)
	requireJobIDs(t, pending)
}
