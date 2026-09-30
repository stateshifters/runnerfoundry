//go:build e2e

package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func exerciseQueueRetryContract(t *testing.T, store stateConformanceStore) {
	t.Helper()
	fixture := prepareQueueRetryFixture(t, store)
	firstAttempt := retryFirstQueueAttempt(t, store, fixture)
	assertQueueRetryOrdering(t, store, fixture, firstAttempt)
	exerciseReturnedClaimPrecisionContract(t, store)
}

type queueRetryFixture struct {
	runnerPool core.RunnerPool
	firstJob   core.Job
	secondJob  core.Job
}

func prepareQueueRetryFixture(t *testing.T, store stateConformanceStore) queueRetryFixture {
	t.Helper()
	runnerPool := core.RunnerPool("state-conformance-queue")
	firstRequest := stateConformanceRequest("queue-first", runnerPool, stateConformanceEpoch)
	secondRequest := stateConformanceRequest("queue-second", runnerPool, stateConformanceEpoch.Add(time.Second))
	firstJob := requireQueued(t, store, firstRequest, core.QueueQueued)
	replayed := requireQueued(t, store, firstRequest, core.QueueAlreadyQueued)
	if replayed.ID != firstJob.ID {
		t.Fatalf("replayed job ID = %s, want stable ID %s", replayed.ID, firstJob.ID)
	}
	secondJob := requireQueued(t, store, secondRequest, core.QueueQueued)
	requireStateConformanceMember(t, store, "queue-member", stateConformanceEpoch.Add(2*time.Second))
	return queueRetryFixture{runnerPool: runnerPool, firstJob: firstJob, secondJob: secondJob}
}

func retryFirstQueueAttempt(t *testing.T, store stateConformanceStore, fixture queueRetryFixture) core.ProcessingJob {
	t.Helper()
	firstAttempt := requireClaimed(t, store, fixture.runnerPool,
		stateConformanceClaim("queue-member", stateConformanceEpoch.Add(2*time.Second)))
	if firstAttempt.Job.ID != fixture.firstJob.ID {
		t.Fatalf("first FIFO claim = %s, want %s", firstAttempt.Job.ID, fixture.firstJob.ID)
	}
	requireRetry(t, "retry first provisioning attempt", func() (core.RetryResult, error) {
		return store.RetryUnstarted(context.Background(), firstAttempt)
	}, core.RetryRequeued)
	return firstAttempt
}

func assertQueueRetryOrdering(
	t *testing.T,
	store stateConformanceStore,
	fixture queueRetryFixture,
	firstAttempt core.ProcessingJob,
) {
	t.Helper()
	secondAttempt := requireClaimed(t, store, fixture.runnerPool,
		stateConformanceClaim("queue-member", stateConformanceEpoch.Add(3*time.Second)))
	if secondAttempt.Job.ID != fixture.secondJob.ID {
		t.Fatalf("claim after retry = %s, want queued follower %s", secondAttempt.Job.ID, fixture.secondJob.ID)
	}
	retriedAttempt := requireClaimed(t, store, fixture.runnerPool,
		stateConformanceClaim("queue-member", stateConformanceEpoch.Add(4*time.Second)))
	if retriedAttempt.Job.ID != fixture.firstJob.ID || retriedAttempt.RunnerName == firstAttempt.RunnerName {
		t.Fatalf("tail retry = %+v, want job %s with a new attempt", retriedAttempt, fixture.firstJob.ID)
	}
	requireRetry(t, "stale retry from replaced attempt", func() (core.RetryResult, error) {
		return store.RetryUnstarted(context.Background(), firstAttempt)
	}, core.RetryClaimChanged)
}

func exerciseReturnedClaimPrecisionContract(t *testing.T, store stateConformanceStore) {
	t.Helper()
	ctx := context.Background()
	runnerPool := core.RunnerPool("state-conformance-claim-precision")
	memberID := core.MemberID("claim-precision-member")
	claimedAt := stateConformanceEpoch.Add(40*time.Minute + 123*time.Nanosecond)
	requireStateConformanceMember(t, store, memberID, claimedAt)
	requireQueued(t, store,
		stateConformanceRequest("claim-precision", runnerPool, claimedAt.Add(-time.Second)), core.QueueQueued)
	processing := requireClaimed(t, store, runnerPool, stateConformanceClaim(memberID, claimedAt))
	persisted, err := store.ProcessingByPool(ctx, runnerPool)
	if err != nil || len(persisted) != 1 || !persisted[0].ClaimedAt.Equal(processing.ClaimedAt) {
		t.Fatalf("returned claim time = %s, persisted processing = %#v, error = %v", processing.ClaimedAt, persisted, err)
	}
	requireRetry(t, "retry using returned canonical claim", func() (core.RetryResult, error) {
		return store.RetryUnstarted(ctx, processing)
	}, core.RetryRequeued)
}

func exerciseLifecycleObservationContract(t *testing.T, store stateConformanceStore) {
	t.Helper()
	ctx := context.Background()
	acceptedRequest := stateConformanceRequest(
		"delayed-acceptance", "state-conformance-acceptance", stateConformanceEpoch.Add(10*time.Second),
	)
	requireQueued(t, store, acceptedRequest, core.QueueQueued)
	acceptedIdentity := core.JobIdentity{Source: acceptedRequest.Source}
	requireObservation(t, "delayed runner acceptance", func() (core.ObservationResult, error) {
		return store.ObserveRunnerAcceptance(ctx, acceptedIdentity)
	}, core.ObservationApplied)
	requireObservation(t, "replayed runner acceptance", func() (core.ObservationResult, error) {
		return store.ObserveRunnerAcceptance(ctx, acceptedIdentity)
	}, core.ObservationAlreadyApplied)
	requireQueued(t, store, acceptedRequest, core.QueueSourceTerminal)

	terminalRequest := stateConformanceRequest(
		"terminal-before-demand", "state-conformance-terminal", stateConformanceEpoch.Add(20*time.Second),
	)
	terminalIdentity := core.JobIdentity{Source: terminalRequest.Source}
	requireObservation(t, "terminal observation before demand", func() (core.ObservationResult, error) {
		return store.Terminalize(ctx, terminalIdentity)
	}, core.ObservationApplied)
	requireObservation(t, "replayed terminal observation", func() (core.ObservationResult, error) {
		return store.Terminalize(ctx, terminalIdentity)
	}, core.ObservationAlreadyApplied)
	requireQueued(t, store, terminalRequest, core.QueueSourceTerminal)
}

func exerciseProvisioningFailureContract(t *testing.T, store stateConformanceStore) {
	t.Helper()
	ctx := context.Background()
	runnerPool := core.RunnerPool("state-conformance-failure")
	request := stateConformanceRequest("reported-provisioning-failure", runnerPool, stateConformanceEpoch.Add(30*time.Second))
	requireQueued(t, store, request, core.QueueQueued)
	requireStateConformanceMember(t, store, "failure-member", stateConformanceEpoch.Add(31*time.Second))
	processing := requireClaimed(t, store, runnerPool,
		stateConformanceClaim("failure-member", stateConformanceEpoch.Add(31*time.Second)))

	requireRetry(t, "reserve provisioning failure", func() (core.RetryResult, error) {
		return store.ReserveProvisioningFailure(ctx, processing)
	}, core.RetryFailureReportingPending)
	requireRetry(t, "repeat provisioning failure reservation", func() (core.RetryResult, error) {
		return store.ReserveProvisioningFailure(ctx, processing)
	}, core.RetryFailureReportingPending)
	requireRetry(t, "record reported provisioning failure", func() (core.RetryResult, error) {
		return store.RecordProvisioningFailureReported(ctx, processing)
	}, core.RetryFailureFinalizationPending)
	requireRetry(t, "repeat reported provisioning failure", func() (core.RetryResult, error) {
		return store.RecordProvisioningFailureReported(ctx, processing)
	}, core.RetryFailureFinalizationPending)

	finalized, err := store.FinalizeProvisioningFailure(ctx, processing)
	if err != nil || finalized.Outcome != core.TerminalRemoved {
		t.Fatalf("finalize provisioning failure = %+v, error = %v", finalized, err)
	}
	finalized, err = store.FinalizeProvisioningFailure(ctx, processing)
	if err != nil || finalized.Outcome != core.TerminalMissing {
		t.Fatalf("repeat provisioning failure finalization = %+v, error = %v", finalized, err)
	}
}
