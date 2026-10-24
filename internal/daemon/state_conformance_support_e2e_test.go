//go:build e2e

package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	statecontract "github.com/stateshifters/runnerfoundry/internal/state"
)

var stateConformanceEpoch = time.Date(2020, time.January, 20, 12, 0, 0, 0, time.UTC)

func stateConformanceRequest(reference string, runnerPool core.RunnerPool, enqueuedAt time.Time) core.JobRequest {
	return core.JobRequest{
		Source:     core.SourceReference{Adapter: "state-conformance", ID: core.SourceIdentifier(reference)},
		Scope:      core.DemandScope{Adapter: "state-conformance", ID: core.SourceIdentifier("scope-" + reference)},
		RunnerPool: runnerPool,
		EnqueuedAt: enqueuedAt,
	}
}

func stateConformanceClaim(member core.MemberID, claimedAt time.Time) core.ProcessingClaim {
	return core.ProcessingClaim{
		MemberID:            member,
		AttemptID:           core.NewAttemptID(),
		ClaimedAt:           claimedAt,
		InventoryEndpointID: "state-conformance:provider",
	}
}

func requireStateConformanceMember(
	t *testing.T,
	store statecontract.MemberRegistry,
	memberID core.MemberID,
	heartbeat time.Time,
) {
	t.Helper()
	if err := store.PublishMemberState(context.Background(), memberID, heartbeat, nil); err != nil {
		t.Fatalf("publish state conformance member %q: %v", memberID, err)
	}
}

func requireQueued(
	t *testing.T,
	store statecontract.JobQueueStore,
	request core.JobRequest,
	want core.QueueOutcome,
) core.Job {
	t.Helper()
	result, err := store.Queue(context.Background(), request)
	if err != nil || result.Outcome != want {
		t.Fatalf("queue %q = %+v, error = %v, want %s", request.Source.ID, result, err, want)
	}
	return result.Job
}

func requireClaimed(
	t *testing.T,
	store statecontract.ProcessingJobStore,
	runnerPool core.RunnerPool,
	claim core.ProcessingClaim,
) core.ProcessingJob {
	t.Helper()
	result, err := store.ClaimOldest(context.Background(), runnerPool, claim)
	if err != nil || result.Outcome != core.ClaimClaimed {
		t.Fatalf("claim oldest from %q = %+v, error = %v", runnerPool, result, err)
	}
	return result.Processing
}

func requireObservation(
	t *testing.T,
	description string,
	operation func() (core.ObservationResult, error),
	want core.ObservationOutcome,
) {
	t.Helper()
	result, err := operation()
	if err != nil || result.Outcome != want {
		t.Fatalf("%s = %+v, error = %v, want %s", description, result, err, want)
	}
}

func requireRetry(
	t *testing.T,
	description string,
	operation func() (core.RetryResult, error),
	want core.RetryOutcome,
) {
	t.Helper()
	result, err := operation()
	if err != nil || result.Outcome != want {
		t.Fatalf("%s = %+v, error = %v, want %s", description, result, err, want)
	}
}
