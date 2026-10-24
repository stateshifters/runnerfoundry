package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestClaimOldestReturnsPersistedClaimTime(t *testing.T) {
	t.Parallel()
	claim, canonical := subMicrosecondClaim()
	result, err := newStore(claimPrecisionProtocol(canonical).executor()).
		ClaimOldest(context.Background(), "linux", claim)
	if err != nil || result.Outcome != core.ClaimClaimed || !result.Processing.ClaimedAt.Equal(canonical) {
		t.Fatalf("claim = (%#v, %v), want persisted time %s", result, err, canonical)
	}
}

func TestPersistedClaimResultRemainsEligibleForRetry(t *testing.T) {
	t.Parallel()
	claim, canonical := subMicrosecondClaim()
	claimed, err := newStore(claimPrecisionProtocol(canonical).executor()).
		ClaimOldest(context.Background(), "linux", claim)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	persisted := claimed.Processing
	persisted.ClaimedAt = canonical
	retryProtocol := &recordedTransaction{rows: []rowStub{
		rowFromValues(postgresClaimedOutcomeValues(persisted, false, false)),
		rowFromValues(queuedJobValues(persisted.Job.ID, core.JobRequest{
			Source: persisted.Job.Source, RunnerPool: persisted.Job.RunnerPool, EnqueuedAt: persisted.Job.EnqueuedAt,
		})),
	}}
	retry, err := newStore(retryProtocol.executor()).RetryUnstarted(context.Background(), claimed.Processing)
	if err != nil || retry.Outcome != core.RetryRequeued {
		t.Fatalf("retry = (%#v, %v), claimed time %s, persisted time %s",
			retry, err, claimed.Processing.ClaimedAt, canonical)
	}
}

func subMicrosecondClaim() (core.ProcessingClaim, time.Time) {
	claimedAt := fixedTime().Add(123 * time.Nanosecond)
	return core.ProcessingClaim{
		MemberID:            testMemberIDA,
		AttemptID:           core.AttemptID{1},
		ClaimedAt:           claimedAt,
		InventoryEndpointID: "docker:member-a",
	}, claimedAt.Truncate(time.Microsecond)
}

func claimPrecisionProtocol(canonical time.Time) *recordedTransaction {
	request := recoveryRequest()
	return &recordedTransaction{rows: []rowStub{
		rowFromValues(queuedJobValues(testJobID1, request)),
		rowFromValues(queuedJobValues(testJobID1, request)),
		rowFromValues([]any{canonical}),
	}}
}
