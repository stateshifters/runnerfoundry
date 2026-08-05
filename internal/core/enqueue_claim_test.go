package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestEnqueueAssignsIdentityAndDeduplicatesActiveSourceRequest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repository := core.NewMemoryJobStore()
	first := enqueue(ctx, t, repository, "request-1", "linux")
	duplicate, created, err := repository.Enqueue(ctx, core.JobRequest{
		Source: first.Job.Source, RunnerPool: "other", EnqueuedAt: fixedTime.Add(time.Hour),
	})
	requireNoError(t, err)
	if created || duplicate.Job.ID != first.Job.ID {
		t.Fatalf("duplicate = (%d, %t), want existing job %d", duplicate.Job.ID, created, first.Job.ID)
	}
}

func TestClaimIsFIFOWithinPoolAndClaimOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repository := core.NewMemoryJobStore()
	oldest := enqueue(ctx, t, repository, "oldest", "linux")
	enqueue(ctx, t, repository, "other-pool", "windows")
	newest := enqueue(ctx, t, repository, "newest", "linux")
	first := claim(ctx, t, repository, "linux", "member-1", fixedTime.Add(time.Minute))
	second := claim(ctx, t, repository, "linux", "member-2", fixedTime.Add(time.Minute))

	if first.Job.ID != oldest.Job.ID || second.Job.ID != newest.Job.ID {
		t.Fatalf("claims = (%d, %d), want (%d, %d)", first.Job.ID, second.Job.ID, oldest.Job.ID, newest.Job.ID)
	}
	requireState(t, first, core.JobStateAttempting)
	_, found, err := repository.ClaimOldest(ctx, "linux", memberID("member-3"), fixedTime.Add(time.Minute))
	requireNoError(t, err)
	if found {
		t.Fatal("already claimed job was claimed again")
	}
}

func TestBeginAcquisitionMarksTheExistingAttemptOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := core.NewMemoryJobStore()
	record := enqueue(ctx, t, store, "acquisition-start", "linux")
	claim(ctx, t, store, "linux", "member-1", fixedTime)
	at := fixedTime.Add(time.Minute)

	started, err := store.BeginAcquisition(ctx, record.Job.ID, at)
	requireNoError(t, err)
	if !started.AcquisitionStartedAt.Equal(at) {
		t.Fatalf("acquisition started at = %v, want %v", started.AcquisitionStartedAt, at)
	}
	repeated, err := store.BeginAcquisition(ctx, record.Job.ID, at.Add(time.Minute))
	requireNoError(t, err)
	if !repeated.AcquisitionStartedAt.Equal(at) {
		t.Fatalf("repeated begin changed marker to %v", repeated.AcquisitionStartedAt)
	}
}

func TestConfirmAcquisitionMarksOnlyABegunAttemptOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := core.NewMemoryJobStore()
	record := enqueue(ctx, t, store, "acquisition-confirm", "linux")
	claim(ctx, t, store, "linux", "member-1", fixedTime)
	_, err := store.ConfirmAcquisition(ctx, record.Job.ID, fixedTime)
	requireErrorIs(t, err, core.ErrInvalidTransition)
	_, err = store.BeginAcquisition(ctx, record.Job.ID, fixedTime)
	requireNoError(t, err)
	confirmedAt := fixedTime.Add(time.Minute)

	confirmed, err := store.ConfirmAcquisition(ctx, record.Job.ID, confirmedAt)
	requireNoError(t, err)
	if !confirmed.AcquiredAt.Equal(confirmedAt) {
		t.Fatalf("acquired at = %v, want %v", confirmed.AcquiredAt, confirmedAt)
	}
	repeated, err := store.ConfirmAcquisition(ctx, record.Job.ID, confirmedAt.Add(time.Minute))
	requireNoError(t, err)
	if !repeated.AcquiredAt.Equal(confirmedAt) {
		t.Fatalf("repeated confirmation changed marker to %v", repeated.AcquiredAt)
	}
}

func TestPendingAcquisitionsFindBegunUnconfirmedAttemptsInClaimOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := setupPendingAcquisitions(ctx, t)

	pending, err := fixture.store.PendingAcquisitions(ctx, 1)
	requireNoError(t, err)
	requireJobIDs(t, pending, fixture.first.Job.ID)
	_, err = fixture.store.ConfirmAcquisition(ctx, fixture.first.Job.ID, fixedTime.Add(3*time.Minute))
	requireNoError(t, err)
	pending, err = fixture.store.PendingAcquisitions(ctx, 10)
	requireNoError(t, err)
	requireJobIDsUnordered(t, pending, fixture.third.Job.ID, fixture.second.Job.ID)
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService}
	_, err = fixture.store.SetResultPending(ctx, fixture.second.Job.ID, result)
	requireNoError(t, err)
	_, err = fixture.store.SetResultPending(ctx, fixture.third.Job.ID, result)
	requireNoError(t, err)
	pending, err = fixture.store.PendingAcquisitions(ctx, 10)
	requireNoError(t, err)
	requireJobIDs(t, pending)
	pending, err = fixture.store.PendingAcquisitions(ctx, 0)
	requireNoError(t, err)
	requireJobIDs(t, pending)
}

type pendingAcquisitionFixture struct {
	store  *core.MemoryJobStore
	first  core.JobRecord
	second core.JobRecord
	third  core.JobRecord
}

func setupPendingAcquisitions(ctx context.Context, t *testing.T) pendingAcquisitionFixture {
	t.Helper()
	fixture := pendingAcquisitionFixture{store: core.NewMemoryJobStore()}
	fixture.first = enqueue(ctx, t, fixture.store, "pending-acquisition-first", "linux")
	fixture.second = enqueue(ctx, t, fixture.store, "pending-acquisition-second", "windows")
	fixture.third = enqueue(ctx, t, fixture.store, "pending-acquisition-third", "macos")
	claim(ctx, t, fixture.store, "linux", "member-1", fixedTime)
	claim(ctx, t, fixture.store, "windows", "member-1", fixedTime.Add(time.Minute))
	claim(ctx, t, fixture.store, "macos", "member-1", fixedTime)
	_, err := fixture.store.BeginAcquisition(ctx, fixture.first.Job.ID, fixedTime.Add(2*time.Minute))
	requireNoError(t, err)
	_, err = fixture.store.BeginAcquisition(ctx, fixture.second.Job.ID, fixedTime.Add(2*time.Minute))
	requireNoError(t, err)
	_, err = fixture.store.BeginAcquisition(ctx, fixture.third.Job.ID, fixedTime.Add(2*time.Minute))
	requireNoError(t, err)
	return fixture
}

func TestAcquisitionMarkersRejectMissingAndNonAttemptingJobs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := core.NewMemoryJobStore()
	queued := enqueue(ctx, t, store, "acquisition-invalid", "linux")
	_, err := store.BeginAcquisition(ctx, jobID("missing"), fixedTime)
	requireErrorIs(t, err, core.ErrJobNotFound)
	_, err = store.BeginAcquisition(ctx, queued.Job.ID, fixedTime)
	requireErrorIs(t, err, core.ErrInvalidTransition)
	_, err = store.ConfirmAcquisition(ctx, jobID("missing"), fixedTime)
	requireErrorIs(t, err, core.ErrJobNotFound)
}
