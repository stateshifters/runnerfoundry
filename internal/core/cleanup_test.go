package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestMemberAttemptsBecomeResultPendingWithoutAffectingOtherMembers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repository := core.NewMemoryJobStore()
	first := enqueue(ctx, t, repository, "expired-linux", "linux")
	second := enqueue(ctx, t, repository, "expired-windows", "windows")
	other := enqueue(ctx, t, repository, "active", "macos")
	claim(ctx, t, repository, "linux", "expired", fixedTime)
	claim(ctx, t, repository, "windows", "expired", fixedTime)
	claim(ctx, t, repository, "macos", "active", fixedTime)
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService}

	pending, err := repository.SetMemberAttemptsResultPending(ctx, memberID("expired"), result)
	requireNoError(t, err)
	if len(pending) != 2 || pending[0].Job.ID != first.Job.ID || pending[1].Job.ID != second.Job.ID {
		t.Fatalf("pending = %#v", pending)
	}
	active, found, err := repository.Lookup(ctx, other.Job.ID)
	requireNoError(t, err)
	if !found {
		t.Fatal("active member job is missing")
	}
	requireState(t, active, core.JobStateAttempting)
}

func TestAttemptsByMemberReturnsOnlyOwnedAttemptingJobs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := core.NewMemoryJobStore()
	first := enqueue(ctx, t, store, "owned-linux", "linux")
	second := enqueue(ctx, t, store, "owned-windows", "windows")
	enqueue(ctx, t, store, "other", "macos")
	claim(ctx, t, store, "linux", "member-1", fixedTime)
	claim(ctx, t, store, "windows", "member-1", fixedTime)
	claim(ctx, t, store, "macos", "member-2", fixedTime)

	attempts, err := store.AttemptsByMember(ctx, memberID("member-1"), "linux")
	requireNoError(t, err)
	if len(attempts) != 1 || attempts[0].Job.ID != first.Job.ID {
		t.Fatalf("attempts = %#v", attempts)
	}
	windows, err := store.AttemptsByMember(ctx, memberID("member-1"), "windows")
	requireNoError(t, err)
	if len(windows) != 1 || windows[0].Job.ID != second.Job.ID {
		t.Fatalf("windows attempts = %#v", windows)
	}
}

func TestOnlyUnstartedAttemptsAtCutoffBecomeResultPendingOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fixture := setupTimeoutAttempts(ctx, t)
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService}

	pending, err := fixture.store.SetUnstartedAttemptsResultPending(ctx, "linux", fixedTime.Add(time.Minute), result)
	requireNoError(t, err)
	if len(pending) != 1 || pending[0].Job.ID != fixture.timedOut.Job.ID {
		t.Fatalf("pending = %#v", pending)
	}
	pending, err = fixture.store.SetUnstartedAttemptsResultPending(ctx, "linux", fixedTime.Add(time.Minute), result)
	requireNoError(t, err)
	if len(pending) != 0 {
		t.Fatalf("repeat scan returned %d jobs", len(pending))
	}
	recentRecord, found, err := fixture.store.Lookup(ctx, fixture.recent.Job.ID)
	requireNoError(t, err)
	if !found {
		t.Fatal("recent job is missing")
	}
	requireState(t, recentRecord, core.JobStateAttempting)
	otherRecord, found, err := fixture.store.Lookup(ctx, fixture.otherPool.Job.ID)
	requireNoError(t, err)
	if !found {
		t.Fatal("other-pool job is missing")
	}
	requireState(t, otherRecord, core.JobStateAttempting)
}

type timeoutAttemptsFixture struct {
	store     *core.MemoryJobStore
	timedOut  core.JobRecord
	recent    core.JobRecord
	otherPool core.JobRecord
}

func setupTimeoutAttempts(ctx context.Context, t *testing.T) timeoutAttemptsFixture {
	t.Helper()

	fixture := timeoutAttemptsFixture{store: core.NewMemoryJobStore()}
	fixture.timedOut = enqueue(ctx, t, fixture.store, "timed-out", "linux")
	fixture.recent = enqueue(ctx, t, fixture.store, "recent", "linux")
	fixture.otherPool = enqueue(ctx, t, fixture.store, "other-pool", "windows")
	store := fixture.store
	started := enqueue(ctx, t, store, "started", "macos")
	claim(ctx, t, store, "linux", "member", fixedTime)
	claim(ctx, t, store, "linux", "member", fixedTime.Add(2*time.Minute))
	claim(ctx, t, store, "windows", "member", fixedTime)
	claim(ctx, t, store, "macos", "member", fixedTime)
	_, err := store.MarkStartedBySource(ctx, started.Job.Source, fixedTime.Add(time.Second))
	requireNoError(t, err)
	return fixture
}
