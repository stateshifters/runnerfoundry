package core_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/meza/RunnerFoundry/internal/core"
)

var fixedTime = time.Date(2026, time.August, 5, 10, 0, 0, 0, time.UTC)

func enqueue(
	ctx context.Context,
	t *testing.T,
	repository *core.MemoryJobStore,
	sourceID string,
	runnerPool core.RunnerPool,
) core.JobRecord {
	t.Helper()
	return enqueueAt(ctx, t, repository, sourceID, runnerPool, fixedTime)
}

func enqueueAt(
	ctx context.Context,
	t *testing.T,
	repository *core.MemoryJobStore,
	sourceID string,
	runnerPool core.RunnerPool,
	enqueuedAt time.Time,
) core.JobRecord {
	t.Helper()

	record, created, err := repository.Enqueue(ctx, core.JobRequest{
		Source:     core.SourceReference{Adapter: "git", ID: sourceID},
		RunnerPool: runnerPool,
		EnqueuedAt: enqueuedAt,
	})
	requireNoError(t, err)
	if !created {
		t.Fatalf("source request %q did not create a job", sourceID)
	}
	return record
}

func claim(
	ctx context.Context,
	t *testing.T,
	repository *core.MemoryJobStore,
	runnerPool core.RunnerPool,
	member string,
	claimedAt time.Time,
) core.JobRecord {
	t.Helper()

	record, found, err := repository.ClaimOldest(ctx, runnerPool, memberID(member), claimedAt)
	requireNoError(t, err)
	if !found {
		t.Fatalf("Runner Pool %q had no queued job", runnerPool)
	}
	return record
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func requireErrorIs(t *testing.T, err error, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want %v", err, target)
	}
}

func requireState(t *testing.T, record core.JobRecord, state core.JobState) {
	t.Helper()
	if record.State != state {
		t.Fatalf("state = %q, want %q", record.State, state)
	}
}

func requireJobIDs(t *testing.T, records []core.JobRecord, expected ...core.JobID) {
	t.Helper()
	if len(records) != len(expected) {
		t.Fatalf("job count = %d, want %d: %#v", len(records), len(expected), records)
	}
	for index, jobID := range expected {
		if records[index].Job.ID != jobID {
			t.Fatalf("job %d = %s, want %s", index, records[index].Job.ID, jobID)
		}
	}
}

func requireJobIDsUnordered(t *testing.T, records []core.JobRecord, expected ...core.JobID) {
	t.Helper()
	if len(records) != len(expected) {
		t.Fatalf("job count = %d, want %d: %#v", len(records), len(expected), records)
	}
	wanted := make(map[core.JobID]struct{}, len(expected))
	for _, jobID := range expected {
		wanted[jobID] = struct{}{}
	}
	for _, record := range records {
		if _, exists := wanted[record.Job.ID]; !exists {
			t.Fatalf("unexpected job %s in %#v", record.Job.ID, records)
		}
	}
}

func memberID(name string) core.MemberID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("member:"+name))
}

func jobID(name string) core.JobID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("job:"+name))
}
