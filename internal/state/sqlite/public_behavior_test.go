package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestStorePublicQueueAndProcessingLookupBehaviors(t *testing.T) {
	t.Parallel()
	store := newStore(successfulBehaviorExecutor())
	ctx := context.Background()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-3"},
		RunnerPool: "linux", EnqueuedAt: fixedTime()}
	queued, err := store.Queue(ctx, request)
	if err != nil || queued.Outcome != core.QueueQueued || queued.Job.ID != testJobID {
		t.Fatalf("queue = %#v, %v", queued, err)
	}
	claimed, err := store.ClaimOldest(ctx, "linux", validProcessingClaim())
	if err != nil || claimed.Outcome != core.ClaimClaimed || claimed.Processing.MemberID != testMemberID {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	processingJobs, err := store.ProcessingByMember(ctx, testMemberID, "linux")
	if err != nil || len(processingJobs) != 1 || processingJobs[0].Job.ID != testJobID ||
		!processingJobs[0].ClaimedAt.Equal(fixedTime()) || processingJobs[0].InventoryEndpointID != "test:endpoint" ||
		!processingJobs[0].RunnerAccepted || !processingJobs[0].SourceTerminal {
		t.Fatalf("processing = %#v, %v", processingJobs, err)
	}
}

func TestStorePublicSourceLifecycleBehaviors(t *testing.T) {
	t.Parallel()
	store := newStore(successfulBehaviorExecutor())
	ctx := context.Background()
	source := core.SourceReference{Adapter: "github", ID: "request-3"}
	if _, err := store.ObserveRunnerAcceptance(ctx, core.JobIdentity{Source: source}); err != nil {
		t.Fatalf("mark accepted: %v", err)
	}
	if _, err := store.Terminalize(ctx, core.JobIdentity{Source: source}); err != nil {
		t.Fatalf("terminalize: %v", err)
	}
	if result, err := store.RemoveTerminal(ctx, testJobID); err != nil || result.Outcome != core.TerminalRemoved {
		t.Fatalf("remove processing: %#v, %v", result, err)
	}
}

func TestStorePublicCapacityBehaviors(t *testing.T) {
	t.Parallel()
	store := newStore(successfulBehaviorExecutor())
	ctx, now := context.Background(), fixedTime()
	if err := store.PublishMemberState(ctx, testMemberID, now,
		[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 2}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if count, err := store.CountLiveMembers(ctx, now); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if capacity, err := store.AvailableRunnerPoolCapacity(ctx, "linux", now); err != nil || capacity != 2 {
		t.Fatalf("capacity=%d err=%v", capacity, err)
	}
	if err := store.WithdrawMemberCapacity(ctx, testMemberID); err != nil {
		t.Fatalf("withdraw=%v", err)
	}
}

func TestStoreCloseUsesOptionalDatabaseCloser(t *testing.T) {
	t.Parallel()
	if err := newStore(executorStub{}).Close(); err != nil {
		t.Fatalf("empty close: %v", err)
	}
	store := newStore(executorStub{})
	store.close = func() error { return errors.New("close") }
	if err := store.Close(); err == nil {
		t.Fatal("expected close error")
	}
}

func successfulBehaviorExecutor() executorStub {
	transaction := transactionStub{
		execute: successfulExecute,
		query: func(_ context.Context, statement string, _ ...any) (rowsStub, error) {
			switch statement {
			case processingByMemberSQL:
				return rowsFromValues([][]any{claimedProcessingJobValues()}), nil
			default:
				return rowsFromValues(nil), nil
			}
		},
		queryRow: func(_ context.Context, statement string, _ ...any) rowStub {
			switch statement {
			case countLiveMembersSQL:
				return rowFromValues([]any{1})
			case availableCapacitySQL:
				return rowFromValues([]any{3, 0, 1})
			default:
				return rowFromValues(queuedJobValues())
			}
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
	return executorStub{begin: transactionBeginning(transaction), queryRow: transaction.queryRow}
}

func processingJobValues() []any {
	return []any{testJobIDText, "github", "request-3", "linux", testMemberIDText,
		string(core.AttemptRunnerName(testJobID, core.AttemptID{1})), true, true, ""}
}

func claimedProcessingJobValues() []any {
	return []any{testJobIDText, "github", "request-3", "linux", testMemberIDText,
		string(core.AttemptRunnerName(testJobID, core.AttemptID{1})), fixedTime().UnixNano(), "test:endpoint", true, true, ""}
}

func rowFromValues(values []any) rowStub {
	return rowStub{scan: func(destinations ...any) error { return assignDestinations(destinations, values) }}
}

func rowsFromValues(values [][]any) rowsStub {
	index := -1
	return rowsStub{close: func() error { return nil }, err: func() error { return nil },
		next: func() bool { index++; return index < len(values) },
		scan: func(destinations ...any) error { return assignDestinations(destinations, values[index]) }}
}

func fixedTime() time.Time { return time.Date(2026, time.August, 5, 15, 0, 0, 0, time.UTC) }
