package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestStorePublicQueueAndProcessingLookupBehaviors(t *testing.T) {
	t.Parallel()
	store := NewStore(successfulBehaviorExecutor())
	ctx := context.Background()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-3"},
		RunnerPool: "linux", EnqueuedAt: fixedTime()}
	queued, err := store.Queue(ctx, request)
	if err != nil || !queued.Enqueued || queued.Job.ID != testJobID {
		t.Fatalf("queue = %#v, %v", queued, err)
	}
	claimed, err := store.ClaimOldest(ctx, "linux", core.ProcessingClaim{MemberID: testMemberID})
	if err != nil || !claimed.Taken || claimed.Processing.MemberID != testMemberID {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	processingJobs, err := store.ProcessingByMember(ctx, testMemberID, "linux")
	if err != nil || len(processingJobs) != 1 || processingJobs[0].Job.ID != testJobID ||
		!processingJobs[0].RunnerStarted || !processingJobs[0].SourceTerminal {
		t.Fatalf("processing = %#v, %v", processingJobs, err)
	}
}

func TestStorePublicSourceLifecycleBehaviors(t *testing.T) {
	t.Parallel()
	store := NewStore(successfulBehaviorExecutor())
	ctx := context.Background()
	source := core.SourceReference{Adapter: "github", ID: "request-3"}
	if err := store.MarkRunnerStarted(ctx, source); err != nil {
		t.Fatalf("mark started: %v", err)
	}
	if err := store.TerminalizeSource(ctx, source); err != nil {
		t.Fatalf("terminalize: %v", err)
	}
	if result, err := store.RemoveTerminal(ctx, testJobID); err != nil || !result.Applied {
		t.Fatalf("remove processing: %#v, %v", result, err)
	}
}

func TestStorePublicCapacityBehaviors(t *testing.T) {
	t.Parallel()
	store := NewStore(successfulBehaviorExecutor())
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

func TestStorePublicRegistrationBehaviors(t *testing.T) {
	t.Parallel()
	store := NewStore(successfulBehaviorExecutor())
	ctx := context.Background()
	registration := githubsource.Registration{RepositoryURL: "repo", RunnerPool: "linux", InstallationID: 1,
		RunnerGroupID: 2, ScaleSetID: 3}
	if err := store.UpsertRegistration(ctx, registration); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	actual, found, err := store.Registration(ctx, "repo", "linux")
	if err != nil || !found || actual != registration {
		t.Fatalf("registration=%#v found=%t err=%v", actual, found, err)
	}
	registrations, err := store.Registrations(ctx)
	if err != nil || len(registrations) != 1 {
		t.Fatalf("registrations=%#v err=%v", registrations, err)
	}
	if err := store.DeleteRegistration(ctx, "repo", "linux"); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestStoreCloseUsesOptionalDatabaseCloser(t *testing.T) {
	t.Parallel()
	if err := NewStore(Executor{}).Close(); err != nil {
		t.Fatalf("empty close: %v", err)
	}
	store := NewStore(Executor{})
	store.close = func() error { return errors.New("close") }
	if err := store.Close(); err == nil {
		t.Fatal("expected close error")
	}
}

func successfulBehaviorExecutor() Executor {
	transaction := Transaction{
		execute: successfulExecute,
		query: func(_ context.Context, statement string, _ ...any) (Rows, error) {
			switch statement {
			case registrationsSQL:
				return rowsFromValues([][]any{{"repo", "linux", int64(1), int64(2), int64(3)}}), nil
			case processingByMemberSQL:
				return rowsFromValues([][]any{processingJobValues()}), nil
			default:
				return rowsFromValues(nil), nil
			}
		},
		queryRow: func(_ context.Context, statement string, _ ...any) Row {
			switch statement {
			case countLiveMembersSQL:
				return rowFromValues([]any{1})
			case availableCapacitySQL:
				return rowFromValues([]any{3, 0, 1})
			case registrationSQL:
				return rowFromValues([]any{"repo", "linux", int64(1), int64(2), int64(3)})
			default:
				return rowFromValues(queuedJobValues())
			}
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
	return Executor{begin: transactionBeginning(transaction), queryRow: transaction.queryRow}
}

func processingJobValues() []any {
	return []any{testJobIDText, "github", "request-3", "linux", testMemberIDText, true, true}
}

func rowFromValues(values []any) Row {
	return Row{scan: func(destinations ...any) error { return assignDestinations(destinations, values) }}
}

func rowsFromValues(values [][]any) Rows {
	index := -1
	return Rows{close: func() error { return nil }, err: func() error { return nil },
		next: func() bool { index++; return index < len(values) },
		scan: func(destinations ...any) error { return assignDestinations(destinations, values[index]) }}
}

func fixedTime() time.Time { return time.Date(2026, time.August, 5, 15, 0, 0, 0, time.UTC) }
