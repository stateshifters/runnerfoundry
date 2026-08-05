package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	statecontract "github.com/meza/RunnerFoundry/internal/state"
)

func TestStorePublicQueueAndProcessingBehaviors(t *testing.T) {
	t.Parallel()
	store := NewStore(successfulBehaviorExecutor())
	ctx := context.Background()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-3"},
		RunnerPool: "linux", EnqueuedAt: fixedTime()}
	job, created, err := store.Enqueue(ctx, request)
	if err != nil || !created || job.ID != testJobID {
		t.Fatalf("enqueue = %#v, %t, %v", job, created, err)
	}
	processing, found, err := store.TakeOldest(ctx, "linux", testMemberID)
	if err != nil || !found || processing.MemberID != testMemberID {
		t.Fatalf("take = %#v, %t, %v", processing, found, err)
	}
	processingJobs, err := store.ProcessingByMember(ctx, testMemberID, "linux")
	if err != nil || len(processingJobs) != 1 || processingJobs[0].Job.ID != testJobID {
		t.Fatalf("processing = %#v, %v", processingJobs, err)
	}
	if err := store.CancelQueued(ctx, request.Source); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := store.RemoveProcessing(ctx, testJobID); err != nil {
		t.Fatalf("remove processing: %v", err)
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
	liveMembers, err := store.ListLiveMembers(ctx, now)
	assertMembers(t, liveMembers, err)
	expiredMembers, err := store.ListExpiredMembers(ctx, now)
	assertMembers(t, expiredMembers, err)
	if count, err := store.CountLiveMembers(ctx, now); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if capacity, err := store.AvailableRunnerPoolCapacity(ctx, "linux", now); err != nil || capacity != 2 {
		t.Fatalf("capacity=%d err=%v", capacity, err)
	}
}

func TestStorePublicRegistrationBehaviors(t *testing.T) {
	t.Parallel()
	store := NewStore(successfulBehaviorExecutor())
	ctx := context.Background()
	registration := statecontract.Registration{RepositoryURL: "repo", RunnerPool: "linux", InstallationID: 1,
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

func assertMembers(t *testing.T, members []core.MemberID, err error) {
	t.Helper()
	if err != nil || len(members) != 1 || members[0] != testMemberID {
		t.Fatalf("members = %#v, %v", members, err)
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
			case listLiveMembersSQL, listExpiredMembersSQL:
				return rowsFromValues([][]any{{testMemberIDText}}), nil
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
	return []any{testJobIDText, "github", "request-3", "linux", testMemberIDText}
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
