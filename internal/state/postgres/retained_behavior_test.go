package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
	statecontract "github.com/meza/RunnerFoundry/internal/state"
)

func TestPublishAndExpireMember(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	transaction := Transaction{execute: successfulExecute, commit: successfulCompletion, rollback: successfulCompletion}
	store := NewStore(Executor{begin: transactionBeginning(transaction)})
	if err := store.PublishMemberState(context.Background(), testMemberIDA, now,
		[]core.RunnerPoolCapacity{{RunnerPool: "linux", Capacity: 30}, {RunnerPool: "disabled", Capacity: 0}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := store.ExpireMember(context.Background(), testMemberIDA); err != nil {
		t.Fatalf("expire: %v", err)
	}
}

func TestMemberQueries(t *testing.T) {
	t.Parallel()
	now := fixedTime()
	transaction := Transaction{query: func(context.Context, string, ...any) (Rows, error) {
		return rowsFromValues([][]any{{testMemberIDA}}), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	store := NewStore(Executor{begin: transactionBeginning(transaction), queryRow: func(context.Context, string, ...any) Row {
		return rowFromValues([]any{1})
	}})
	live, err := store.ListLiveMembers(context.Background(), now)
	assertSingleMember(t, live, err)
	expired, err := store.ListExpiredMembers(context.Background(), now)
	assertSingleMember(t, expired, err)
	if count, err := store.CountLiveMembers(context.Background(), now); err != nil || count != 1 {
		t.Fatalf("count = %d, %v", count, err)
	}
}

func TestAvailableCapacity(t *testing.T) {
	t.Parallel()
	store := NewStore(Executor{queryRow: func(context.Context, string, ...any) Row {
		return rowFromValues([]any{30, 7, 5})
	}})
	now := fixedTime()
	if capacity, err := store.AvailableRunnerPoolCapacity(context.Background(), "linux", now); err != nil || capacity != 18 {
		t.Fatalf("capacity = %d, %v", capacity, err)
	}
}

func assertSingleMember(t *testing.T, members []core.MemberID, err error) {
	t.Helper()
	if err != nil || len(members) != 1 || members[0] != testMemberIDA {
		t.Fatalf("members = %#v, %v", members, err)
	}
}

func TestRegistrationBehavior(t *testing.T) {
	t.Parallel()
	registration := statecontract.Registration{RepositoryURL: "https://example.test/repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	values := []any{registration.RepositoryURL, string(registration.RunnerPool), int64(1), int64(2), int64(3)}
	transaction := Transaction{execute: successfulExecute,
		query:  func(context.Context, string, ...any) (Rows, error) { return rowsFromValues([][]any{values}), nil },
		commit: successfulCompletion, rollback: successfulCompletion}
	store := NewStore(Executor{begin: transactionBeginning(transaction),
		queryRow: func(context.Context, string, ...any) Row { return rowFromValues(values) }})
	if err := store.UpsertRegistration(context.Background(), registration); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	actual, found, err := store.Registration(context.Background(), registration.RepositoryURL, registration.RunnerPool)
	if err != nil || !found || actual != registration {
		t.Fatalf("registration = %#v, %t, %v", actual, found, err)
	}
	registrations, err := store.Registrations(context.Background())
	if err != nil || len(registrations) != 1 || registrations[0] != registration {
		t.Fatalf("registrations = %#v, %v", registrations, err)
	}
}

func TestRegistrationLookupHandlesMissingRow(t *testing.T) {
	t.Parallel()
	store := registrationLookupErrorStore(sql.ErrNoRows)
	_, found, err := store.Registration(context.Background(), "repo", "linux")
	if err != nil || found {
		t.Fatalf("found=%t error=%v", found, err)
	}
}

func TestRegistrationLookupReportsBrokenRow(t *testing.T) {
	t.Parallel()
	store := registrationLookupErrorStore(errors.New("protocol"))
	_, _, err := store.Registration(context.Background(), "repo", "linux")
	if err == nil {
		t.Fatal("expected lookup error")
	}
}

func registrationLookupErrorStore(scanErr error) *Store {
	return NewStore(Executor{queryRow: func(context.Context, string, ...any) Row {
		return Row{scan: func(...any) error { return scanErr }}
	}})
}
