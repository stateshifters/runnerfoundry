package sqlite

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	sqlitestate "github.com/stateshifters/runnerfoundry/internal/state/sqlite"
)

func TestStorePersistsGitHubRegistrationAndDeclarationState(t *testing.T) {
	registration := githubsource.Registration{GitHubAppID: 7, RepositoryURL: "https://example.test/repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	executor := sqliteExecutor(func(statement string) [][]any {
		switch {
		case strings.Contains(statement, "github_member_runner_pools"):
			return [][]any{{"member-1", "linux", "default"}}
		case strings.Contains(statement, "github_"):
			return [][]any{{int64(7), registration.RepositoryURL, "linux", int64(1), int64(2), int64(3)}}
		default:
			return nil
		}
	}, []any{int64(7), registration.RepositoryURL, "linux", int64(1), int64(2), int64(3)})
	store := NewStore(executor)
	ctx := context.Background()
	assertRegistrationMutations(ctx, t, store, registration)
	assertRegistrationLists(ctx, t, store, registration)
	assertDeclarations(ctx, t, store)
}

func assertRegistrationMutations(ctx context.Context, t *testing.T, store *Store, registration githubsource.Registration) {
	t.Helper()
	if err := store.UpsertRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func(context.Context, githubsource.Registration) error{
		store.DeleteRegistrationState, store.DeleteActiveRegistration, store.DeleteOwnedRegistration,
	} {
		if err := operation(ctx, registration); err != nil {
			t.Fatal(err)
		}
	}
}

func assertRegistrationLists(ctx context.Context, t *testing.T, store *Store, registration githubsource.Registration) {
	t.Helper()
	active, err := store.Registrations(ctx)
	if err != nil || len(active) != 1 || active[0] != registration {
		t.Fatalf("active registrations = %#v, error = %v", active, err)
	}
	owned, err := store.OwnedRegistrations(ctx)
	if err != nil || len(owned) != 1 || owned[0] != registration {
		t.Fatalf("owned registrations = %#v, error = %v", owned, err)
	}
}

func assertDeclarations(ctx context.Context, t *testing.T, store *Store) {
	t.Helper()
	declarations := []githubsource.RunnerPoolDeclaration{{RunnerPool: "linux", RunnerGroup: "default"}}
	err := store.ReplaceGitHubRunnerPoolDeclarations(ctx, "member-1", declarations)
	if err != nil {
		t.Fatal(err)
	}
	live, err := store.LiveGitHubRunnerPoolDeclarations(ctx, time.Unix(1_700_000_000, 0).UTC())
	if err != nil || len(live) != 1 || live[0].MemberID != "member-1" || live[0].RunnerPool != "linux" {
		t.Fatalf("live declarations = %#v, error = %v", live, err)
	}
}

func TestStoreRejectsInvalidDemand(t *testing.T) {
	store := NewStore(sqlitestate.Executor{})
	if _, err := store.ReconcileAssignedDemand(context.Background(), githubsource.AssignedDemand{}); err == nil {
		t.Fatal("expected invalid demand error")
	}
}

func TestAssignedDemandAddsMissingAggregateJobs(t *testing.T) {
	demand := githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "observation", DesiredJobs: 1, ObservedAt: time.Unix(1_700_000_000, 0).UTC()}
	source, err := githubsource.AggregateDemandSource(demand, 1)
	if err != nil {
		t.Fatal(err)
	}
	transaction := sqlitestate.NewTransaction(
		func(context.Context, string, ...any) (sqlitestate.Result, error) {
			return sqlitestate.NewResult(func() (int64, error) { return 1, nil }), nil
		},
		func(context.Context, string, ...any) (sqlitestate.Rows, error) { return sqliteRows(nil), nil },
		func(_ context.Context, statement string, _ ...any) sqlitestate.Row {
			if strings.Contains(statement, "SELECT job_id") {
				return sqliteRow([]any{"00000000-0000-0000-0000-000000000001", string(source.Adapter), string(source.ID), "linux", demand.ObservedAt.UnixNano()})
			}
			return sqliteRow([]any{string(core.QueueQueued)})
		}, func(context.Context) error { return nil }, func(context.Context) error { return nil })
	store := NewStore(sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) { return transaction, nil }, nil))
	result, err := store.ReconcileAssignedDemand(context.Background(), demand)
	want := githubsource.AssignedDemandResult{ActiveJobs: 1, QueuedJobs: 1, JobsAdded: 1}
	if err != nil || result != want {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
}

func TestAssignedDemandRemovesNewestAggregateQueuedJob(t *testing.T) {
	demand := githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "current", DesiredJobs: 3, ObservedAt: time.Unix(1_700_000_000, 0).UTC()}
	previous := demand
	previous.ObservationID = "previous"
	first := aggregateDemandSource(t, previous, 1)
	second := aggregateDemandSource(t, previous, 2)
	third := aggregateDemandSource(t, previous, 3)
	exactID := encodedSourceID(t, githubsource.SourceIdentity{
		Repository: demand.Repository, RunnerPool: demand.RunnerPool, RequestID: 42,
	})
	exact := core.SourceReference{Adapter: githubsource.AdapterName, ID: exactID}
	transaction := sqliteRemovalTransaction(demand, first, second, third, exact)
	store := NewStore(sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) { return transaction, nil }, nil))
	result, err := store.ReconcileAssignedDemand(context.Background(), demand)
	want := githubsource.AssignedDemandResult{ActiveJobs: 3, QueuedJobs: 2, ProcessingJobs: 1, JobsRemoved: 1}
	if err != nil || result != want {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
}

func sqliteRemovalTransaction(
	demand githubsource.AssignedDemand,
	first, second, third, exact core.SourceReference,
) sqlitestate.Transaction {
	return sqlitestate.NewTransaction(
		func(context.Context, string, ...any) (sqlitestate.Result, error) {
			return sqlitestate.NewResult(func() (int64, error) { return 1, nil }), nil
		},
		func(context.Context, string, ...any) (sqlitestate.Rows, error) {
			return sqliteRows([][]any{
				{"00000000-0000-0000-0000-000000000001", string(first.Adapter), string(first.ID), "linux", demand.ObservedAt.Add(-time.Minute).UnixNano(), false},
				{"00000000-0000-0000-0000-000000000002", string(second.Adapter), string(second.ID), "linux", demand.ObservedAt.UnixNano(), false},
				{"00000000-0000-0000-0000-000000000003", string(third.Adapter), string(third.ID), "linux", demand.ObservedAt.UnixNano(), false},
				{"00000000-0000-0000-0000-000000000004", string(exact.Adapter), string(exact.ID), "linux", demand.ObservedAt.UnixNano(), true},
			}), nil
		}, nil, func(context.Context) error { return nil }, func(context.Context) error { return nil })
}

func aggregateDemandSource(t *testing.T, demand githubsource.AssignedDemand, ordinal int) core.SourceReference {
	t.Helper()
	source, err := githubsource.AggregateDemandSource(demand, ordinal)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func encodedSourceID(t *testing.T, identity githubsource.SourceIdentity) core.SourceIdentifier {
	t.Helper()
	sourceID, err := githubsource.EncodeSourceID(identity)
	if err != nil {
		t.Fatal(err)
	}
	return sourceID
}

func sqliteExecutor(rowsFor func(string) [][]any, queryRowValues []any) sqlitestate.Executor {
	return sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) {
		return sqlitestate.NewTransaction(
			func(context.Context, string, ...any) (sqlitestate.Result, error) {
				return sqlitestate.NewResult(func() (int64, error) { return 1, nil }), nil
			},
			func(_ context.Context, statement string, _ ...any) (sqlitestate.Rows, error) {
				return sqliteRows(rowsFor(statement)), nil
			},
			func(context.Context, string, ...any) sqlitestate.Row { return sqliteRow(queryRowValues) },
			func(context.Context) error { return nil },
			func(context.Context) error { return nil },
		), nil
	}, func(context.Context, string, ...any) sqlitestate.Row { return sqliteRow(queryRowValues) })
}

func sqliteRow(values []any) sqlitestate.Row {
	return sqlitestate.NewRow(func(destinations ...any) error { return assignSQLiteValues(destinations, values) })
}

func sqliteRows(values [][]any) sqlitestate.Rows {
	index := -1
	return sqlitestate.NewRows(func() error { return nil }, func() error { return nil },
		func() bool { index++; return index < len(values) },
		func(destinations ...any) error { return assignSQLiteValues(destinations, values[index]) })
}

func assignSQLiteValues(destinations, values []any) error {
	if len(destinations) != len(values) {
		return errors.New("destination count")
	}
	for index, destination := range destinations {
		target := reflect.ValueOf(destination)
		value := reflect.ValueOf(values[index])
		if value.Type().AssignableTo(target.Elem().Type()) {
			target.Elem().Set(value)
		} else if value.Type().ConvertibleTo(target.Elem().Type()) {
			target.Elem().Set(value.Convert(target.Elem().Type()))
		} else {
			return errors.New("value type")
		}
	}
	return nil
}
