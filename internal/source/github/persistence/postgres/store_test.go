package postgres

import (
	"context"
	"errors"
	"io/fs"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
)

func TestStorePersistsGitHubRegistrationAndDeclarationState(t *testing.T) {
	registration := githubsource.Registration{GitHubAppID: 7, RepositoryURL: "https://example.test/repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	executor := postgresExecutor(func(statement string) [][]any {
		switch {
		case strings.Contains(statement, "github_member_runner_pools"):
			return [][]any{{"member-1", "linux", "default"}}
		case strings.Contains(statement, "github_"):
			return [][]any{{int64(7), registration.RepositoryURL, "linux", int64(1), int64(2), int64(3)}}
		default:
			return nil
		}
	}, []any{int64(7), registration.RepositoryURL, "linux", int64(1), int64(2), int64(3)})
	store := newExecutorStore(executor)
	_ = NewStore(postgresstate.Connection{})
	_ = Migrations()
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
	store := newExecutorStore(postgresstate.Executor{})
	if _, err := store.ReconcileAssignedDemand(context.Background(), githubsource.AssignedDemand{}); err == nil {
		t.Fatal("expected invalid demand error")
	}
}

func TestMigrationsExposeOnlyOwnerLocalFiles(t *testing.T) {
	entries, err := fs.ReadDir(Migrations(), ".")
	if err != nil || len(entries) != 4 {
		t.Fatalf("entries = %d, error = %v", len(entries), err)
	}
	for _, name := range []string{"../migration.sql", "999_missing.sql"} {
		if _, err := Migrations().Open(name); err == nil {
			t.Fatalf("open %q succeeded", name)
		}
	}
}

func TestStoreReportsTransactionFailure(t *testing.T) {
	expectedErr := errors.New("begin")
	store := newExecutorStore(postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) {
		return postgresstate.Transaction{}, expectedErr
	}, nil))
	if err := store.DeleteActiveRegistration(context.Background(), githubsource.Registration{}); !errors.Is(err, expectedErr) {
		t.Fatalf("error = %v", err)
	}
}

func TestAssignedDemandAddsMissingAggregateJobs(t *testing.T) {
	demand := githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "observation", DesiredJobs: 2, ObservedAt: time.Unix(1_700_000_000, 0).UTC()}
	existingSource, err := githubsource.AggregateDemandSource(demand, 1)
	if err != nil {
		t.Fatal(err)
	}
	addedSource, err := githubsource.AggregateDemandSource(demand, 2)
	if err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	executor := postgresExecutor(func(statement string) [][]any {
		if strings.Contains(statement, "UNION ALL") {
			return [][]any{{uuid.MustParse("00000000-0000-0000-0000-000000000001"),
				string(existingSource.Adapter), string(existingSource.ID), "queued", demand.ObservedAt}}
		}
		return nil
	}, []any{jobID, string(addedSource.Adapter), string(addedSource.ID), "linux", demand.ObservedAt})
	result, err := newExecutorStore(executor).ReconcileAssignedDemand(context.Background(), demand)
	want := githubsource.AssignedDemandResult{ActiveJobs: 2, QueuedJobs: 2, JobsAdded: 1}
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
	transaction := postgresRemovalTransaction(demand, first, second, third, exact)
	executor := postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) { return transaction, nil }, nil)
	result, err := newExecutorStore(executor).ReconcileAssignedDemand(context.Background(), demand)
	want := githubsource.AssignedDemandResult{ActiveJobs: 3, QueuedJobs: 2, ProcessingJobs: 1, JobsRemoved: 1}
	if err != nil || result != want {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
}

func postgresRemovalTransaction(
	demand githubsource.AssignedDemand,
	first, second, third, exact core.SourceReference,
) postgresstate.Transaction {
	return postgresstate.NewTransaction(
		func(context.Context, string, ...any) (postgresstate.Result, error) {
			return postgresstate.NewResult(func() (int64, error) { return 1, nil }), nil
		},
		func(context.Context, string, ...any) (postgresstate.Rows, error) {
			return postgresRows([][]any{
				{uuid.MustParse("00000000-0000-0000-0000-000000000001"), string(first.Adapter), string(first.ID), "queued", demand.ObservedAt.Add(-time.Minute)},
				{uuid.MustParse("00000000-0000-0000-0000-000000000002"), string(second.Adapter), string(second.ID), "queued", demand.ObservedAt},
				{uuid.MustParse("00000000-0000-0000-0000-000000000003"), string(third.Adapter), string(third.ID), "queued", demand.ObservedAt},
				{uuid.MustParse("00000000-0000-0000-0000-000000000004"), string(exact.Adapter), string(exact.ID), "processing", demand.ObservedAt},
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

func postgresExecutor(rowsFor func(string) [][]any, queryRowValues []any) postgresstate.Executor {
	return postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) {
		return postgresstate.NewTransaction(
			func(context.Context, string, ...any) (postgresstate.Result, error) {
				return postgresstate.NewResult(func() (int64, error) { return 1, nil }), nil
			},
			func(_ context.Context, statement string, _ ...any) (postgresstate.Rows, error) {
				return postgresRows(rowsFor(statement)), nil
			},
			func(context.Context, string, ...any) postgresstate.Row { return postgresRow(queryRowValues) },
			func(context.Context) error { return nil },
			func(context.Context) error { return nil },
		), nil
	}, func(context.Context, string, ...any) postgresstate.Row { return postgresRow(queryRowValues) })
}

func postgresRow(values []any) postgresstate.Row {
	return postgresstate.NewRow(func(destinations ...any) error { return assignPostgresValues(destinations, values) })
}

func postgresRows(values [][]any) postgresstate.Rows {
	index := -1
	return postgresstate.NewRows(func() error { return nil }, func() error { return nil },
		func() bool { index++; return index < len(values) },
		func(destinations ...any) error { return assignPostgresValues(destinations, values[index]) })
}

func assignPostgresValues(destinations, values []any) error {
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
