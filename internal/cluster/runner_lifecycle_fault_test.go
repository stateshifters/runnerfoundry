package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func testRunnerLifecycle() scaler.RunnerLifecycle {
	return scaler.RunnerLifecycle{NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", Provider: scaler.RunnerProviderLibvirt,
		RunnerName: "linux-42", RequestID: 42, AssignmentID: 7, CreatedAt: time.Now().UTC(), Phase: scaler.RunnerReapPending}
}

func TestClusterRunnerLifecycleReportsConnectionFailures(t *testing.T) {
	store := NewStore(postgresconfig.Settings{})
	store.connector = func(context.Context) (*databaseConnection, error) { return nil, databaseFailure() }
	ctx := context.Background()
	runner := testRunnerLifecycle()
	checks := []func() error{
		func() error { return store.RegisterRunner(ctx, runner) },
		func() error { return store.ConfirmRunnerWaiting(ctx, runner, time.Now()) },
		func() error {
			return store.MarkRunnerBusy(ctx, "linux", "repo", scaler.RunnerIdentity{RequestID: 42}, 1, time.Now())
		},
		func() error {
			return store.RequestRunnerReap(ctx, "linux", "repo", scaler.RunnerIdentity{RequestID: 42}, "reason", time.Now())
		},
		func() error {
			_, err := store.RequestExpiredRunnerReaps(ctx, "linux", "repo", time.Now(), time.Now())
			return err
		},
		func() error { _, err := store.PendingRunnerReaps(ctx, "linux", "repo"); return err },
		func() error { _, err := store.PendingNodeRunnerReaps(ctx, "runner-host-1"); return err },
		func() error { _, err := store.RunnerLifecycles(ctx, "linux", "repo"); return err },
		func() error { return store.ConfirmRunnerReaped(ctx, runner) },
	}
	for index, check := range checks {
		if err := check(); err == nil {
			t.Fatalf("check %d did not fail", index)
		}
	}
}

func TestClusterRunnerLifecycleRegistrationFailures(t *testing.T) {
	runner := testRunnerLifecycle()
	connections := []*fakeDatabaseConnection{
		{execErrors: []error{databaseFailure()}},
		{execTag: pgconn.NewCommandTag("INSERT 0 0"), rowResults: []fakeRow{{err: databaseFailure()}}},
		{execErrors: []error{nil, databaseFailure()}},
	}
	for index, connection := range connections {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			candidate := runner
			if index < 3 {
				candidate.Provider = scaler.RunnerProviderDocker
			}
			if err := storeWithConnection(connection).RegisterRunner(context.Background(), candidate); err == nil {
				t.Fatal("expected registration failure")
			}
		})
	}
}

func TestClusterRunnerLifecycleSimpleTransitionFailures(t *testing.T) {
	ctx := context.Background()
	runner := testRunnerLifecycle()
	assertDirectTransitionFailures(t, func(store *Store) error {
		return store.ConfirmRunnerWaiting(ctx, runner, time.Now())
	})
	assertTransactionalTransitionFailures(t, func(store *Store) error {
		return store.MarkRunnerBusy(ctx, "linux", "repo", scaler.RunnerIdentity{RequestID: 42}, 1, time.Now())
	})
	assertRunnerReapTransitionFailures(ctx, t)
}

func assertDirectTransitionFailures(t *testing.T, operation func(*Store) error) {
	t.Helper()
	connections := []*fakeDatabaseConnection{
		{execErrors: []error{databaseFailure()}},
		{execTag: pgconn.NewCommandTag("UPDATE 0")},
	}
	assertStoreOperationsFail(t, operation, connections)
}

func assertTransactionalTransitionFailures(t *testing.T, operation func(*Store) error) {
	t.Helper()
	connections := []*fakeDatabaseConnection{
		{transaction: &fakeDatabaseTransaction{execErrors: []error{databaseFailure()}}},
		{transaction: &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}}},
	}
	assertStoreOperationsFail(t, operation, connections)
}

func assertStoreOperationsFail(
	t *testing.T,
	operation func(*Store) error,
	connections []*fakeDatabaseConnection,
) {
	t.Helper()
	for _, connection := range connections {
		if err := operation(storeWithConnection(connection)); err == nil {
			t.Fatal("expected transition failure")
		}
	}
}

func assertRunnerReapTransitionFailures(ctx context.Context, t *testing.T) {
	t.Helper()
	if err := storeWithConnection(&fakeDatabaseConnection{transaction: &fakeDatabaseTransaction{execErrors: []error{databaseFailure()}}}).
		RequestRunnerReap(ctx, "linux", "repo", scaler.RunnerIdentity{RequestID: 42}, "reason", time.Now()); err == nil {
		t.Fatal("expected reap update failure")
	}
}

func TestClusterExpiredRunnerLifecycleReportsTransactionFailures(t *testing.T) {
	connections := []*fakeDatabaseConnection{
		{beginErr: databaseFailure()},
		{transaction: &fakeDatabaseTransaction{execErrors: []error{databaseFailure()}}},
		{transaction: &fakeDatabaseTransaction{commitErr: databaseFailure()}},
	}
	for index, connection := range connections {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			if _, err := storeWithConnection(connection).RequestExpiredRunnerReaps(
				context.Background(), "linux", "repo", time.Now(), time.Now(),
			); err == nil {
				t.Fatal("expected timeout transaction failure")
			}
		})
	}
}

func TestClusterExpiredRunnerLifecycleReportsAssignmentReleaseFailure(t *testing.T) {
	transaction := &fakeDatabaseTransaction{execErrors: []error{nil, databaseFailure()}}
	if _, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).RequestExpiredRunnerReaps(
		context.Background(), "linux", "repo", time.Now(), time.Now(),
	); err == nil {
		t.Fatal("expected assignment release failure")
	}
}

func TestClusterRunnerLifecycleQueriesReportFailures(t *testing.T) {
	operations := []func(*Store) error{
		func(store *Store) error {
			_, err := store.PendingRunnerReaps(context.Background(), "linux", "repo")
			return err
		},
		func(store *Store) error {
			_, err := store.PendingNodeRunnerReaps(context.Background(), "runner-host-1")
			return err
		},
		func(store *Store) error {
			_, err := store.RunnerLifecycles(context.Background(), "linux", "repo")
			return err
		},
	}
	for operationIndex, operation := range operations {
		for failureIndex, connection := range []*fakeDatabaseConnection{
			{queryErr: databaseFailure()},
			{rowsResult: &fakeDatabaseRows{remaining: 1, scanErr: databaseFailure()}},
			{rowsResult: &fakeDatabaseRows{rowsErr: databaseFailure()}},
		} {
			t.Run(string(rune('a'+operationIndex))+string(rune('a'+failureIndex)), func(t *testing.T) {
				if err := operation(storeWithConnection(connection)); err == nil {
					t.Fatal("expected query failure")
				}
			})
		}
	}
}

func TestClusterConfirmRunnerReapedReportsTransactionFailures(t *testing.T) {
	runner := testRunnerLifecycle()
	connections := []*fakeDatabaseConnection{
		{beginErr: databaseFailure()},
		{transaction: &fakeDatabaseTransaction{execErrors: []error{databaseFailure()}}},
		{transaction: &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("DELETE 0")}}},
		{transaction: &fakeDatabaseTransaction{commitErr: databaseFailure()}},
	}
	for index, connection := range connections {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			if err := storeWithConnection(connection).ConfirmRunnerReaped(context.Background(), runner); err == nil {
				t.Fatal("expected confirmation failure")
			}
		})
	}
}
