package cluster

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
)

func TestRegisterLibvirtRunnerReportsDatabaseFailures(t *testing.T) {
	runner := testClusterLibvirtRunner()
	connectionFailure := NewStore(postgresconfig.Settings{})
	connectionFailure.connector = func(context.Context) (*databaseConnection, error) { return nil, databaseFailure() }
	if err := connectionFailure.RegisterLibvirtRunner(context.Background(), 1, runner); err == nil {
		t.Fatal("expected connection failure")
	}
	for name, connection := range map[string]*fakeDatabaseConnection{
		"insert":   {execErrors: []error{databaseFailure()}},
		"inspect":  {execTag: pgconn.NewCommandTag("INSERT 0 0"), rowResults: []fakeRow{{err: databaseFailure()}}},
		"conflict": {execTag: pgconn.NewCommandTag("INSERT 0 0"), rowResults: []fakeRow{{scan: scanBoolean(t, false)}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := storeWithConnection(connection).RegisterLibvirtRunner(context.Background(), 1, runner); err == nil {
				t.Fatal("expected failure")
			}
		})
	}
	duplicate := &fakeDatabaseConnection{execTag: pgconn.NewCommandTag("INSERT 0 0"), rowResults: []fakeRow{{scan: scanBoolean(t, true)}}}
	if err := storeWithConnection(duplicate).RegisterLibvirtRunner(context.Background(), 1, runner); err != nil {
		t.Fatal(err)
	}
}

func TestCompleteLibvirtJobReportsDatabaseFailures(t *testing.T) {
	completion := JobCompletion{ScaleSet: "linux", Repository: "repo", RequestID: 42}
	connections := []*fakeDatabaseConnection{
		{beginErr: databaseFailure()},
		{transaction: &fakeDatabaseTransaction{execErrors: []error{databaseFailure()}}},
		{transaction: &fakeDatabaseTransaction{execErrors: []error{nil, databaseFailure()}}},
		{transaction: &fakeDatabaseTransaction{execErrors: []error{nil, nil, databaseFailure()}}},
		{transaction: &fakeDatabaseTransaction{commitErr: databaseFailure()}},
	}
	for index, connection := range connections {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			if err := storeWithConnection(connection).CompleteJob(context.Background(), completion); err == nil {
				t.Fatal("expected failure")
			}
		})
	}
}

func TestClusterLibvirtLifecycleReportsConnectionFailures(t *testing.T) {
	store := NewStore(postgresconfig.Settings{})
	store.connector = func(context.Context) (*databaseConnection, error) { return nil, databaseFailure() }
	if err := store.CompleteJob(context.Background(), JobCompletion{}); err == nil {
		t.Fatal("expected completion failure")
	}
	if _, err := store.PendingLibvirtReaps(context.Background(), "runner-host-1"); err == nil {
		t.Fatal("expected pending failure")
	}
	if err := store.ConfirmLibvirtReaped(context.Background(), testClusterLibvirtRunner()); err == nil {
		t.Fatal("expected confirmation failure")
	}
}

func TestPendingLibvirtReapsReportsDatabaseFailures(t *testing.T) {
	connections := []*fakeDatabaseConnection{
		{queryErr: databaseFailure()},
		{rowsResult: &fakeDatabaseRows{remaining: 1, scanErr: databaseFailure()}},
		{rowsResult: &fakeDatabaseRows{rowsErr: databaseFailure()}},
	}
	for index, connection := range connections {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			if _, err := storeWithConnection(connection).PendingLibvirtReaps(context.Background(), "runner-host-1"); err == nil {
				t.Fatal("expected failure")
			}
		})
	}
}

func TestConfirmLibvirtReapedReportsDatabaseFailures(t *testing.T) {
	runner := testClusterLibvirtRunner()
	for _, connection := range []*fakeDatabaseConnection{{execErrors: []error{databaseFailure()}}, {execTag: pgconn.NewCommandTag("DELETE 0")}} {
		if err := storeWithConnection(connection).ConfirmLibvirtReaped(context.Background(), runner); err == nil {
			t.Fatal("expected failure")
		}
	}
}

func testClusterLibvirtRunner() LibvirtRunnerLifecycle {
	return LibvirtRunnerLifecycle{NodeID: "runner-host-1", ScaleSet: "linux", Repository: "repo", RequestID: 42, RunnerName: "linux-42"}
}

func scanBoolean(t *testing.T, value bool) func(...any) {
	t.Helper()
	return func(destinations ...any) { *destination[bool](t, destinations[0]) = value }
}
