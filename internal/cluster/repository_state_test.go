package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func TestPostgreSQLStoreStartsRepositoryControllerAndReturnsSnapshot(t *testing.T) {
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{repositoryControlRow(t, 0, 1, false)}}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
	var _ repositorystate.Store = store
	snapshot, err := store.StartController(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Control.AppID != 7 || snapshot.Control.ControllerEpoch != 1 || snapshot.Control.Initialized {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if len(transaction.executions) != 2 || transaction.executions[0].arguments[0] != int64(7) {
		t.Fatalf("parameterized executions = %#v", transaction.executions)
	}
}

func TestPostgreSQLRepositoryMutationsRejectStaleControllerEpoch(t *testing.T) {
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{{err: pgx.ErrNoRows}}}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
	err := store.UpsertRegistration(context.Background(), 7, 1, repositorystate.Registration{
		RepositoryURL: "repo", ScaleSetName: "linux", RunnerGroup: "Default", GitHubScaleSetID: 9,
	})
	if !errors.Is(err, repositorystate.ErrControllerFenced) {
		t.Fatalf("stale controller error = %v", err)
	}
	if len(transaction.executions) != 0 {
		t.Fatalf("stale controller executions = %#v", transaction.executions)
	}
}

func TestPostgreSQLDiscoveryUsesGenerationForAuthoritativeDeletion(t *testing.T) {
	completedAt := time.Unix(100, 0).UTC()
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{
		{scan: repositoryControlScan(t, 7, 4, 2, true, &completedAt)},
		repositoryControlRow(t, 5, 2, true),
	}}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
	snapshot, err := store.ApplyDiscovery(context.Background(), 7, 2,
		[]githubapp.InstallationID{11, 12}, []githubapp.InstallationID{12},
		[]repositorystate.Repository{{URL: "repo", InstallationID: 11}}, completedAt)
	if err != nil || snapshot.Control.DiscoveryGeneration != 5 {
		t.Fatalf("snapshot = %#v, %v", snapshot, err)
	}
	if len(transaction.executions) != 12 {
		t.Fatalf("discovery executions = %#v", transaction.executions)
	}
	cleanup := transaction.executions[8]
	if len(cleanup.arguments) != 4 || cleanup.arguments[0] != int64(7) ||
		cleanup.arguments[1] != githubapp.InstallationID(11) || cleanup.arguments[2] != int64(5) ||
		cleanup.arguments[3] != completedAt {
		t.Fatalf("assigned cleanup arguments = %#v", cleanup.arguments)
	}
	last := transaction.executions[len(transaction.executions)-1]
	if len(last.arguments) != 3 || last.arguments[1] != githubapp.InstallationID(11) || last.arguments[2] != int64(5) {
		t.Fatalf("authoritative deletion arguments = %#v", last.arguments)
	}
}

func TestPostgreSQLRepositorySnapshotLoadsRows(t *testing.T) {
	observedAt := time.Unix(100, 5).UTC()
	connection := &fakeDatabaseConnection{
		rowResults: []fakeRow{
			{scan: repositoryControlScan(t, 7, 3, 4, true, &observedAt)},
		},
		rowsResults: []rowIterator{
			&repositoryStateRows{rows: []func(...any){func(destinations ...any) {
				*destination[int64](t, destinations[0]) = 7
				*destination[string](t, destinations[1]) = "repo"
				*destination[githubapp.InstallationID](t, destinations[2]) = 11
				*destination[int64](t, destinations[3]) = 3
			}}},
			&repositoryStateRows{rows: []func(...any){func(destinations ...any) {
				*destination[int64](t, destinations[0]) = 7
				*destination[string](t, destinations[1]) = "repo"
				*destination[string](t, destinations[2]) = "linux"
				*destination[string](t, destinations[3]) = "Default"
				*destination[int](t, destinations[4]) = 21
			}}},
		},
	}
	store := storeWithConnection(connection)
	snapshot, err := store.LoadSnapshot(context.Background(), 7)
	if err != nil || len(snapshot.Repositories) != 1 || len(snapshot.Registrations) != 1 ||
		!snapshot.Control.LastSuccessfulDiscovery.Equal(observedAt) {
		t.Fatalf("snapshot = %#v, %v", snapshot, err)
	}
}

func TestPostgreSQLRepositorySnapshotLoadsEmptyState(t *testing.T) {
	store := storeWithConnection(&fakeDatabaseConnection{rowResults: []fakeRow{{err: pgx.ErrNoRows}}})
	empty, err := store.LoadSnapshot(context.Background(), 8)
	if err != nil || empty.Control.AppID != 8 || len(empty.Repositories) != 0 {
		t.Fatalf("empty snapshot = %#v, %v", empty, err)
	}
}

func TestPostgreSQLRepositoryControllerReportsLifecycleFailures(t *testing.T) {
	failure := errors.New("failed")
	tests := map[string]*fakeDatabaseConnection{
		"begin":  {beginErr: failure},
		"insert": {transaction: &fakeDatabaseTransaction{execErrors: []error{failure}}},
		"update": {transaction: &fakeDatabaseTransaction{execErrors: []error{nil, failure}}},
		"load": {
			transaction: &fakeDatabaseTransaction{rowResults: []fakeRow{{err: failure}}},
		},
		"commit": {
			transaction: &fakeDatabaseTransaction{rowResults: []fakeRow{repositoryControlRow(t, 0, 1, false)}, commitErr: failure},
		},
	}
	for name, connection := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := storeWithConnection(connection).StartController(context.Background(), 7); err == nil {
				t.Fatal("expected controller start failure")
			}
		})
	}
	store := NewStore(Settings{})
	store.connector = func(context.Context) (*databaseConnection, error) { return nil, failure }
	if _, err := store.StartController(context.Background(), 7); err == nil {
		t.Fatal("expected connector failure")
	}
}

func TestPostgreSQLRepositoryDiscoveryReportsEveryWriteFailure(t *testing.T) {
	failure := errors.New("failed")
	if _, err := storeWithConnection(&fakeDatabaseConnection{beginErr: failure}).ApplyDiscovery(
		context.Background(), 7, 2, nil, nil, nil, time.Unix(1, 0)); err == nil {
		t.Fatal("expected discovery begin failure")
	}
	for failureIndex := 0; failureIndex < 5; failureIndex++ {
		t.Run(fmt.Sprintf("write %d", failureIndex), func(t *testing.T) {
			errorsByExecution := make([]error, 5)
			errorsByExecution[failureIndex] = failure
			transaction := &fakeDatabaseTransaction{
				execErrors: errorsByExecution,
				rowResults: []fakeRow{repositoryControlRow(t, 1, 2, true)},
			}
			_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyDiscovery(
				context.Background(), 7, 2, []githubapp.InstallationID{11}, nil,
				[]repositorystate.Repository{{URL: "repo", InstallationID: 11}}, time.Unix(1, 0))
			if err == nil {
				t.Fatal("expected discovery write failure")
			}
		})
	}
}

func TestPostgreSQLDiscoveryReportsRepositoryReplacementFailure(t *testing.T) {
	failure := errors.New("failed")
	executionErrors := make([]error, 6)
	executionErrors[5] = failure
	transaction := &fakeDatabaseTransaction{
		execErrors: executionErrors,
		rowResults: []fakeRow{repositoryControlRow(t, 1, 2, true)},
	}
	_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyDiscovery(
		context.Background(), 7, 2, nil, nil,
		[]repositorystate.Repository{{URL: "repo", InstallationID: 11}}, time.Unix(1, 0),
	)
	if !errors.Is(err, failure) {
		t.Fatalf("repository replacement error = %v", err)
	}
}

func TestPostgreSQLRepositoryDiscoveryReportsLifecycleFailures(t *testing.T) {
	failure := errors.New("failed")
	for name, transaction := range map[string]*fakeDatabaseTransaction{
		"lock":     {rowResults: []fakeRow{{err: failure}}},
		"snapshot": {rowResults: []fakeRow{repositoryControlRow(t, 1, 2, true), {err: failure}}},
		"commit": {
			rowResults: []fakeRow{repositoryControlRow(t, 1, 2, true), repositoryControlRow(t, 2, 2, true)},
			commitErr:  failure,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyDiscovery(
				context.Background(), 7, 2, nil, nil, nil, time.Unix(1, 0))
			if err == nil {
				t.Fatal("expected discovery failure")
			}
		})
	}
}

func TestPostgreSQLRegistrationMutationsSucceedAndReportFailures(t *testing.T) {
	registration := repositorystate.Registration{RepositoryURL: "repo", ScaleSetName: "linux", RunnerGroup: "Default", GitHubScaleSetID: 9}
	if err := storeWithConnection(&fakeDatabaseConnection{beginErr: errors.New("failed")}).UpsertRegistration(
		context.Background(), 7, 2, registration); err == nil {
		t.Fatal("expected registration begin failure")
	}
	for name, operation := range map[string]func(*Store) error{
		"upsert": func(store *Store) error { return store.UpsertRegistration(context.Background(), 7, 2, registration) },
		"delete": func(store *Store) error { return store.DeleteRegistration(context.Background(), 7, 2, "repo", "linux") },
	} {
		t.Run(name, func(t *testing.T) { assertPostgreSQLRegistrationMutation(t, operation) })
	}
}

func TestDeleteRegistrationWithdrawsAssignedRunnersBeforeRegistration(t *testing.T) {
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{repositoryControlRow(t, 1, 2, true)}}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
	if err := store.DeleteRegistration(context.Background(), 7, 2, "repo", "linux"); err != nil {
		t.Fatal(err)
	}
	if len(transaction.executions) != 4 {
		t.Fatalf("registration withdrawal executions = %#v", transaction.executions)
	}
	cleanup := transaction.executions[0]
	if len(cleanup.arguments) != 4 || cleanup.arguments[1] != int64(7) ||
		cleanup.arguments[2] != "repo" || cleanup.arguments[3] != "linux" {
		t.Fatalf("registration cleanup arguments = %#v", cleanup.arguments)
	}
	deletion := transaction.executions[3]
	if len(deletion.arguments) != 3 || deletion.arguments[0] != int64(7) ||
		deletion.arguments[1] != "repo" || deletion.arguments[2] != "linux" {
		t.Fatalf("registration deletion arguments = %#v", deletion.arguments)
	}
}

func assertPostgreSQLRegistrationMutation(t *testing.T, operation func(*Store) error) {
	t.Helper()
	for name, transaction := range map[string]*fakeDatabaseTransaction{
		"success": {rowResults: []fakeRow{repositoryControlRow(t, 1, 2, true)}},
		"write": {
			rowResults: []fakeRow{repositoryControlRow(t, 1, 2, true)}, execErrors: []error{errors.New("failed")},
		},
		"commit": {
			rowResults: []fakeRow{repositoryControlRow(t, 1, 2, true)}, commitErr: errors.New("failed"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := operation(storeWithConnection(&fakeDatabaseConnection{transaction: transaction}))
			if name == "success" && err != nil {
				t.Fatal(err)
			}
			if name != "success" && err == nil {
				t.Fatal("expected registration failure")
			}
		})
	}
}

func TestPostgreSQLRepositorySnapshotReportsQueryAndRowFailures(t *testing.T) {
	failure := errors.New("failed")
	validControl := repositoryControlRow(t, 1, 2, true)
	tests := map[string]*fakeDatabaseConnection{
		"control":                 {rowResults: []fakeRow{{err: failure}}},
		"repositories query":      {rowResults: []fakeRow{validControl}, queryErrors: []error{failure}},
		"repositories scan":       {rowResults: []fakeRow{validControl}, rowsResults: []rowIterator{&repositoryStateRows{rows: []func(...any){nil}}}},
		"repositories iteration":  {rowResults: []fakeRow{validControl}, rowsResults: []rowIterator{&repositoryStateRows{rowsErr: failure}}},
		"registrations query":     {rowResults: []fakeRow{validControl}, queryErrors: []error{nil, failure}},
		"registrations scan":      {rowResults: []fakeRow{validControl}, rowsResults: []rowIterator{&repositoryStateRows{}, &repositoryStateRows{rows: []func(...any){nil}}}},
		"registrations iteration": {rowResults: []fakeRow{validControl}, rowsResults: []rowIterator{&repositoryStateRows{}, &repositoryStateRows{rowsErr: failure}}},
	}
	for name, connection := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := storeWithConnection(connection).LoadSnapshot(context.Background(), 7); err == nil {
				t.Fatal("expected snapshot failure")
			}
		})
	}
	store := NewStore(Settings{})
	store.connector = func(context.Context) (*databaseConnection, error) { return nil, failure }
	if _, err := store.LoadSnapshot(context.Background(), 7); err == nil {
		t.Fatal("expected snapshot connector failure")
	}
}

type repositoryStateRows struct {
	rows    []func(...any)
	index   int
	rowsErr error
}

func (*repositoryStateRows) Close()          {}
func (rows *repositoryStateRows) Err() error { return rows.rowsErr }
func (rows *repositoryStateRows) Next() bool {
	if rows.index >= len(rows.rows) {
		return false
	}
	rows.index++
	return true
}
func (rows *repositoryStateRows) Scan(destinations ...any) error {
	scan := rows.rows[rows.index-1]
	if scan == nil {
		return errors.New("scan failed")
	}
	scan(destinations...)
	return nil
}

func repositoryControlRow(t *testing.T, generation int64, epoch int64, initialized bool) fakeRow {
	t.Helper()
	return fakeRow{scan: repositoryControlScan(t, 7, generation, epoch, initialized, nil)}
}

func repositoryControlScan(t *testing.T, appID int64, generation int64, epoch int64, initialized bool, observedAt *time.Time) func(...any) {
	t.Helper()
	return func(destinations ...any) {
		*destination[int64](t, destinations[0]) = appID
		*destination[int64](t, destinations[1]) = generation
		*destination[*time.Time](t, destinations[2]) = observedAt
		*destination[int64](t, destinations[3]) = epoch
		*destination[bool](t, destinations[4]) = initialized
	}
}
