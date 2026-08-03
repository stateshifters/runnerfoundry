package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestAssignmentSuccessPathsUseInMemoryDatabase(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	claim := fakeRow{scan: scanClaimedAssignment(t, now)}
	claimed, err := storeWithConnection(&fakeDatabaseConnection{rowResults: []fakeRow{claim}}).
		ClaimForStart(context.Background(), "node")
	if err != nil || claimed.ID != 1 {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	oldest, contributor, identifier := assignmentRows(t)
	assigned, err := storeWithConnection(&fakeDatabaseConnection{transaction: &fakeDatabaseTransaction{
		rowResults: []fakeRow{oldest, contributor, identifier},
	}}).AssignOldest(context.Background(), "set", now, time.Minute)
	if err != nil || assigned == nil {
		t.Fatalf("assignment = %#v, %v", assigned, err)
	}
	assertNoError(t, storeWithConnection(&fakeDatabaseConnection{}).ConfirmAcquisition(context.Background(), 1))
	assertNoError(t, storeWithConnection(&fakeDatabaseConnection{}).CompleteDemand(context.Background(), "set", "repo", 1))
}

func TestAssignmentRowsDecodeInMemory(t *testing.T) {
	rows := scriptedRows(scanClaimedAssignment(t, time.Unix(1, 0).UTC()))
	assignments, err := scanAssignments(newDatabaseRows(rows))
	if err != nil || len(assignments) != 1 || assignments[0].ID != 1 {
		t.Fatalf("assignments = %#v, %v", assignments, err)
	}
}

func TestLeadershipSuccessPathsUseInMemoryDatabase(t *testing.T) {
	for _, acquire := range []func(*Store) (*LeaderLease, error){
		func(store *Store) (*LeaderLease, error) { return store.TryLeader(context.Background()) },
		func(store *Store) (*LeaderLease, error) { return store.WaitForLeader(context.Background()) },
	} {
		connection := &fakeDatabaseConnection{rowResults: []fakeRow{booleanRow(t, true)}}
		lease, err := acquire(storeWithConnection(connection))
		if err != nil || lease == nil {
			t.Fatalf("lease = %#v, %v", lease, err)
		}
		assertNoError(t, lease.Close(context.Background()))
	}
}

func TestNodeSuccessPathsUseInMemoryDatabase(t *testing.T) {
	store := storeWithConnection(&fakeDatabaseConnection{})
	assertNoError(t, store.Publish(context.Background(), "node", NodeHealthy, nil, time.Unix(1, 0).UTC()))
	rows := scriptedRows(func(destinations ...any) {
		*destination[string](t, destinations[0]) = "set"
		*destination[string](t, destinations[1]) = "group"
		*destination[bool](t, destinations[2]) = false
		*destination[int](t, destinations[3]) = 4
	})
	statuses, err := scanScaleSetStatuses(newDatabaseRows(rows))
	if err != nil || len(statuses) != 1 || statuses[0].EffectiveCapacity != 0 {
		t.Fatalf("statuses = %#v, %v", statuses, err)
	}
	store = storeWithConnection(&fakeDatabaseConnection{rowsResult: scriptedRows()})
	if statuses, err = store.ScaleSets(context.Background(), time.Unix(1, 0).UTC(), time.Minute); err != nil || len(statuses) != 0 {
		t.Fatalf("empty statuses = %#v, %v", statuses, err)
	}
}

func TestLibvirtLifecycleSuccessPathsUseInMemoryDatabase(t *testing.T) {
	runner := LibvirtRunnerLifecycle{NodeID: "node", ScaleSet: "set", Repository: "repo", RequestID: 1, RunnerName: "runner"}
	assertNoError(t, storeWithConnection(&fakeDatabaseConnection{}).RegisterLibvirtRunner(context.Background(), 1, runner))
	assertNoError(t, storeWithConnection(&fakeDatabaseConnection{}).CompleteJob(context.Background(), JobCompletion{
		ScaleSet: "set", Repository: "repo", RequestID: 1,
	}))
	rows := scriptedRows(scanLibvirtRunner(t, runner))
	reaps, err := storeWithConnection(&fakeDatabaseConnection{rowsResult: rows}).PendingLibvirtReaps(context.Background(), "node")
	if err != nil || len(reaps) != 1 {
		t.Fatalf("reaps = %#v, %v", reaps, err)
	}
	assertNoError(t, storeWithConnection(&fakeDatabaseConnection{}).ConfirmLibvirtReaped(context.Background(), runner))
}

func TestRunnerLifecycleSuccessPathsUseInMemoryDatabase(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	runner := scaler.RunnerLifecycle{NodeID: "node", ScaleSet: "set", Repository: "repo", Provider: scaler.RunnerProviderLibvirt,
		RunnerName: "runner", RequestID: 1, AssignmentID: 2, CreatedAt: now}
	store := storeWithConnection(&fakeDatabaseConnection{})
	assertNoError(t, store.RegisterRunner(context.Background(), runner))
	assertNoError(t, store.ConfirmRunnerWaiting(context.Background(), runner, now))
	assertNoError(t, store.MarkRunnerBusy(context.Background(), "set", "repo", 1, "runner", 3, now))
	assertNoError(t, store.RequestRunnerReap(context.Background(), "set", "repo", 1, "runner", "done", now))
	changed, err := store.RequestExpiredRunnerReaps(context.Background(), "set", "repo", now, now)
	if err != nil || changed != 1 {
		t.Fatalf("expired = %d, %v", changed, err)
	}
	if changed, err = store.RequestExcessWaitingRunnerReaps(context.Background(), "set", "repo", 1, "excess", now); err != nil || changed != 0 {
		t.Fatalf("excess = %d, %v", changed, err)
	}
	assertNoError(t, store.ConfirmRunnerReaped(context.Background(), runner))
}

func TestRunnerRegistrationAcceptsExistingAndNonLibvirtRunners(t *testing.T) {
	runner := scaler.RunnerLifecycle{NodeID: "node", ScaleSet: "set", Repository: "repo", Provider: scaler.RunnerProviderDocker,
		RunnerName: "runner", RequestID: 1, AssignmentID: 2}
	assertNoError(t, storeWithConnection(&fakeDatabaseConnection{}).RegisterRunner(context.Background(), runner))
	connection := &fakeDatabaseConnection{
		execTag: pgconn.NewCommandTag("INSERT 0 0"), rowResults: []fakeRow{booleanRow(t, true)},
	}
	assertNoError(t, storeWithConnection(connection).RegisterRunner(context.Background(), runner))
}

func TestRunnerLifecycleRowsDecodeEpochAsUnset(t *testing.T) {
	runner := scaler.RunnerLifecycle{NodeID: "node", ScaleSet: "set", Repository: "repo", Provider: scaler.RunnerProviderDocker,
		RunnerName: "runner", RequestID: 1, Phase: scaler.RunnerWaitingForJob, WaitingSince: time.Unix(0, 0).UTC()}
	rows := scriptedRows(scanRunnerLifecycle(t, runner))
	store := storeWithConnection(&fakeDatabaseConnection{rowsResult: rows})
	runners, err := store.RunnerLifecycles(context.Background(), "set", "repo")
	if err != nil || len(runners) != 1 || !runners[0].WaitingSince.IsZero() {
		t.Fatalf("runners = %#v, %v", runners, err)
	}
}

func TestMarkRunnerBusyBindsIdentityFreeRunnerToStartedRequest(t *testing.T) {
	connection := &fakeDatabaseConnection{}
	store := storeWithConnection(connection)
	if err := store.MarkRunnerBusy(
		context.Background(), "linux", "repo", 42, "linux-1", 99, time.Unix(10, 0).UTC(),
	); err != nil {
		t.Fatal(err)
	}
	if len(connection.executions) != 2 {
		t.Fatalf("busy binding executions = %#v", connection.executions)
	}
	if !strings.Contains(connection.executions[0].statement, "request_id = CASE") ||
		connection.executions[0].arguments[5] != int64(42) {
		t.Fatalf("runner request binding = %#v", connection.executions[0])
	}
	if connection.executions[1].arguments[0] != int64(42) {
		t.Fatalf("legacy runner request binding = %#v", connection.executions[1])
	}
}

func TestPrimaryConnectorReportsInjectedConnectionFailure(t *testing.T) {
	originalResolver := productionConnectionSettingResolver
	t.Cleanup(func() { productionConnectionSettingResolver = originalResolver })
	productionConnectionSettingResolver = testConnectionResolver(nil)
	store := NewStore(Settings{Role: "runnerfoundry"})
	store.connectConfig = func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error) {
		return nil, errors.New("failed")
	}
	if _, err := store.connectPostgreSQL(context.Background()); err == nil {
		t.Fatal("expected connection failure")
	}
}

type scriptedDatabaseRows struct {
	scans   []func(...any)
	current int
}

func (*scriptedDatabaseRows) Close()     {}
func (*scriptedDatabaseRows) Err() error { return nil }
func (rows *scriptedDatabaseRows) Next() bool {
	if rows.current == len(rows.scans) {
		return false
	}
	rows.current++
	return true
}
func (rows *scriptedDatabaseRows) Scan(destinations ...any) error {
	rows.scans[rows.current-1](destinations...)
	return nil
}

func scriptedRows(scans ...func(...any)) *scriptedDatabaseRows {
	return &scriptedDatabaseRows{scans: scans}
}

func scanClaimedAssignment(t *testing.T, queueTime time.Time) func(...any) {
	t.Helper()
	return scanAssignment(t, Assignment{
		ID: 1, NodeID: "node", ScaleSet: "set", Repository: "repo", RequestID: 2,
		QueueTime: queueTime, InstallationID: 3, GitHubScaleSetID: 4, State: AssignmentReady,
	})
}

func scanAssignment(t *testing.T, assignment Assignment) func(...any) {
	t.Helper()
	return func(destinations ...any) {
		*destination[AssignmentID](t, destinations[0]) = assignment.ID
		*destination[NodeID](t, destinations[1]) = assignment.NodeID
		*destination[string](t, destinations[2]) = assignment.ScaleSet
		*destination[string](t, destinations[3]) = assignment.Repository
		*destination[int64](t, destinations[4]) = assignment.RequestID
		*destination[time.Time](t, destinations[5]) = assignment.QueueTime
		*destination[githubapp.InstallationID](t, destinations[6]) = assignment.InstallationID
		*destination[int](t, destinations[7]) = assignment.GitHubScaleSetID
		*destination[int64](t, destinations[8]) = assignment.ControllerEpoch
		*destination[bool](t, destinations[9]) = assignment.IdentityFree
		*destination[AssignmentState](t, destinations[10]) = assignment.State
	}
}

func scanLibvirtRunner(t *testing.T, runner LibvirtRunnerLifecycle) func(...any) {
	t.Helper()
	return func(destinations ...any) {
		*destination[NodeID](t, destinations[0]) = runner.NodeID
		*destination[string](t, destinations[1]) = runner.ScaleSet
		*destination[string](t, destinations[2]) = runner.Repository
		*destination[int64](t, destinations[3]) = runner.RequestID
		*destination[string](t, destinations[4]) = runner.RunnerName
	}
}

func scanRunnerLifecycle(t *testing.T, runner scaler.RunnerLifecycle) func(...any) {
	t.Helper()
	return func(destinations ...any) {
		values := []any{&runner.NodeID, &runner.ScaleSet, &runner.Repository, &runner.Provider, &runner.RunnerName,
			&runner.RequestID, &runner.AssignmentID, &runner.InstallationID, &runner.GitHubScaleSetID,
			&runner.GitHubRunnerID, &runner.Phase, &runner.CreatedAt, &runner.WaitingSince, &runner.UpdatedAt, &runner.ReapReason}
		for index, value := range values {
			assignDestination(t, destinations[index], value)
		}
	}
}

func assignDestination(t *testing.T, destinationValue any, source any) {
	t.Helper()
	switch value := source.(type) {
	case *string:
		*destination[string](t, destinationValue) = *value
	case *int:
		*destination[int](t, destinationValue) = *value
	case *int64:
		*destination[int64](t, destinationValue) = *value
	case *scaler.RunnerProvider:
		*destination[scaler.RunnerProvider](t, destinationValue) = *value
	case *scaler.RunnerLifecyclePhase:
		*destination[scaler.RunnerLifecyclePhase](t, destinationValue) = *value
	case *time.Time:
		*destination[time.Time](t, destinationValue) = *value
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
