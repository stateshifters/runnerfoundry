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
)

func TestObserveAssignedJobsReturnsEpochFencedCapacity(t *testing.T) {
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{{scan: func(destinations ...any) {
		*destination[int](t, destinations[0]) = 2
		*destination[int](t, destinations[1]) = 3
	}}}}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
	capacity, err := store.ObserveAssignedJobs(context.Background(), assignedObservationFixture())
	if err != nil {
		t.Fatal(err)
	}
	assertAssignedCapacity(t, capacity.ActiveRunners, capacity.AvailableCapacity)
	assertAssignedObservationTransitions(t, transaction)
	assertAssignedCapacityQuery(t, transaction.queries[0].statement)
}

func assertAssignedCapacity(t *testing.T, activeRunners int, availableCapacity int) {
	t.Helper()
	if activeRunners != 2 || availableCapacity != 3 {
		t.Fatalf("assigned capacity = active %d, available %d", activeRunners, availableCapacity)
	}
}

func assertAssignedObservationTransitions(t *testing.T, transaction *fakeDatabaseTransaction) {
	t.Helper()
	if len(transaction.executions) != 7 {
		t.Fatalf("assigned observation executions = %d, want 7", len(transaction.executions))
	}
	if !strings.Contains(transaction.executions[1].statement, "SET controller_epoch = $4") ||
		!strings.Contains(transaction.executions[1].statement, "EXISTS") {
		t.Fatalf("assigned epoch rollover did not adopt lifecycle-backed work: %s", transaction.executions[1].statement)
	}
	if !strings.Contains(transaction.executions[2].statement, "assignment.controller_epoch <> $4") ||
		!strings.Contains(transaction.executions[2].statement, "NOT EXISTS") {
		t.Fatalf("assigned epoch rollover did not fence only unstarted work: %s", transaction.executions[2].statement)
	}
	if !strings.Contains(transaction.executions[3].statement, "assignment.state = 'ready'") {
		t.Fatalf("assigned decrease did not cancel unclaimed work: %s", transaction.executions[3].statement)
	}
	if !strings.Contains(transaction.executions[4].statement, "repository = $3 AND phase = 'busy'") {
		t.Fatalf("assigned cleanup did not count all busy repository runners: %s", transaction.executions[4].statement)
	}
}

func assertAssignedCapacityQuery(t *testing.T, statement string) {
	t.Helper()
	for _, required := range []string{"eligible_contributions", "repository_assignments", "repository_unassigned_lifecycle"} {
		if !strings.Contains(statement, required) {
			t.Fatalf("assigned capacity query missing %q: %s", required, statement)
		}
	}
}

func TestRepeatedAssignedObservationCountsPendingAssignmentsAsActive(t *testing.T) {
	capacityRow := func(destinations ...any) {
		*destination[int](t, destinations[0]) = 3
		*destination[int](t, destinations[1]) = 7
	}
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{{scan: capacityRow}, {scan: capacityRow}}}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
	for range 2 {
		capacity, err := store.ObserveAssignedJobs(context.Background(), assignedObservationFixture())
		if err != nil {
			t.Fatal(err)
		}
		if capacity.ActiveRunners != 3 {
			t.Fatalf("repeated assigned active capacity = %d, want 3", capacity.ActiveRunners)
		}
	}
}

func TestObserveAssignedJobsRejectsObsoleteEpoch(t *testing.T) {
	transaction := &fakeDatabaseTransaction{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 0")}}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
	_, err := store.ObserveAssignedJobs(context.Background(), assignedObservationFixture())
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("obsolete assigned observation error = %v", err)
	}
}

func TestAssignAssignedJobReturnsIdentityFreeReadyAssignment(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	connection := &fakeDatabaseConnection{rowResults: []fakeRow{{scan: func(destinations ...any) {
		*destination[AssignmentID](t, destinations[0]) = 9
		*destination[NodeID](t, destinations[1]) = "node-a"
		*destination[string](t, destinations[2]) = "linux"
		*destination[string](t, destinations[3]) = "repo"
		*destination[time.Time](t, destinations[4]) = now
		*destination[githubapp.InstallationID](t, destinations[5]) = 7
		*destination[int](t, destinations[6]) = 11
		*destination[int64](t, destinations[7]) = 4
		*destination[AssignmentState](t, destinations[8]) = AssignmentReady
	}}}}
	assignment, err := storeWithConnection(connection).AssignAssignedJob(
		context.Background(), assignedObservationFixture(), now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if assignment.ID != 9 || assignment.RequestID != 0 || !assignment.IdentityFree ||
		assignment.ControllerEpoch != 4 || assignment.State != AssignmentReady {
		t.Fatalf("identity-free assignment = %#v", assignment)
	}
	if !strings.Contains(connection.queries[0].statement, "eligible_contributions") ||
		connection.queries[0].arguments[0] != now.Add(-time.Minute) {
		t.Fatalf("assigned contributor policy query = %#v", connection.queries[0])
	}
}

func TestAssignAssignedJobReturnsNoAssignmentForSatisfiedObservation(t *testing.T) {
	store := storeWithConnection(&fakeDatabaseConnection{rowResults: []fakeRow{{err: pgx.ErrNoRows}}})
	_, err := store.AssignAssignedJob(context.Background(), assignedObservationFixture(), time.Now().UTC(), time.Minute)
	if !errors.Is(err, ErrNoAssignment) {
		t.Fatalf("satisfied assigned observation error = %v", err)
	}
}

func TestStartingLoadsEpochAdoptedIdentityFreeAssignmentForRepair(t *testing.T) {
	queueTime := time.Unix(100, 0).UTC()
	rows := scriptedRows(scanAssignment(t, Assignment{
		ID: 9, NodeID: "node-a", ScaleSet: "linux", Repository: "repo", QueueTime: queueTime,
		InstallationID: 7, GitHubScaleSetID: 11, ControllerEpoch: 5,
		IdentityFree: true, State: AssignmentStarting,
	}))
	assignments, err := storeWithConnection(&fakeDatabaseConnection{rowsResult: rows}).Starting(
		context.Background(), "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 1 || !assignments[0].IdentityFree ||
		assignments[0].ControllerEpoch != 5 || assignments[0].State != AssignmentStarting {
		t.Fatalf("adopted starting assignments = %#v", assignments)
	}
}

func assignedObservationFixture() AssignedJobObservation {
	return AssignedJobObservation{
		AppID: 1, Repository: "repo", ScaleSet: "linux", ControllerEpoch: 4,
		AssignedJobs: 2, ObservedAt: time.Unix(100, 0).UTC(),
	}
}
