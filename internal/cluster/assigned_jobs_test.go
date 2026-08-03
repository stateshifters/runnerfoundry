package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestObserveAssignedJobsReturnsEpochFencedCapacity(t *testing.T) {
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{{scan: func(destinations ...any) {
		*destination[int](t, destinations[0]) = 2
		*destination[int](t, destinations[1]) = 3
	}}, {scan: func(destinations ...any) { *destination[int](t, destinations[0]) = 0 }}}}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
	capacity, err := store.ObserveAssignedJobs(context.Background(), assignedObservationFixture())
	if err != nil {
		t.Fatal(err)
	}
	assertAssignedCapacity(t, capacity.ActiveRunners, capacity.AvailableCapacity)
	assertAssignedObservationTransitions(t, transaction)
	assertAssignedCapacityQuery(t, transaction.queries[0].statement)
}

func TestObserveAssignedJobsQueuesIdentityFreeObligations(t *testing.T) {
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{{scan: func(destinations ...any) {
		*destination[int](t, destinations[0]) = 0
		*destination[int](t, destinations[1]) = 3
	}}, {scan: func(destinations ...any) { *destination[int](t, destinations[0]) = 0 }}}}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})
	if _, err := store.ObserveAssignedJobs(context.Background(), assignedObservationFixture()); err != nil {
		t.Fatal(err)
	}
	foundInsert := false
	for _, execution := range transaction.executions {
		if strings.Contains(execution.statement, "INSERT INTO scaler_demand") &&
			strings.Contains(execution.statement, "'assigned_count'") {
			foundInsert = true
		}
	}
	if !foundInsert {
		t.Fatalf("assigned observation did not queue identity-free obligations: %#v", transaction.executions)
	}
}

func TestObserveAssignedJobsCancelsNewestUnclaimedObligations(t *testing.T) {
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{{scan: func(destinations ...any) {
		*destination[int](t, destinations[0]) = 2
		*destination[int](t, destinations[1]) = 3
	}}, {scan: func(destinations ...any) { *destination[int](t, destinations[0]) = 3 }}}}
	observation := assignedObservationFixture()
	observation.ControllerEpoch = 14
	observation.AssignedJobs = 2
	if _, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ObserveAssignedJobs(
		context.Background(), observation,
	); err != nil {
		t.Fatal(err)
	}
	lastExecution := transaction.executions[len(transaction.executions)-1]
	if !strings.Contains(transaction.executions[3].statement, "UPDATE scaler_demand") {
		t.Fatalf("newest cancellation ran before queued-obligation adoption: %#v", transaction.executions)
	}
	if !strings.Contains(lastExecution.statement, "ORDER BY demand.queue_time DESC, demand.observation_id DESC") ||
		lastExecution.arguments[3] != 3 {
		t.Fatalf("assigned obligation cancellation = %#v", lastExecution)
	}
}

func TestObserveAssignedJobsAdoptsQueuedObligationsAcrossControllerEpoch(t *testing.T) {
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{
		availableAssignedJobCapacityRow(t), queuedObligationCountRow(t, 2),
	}}
	observation := assignedObservationFixture()
	observation.ControllerEpoch = 14
	observation.AssignedJobs = 2
	if _, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ObserveAssignedJobs(
		context.Background(), observation,
	); err != nil {
		t.Fatal(err)
	}

	adoption := findQueuedObligationAdoption(t, transaction.executions)
	assertQueuedObligationAdoption(t, adoption, 14)
	assertNoQueuedObligationRecreation(t, transaction.executions)
}

func findQueuedObligationAdoption(t *testing.T, executions []fakeDatabaseExecution) fakeDatabaseExecution {
	t.Helper()
	for _, execution := range executions {
		if strings.Contains(execution.statement, "UPDATE scaler_demand") &&
			strings.Contains(execution.statement, "demand.source = 'assigned_count'") {
			return execution
		}
	}
	t.Fatal("epoch rollover did not adopt queued assigned-count obligations")
	return fakeDatabaseExecution{}
}

func assertQueuedObligationAdoption(t *testing.T, adoption fakeDatabaseExecution, expectedEpoch int64) {
	t.Helper()
	for _, required := range []string{
		"SET controller_epoch = $4", "NOT EXISTS", "demand.app_id = $1",
		"demand.repository = $2", "demand.scale_set = $3",
	} {
		if !strings.Contains(adoption.statement, required) {
			t.Fatalf("queued obligation adoption missing %q: %s", required, adoption.statement)
		}
	}
	for _, forbidden := range []string{"queue_time =", "attempt =", "next_attempt_at ="} {
		if strings.Contains(adoption.statement, forbidden) {
			t.Fatalf("queued obligation adoption changed durable FIFO or retry field %q: %s", forbidden, adoption.statement)
		}
	}
	if adoption.arguments[3] != expectedEpoch {
		t.Fatalf("queued obligation adopted epoch = %#v, want %d", adoption.arguments[3], expectedEpoch)
	}
}

func assertNoQueuedObligationRecreation(t *testing.T, executions []fakeDatabaseExecution) {
	t.Helper()
	for _, execution := range executions {
		if strings.Contains(execution.statement, "INSERT INTO scaler_demand") ||
			strings.Contains(execution.statement, "DELETE FROM scaler_demand") {
			t.Fatalf("unchanged authoritative count recreated queued obligations: %s", execution.statement)
		}
	}
}

func assertAssignedCapacity(t *testing.T, activeRunners int, availableCapacity int) {
	t.Helper()
	if activeRunners != 2 || availableCapacity != 3 {
		t.Fatalf("assigned capacity = active %d, available %d", activeRunners, availableCapacity)
	}
}

func assertAssignedObservationTransitions(t *testing.T, transaction *fakeDatabaseTransaction) {
	t.Helper()
	if len(transaction.executions) != 8 {
		t.Fatalf("assigned observation executions = %d, want 8", len(transaction.executions))
	}
	if !strings.Contains(transaction.executions[1].statement, "SET controller_epoch = $4") ||
		!strings.Contains(transaction.executions[1].statement, "EXISTS") {
		t.Fatalf("assigned epoch rollover did not adopt lifecycle-backed work: %s", transaction.executions[1].statement)
	}
	if !strings.Contains(transaction.executions[2].statement, "assignment.controller_epoch <> $4") ||
		!strings.Contains(transaction.executions[2].statement, "NOT EXISTS") {
		t.Fatalf("assigned epoch rollover did not fence only unstarted work: %s", transaction.executions[2].statement)
	}
	if !strings.Contains(transaction.executions[3].statement, "UPDATE scaler_demand") ||
		!strings.Contains(transaction.executions[3].statement, "NOT EXISTS") {
		t.Fatalf("assigned epoch rollover did not adopt unclaimed obligations: %s", transaction.executions[3].statement)
	}
	if !strings.Contains(transaction.executions[4].statement, "assignment.state = 'ready'") {
		t.Fatalf("assigned decrease did not cancel unclaimed work: %s", transaction.executions[4].statement)
	}
	if !strings.Contains(transaction.executions[5].statement, "repository = $3 AND phase = 'busy'") {
		t.Fatalf("assigned cleanup did not count all busy repository runners: %s", transaction.executions[5].statement)
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

func TestObserveAssignedJobsDoesNotDuplicateLifecycleBackedCapacity(t *testing.T) {
	capacityRow := func(destinations ...any) {
		*destination[int](t, destinations[0]) = 3
		*destination[int](t, destinations[1]) = 7
	}
	queuedRow := func(destinations ...any) { *destination[int](t, destinations[0]) = 0 }
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{
		{scan: capacityRow}, {scan: queuedRow}, {scan: capacityRow}, {scan: queuedRow},
	}}
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
	for _, execution := range transaction.executions {
		if strings.Contains(execution.statement, "INSERT INTO scaler_demand") {
			t.Fatalf("lifecycle-backed capacity duplicated assigned obligations: %s", execution.statement)
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
