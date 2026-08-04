package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestClusterMessageUpdateRejectsStaleEpochBeforeMutation(t *testing.T) {
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{clusterEpochRow(t, 2)}}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})

	_, err := store.ApplyMessageUpdate(context.Background(), validClusterMessageUpdate())
	if !errors.Is(err, scaler.ErrNotLeader) {
		t.Fatalf("message update error = %v, want %v", err, scaler.ErrNotLeader)
	}
	if len(transaction.queries) != 1 || !strings.Contains(transaction.queries[0].statement, "scaler_repository_controls") ||
		!strings.Contains(transaction.queries[0].statement, "FOR SHARE") {
		t.Fatalf("epoch queries = %#v", transaction.queries)
	}
	if len(transaction.executions) != 0 || transaction.rollbackCalls != 1 {
		t.Fatalf("stale update executions = %#v, rollbacks = %d", transaction.executions, transaction.rollbackCalls)
	}
}

func TestClusterMessageUpdateRollsBackDemandFailure(t *testing.T) {
	assertClusterMessageRollback(t, clusterMessageUpdateWithDemand(), 0, "INSERT INTO scaler_demand")
}

func TestClusterMessageUpdateRollsBackLifecycleFailure(t *testing.T) {
	assertClusterMessageRollback(t, clusterMessageUpdateWithStart(), 0, "UPDATE scaler_runner_lifecycle")
}

func TestClusterMessageUpdateRollsBackCompletionFailure(t *testing.T) {
	assertClusterMessageRollback(t, clusterMessageUpdateWithCompletion(), 0, "UPDATE scaler_runner_lifecycle")
}

func TestClusterMessageUpdateTreatsMissingLifecycleMessagesAsNoOps(t *testing.T) {
	tests := []struct {
		name   string
		update scaler.MessageUpdate
	}{
		{name: "start", update: clusterMessageUpdateWithNameOnlyStart("missing")},
		{name: "completion", update: clusterMessageUpdateWithNameOnlyCompletion("missing")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transaction := successfulClusterMessageTransaction(t)
			transaction.execTags = []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}
			if _, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyMessageUpdate(
				context.Background(), test.update,
			); err != nil {
				t.Fatal(err)
			}
			if transaction.rollbackCalls != 0 {
				t.Fatalf("rollback calls = %d", transaction.rollbackCalls)
			}
		})
	}
}

func TestClusterMessageUpdateCompletesDemandWhenPositiveLifecycleIsMissing(t *testing.T) {
	transaction := &fakeDatabaseTransaction{rowResults: []fakeRow{
		clusterEpochRow(t, 3), integerCountRow(t, 0), integerCountRow(t, 0),
		availableAssignedJobCapacityRow(t), queuedObligationCountRow(t, 0),
		capacityObligationQueueSnapshotRow(t, 0, 0, nil),
	}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}}
	update := clusterMessageUpdateWithCompletion()

	if _, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyMessageUpdate(
		context.Background(), update,
	); err != nil {
		t.Fatal(err)
	}
	var removed bool
	for _, execution := range transaction.executions {
		if strings.Contains(execution.statement, "DELETE FROM scaler_demand") {
			removed = true
		}
	}
	if !removed {
		t.Fatalf("message executions = %#v, want completed demand removal", transaction.executions)
	}
}

func TestClusterMessageUpdateRollsBackMismatchedCompletionIdentity(t *testing.T) {
	transaction := &fakeDatabaseTransaction{
		execTags:   []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")},
		rowResults: []fakeRow{clusterEpochRow(t, 3), integerCountRow(t, 1), integerCountRow(t, 1)},
	}
	_, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyMessageUpdate(
		context.Background(), clusterMessageUpdateWithCompletion(),
	)
	if !errors.Is(err, scaler.ErrRunnerIdentityMismatch) {
		t.Fatalf("completion error = %v, want mismatch", err)
	}
	if transaction.rollbackCalls != 1 {
		t.Fatalf("rollback calls = %d, want 1", transaction.rollbackCalls)
	}
}

func TestClusterMessageUpdateRollsBackStatisticsWithPriorEffects(t *testing.T) {
	assertClusterMessageRollback(
		t, clusterMessageUpdateWithDemandAndStart(), 3, "INSERT INTO scaler_assigned_job_observations",
	)
}

func TestClusterMessageUpdateReturnsCommittedQueueReconciliationAndSnapshot(t *testing.T) {
	oldestRunnableAt := time.Unix(80, 0).UTC()
	transaction := committedClusterMessageTransaction(t, oldestRunnableAt)
	update := clusterMessageUpdateWithDemand()
	update.AssignedJobs = 2
	result, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyMessageUpdate(
		context.Background(), update,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertCommittedClusterQueueSnapshot(t, result, oldestRunnableAt)
}

func committedClusterMessageTransaction(t *testing.T, oldestRunnableAt time.Time) *fakeDatabaseTransaction {
	t.Helper()
	transaction := successfulClusterMessageTransaction(t)
	transaction.execTags = []pgconn.CommandTag{
		pgconn.NewCommandTag("INSERT 0 1"),
		pgconn.NewCommandTag("INSERT 0 1"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("INSERT 0 2"),
	}
	transaction.rowResults[len(transaction.rowResults)-1] = capacityObligationQueueSnapshotRow(
		t, 1, 2, &oldestRunnableAt,
	)
	return transaction
}

func assertCommittedClusterQueueSnapshot(
	t *testing.T,
	result scaler.MessageUpdateResult,
	oldestRunnableAt time.Time,
) {
	t.Helper()
	if result.AssignedObligations != (scaler.AssignedObligationDecision{Create: 2}) {
		t.Fatalf("assigned obligation decision = %#v", result.AssignedObligations)
	}
	wantSnapshot := scaler.CapacityObligationQueueSnapshot{
		JobAvailable: 1, AssignedCount: 2, NewlyQueued: 3, OldestRunnableAt: &oldestRunnableAt,
	}
	if result.QueueSnapshot.JobAvailable != wantSnapshot.JobAvailable ||
		result.QueueSnapshot.AssignedCount != wantSnapshot.AssignedCount ||
		result.QueueSnapshot.NewlyQueued != wantSnapshot.NewlyQueued ||
		result.QueueSnapshot.Canceled != wantSnapshot.Canceled ||
		result.QueueSnapshot.OldestRunnableAt == nil ||
		!result.QueueSnapshot.OldestRunnableAt.Equal(*wantSnapshot.OldestRunnableAt) {
		t.Fatalf("queue snapshot = %#v, want %#v", result.QueueSnapshot, wantSnapshot)
	}
}

func TestClusterMessageUpdateCountsOnlyNewExactObligations(t *testing.T) {
	transaction := successfulClusterMessageTransaction(t)
	transaction.execTags = []pgconn.CommandTag{
		pgconn.NewCommandTag("INSERT 0 1"), pgconn.NewCommandTag("INSERT 0 0"),
	}
	transaction.rowResults[len(transaction.rowResults)-1] = capacityObligationQueueSnapshotRow(t, 1, 0, nil)
	update := clusterMessageUpdateWithDemand()
	update.Available = append(update.Available, update.Available[0])

	result, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyMessageUpdate(
		context.Background(), update,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.QueueSnapshot.NewlyQueued != 1 || result.QueueSnapshot.JobAvailable != 1 {
		t.Fatalf("deduplicated exact queue snapshot = %#v", result.QueueSnapshot)
	}
}

func TestClusterMessageUpdateReturnsActuallyCanceledAggregateObligations(t *testing.T) {
	transaction := successfulClusterMessageTransaction(t)
	transaction.execTags = []pgconn.CommandTag{
		pgconn.NewCommandTag("INSERT 0 1"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("UPDATE 0"),
		pgconn.NewCommandTag("DELETE 2"),
	}
	transaction.rowResults[2] = queuedObligationCountRow(t, 2)
	transaction.rowResults[len(transaction.rowResults)-1] = capacityObligationQueueSnapshotRow(t, 0, 0, nil)

	result, err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).ApplyMessageUpdate(
		context.Background(), validClusterMessageUpdate(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.AssignedObligations.Cancel != 2 || result.QueueSnapshot.Canceled != 2 {
		t.Fatalf("aggregate cancellation result = %#v", result)
	}
}

func capacityObligationQueueSnapshotRow(
	t *testing.T,
	jobAvailable int,
	assignedCount int,
	oldestRunnableAt *time.Time,
) fakeRow {
	t.Helper()
	return fakeRow{scan: func(destinations ...any) {
		*destination[int](t, destinations[0]) = jobAvailable
		*destination[int](t, destinations[1]) = assignedCount
		*destination[*time.Time](t, destinations[2]) = oldestRunnableAt
	}}
}

func assertClusterMessageRollback(
	t *testing.T,
	update scaler.MessageUpdate,
	executionFail int,
	wantStatement string,
) {
	t.Helper()
	failure := errors.New("failed")
	execErrors := make([]error, executionFail+1)
	execErrors[executionFail] = failure
	transaction := &fakeDatabaseTransaction{
		execErrors: execErrors,
		rowResults: []fakeRow{clusterEpochRow(t, 3)},
	}
	store := storeWithConnection(&fakeDatabaseConnection{transaction: transaction})

	if _, err := store.ApplyMessageUpdate(context.Background(), update); !errors.Is(err, failure) {
		t.Fatalf("message update error = %v, want %v", err, failure)
	}
	if transaction.rollbackCalls != 1 {
		t.Fatalf("rollback calls = %d", transaction.rollbackCalls)
	}
	failedStatement := transaction.executions[len(transaction.executions)-1].statement
	if !strings.Contains(failedStatement, wantStatement) {
		t.Fatalf("failed statement = %q, want %q", failedStatement, wantStatement)
	}
}

func clusterEpochRow(t *testing.T, epoch int64) fakeRow {
	t.Helper()
	return fakeRow{scan: func(destinations ...any) {
		if len(destinations) != 1 {
			t.Fatalf("epoch scan destinations = %d", len(destinations))
		}
		destination, ok := destinations[0].(*int64)
		if !ok {
			t.Fatalf("epoch scan destination = %T", destinations[0])
		}
		*destination = epoch
	}}
}

func validClusterMessageUpdate() scaler.MessageUpdate {
	return scaler.MessageUpdate{
		AppID: 7, Repository: "repo", ScaleSet: "linux", ControllerEpoch: 3,
		ObservedAt: time.Unix(100, 0).UTC(),
	}
}

func clusterMessageUpdateWithDemand() scaler.MessageUpdate {
	update := validClusterMessageUpdate()
	update.Available = []scaler.JobDemand{{Repository: "repo", RequestID: 42, QueueTime: update.ObservedAt}}
	return update
}

func clusterMessageUpdateWithStart() scaler.MessageUpdate {
	update := validClusterMessageUpdate()
	update.Started = []scaler.RunnerMessageIdentity{{RunnerIdentity: scaler.RunnerIdentity{RequestID: 42, RunnerName: "linux-42"}}}
	return update
}

func clusterMessageUpdateWithCompletion() scaler.MessageUpdate {
	update := validClusterMessageUpdate()
	update.Completed = []scaler.RunnerMessageIdentity{{RunnerIdentity: scaler.RunnerIdentity{RequestID: 42, RunnerName: "linux-42"}}}
	return update
}

func clusterMessageUpdateWithNameOnlyStart(runnerName string) scaler.MessageUpdate {
	update := validClusterMessageUpdate()
	update.Started = []scaler.RunnerMessageIdentity{{RunnerIdentity: scaler.RunnerIdentity{RunnerName: runnerName}}}
	return update
}

func clusterMessageUpdateWithNameOnlyCompletion(runnerName string) scaler.MessageUpdate {
	update := validClusterMessageUpdate()
	update.Completed = []scaler.RunnerMessageIdentity{{RunnerIdentity: scaler.RunnerIdentity{RunnerName: runnerName}}}
	return update
}

func clusterMessageUpdateWithDemandAndStart() scaler.MessageUpdate {
	update := clusterMessageUpdateWithDemand()
	update.Started = []scaler.RunnerMessageIdentity{{RunnerIdentity: scaler.RunnerIdentity{RequestID: 42, RunnerName: "linux-42"}}}
	return update
}
