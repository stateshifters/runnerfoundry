package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

func TestClusterMessageUpdateRollsBackStatisticsWithPriorEffects(t *testing.T) {
	assertClusterMessageRollback(
		t, clusterMessageUpdateWithDemandAndStart(), 3, "INSERT INTO scaler_assigned_job_observations",
	)
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
	update.Started = []scaler.RunnerMessageIdentity{{RequestID: 42, RunnerName: "linux-42"}}
	return update
}

func clusterMessageUpdateWithCompletion() scaler.MessageUpdate {
	update := validClusterMessageUpdate()
	update.Completed = []scaler.RunnerMessageIdentity{{RequestID: 42, RunnerName: "linux-42"}}
	return update
}

func clusterMessageUpdateWithDemandAndStart() scaler.MessageUpdate {
	update := clusterMessageUpdateWithDemand()
	update.Started = []scaler.RunnerMessageIdentity{{RequestID: 42, RunnerName: "linux-42"}}
	return update
}
