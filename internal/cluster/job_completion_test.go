package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestClusterJobCompletionReconcilesInvalidRequestByRunnerNameOnly(t *testing.T) {
	transaction := &fakeDatabaseTransaction{}
	connection := &fakeDatabaseConnection{transaction: transaction}
	completion := JobCompletion{
		ScaleSet: "linux", Repository: "repo", RunnerIdentity: scaler.RunnerIdentity{RunnerName: "linux-legacy"},
	}

	if err := storeWithConnection(connection).CompleteJob(context.Background(), completion); err != nil {
		t.Fatalf("expected runner-name reconciliation, got %v", err)
	}
	if len(transaction.executions) != 4 {
		t.Fatalf("invalid completion statements = %d, want lifecycle and assignment statements", len(transaction.executions))
	}
	for _, execution := range transaction.executions[:2] {
		if len(execution.arguments) != 3 || execution.arguments[2] != "linux-legacy" {
			t.Fatalf("invalid completion identity arguments = %#v", execution.arguments)
		}
	}
	assignmentCompletion := transaction.executions[2]
	if !strings.Contains(assignmentCompletion.statement, "scaler_assignments") ||
		strings.Contains(assignmentCompletion.statement, "DELETE FROM scaler_demand") {
		t.Fatalf("identity-free completion statement = %s", assignmentCompletion.statement)
	}
	if !strings.Contains(transaction.executions[3].statement, "source = 'assigned_count'") {
		t.Fatalf("identity-free obligation completion statement = %s", transaction.executions[3].statement)
	}
}

func TestClusterStoreBridgesGenericRunnerLifecycleCompletion(t *testing.T) {
	var completionStore scaler.RunnerJobCompletionStore = storeWithConnection(&fakeDatabaseConnection{})
	err := completionStore.CompleteRunnerLifecycleJob(context.Background(), scaler.JobCompletion{
		ScaleSet: "linux", Repository: "repo",
		RunnerIdentity: scaler.RunnerIdentity{RequestID: 42, RunnerName: "linux-42"},
	}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterJobCompletionPreservesPositiveRequestIdentity(t *testing.T) {
	transaction := &fakeDatabaseTransaction{}
	connection := &fakeDatabaseConnection{transaction: transaction}
	completion := JobCompletion{
		ScaleSet: "linux", Repository: "repo",
		RunnerIdentity: scaler.RunnerIdentity{RequestID: 42, RunnerName: "different-runner"},
	}

	if err := storeWithConnection(connection).CompleteJob(context.Background(), completion); err != nil {
		t.Fatalf("expected positive completion, got %v", err)
	}
	if len(transaction.executions) != 6 {
		t.Fatalf("positive completion statements = %d, want lifecycle, assignment, and demand updates", len(transaction.executions))
	}
	if !strings.Contains(transaction.executions[2].statement, "demand.source = 'assigned_count'") ||
		!strings.Contains(transaction.executions[4].statement, "scaler_assignments") ||
		!strings.Contains(transaction.executions[5].statement, "DELETE FROM scaler_demand") {
		t.Fatalf("positive completion did not retain assignment and demand updates: %#v", transaction.executions)
	}
}

func TestClusterJobCompletionKeepsInvalidReconciliationFailuresRetryable(t *testing.T) {
	failure := errors.New("database unavailable")
	transaction := &fakeDatabaseTransaction{execErrors: []error{nil, failure}}
	connection := &fakeDatabaseConnection{transaction: transaction}

	err := storeWithConnection(connection).CompleteJob(context.Background(), JobCompletion{
		ScaleSet: "linux", Repository: "repo", RunnerIdentity: scaler.RunnerIdentity{RunnerName: "linux-legacy"},
	})
	if !errors.Is(err, failure) {
		t.Fatalf("expected reconciliation failure, got %v", err)
	}
	if transaction.rollbackCalls != 1 {
		t.Fatalf("rollback calls = %d, want 1", transaction.rollbackCalls)
	}
}

func TestClusterJobCompletionReportsObservedAssignmentFailure(t *testing.T) {
	failure := errors.New("database unavailable")
	transaction := &fakeDatabaseTransaction{execErrors: []error{nil, nil, failure}}
	err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).CompleteJob(
		context.Background(), JobCompletion{ScaleSet: "linux", Repository: "repo",
			RunnerIdentity: scaler.RunnerIdentity{RunnerName: "runner"}},
	)
	if !errors.Is(err, failure) {
		t.Fatalf("observed assignment completion error = %v", err)
	}
	if transaction.rollbackCalls != 1 {
		t.Fatalf("rollback calls = %d, want 1", transaction.rollbackCalls)
	}
}

func TestClusterJobCompletionReportsPositiveDemandFailure(t *testing.T) {
	failure := errors.New("database unavailable")
	transaction := &fakeDatabaseTransaction{execErrors: []error{nil, nil, nil, nil, failure}}
	err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).CompleteJob(
		context.Background(), JobCompletion{ScaleSet: "linux", Repository: "repo",
			RunnerIdentity: scaler.RunnerIdentity{RequestID: 42}},
	)
	if !errors.Is(err, failure) {
		t.Fatalf("positive demand completion error = %v", err)
	}
}

func TestClusterJobCompletionReportsAggregateObligationFailure(t *testing.T) {
	failure := errors.New("database unavailable")
	transaction := &fakeDatabaseTransaction{execErrors: []error{nil, nil, nil, failure}}
	err := storeWithConnection(&fakeDatabaseConnection{transaction: transaction}).CompleteJob(
		context.Background(), JobCompletion{ScaleSet: "linux", Repository: "repo",
			RunnerIdentity: scaler.RunnerIdentity{RunnerName: "runner"}},
	)
	if !errors.Is(err, failure) {
		t.Fatalf("aggregate obligation completion error = %v", err)
	}
}
