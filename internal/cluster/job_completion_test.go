package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestClusterJobCompletionReconcilesInvalidRequestByRunnerNameOnly(t *testing.T) {
	transaction := &fakeDatabaseTransaction{}
	connection := &fakeDatabaseConnection{transaction: transaction}
	completion := JobCompletion{
		ScaleSet: "linux", Repository: "repo", RequestID: 0, RunnerName: "linux-legacy",
	}

	if err := storeWithConnection(connection).CompleteJob(context.Background(), completion); err != nil {
		t.Fatalf("expected runner-name reconciliation, got %v", err)
	}
	if len(transaction.executions) != 2 {
		t.Fatalf("invalid completion statements = %d, want lifecycle statements only", len(transaction.executions))
	}
	for _, execution := range transaction.executions {
		if strings.Contains(execution.statement, "scaler_assignments") || strings.Contains(execution.statement, "DELETE FROM scaler_demand") {
			t.Fatalf("invalid completion mutated assignment or demand state: %s", execution.statement)
		}
		if len(execution.arguments) != 4 || execution.arguments[2] != int64(0) || execution.arguments[3] != "linux-legacy" {
			t.Fatalf("invalid completion identity arguments = %#v", execution.arguments)
		}
	}
}

func TestClusterJobCompletionPreservesPositiveRequestIdentity(t *testing.T) {
	transaction := &fakeDatabaseTransaction{}
	connection := &fakeDatabaseConnection{transaction: transaction}
	completion := JobCompletion{
		ScaleSet: "linux", Repository: "repo", RequestID: 42, RunnerName: "different-runner",
	}

	if err := storeWithConnection(connection).CompleteJob(context.Background(), completion); err != nil {
		t.Fatalf("expected positive completion, got %v", err)
	}
	if len(transaction.executions) != 4 {
		t.Fatalf("positive completion statements = %d, want lifecycle, assignment, and demand updates", len(transaction.executions))
	}
	if !strings.Contains(transaction.executions[2].statement, "scaler_assignments") ||
		!strings.Contains(transaction.executions[3].statement, "DELETE FROM scaler_demand") {
		t.Fatalf("positive completion did not retain assignment and demand updates: %#v", transaction.executions)
	}
}

func TestClusterJobCompletionKeepsInvalidReconciliationFailuresRetryable(t *testing.T) {
	failure := errors.New("database unavailable")
	transaction := &fakeDatabaseTransaction{execErrors: []error{nil, failure}}
	connection := &fakeDatabaseConnection{transaction: transaction}

	err := storeWithConnection(connection).CompleteJob(context.Background(), JobCompletion{
		ScaleSet: "linux", Repository: "repo", RunnerName: "linux-legacy",
	})
	if !errors.Is(err, failure) {
		t.Fatalf("expected reconciliation failure, got %v", err)
	}
	if transaction.rollbackCalls != 1 {
		t.Fatalf("rollback calls = %d, want 1", transaction.rollbackCalls)
	}
}
