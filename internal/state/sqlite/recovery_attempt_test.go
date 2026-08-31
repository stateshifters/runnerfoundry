package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestRecoverUnstartedProcessingRequestsGuardedTransition(t *testing.T) {
	statements := make([]string, 0, 2)
	arguments := make([][]any, 0, 2)
	transaction := Transaction{execute: func(_ context.Context, statement string, supplied ...any) (Result, error) {
		statements = append(statements, statement)
		arguments = append(arguments, supplied)
		return successfulResult(), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID, RunnerPool: "linux", EnqueuedAt: fixedTime().Add(time.Minute)},
		RunnerName: core.AttemptRunnerName(testJobID, core.AttemptID{1}),
		ClaimedAt:  fixedTime(), InventoryEndpointID: "docker:member-a"}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RecoverUnstartedProcessing(context.Background(), processing)
	if err != nil || result.Outcome != core.RetryRequeued || len(statements) != 2 ||
		!strings.Contains(statements[0], "claimed_at = ?") || !strings.Contains(statements[0], "runner_started = 0") ||
		!strings.Contains(statements[0], "source_tombstones") || !strings.Contains(statements[0], "runner_name = ?") ||
		!strings.Contains(statements[1], "runner_name = ?") || len(arguments) != 2 || len(arguments[0]) != 5 ||
		len(arguments[1]) != 4 || arguments[0][4] != processing.RunnerName || arguments[1][3] != processing.RunnerName {
		t.Fatalf("recovery = (%#v, %v), statements=%#v arguments=%#v", result, err, statements, arguments)
	}
}

func TestRetryUnstartedProcessingRejectsReplacementClaim(t *testing.T) {
	stale := core.ProcessingJob{Job: core.Job{ID: testJobID, RunnerPool: "linux", EnqueuedAt: fixedTime().Add(time.Minute)},
		RunnerName: core.AttemptRunnerName(testJobID, core.AttemptID{1}),
		ClaimedAt:  fixedTime(), InventoryEndpointID: "docker:member-a"}
	replacement := stale
	replacement.RunnerName = core.AttemptRunnerName(testJobID, core.AttemptID{2})
	replacement.ClaimedAt = stale.ClaimedAt.Add(time.Minute)
	replacement.InventoryEndpointID = "docker:member-b"
	transaction := Transaction{
		execute: func(context.Context, string, ...any) (Result, error) {
			t.Fatal("stale retry mutated replacement claim")
			return Result{}, nil
		},
		queryRow: constantRow(rowFromValues(sqliteClaimedValues(replacement, false, false))),
		commit:   successfulCompletion, rollback: successfulCompletion,
	}

	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), stale)
	if err != nil || result.Outcome != core.RetryClaimChanged {
		t.Fatalf("stale retry = (%#v, %v)", result, err)
	}
}

func TestRecoverUnstartedProcessingClassifiesReplacementBeforeLifecycle(t *testing.T) {
	stale := retryProcessingJob()
	replacement := stale
	replacement.RunnerName = core.AttemptRunnerName(testJobID, core.AttemptID{2})
	replacement.ClaimedAt = stale.ClaimedAt.Add(time.Minute)
	replacement.InventoryEndpointID = "docker:member-b"
	for name, state := range map[string]sqliteReplacementLifecycle{
		"accepted":  {accepted: true},
		"terminal":  {terminal: true},
		"reporting": {failure: core.ProvisioningFailureReporting},
		"reported":  {failure: core.ProvisioningFailureReported},
	} {
		t.Run(name, func(t *testing.T) {
			assertSQLiteReplacementRecovery(t, stale, replacement, state)
		})
	}
}

type sqliteReplacementLifecycle struct {
	accepted bool
	terminal bool
	failure  core.ProvisioningFailureState
}

func assertSQLiteReplacementRecovery(
	t *testing.T,
	stale core.ProcessingJob,
	replacement core.ProcessingJob,
	state sqliteReplacementLifecycle,
) {
	t.Helper()
	values := sqliteClaimedValues(replacement, state.accepted, state.terminal)
	values[10] = string(state.failure)
	executeCalls := 0
	transaction := Transaction{
		execute: func(ctx context.Context, statement string, arguments ...any) (Result, error) {
			executeCalls++
			return outcomeExecute(0)(ctx, statement, arguments...)
		},
		queryRow: constantRow(rowFromValues(values)),
		commit:   successfulCompletion, rollback: successfulCompletion,
	}

	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RecoverUnstartedProcessing(context.Background(), stale)
	if err != nil || result.Outcome != core.RetryClaimChanged || executeCalls != 1 {
		t.Fatalf("recovery = (%#v, %v), execute calls=%d", result, err, executeCalls)
	}
}
