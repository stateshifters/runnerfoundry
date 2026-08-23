package sqlite

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestQueueClassifiesProcessingAndTerminalSources(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]core.QueueOutcome{
		"processing": core.QueueProcessing, "source terminal": core.QueueSourceTerminal,
	} {
		t.Run(name, func(t *testing.T) {
			commits := 0
			transaction := Transaction{execute: outcomeExecute(0), queryRow: queueClassificationRows(want),
				commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
			result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).Queue(context.Background(), core.JobRequest{
				Source: core.SourceReference{Adapter: "github", ID: "request-3"}, RunnerPool: "linux",
			})
			if err != nil || result.Outcome != want || commits != 1 {
				t.Fatalf("queue = (%#v, %v), commits=%d", result, err, commits)
			}
		})
	}
}

func TestRetryUnstartedClassifiesEveryGuardedNoOp(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		row  Row
		rows int64
		want core.RetryOutcome
	}{
		"missing":                    {row: Row{scan: func(...any) error { return sql.ErrNoRows }}, want: core.RetryMissing},
		"runner started":             {row: rowFromValues(processingOutcomeValues(true, false)), want: core.RetryRunnerStarted},
		"source terminal":            {row: rowFromValues(processingOutcomeValues(false, true)), want: core.RetrySourceTerminal},
		"terminal wins over started": {row: rowFromValues(processingOutcomeValues(true, true)), want: core.RetrySourceTerminal},
		"claim changed":              {row: rowFromValues(processingOutcomeValues(false, false)), rows: 0, want: core.RetryClaimChanged},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			commits := 0
			transaction := Transaction{queryRow: constantRow(test.row), execute: outcomeExecute(test.rows),
				commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
			result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
				RetryUnstarted(context.Background(), testJobID)
			if err != nil || result.Outcome != test.want || commits != 1 {
				t.Fatalf("retry = (%#v, %v), commits=%d", result, err, commits)
			}
		})
	}
}

func TestRecoverUnstartedClassifiesEveryLostGuard(t *testing.T) {
	t.Parallel()
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID}, ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	tests := map[string]struct {
		row  Row
		want core.RetryOutcome
	}{
		"missing":                    {row: Row{scan: func(...any) error { return sql.ErrNoRows }}, want: core.RetryMissing},
		"runner started":             {row: rowFromValues(claimedOutcomeValues(true, false)), want: core.RetryRunnerStarted},
		"source terminal":            {row: rowFromValues(claimedOutcomeValues(false, true)), want: core.RetrySourceTerminal},
		"terminal wins over started": {row: rowFromValues(claimedOutcomeValues(true, true)), want: core.RetrySourceTerminal},
		"claim changed":              {row: rowFromValues(claimedOutcomeValues(false, false)), want: core.RetryClaimChanged},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			commits := 0
			transaction := Transaction{execute: outcomeExecute(0), queryRow: constantRow(test.row),
				commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
			result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
				RecoverUnstartedProcessing(context.Background(), processing)
			if err != nil || result.Outcome != test.want || commits != 1 {
				t.Fatalf("recovery = (%#v, %v), commits=%d", result, err, commits)
			}
		})
	}
}

func TestRemoveTerminalClassifiesMissingAndNonTerminalOwnership(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		exists bool
		want   core.TerminalRemovalOutcome
	}{"missing": {want: core.TerminalMissing}, "not terminal": {exists: true, want: core.TerminalNotTerminal}} {
		t.Run(name, func(t *testing.T) {
			commits := 0
			transaction := Transaction{execute: outcomeExecute(0), queryRow: constantRow(rowFromValues([]any{test.exists})),
				commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
			result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
				RemoveTerminal(context.Background(), testJobID)
			if err != nil || result.Outcome != test.want || commits != 1 {
				t.Fatalf("remove = (%#v, %v), commits=%d", result, err, commits)
			}
		})
	}
}

func TestRetryRollsBackWhenProcessingOwnershipWasNotDeleted(t *testing.T) {
	t.Parallel()
	executeCalls, commits := 0, 0
	transaction := Transaction{queryRow: constantRow(rowFromValues(processingOutcomeValues(false, false))),
		execute: func(context.Context, string, ...any) (Result, error) {
			executeCalls++
			rows := int64(1)
			if executeCalls == 2 {
				rows = 0
			}
			return Result{rowsAffected: func() (int64, error) { return rows, nil }}, nil
		}, commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), testJobID); err == nil || commits != 0 {
		t.Fatalf("retry ownership deletion error = %v, commits=%d", err, commits)
	}
}

func TestRetryClassifiesSourceTerminalAfterGuardedRequeueLoses(t *testing.T) {
	t.Parallel()
	rows := []Row{
		rowFromValues(processingOutcomeValues(false, false)),
		rowFromValues(processingOutcomeValues(false, true)),
	}
	transaction := Transaction{
		queryRow: func(context.Context, string, ...any) Row {
			row := rows[0]
			rows = rows[1:]
			return row
		},
		execute: outcomeExecute(0), commit: successfulCompletion, rollback: successfulCompletion,
	}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), testJobID)
	if err != nil || result.Outcome != core.RetrySourceTerminal {
		t.Fatalf("retry = (%#v, %v)", result, err)
	}
}

func TestRecoveryRollsBackWhenProcessingOwnershipWasNotDeleted(t *testing.T) {
	t.Parallel()
	executeCalls, commits := 0, 0
	transaction := Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		executeCalls++
		rows := int64(1)
		if executeCalls == 2 {
			rows = 0
		}
		return Result{rowsAffected: func() (int64, error) { return rows, nil }}, nil
	}, commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID}, ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RecoverUnstartedProcessing(context.Background(), processing); err == nil || commits != 0 {
		t.Fatalf("recovery ownership deletion error = %v, commits=%d", err, commits)
	}
}

func processingOutcomeValues(started, terminal bool) []any {
	values := processingJobValues()
	values[6], values[7] = started, terminal
	return values
}

func claimedOutcomeValues(started, terminal bool) []any {
	return []any{testJobIDText, "github", "request-3", "linux", string(testMemberID),
		string(core.AttemptRunnerName(testJobID, core.JobID{})), fixedTime().UnixNano(),
		"docker:member-a", started, terminal}
}

func outcomeExecute(rows int64) func(context.Context, string, ...any) (Result, error) {
	return func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return rows, nil }}, nil
	}
}

func queueClassificationRows(outcome core.QueueOutcome) func(context.Context, string, ...any) Row {
	rows := []Row{{scan: func(...any) error { return sql.ErrNoRows }}, rowFromValues([]any{string(outcome)})}
	return func(context.Context, string, ...any) Row {
		row := rows[0]
		rows = rows[1:]
		return row
	}
}
