package postgres

import (
	"context"
	"database/sql"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestQueueClassifiesProcessingAndTerminalSources(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]core.QueueOutcome{
		"processing": core.QueueProcessing, "source terminal": core.QueueSourceTerminal,
	} {
		t.Run(name, func(t *testing.T) {
			commits := 0
			rows := []Row{noPostgresRow(), noPostgresRow(), rowFromValues([]any{string(want)})}
			transaction := postgresOutcomeTransaction(rows, 1, &commits)
			result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).Queue(context.Background(), core.JobRequest{
				Source: core.SourceReference{Adapter: "github", ID: "request-1"}, RunnerPool: "linux",
			})
			if err != nil || result.Outcome != want || commits != 1 {
				t.Fatalf("queue = (%#v, %v), commits=%d", result, err, commits)
			}
		})
	}
}

func TestRetryUnstartedClassifiesEveryGuardedNoOp(t *testing.T) {
	t.Parallel()
	job := recoveryProcessing().Job
	assertPostgresRetryOutcomes(t, map[string]struct {
		rows []Row
		want core.RetryOutcome
	}{
		"missing":        {rows: []Row{noPostgresRow()}, want: core.RetryMissing},
		"runner started": {rows: []Row{rowFromValues(postgresProcessingOutcomeValues(job, true, false))}, want: core.RetryRunnerStarted},
		"source terminal": {rows: []Row{rowFromValues(postgresProcessingOutcomeValues(job, false, true))},
			want: core.RetrySourceTerminal},
		"terminal wins over started": {rows: []Row{rowFromValues(postgresProcessingOutcomeValues(job, true, true))}, want: core.RetrySourceTerminal},
	})
}

func TestRetryUnstartedReclassifiesAfterSerialization(t *testing.T) {
	t.Parallel()
	job := recoveryProcessing().Job
	assertPostgresRetryOutcomes(t, map[string]struct {
		rows []Row
		want core.RetryOutcome
	}{
		"claim changed": {rows: []Row{rowFromValues(postgresProcessingOutcomeValues(job, false, false)), noPostgresRow(),
			rowFromValues(postgresProcessingOutcomeValues(job, false, false))}, want: core.RetryClaimChanged},
		"source terminal after serialization": {rows: []Row{rowFromValues(postgresProcessingOutcomeValues(job, false, false)), noPostgresRow(),
			rowFromValues(postgresProcessingOutcomeValues(job, false, true))}, want: core.RetrySourceTerminal},
		"missing after serialization": {rows: []Row{rowFromValues(postgresProcessingOutcomeValues(job, false, false)), noPostgresRow(),
			noPostgresRow()}, want: core.RetryMissing},
		"runner started after serialization": {rows: []Row{rowFromValues(postgresProcessingOutcomeValues(job, false, false)), noPostgresRow(),
			rowFromValues(postgresProcessingOutcomeValues(job, true, false))}, want: core.RetryRunnerStarted},
	})
}

func assertPostgresRetryOutcomes(t *testing.T, tests map[string]struct {
	rows []Row
	want core.RetryOutcome
}) {
	t.Helper()
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			commits := 0
			transaction := postgresOutcomeTransaction(test.rows, 1, &commits)
			result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
				RetryUnstarted(context.Background(), testJobID1)
			if err != nil || result.Outcome != test.want || commits != 1 {
				t.Fatalf("retry = (%#v, %v), commits=%d", result, err, commits)
			}
		})
	}
}

func TestRecoverUnstartedClassifiesEveryLostGuard(t *testing.T) {
	t.Parallel()
	processing := recoveryProcessing()
	tests := map[string]struct {
		classification Row
		want           core.RetryOutcome
	}{
		"missing":                    {classification: noPostgresRow(), want: core.RetryMissing},
		"runner started":             {classification: rowFromValues(postgresClaimedOutcomeValues(processing, true, false)), want: core.RetryRunnerStarted},
		"source terminal":            {classification: rowFromValues(postgresClaimedOutcomeValues(processing, false, true)), want: core.RetrySourceTerminal},
		"terminal wins over started": {classification: rowFromValues(postgresClaimedOutcomeValues(processing, true, true)), want: core.RetrySourceTerminal},
		"claim changed":              {classification: rowFromValues(postgresClaimedOutcomeValues(processing, false, false)), want: core.RetryClaimChanged},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			commits := 0
			transaction := postgresOutcomeTransaction([]Row{noPostgresRow(), test.classification}, 1, &commits)
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
			transaction := postgresOutcomeTransaction([]Row{rowFromValues([]any{test.exists})}, 0, &commits)
			result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
				RemoveTerminal(context.Background(), testJobID1)
			if err != nil || result.Outcome != test.want || commits != 1 {
				t.Fatalf("remove = (%#v, %v), commits=%d", result, err, commits)
			}
		})
	}
}

func TestRetryRollsBackWhenProcessingOwnershipWasNotDeleted(t *testing.T) {
	t.Parallel()
	job := recoveryProcessing().Job
	rows := []Row{rowFromValues(postgresProcessingOutcomeValues(job, false, false)),
		rowFromValues(queuedJobValues(testJobID1, recoveryRequest()))}
	executeCalls, commits := 0, 0
	transaction := postgresOutcomeTransaction(rows, 1, &commits)
	baseExecute := transaction.execute
	transaction.execute = func(ctx context.Context, statement string, arguments ...any) (Result, error) {
		executeCalls++
		if executeCalls == 2 {
			return Result{rowsAffected: func() (int64, error) { return 0, nil }}, nil
		}
		return baseExecute(ctx, statement, arguments...)
	}
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), testJobID1); err == nil || commits != 0 {
		t.Fatalf("retry ownership deletion error = %v, commits=%d", err, commits)
	}
}

func TestRecoveryRollsBackWhenProcessingOwnershipWasNotDeleted(t *testing.T) {
	t.Parallel()
	processing := recoveryProcessing()
	commits := 0
	transaction := postgresOutcomeTransaction(
		[]Row{rowFromValues(queuedJobValues(processing.Job.ID, recoveryRequest()))}, 0, &commits,
	)
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		RecoverUnstartedProcessing(context.Background(), processing); err == nil || commits != 0 {
		t.Fatalf("recovery ownership deletion error = %v, commits=%d", err, commits)
	}
}

func postgresOutcomeTransaction(rows []Row, affected int64, commits *int) Transaction {
	return Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return affected, nil }}, nil
	}, queryRow: func(context.Context, string, ...any) Row {
		row := rows[0]
		rows = rows[1:]
		return row
	}, commit: func(context.Context) error { *commits++; return nil }, rollback: successfulCompletion}
}

func postgresProcessingOutcomeValues(job core.Job, started, terminal bool) []any {
	values := processingJobValues(job, testMemberIDA)
	values[5], values[6] = started, terminal
	return values
}

func postgresClaimedOutcomeValues(processing core.ProcessingJob, started, terminal bool) []any {
	return []any{processing.Job.ID, string(processing.Job.Source.Adapter), string(processing.Job.Source.ID),
		string(processing.Job.RunnerPool), testMemberIDA, processing.ClaimedAt, processing.InventoryEndpointID, started, terminal}
}

func noPostgresRow() Row { return Row{scan: func(...any) error { return sql.ErrNoRows }} }
