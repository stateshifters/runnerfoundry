package postgres

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
			rows := []rowStub{noPostgresRow(), noPostgresRow(), rowFromValues([]any{string(want)})}
			transaction := postgresOutcomeTransaction(rows, 1, &commits)
			result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).Queue(context.Background(), core.JobRequest{
				Source: core.SourceReference{Adapter: "github", ID: "request-1"}, RunnerPool: "linux", EnqueuedAt: fixedTime(),
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
		rows []rowStub
		want core.RetryOutcome
	}{
		"missing":         {rows: []rowStub{noPostgresRow()}, want: core.RetryMissing},
		"runner accepted": {rows: []rowStub{rowFromValues(postgresProcessingOutcomeValues(job, true, false))}, want: core.RetryRunnerAccepted},
		"source terminal": {rows: []rowStub{rowFromValues(postgresProcessingOutcomeValues(job, false, true))},
			want: core.RetrySourceTerminal},
		"terminal wins over accepted": {rows: []rowStub{rowFromValues(postgresProcessingOutcomeValues(job, true, true))}, want: core.RetrySourceTerminal},
		"failure reporting pending": {rows: []rowStub{rowFromValues(postgresProcessingFailureStateValues(
			job, core.ProvisioningFailureReporting))}, want: core.RetryFailureReportingPending},
		"failure finalization pending": {rows: []rowStub{rowFromValues(postgresProcessingFailureStateValues(
			job, core.ProvisioningFailureReported))}, want: core.RetryFailureFinalizationPending},
	})
}

func TestRetryUnstartedReclassifiesAfterSerialization(t *testing.T) {
	t.Parallel()
	job := recoveryProcessing().Job
	assertPostgresRetryOutcomes(t, map[string]struct {
		rows []rowStub
		want core.RetryOutcome
	}{
		"claim changed": {rows: []rowStub{rowFromValues(postgresProcessingOutcomeValues(job, false, false)), noPostgresRow(),
			rowFromValues(postgresProcessingOutcomeValues(job, false, false))}, want: core.RetryClaimChanged},
		"source terminal after serialization": {rows: []rowStub{rowFromValues(postgresProcessingOutcomeValues(job, false, false)), noPostgresRow(),
			rowFromValues(postgresProcessingOutcomeValues(job, false, true))}, want: core.RetrySourceTerminal},
		"missing after serialization": {rows: []rowStub{rowFromValues(postgresProcessingOutcomeValues(job, false, false)), noPostgresRow(),
			noPostgresRow()}, want: core.RetryMissing},
		"runner accepted after serialization": {rows: []rowStub{rowFromValues(postgresProcessingOutcomeValues(job, false, false)), noPostgresRow(),
			rowFromValues(postgresProcessingOutcomeValues(job, true, false))}, want: core.RetryRunnerAccepted},
		"failure reporting after serialization": {rows: []rowStub{rowFromValues(postgresProcessingOutcomeValues(job, false, false)), noPostgresRow(),
			rowFromValues(postgresProcessingFailureStateValues(job, core.ProvisioningFailureReporting))}, want: core.RetryFailureReportingPending},
		"failure finalization after serialization": {rows: []rowStub{rowFromValues(postgresProcessingOutcomeValues(job, false, false)), noPostgresRow(),
			rowFromValues(postgresProcessingFailureStateValues(job, core.ProvisioningFailureReported))}, want: core.RetryFailureFinalizationPending},
	})
}

func assertPostgresRetryOutcomes(t *testing.T, tests map[string]struct {
	rows []rowStub
	want core.RetryOutcome
}) {
	t.Helper()
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			commits := 0
			transaction := postgresOutcomeTransaction(test.rows, 1, &commits)
			result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
				RetryUnstarted(context.Background(), retryProcessingJob())
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
		classification rowStub
		want           core.RetryOutcome
	}{
		"missing":                     {classification: noPostgresRow(), want: core.RetryMissing},
		"runner accepted":             {classification: rowFromValues(postgresClaimedOutcomeValues(processing, true, false)), want: core.RetryRunnerAccepted},
		"source terminal":             {classification: rowFromValues(postgresClaimedOutcomeValues(processing, false, true)), want: core.RetrySourceTerminal},
		"terminal wins over accepted": {classification: rowFromValues(postgresClaimedOutcomeValues(processing, true, true)), want: core.RetrySourceTerminal},
		"claim changed":               {classification: rowFromValues(postgresClaimedOutcomeValues(processing, false, false)), want: core.RetryClaimChanged},
		"failure reporting pending": {classification: rowFromValues(postgresClaimedFailureStateValues(
			processing, core.ProvisioningFailureReporting)), want: core.RetryFailureReportingPending},
		"failure finalization pending": {classification: rowFromValues(postgresClaimedFailureStateValues(
			processing, core.ProvisioningFailureReported)), want: core.RetryFailureFinalizationPending},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			commits := 0
			transaction := postgresOutcomeTransaction([]rowStub{noPostgresRow(), test.classification}, 1, &commits)
			result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
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
			transaction := postgresOutcomeTransaction([]rowStub{rowFromValues([]any{test.exists})}, 0, &commits)
			result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
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
	rows := []rowStub{rowFromValues(postgresProcessingOutcomeValues(job, false, false)),
		rowFromValues(queuedJobValues(testJobID1, recoveryRequest()))}
	executeCalls, commits := 0, 0
	transaction := postgresOutcomeTransaction(rows, 1, &commits)
	baseExecute := transaction.execute
	transaction.execute = func(ctx context.Context, statement string, arguments ...any) (resultStub, error) {
		executeCalls++
		if executeCalls == 2 {
			return resultStub{rowsAffected: func() (int64, error) { return 0, nil }}, nil
		}
		return baseExecute(ctx, statement, arguments...)
	}
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), retryProcessingJob()); err == nil || commits != 0 {
		t.Fatalf("retry ownership deletion error = %v, commits=%d", err, commits)
	}
}

func TestRecoveryRollsBackWhenProcessingOwnershipWasNotDeleted(t *testing.T) {
	t.Parallel()
	processing := recoveryProcessing()
	commits := 0
	transaction := postgresOutcomeTransaction(
		[]rowStub{rowFromValues(queuedJobValues(processing.Job.ID, recoveryRequest()))}, 0, &commits,
	)
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RecoverUnstartedProcessing(context.Background(), processing); err == nil || commits != 0 {
		t.Fatalf("recovery ownership deletion error = %v, commits=%d", err, commits)
	}
}

func postgresOutcomeTransaction(rows []rowStub, affected int64, commits *int) transactionStub {
	return transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
		return resultStub{rowsAffected: func() (int64, error) { return affected, nil }}, nil
	}, queryRow: func(context.Context, string, ...any) rowStub {
		row := rows[0]
		rows = rows[1:]
		return row
	}, commit: func(context.Context) error { *commits++; return nil }, rollback: successfulCompletion}
}

func postgresProcessingOutcomeValues(job core.Job, accepted, terminal bool) []any {
	return postgresClaimedOutcomeValues(retryProcessingJobWith(job), accepted, terminal)
}

func postgresClaimedOutcomeValues(processing core.ProcessingJob, accepted, terminal bool) []any {
	return []any{processing.Job.ID, string(processing.Job.Source.Adapter), string(processing.Job.Source.ID),
		string(processing.Job.RunnerPool), testMemberIDA, string(processing.RunnerName), processing.ClaimedAt,
		processing.InventoryEndpointID, accepted, terminal, ""}
}

func postgresProcessingFailureStateValues(job core.Job, status core.ProvisioningFailureState) []any {
	values := postgresProcessingOutcomeValues(job, false, false)
	values[10] = string(status)
	return values
}

func retryProcessingJob() core.ProcessingJob {
	return retryProcessingJobWith(recoveryProcessing().Job)
}

func retryProcessingJobWith(job core.Job) core.ProcessingJob {
	return core.ProcessingJob{
		Job:                 job,
		MemberID:            testMemberIDA,
		RunnerName:          core.AttemptRunnerName(job.ID, core.AttemptID{1}),
		ClaimedAt:           fixedTime(),
		InventoryEndpointID: "docker:member-a",
	}
}

func postgresClaimedFailureStateValues(
	processing core.ProcessingJob,
	status core.ProvisioningFailureState,
) []any {
	values := postgresClaimedOutcomeValues(processing, false, false)
	values[10] = string(status)
	return values
}

func noPostgresRow() rowStub { return rowStub{scan: func(...any) error { return sql.ErrNoRows }} }
