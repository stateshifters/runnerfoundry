package sqlite

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestConditionalTerminalObservationTombstonesExactUnacceptedProcessing(t *testing.T) {
	identity := core.JobIdentity{JobID: testJobID, Source: core.SourceReference{Adapter: "github", ID: "aggregate"}}
	executeCalls := 0
	transaction := transactionStub{
		queryRow: constantRow(rowFromValues([]any{"github", "aggregate"})),
		execute: func(context.Context, string, ...any) (resultStub, error) {
			executeCalls++
			rows := int64(0)
			if executeCalls == 1 {
				rows = 1
			}
			return resultStub{rowsAffected: func() (int64, error) { return rows, nil }}, nil
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}

	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		ObserveTerminalIfUnaccepted(context.Background(), identity)

	if err != nil || result.Outcome != core.ConditionalTerminalApplied || executeCalls != 2 {
		t.Fatalf("conditional terminal = (%#v, %v), writes = %d", result, err, executeCalls)
	}
}

func TestConditionalTerminalObservationClassifiesProtectedReplayAndMissing(t *testing.T) {
	identity := core.JobIdentity{JobID: testJobID, Source: core.SourceReference{Adapter: "github", ID: "aggregate"}}
	for name, test := range map[string]struct {
		rows []rowStub
		want core.ConditionalTerminalOutcome
	}{
		"accepted or reserved": {rows: []rowStub{
			rowFromValues([]any{"github", "aggregate"}), rowFromValues([]any{"protected"}),
		}, want: core.ConditionalTerminalProtected},
		"tombstone replay": {rows: []rowStub{
			rowFromValues([]any{"github", "aggregate"}), rowFromValues([]any{"already_applied"}),
		}, want: core.ConditionalTerminalAlreadyApplied},
		"missing": {rows: []rowStub{{scan: func(...any) error { return sql.ErrNoRows }}},
			want: core.ConditionalTerminalMissing},
	} {
		t.Run(name, func(t *testing.T) {
			assertConditionalTerminalOutcome(t, identity, test.rows, test.want)
		})
	}
}

func assertConditionalTerminalOutcome(
	t *testing.T,
	identity core.JobIdentity,
	rows []rowStub,
	want core.ConditionalTerminalOutcome,
) {
	t.Helper()
	transaction := transactionStub{
		queryRow: rowsInOrder(rows...), execute: outcomeExecute(0),
		commit: successfulCompletion, rollback: successfulCompletion,
	}
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		ObserveTerminalIfUnaccepted(context.Background(), identity)
	if err != nil || result.Outcome != want {
		t.Fatalf("conditional terminal = (%#v, %v), want %q", result, err, want)
	}
}

func TestQueueClassifiesProcessingAndTerminalSources(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]core.QueueOutcome{
		"processing": core.QueueProcessing, "source terminal": core.QueueSourceTerminal,
	} {
		t.Run(name, func(t *testing.T) {
			commits := 0
			transaction := transactionStub{execute: outcomeExecute(0), queryRow: queueClassificationRows(want),
				commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
			result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).Queue(context.Background(), core.JobRequest{
				Source: core.SourceReference{Adapter: "github", ID: "request-3"}, RunnerPool: "linux", EnqueuedAt: fixedTime(),
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
		row  rowStub
		rows int64
		want core.RetryOutcome
	}{
		"missing":                     {row: rowStub{scan: func(...any) error { return sql.ErrNoRows }}, want: core.RetryMissing},
		"runner accepted":             {row: rowFromValues(processingOutcomeValues(true, false)), want: core.RetryRunnerAccepted},
		"source terminal":             {row: rowFromValues(processingOutcomeValues(false, true)), want: core.RetrySourceTerminal},
		"terminal wins over accepted": {row: rowFromValues(processingOutcomeValues(true, true)), want: core.RetrySourceTerminal},
		"claim changed":               {row: rowFromValues(processingOutcomeValues(false, false)), rows: 0, want: core.RetryClaimChanged},
		"failure reporting pending": {row: rowFromValues(processingFailureStateValues(
			core.ProvisioningFailureReporting)), want: core.RetryFailureReportingPending},
		"failure finalization pending": {row: rowFromValues(processingFailureStateValues(
			core.ProvisioningFailureReported)), want: core.RetryFailureFinalizationPending},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			commits := 0
			transaction := transactionStub{queryRow: constantRow(test.row), execute: outcomeExecute(test.rows),
				commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
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
	processing := retryProcessingJob()
	processing.InventoryEndpointID = "docker:member-a"
	tests := map[string]struct {
		row  rowStub
		want core.RetryOutcome
	}{
		"missing":                     {row: rowStub{scan: func(...any) error { return sql.ErrNoRows }}, want: core.RetryMissing},
		"runner accepted":             {row: rowFromValues(claimedOutcomeValues(true, false)), want: core.RetryRunnerAccepted},
		"source terminal":             {row: rowFromValues(claimedOutcomeValues(false, true)), want: core.RetrySourceTerminal},
		"terminal wins over accepted": {row: rowFromValues(claimedOutcomeValues(true, true)), want: core.RetrySourceTerminal},
		"claim changed":               {row: rowFromValues(claimedOutcomeValues(false, false)), want: core.RetryClaimChanged},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			commits := 0
			transaction := transactionStub{execute: outcomeExecute(0), queryRow: constantRow(test.row),
				commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
			result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
				RecoverUnstartedProcessing(context.Background(), processing)
			if err != nil || result.Outcome != test.want || commits != 1 {
				t.Fatalf("recovery = (%#v, %v), commits=%d", result, err, commits)
			}
		})
	}
}

func TestRecoverUnstartedPreservesProvisioningFailurePhase(t *testing.T) {
	t.Parallel()
	processing := retryProcessingJob()
	processing.InventoryEndpointID = "docker:member-a"
	for name, test := range map[string]struct {
		state core.ProvisioningFailureState
		want  core.RetryOutcome
	}{
		"reporting": {state: core.ProvisioningFailureReporting, want: core.RetryFailureReportingPending},
		"reported":  {state: core.ProvisioningFailureReported, want: core.RetryFailureFinalizationPending},
	} {
		t.Run(name, func(t *testing.T) {
			commits := 0
			transaction := transactionStub{execute: outcomeExecute(0), queryRow: constantRow(rowFromValues(claimedFailureStateValues(test.state))),
				commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
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
			transaction := transactionStub{execute: outcomeExecute(0), queryRow: constantRow(rowFromValues([]any{test.exists})),
				commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
			result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
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
	transaction := transactionStub{queryRow: constantRow(rowFromValues(processingOutcomeValues(false, false))),
		execute: func(context.Context, string, ...any) (resultStub, error) {
			executeCalls++
			rows := int64(1)
			if executeCalls == 2 {
				rows = 0
			}
			return resultStub{rowsAffected: func() (int64, error) { return rows, nil }}, nil
		}, commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), retryProcessingJob()); err == nil || commits != 0 {
		t.Fatalf("retry ownership deletion error = %v, commits=%d", err, commits)
	}
}

func TestRetryClassifiesSourceTerminalAfterGuardedRequeueLoses(t *testing.T) {
	t.Parallel()
	rows := []rowStub{
		rowFromValues(processingOutcomeValues(false, false)),
		rowFromValues(processingOutcomeValues(false, true)),
	}
	transaction := transactionStub{
		queryRow: func(context.Context, string, ...any) rowStub {
			row := rows[0]
			rows = rows[1:]
			return row
		},
		execute: outcomeExecute(0), commit: successfulCompletion, rollback: successfulCompletion,
	}
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RetryUnstarted(context.Background(), retryProcessingJob())
	if err != nil || result.Outcome != core.RetrySourceTerminal {
		t.Fatalf("retry = (%#v, %v)", result, err)
	}
}

func TestRecoveryRollsBackWhenProcessingOwnershipWasNotDeleted(t *testing.T) {
	t.Parallel()
	executeCalls, commits := 0, 0
	transaction := transactionStub{execute: func(context.Context, string, ...any) (resultStub, error) {
		executeCalls++
		rows := int64(1)
		if executeCalls == 2 {
			rows = 0
		}
		return resultStub{rowsAffected: func() (int64, error) { return rows, nil }}, nil
	}, commit: func(context.Context) error { commits++; return nil }, rollback: successfulCompletion}
	processing := core.ProcessingJob{Job: core.Job{ID: testJobID}, ClaimedAt: fixedTime(), InventoryEndpointID: "docker:member-a"}
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		RecoverUnstartedProcessing(context.Background(), processing); err == nil || commits != 0 {
		t.Fatalf("recovery ownership deletion error = %v, commits=%d", err, commits)
	}
}

func processingOutcomeValues(accepted, terminal bool) []any {
	values := claimedProcessingJobValues()
	values[8], values[9] = accepted, terminal
	return values
}

func claimedOutcomeValues(accepted, terminal bool) []any {
	return []any{testJobIDText, "github", "request-3", "linux", string(testMemberID),
		string(core.AttemptRunnerName(testJobID, core.AttemptID{1})), fixedTime().UnixNano(),
		"docker:member-a", accepted, terminal, ""}
}

func processingFailureStateValues(status core.ProvisioningFailureState) []any {
	values := processingOutcomeValues(false, false)
	values[10] = string(status)
	return values
}

func claimedFailureStateValues(status core.ProvisioningFailureState) []any {
	values := claimedOutcomeValues(false, false)
	values[10] = string(status)
	return values
}

func outcomeExecute(rows int64) func(context.Context, string, ...any) (resultStub, error) {
	return func(context.Context, string, ...any) (resultStub, error) {
		return resultStub{rowsAffected: func() (int64, error) { return rows, nil }}, nil
	}
}

func queueClassificationRows(outcome core.QueueOutcome) func(context.Context, string, ...any) rowStub {
	rows := []rowStub{rowStub{scan: func(...any) error { return sql.ErrNoRows }}, rowFromValues([]any{string(outcome)})}
	return func(context.Context, string, ...any) rowStub {
		row := rows[0]
		rows = rows[1:]
		return row
	}
}
