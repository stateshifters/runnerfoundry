package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestProvisioningFailureTransitionsUseExactAttemptReservation(t *testing.T) {
	processing := postgresProvisioningFailureProcessing()
	reservedValues := postgresClaimedOutcomeValues(processing, false, false)
	reservedValues[10] = string(core.ProvisioningFailureReporting)
	protocol := &recordedTransaction{rows: []rowStub{rowFromValues(reservedValues)}}

	result, err := newStore(protocol.executor()).ReserveProvisioningFailure(context.Background(), processing)
	if err != nil || result.Outcome != core.RetryFailureReportingPending || protocol.commitCalls != 1 {
		t.Fatalf("reserve = (%#v, %v), commits=%d", result, err, protocol.commitCalls)
	}
	if len(protocol.arguments) != 4 || protocol.arguments[1][3] != processing.RunnerName ||
		protocol.arguments[2][3] != processing.RunnerName {
		t.Fatalf("reservation arguments = %#v", protocol.arguments)
	}
}

func TestProvisioningFailureReservationClassifiesConcurrentLifecycleState(t *testing.T) {
	processing := postgresProvisioningFailureProcessing()
	for name, testCase := range postgresReservationClassificationCases(processing) {
		t.Run(name, func(t *testing.T) {
			protocol := &recordedTransaction{rows: []rowStub{testCase.row}}
			result, err := newStore(protocol.executor()).ReserveProvisioningFailure(context.Background(), processing)
			if err != nil || result.Outcome != testCase.want {
				t.Fatalf("reserve = (%#v, %v)", result, err)
			}
		})
	}
}

func postgresReservationClassificationCases(processing core.ProcessingJob) map[string]struct {
	row  rowStub
	want core.RetryOutcome
} {
	return map[string]struct {
		row  rowStub
		want core.RetryOutcome
	}{
		"missing": {row: rowStub{scan: func(...any) error { return sql.ErrNoRows }}, want: core.RetryMissing},
		"claim changed": {row: rowFromValues(postgresProvisioningFailureValues(
			processing, false, false, "", "another-runner")), want: core.RetryClaimChanged},
		"reported": {row: rowFromValues(postgresProvisioningFailureValues(
			processing, false, false, core.ProvisioningFailureReported, processing.RunnerName)),
			want: core.RetryFailureFinalizationPending},
		"terminal": {row: rowFromValues(postgresProvisioningFailureValues(
			processing, false, true, core.ProvisioningFailureReporting, processing.RunnerName)),
			want: core.RetrySourceTerminal},
		"accepted": {row: rowFromValues(postgresProvisioningFailureValues(
			processing, true, false, "", processing.RunnerName)), want: core.RetryRunnerAccepted},
		"reporting": {row: rowFromValues(postgresProvisioningFailureValues(
			processing, false, false, core.ProvisioningFailureReporting, processing.RunnerName)),
			want: core.RetryFailureReportingPending},
		"unreserved": {row: rowFromValues(postgresProvisioningFailureValues(
			processing, false, false, "", processing.RunnerName)), want: core.RetryClaimChanged},
	}
}

func TestProvisioningFailureTransitionsRollbackProtocolFailures(t *testing.T) {
	wantErr := errors.New("protocol")
	processing := postgresProvisioningFailureProcessing()
	operations := map[string]func(*Store) error{
		"reserve": func(store *Store) error {
			_, err := store.ReserveProvisioningFailure(context.Background(), processing)
			return err
		},
		"record": func(store *Store) error {
			_, err := store.RecordProvisioningFailureReported(context.Background(), processing)
			return err
		},
	}
	for name, operation := range operations {
		for _, failure := range []postgresProvisioningFailureProtocol{
			{beginErr: wantErr},
			{failExecuteAt: 1, executeErr: wantErr},
			{failExecuteAt: 2, executeErr: wantErr},
			{failExecuteAt: 3, executeErr: wantErr},
			{rows: []rowStub{rowStub{scan: func(...any) error { return wantErr }}}},
			{commitErr: wantErr},
		} {
			t.Run(name, func(t *testing.T) {
				protocol := failure
				if err := operation(newStore(protocol.executor())); err == nil {
					t.Fatal("expected transition failure")
				}
			})
		}
	}
}

func TestProvisioningFailureFinalizationReportsProtocolFailures(t *testing.T) {
	wantErr := errors.New("protocol")
	processing := postgresProvisioningFailureProcessing()
	for _, failure := range []postgresProvisioningFailureProtocol{
		{beginErr: wantErr},
		{failExecuteAt: 1, executeErr: wantErr},
		{failExecuteAt: 2, executeErr: wantErr},
		{failExecuteAt: 3, executeErr: wantErr},
		{affectedErr: wantErr},
		{affected: []int64{1, 1, 0}, rows: []rowStub{rowStub{scan: func(...any) error { return wantErr }}}, commitErr: wantErr},
		{commitErr: wantErr},
	} {
		protocol := failure
		if _, err := newStore(protocol.executor()).FinalizeProvisioningFailure(
			context.Background(), processing,
		); err == nil {
			t.Fatal("expected finalization failure")
		}
	}
}

func TestProvisioningFailureReportAndFinalizationRemainSeparateTransactions(t *testing.T) {
	processing := postgresProvisioningFailureProcessing()
	reportedValues := postgresClaimedOutcomeValues(processing, false, false)
	reportedValues[10] = string(core.ProvisioningFailureReported)
	recordProtocol := &recordedTransaction{rows: []rowStub{rowFromValues(reportedValues)}}

	recorded, err := newStore(recordProtocol.executor()).RecordProvisioningFailureReported(
		context.Background(), processing,
	)
	if err != nil || recorded.Outcome != core.RetryFailureFinalizationPending || recordProtocol.commitCalls != 1 {
		t.Fatalf("record = (%#v, %v), commits=%d", recorded, err, recordProtocol.commitCalls)
	}
	if len(recordProtocol.arguments) != 4 || recordProtocol.arguments[1][1] != processing.RunnerName ||
		recordProtocol.arguments[2][3] != processing.RunnerName {
		t.Fatalf("report-recording arguments = %#v", recordProtocol.arguments)
	}

	finalizeProtocol := &recordedTransaction{}
	finalized, err := newStore(finalizeProtocol.executor()).FinalizeProvisioningFailure(
		context.Background(), processing,
	)
	if err != nil || finalized.Outcome != core.TerminalRemoved || finalizeProtocol.commitCalls != 1 {
		t.Fatalf("finalize = (%#v, %v), commits=%d", finalized, err, finalizeProtocol.commitCalls)
	}
	if len(finalizeProtocol.arguments) != 3 || finalizeProtocol.arguments[1][1] != processing.RunnerName ||
		finalizeProtocol.arguments[2][3] != processing.RunnerName {
		t.Fatalf("finalization arguments = %#v", finalizeProtocol.arguments)
	}
}

func TestProvisioningFailureFinalizationClassifiesReplayAndPendingState(t *testing.T) {
	processing := postgresProvisioningFailureProcessing()
	for name, testCase := range map[string]struct {
		exists bool
		want   core.TerminalRemovalOutcome
	}{
		"already removed": {want: core.TerminalMissing},
		"report pending":  {exists: true, want: core.TerminalNotTerminal},
	} {
		t.Run(name, func(t *testing.T) {
			protocol := &postgresProvisioningFailureProtocol{
				affected: []int64{1, 1, 0}, rows: []rowStub{rowFromValues([]any{testCase.exists})},
			}
			result, err := newStore(protocol.executor()).FinalizeProvisioningFailure(
				context.Background(), processing,
			)
			if err != nil || result.Outcome != testCase.want {
				t.Fatalf("finalize = (%#v, %v)", result, err)
			}
		})
	}
}

func postgresProvisioningFailureProcessing() core.ProcessingJob {
	job := core.Job{ID: testJobID1, Source: core.SourceReference{Adapter: "test", ID: "request"}, RunnerPool: "linux"}
	return core.ProcessingJob{Job: job, RunnerName: core.AttemptRunnerName(job.ID, core.AttemptID{1}), MemberID: testMemberIDA}
}

func postgresProvisioningFailureValues(
	processing core.ProcessingJob,
	accepted bool,
	terminal bool,
	status core.ProvisioningFailureState,
	runnerName core.RunnerName,
) []any {
	values := postgresClaimedOutcomeValues(processing, accepted, terminal)
	values[5], values[10] = string(runnerName), string(status)
	return values
}

type postgresProvisioningFailureProtocol struct {
	rows          []rowStub
	affected      []int64
	beginErr      error
	executeErr    error
	failExecuteAt int
	affectedErr   error
	commitErr     error
	executeCalls  int
}

func (protocol *postgresProvisioningFailureProtocol) executor() executorStub {
	return executorStub{begin: protocol.begin}
}

func (protocol *postgresProvisioningFailureProtocol) begin(context.Context) (transactionStub, error) {
	if protocol.beginErr != nil {
		return transactionStub{}, protocol.beginErr
	}
	return transactionStub{
		execute:  protocol.execute,
		queryRow: protocol.queryRow,
		commit:   func(context.Context) error { return protocol.commitErr },
		rollback: func(context.Context) error { return nil },
	}, nil
}

func (protocol *postgresProvisioningFailureProtocol) execute(
	context.Context,
	string,
	...any,
) (resultStub, error) {
	protocol.executeCalls++
	if protocol.executeCalls == protocol.failExecuteAt {
		return resultStub{}, protocol.executeErr
	}
	rows := int64(1)
	if len(protocol.affected) > 0 {
		rows = protocol.affected[0]
		protocol.affected = protocol.affected[1:]
	}
	return resultStub{rowsAffected: func() (int64, error) { return rows, protocol.affectedErr }}, nil
}

func (protocol *postgresProvisioningFailureProtocol) queryRow(context.Context, string, ...any) rowStub {
	if len(protocol.rows) == 0 {
		return rowStub{scan: func(...any) error { return sql.ErrNoRows }}
	}
	row := protocol.rows[0]
	protocol.rows = protocol.rows[1:]
	return row
}
