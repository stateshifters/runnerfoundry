package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestProvisioningFailureTransitionsPreserveDurablePhases(t *testing.T) {
	processing := sqliteProvisioningFailureProcessing()
	assertSQLiteProvisioningFailureTransition(t, core.ProvisioningFailureReporting, false,
		func(store *Store) (core.RetryResult, error) {
			return store.ReserveProvisioningFailure(context.Background(), processing)
		}, core.RetryFailureReportingPending)
	assertSQLiteProvisioningFailureTransition(t, core.ProvisioningFailureReported, false,
		func(store *Store) (core.RetryResult, error) {
			return store.RecordProvisioningFailureReported(context.Background(), processing)
		}, core.RetryFailureFinalizationPending)
	assertSQLiteProvisioningFailureTransition(t, "", true,
		func(store *Store) (core.RetryResult, error) {
			return store.ReserveProvisioningFailure(context.Background(), processing)
		}, core.RetrySourceTerminal)
}

func TestProvisioningFailureReservationClassifiesConcurrentLifecycleState(t *testing.T) {
	processing := sqliteProvisioningFailureProcessing()
	for name, testCase := range sqliteReservationClassificationCases(processing) {
		t.Run(name, func(t *testing.T) {
			protocol := &sqliteProvisioningProtocol{rows: []rowStub{testCase.row}}
			result, err := newStore(protocol.executor()).ReserveProvisioningFailure(context.Background(), processing)
			if err != nil || result.Outcome != testCase.want {
				t.Fatalf("reserve = (%#v, %v)", result, err)
			}
		})
	}
}

func assertSQLiteProvisioningFailureTransition(
	t *testing.T,
	status core.ProvisioningFailureState,
	terminal bool,
	operation func(*Store) (core.RetryResult, error),
	want core.RetryOutcome,
) {
	t.Helper()
	values := processingJobValues()
	values[6], values[7], values[8] = false, terminal, string(status)
	protocol := &sqliteProvisioningProtocol{rows: []rowStub{rowFromValues(values)}}
	result, err := operation(newStore(protocol.executor()))
	if err != nil || result.Outcome != want || protocol.commits != 1 {
		t.Fatalf("transition = (%#v, %v), commits=%d", result, err, protocol.commits)
	}
}

func sqliteReservationClassificationCases(processing core.ProcessingJob) map[string]struct {
	row  rowStub
	want core.RetryOutcome
} {
	return map[string]struct {
		row  rowStub
		want core.RetryOutcome
	}{
		"missing": {row: rowStub{scan: func(...any) error { return sql.ErrNoRows }}, want: core.RetryMissing},
		"claim changed": {row: rowFromValues(sqliteProvisioningFailureValues(
			false, false, "", "another-runner")), want: core.RetryClaimChanged},
		"reported": {row: rowFromValues(sqliteProvisioningFailureValues(
			false, false, core.ProvisioningFailureReported, processing.RunnerName)),
			want: core.RetryFailureFinalizationPending},
		"terminal": {row: rowFromValues(sqliteProvisioningFailureValues(
			false, true, core.ProvisioningFailureReporting, processing.RunnerName)), want: core.RetrySourceTerminal},
		"accepted": {row: rowFromValues(sqliteProvisioningFailureValues(
			true, false, "", processing.RunnerName)), want: core.RetryRunnerAccepted},
		"reporting": {row: rowFromValues(sqliteProvisioningFailureValues(
			false, false, core.ProvisioningFailureReporting, processing.RunnerName)),
			want: core.RetryFailureReportingPending},
		"unreserved": {row: rowFromValues(sqliteProvisioningFailureValues(
			false, false, "", processing.RunnerName)), want: core.RetryClaimChanged},
	}
}

func TestProvisioningFailureTransitionsRollbackProtocolFailures(t *testing.T) {
	wantErr := errors.New("protocol")
	processing := sqliteProvisioningFailureProcessing()
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
		for _, failure := range []sqliteProvisioningProtocol{
			{beginErr: wantErr},
			{failExecuteAt: 1, executeErr: wantErr},
			{failExecuteAt: 2, executeErr: wantErr},
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
	processing := sqliteProvisioningFailureProcessing()
	for _, failure := range []sqliteProvisioningProtocol{
		{beginErr: wantErr},
		{failExecuteAt: 1, executeErr: wantErr},
		{failExecuteAt: 2, executeErr: wantErr},
		{affectedErr: wantErr},
		{affected: []int64{1, 0}, rows: []rowStub{rowStub{scan: func(...any) error { return wantErr }}}},
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

func TestProvisioningFailureFinalizationIsIdempotent(t *testing.T) {
	processing := sqliteProvisioningFailureProcessing()
	removed := &sqliteProvisioningProtocol{}
	result, err := newStore(removed.executor()).FinalizeProvisioningFailure(context.Background(), processing)
	if err != nil || result.Outcome != core.TerminalRemoved || removed.commits != 1 {
		t.Fatalf("finalize = (%#v, %v), commits=%d", result, err, removed.commits)
	}

	missing := &sqliteProvisioningProtocol{affected: []int64{1, 0}, rows: []rowStub{rowFromValues([]any{false})}}
	result, err = newStore(missing.executor()).FinalizeProvisioningFailure(context.Background(), processing)
	if err != nil || result.Outcome != core.TerminalMissing || missing.commits != 1 {
		t.Fatalf("replayed finalize = (%#v, %v), commits=%d", result, err, missing.commits)
	}
}

type sqliteProvisioningProtocol struct {
	rows          []rowStub
	affected      []int64
	commits       int
	beginErr      error
	executeErr    error
	failExecuteAt int
	affectedErr   error
	commitErr     error
	executeCalls  int
}

func (protocol *sqliteProvisioningProtocol) executor() executorStub {
	if protocol.beginErr != nil {
		return executorStub{begin: failingBegin(protocol.beginErr)}
	}
	transaction := transactionStub{
		execute: func(context.Context, string, ...any) (resultStub, error) {
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
		},
		queryRow: func(context.Context, string, ...any) rowStub {
			if len(protocol.rows) == 0 {
				return rowStub{scan: func(...any) error { return sql.ErrNoRows }}
			}
			row := protocol.rows[0]
			protocol.rows = protocol.rows[1:]
			return row
		},
		commit: func(context.Context) error {
			protocol.commits++
			return protocol.commitErr
		},
		rollback: successfulCompletion,
	}
	return executorStub{begin: transactionBeginning(transaction)}
}

func sqliteProvisioningFailureValues(
	accepted bool,
	terminal bool,
	status core.ProvisioningFailureState,
	runnerName core.RunnerName,
) []any {
	values := processingJobValues()
	values[5], values[6], values[7], values[8] = string(runnerName), accepted, terminal, string(status)
	return values
}

func sqliteProvisioningFailureProcessing() core.ProcessingJob {
	job := core.Job{ID: testJobID, Source: core.SourceReference{Adapter: "github", ID: "request-3"}, RunnerPool: "linux"}
	return core.ProcessingJob{Job: job, RunnerName: core.AttemptRunnerName(job.ID, core.AttemptID{1}), MemberID: testMemberID}
}
