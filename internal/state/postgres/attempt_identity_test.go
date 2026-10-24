package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestObserveRunnerAcceptanceGuardsTheExactAttempt(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	attemptID := core.AttemptID{1}
	protocol := &recordedTransaction{rows: []rowStub{
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
	}}
	result, err := newStore(protocol.executor()).ObserveRunnerAcceptance(context.Background(), core.JobIdentity{
		Source: source, JobID: testJobID1, AttemptID: attemptID,
	})
	if err != nil || result.Outcome != core.ObservationApplied {
		t.Fatalf("result=%#v statements=%#v error=%v", result, protocol.statements, err)
	}
	wantRunnerName := core.AttemptRunnerName(testJobID1, attemptID)
	if !strings.Contains(protocol.statements[3], "runner_name") || len(protocol.arguments[3]) < 3 ||
		protocol.arguments[3][2] != wantRunnerName {
		t.Fatalf("acceptance statement=%q arguments=%#v", protocol.statements[3], protocol.arguments[3])
	}
}

func TestObserveRunnerAcceptanceForAnotherAttemptIsUnmatched(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	commits := 0
	statements := make([]string, 0, 2)
	transaction := recordingPostgresOutcomeTransaction([]rowStub{
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
		rowFromValues([]any{false}),
	}, 0, &commits, &statements)
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).ObserveRunnerAcceptance(
		context.Background(), core.JobIdentity{Source: source, JobID: testJobID1, AttemptID: core.AttemptID{2}},
	)
	if err != nil || result.Outcome != core.ObservationUnmatched || commits != 1 {
		t.Fatalf("result=%#v commits=%d error=%v", result, commits, err)
	}
	assertNoPostgresQueuedLifecycleMutations(t, statements)
}

func TestTerminalObservationForExactAttemptTerminalizesSource(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	attemptID := core.AttemptID{2}
	protocol := &recordedTransaction{rows: []rowStub{
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
		rowFromValues([]any{string(source.Adapter), string(source.ID)}),
		rowFromValues([]any{true}),
	}}
	result, err := newStore(protocol.executor()).Terminalize(context.Background(), core.JobIdentity{
		Source: source, JobID: testJobID1, AttemptID: attemptID,
	})
	if err != nil || result.Outcome != core.ObservationApplied || protocol.commitCalls != 1 {
		t.Fatalf("result=%#v statements=%#v error=%v", result, protocol.statements, err)
	}
	wantRunnerName := core.AttemptRunnerName(testJobID1, attemptID)
	if len(protocol.arguments) < 4 || len(protocol.arguments[3]) < 3 || protocol.arguments[3][2] != wantRunnerName {
		t.Fatalf("terminal attempt arguments=%#v", protocol.arguments)
	}
}

func TestTerminalObservationForNoncurrentAttemptDoesNotMutateQueuedState(t *testing.T) {
	t.Parallel()
	source := core.SourceReference{Adapter: "github", ID: "request-1"}
	durableSource := rowFromValues([]any{string(source.Adapter), string(source.ID)})
	missingSource := rowStub{scan: func(...any) error { return sql.ErrNoRows }}
	for name, test := range map[string]struct {
		tombstoned bool
		outcome    core.ObservationOutcome
		sourceRow  rowStub
	}{
		"another attempt with queued retry": {outcome: core.ObservationUnmatched, sourceRow: durableSource},
		"completed attempt replay": {tombstoned: true, outcome: core.ObservationAlreadyApplied,
			sourceRow: missingSource},
	} {
		t.Run(name, func(t *testing.T) {
			assertNoncurrentTerminalAttempt(t, source, test.sourceRow, test.tombstoned, test.outcome)
		})
	}
}

func assertNoncurrentTerminalAttempt(
	t *testing.T,
	source core.SourceReference,
	sourceRow rowStub,
	tombstoned bool,
	want core.ObservationOutcome,
) {
	t.Helper()
	protocol := &recordedTransaction{rows: []rowStub{
		sourceRow,
		sourceRow,
		rowFromValues([]any{false}),
		rowFromValues([]any{tombstoned}),
	}}
	result, err := newStore(protocol.executor()).Terminalize(context.Background(), core.JobIdentity{
		Source: source, JobID: testJobID1, AttemptID: core.AttemptID{2},
	})
	if err != nil || result.Outcome != want || protocol.commitCalls != 1 {
		t.Fatalf("result=%#v statements=%#v error=%v", result, protocol.statements, err)
	}
	assertNoPostgresQueuedLifecycleMutations(t, protocol.statements)
}

func TestAttemptQualifiedTerminalObservationReportsClassificationFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	sourceRows := []rowStub{
		rowFromValues([]any{"github", "request-1"}),
		rowFromValues([]any{"github", "request-1"}),
	}
	failingRow := rowStub{scan: func(...any) error { return protocolErr }}
	for name, classifications := range map[string][]rowStub{
		"attempt lookup":   {failingRow},
		"tombstone lookup": {rowFromValues([]any{false}), failingRow},
	} {
		t.Run(name, func(t *testing.T) {
			rows := append(append([]rowStub{}, sourceRows...), classifications...)
			transaction := transactionStub{
				execute: successfulExecute,
				queryRow: func(context.Context, string, ...any) rowStub {
					row := rows[0]
					rows = rows[1:]
					return row
				},
				rollback: successfulCompletion,
			}
			if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).Terminalize(
				context.Background(), core.JobIdentity{JobID: testJobID1, AttemptID: core.AttemptID{2}},
			); !errors.Is(err, protocolErr) {
				t.Fatalf("Terminalize() error = %v, want %v", err, protocolErr)
			}
		})
	}
}

func recordingPostgresOutcomeTransaction(
	rows []rowStub,
	affected int64,
	commits *int,
	statements *[]string,
) transactionStub {
	return transactionStub{
		execute: func(_ context.Context, statement string, _ ...any) (resultStub, error) {
			*statements = append(*statements, statement)
			return resultStub{rowsAffected: func() (int64, error) { return affected, nil }}, nil
		},
		queryRow: func(context.Context, string, ...any) rowStub {
			row := rows[0]
			rows = rows[1:]
			return row
		},
		commit: func(context.Context) error { *commits++; return nil }, rollback: successfulCompletion,
	}
}

func assertNoPostgresQueuedLifecycleMutations(t *testing.T, statements []string) {
	t.Helper()
	for _, statement := range statements {
		if strings.Contains(statement, "INSERT INTO public.runnerfoundry_source_tombstones") ||
			strings.Contains(statement, "DELETE FROM public.runnerfoundry_queued_jobs") {
			t.Fatalf("attempt-qualified observation mutated queued state: %q", statement)
		}
	}
}
