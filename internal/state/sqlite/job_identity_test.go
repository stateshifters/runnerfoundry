package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

var sqliteLifecycleSource = core.SourceReference{Adapter: "github", ID: "request-3"}

func TestTerminalObservationByJobIDUsesTheDurableSource(t *testing.T) {
	t.Parallel()
	statements, arguments := []string{}, [][]any{}
	transaction := recordingLifecycleTransaction(&statements, &arguments)
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).Terminalize(
		context.Background(), core.JobIdentity{JobID: testJobID},
	)
	assertObservationOutcome(t, result, err, core.ObservationApplied)
	assertTerminalLifecycleStatements(t, statements)
	assertResolvedLifecycleSource(t, arguments[1])
}

func recordingLifecycleTransaction(statements *[]string, arguments *[][]any) transactionStub {
	return transactionStub{
		queryRow: func(_ context.Context, statement string, supplied ...any) rowStub {
			*statements = append(*statements, statement)
			*arguments = append(*arguments, supplied)
			return rowFromValues([]any{"github", "request-3"})
		},
		execute: func(_ context.Context, statement string, supplied ...any) (resultStub, error) {
			*statements = append(*statements, statement)
			*arguments = append(*arguments, supplied)
			return successfulResult(), nil
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
}

func assertTerminalLifecycleStatements(t *testing.T, statements []string) {
	t.Helper()
	if len(statements) != 3 || !strings.Contains(statements[0], "job_id") ||
		!strings.Contains(statements[1], "source_tombstones") || !strings.Contains(statements[2], "queued_jobs") {
		t.Fatalf("statements = %#v", statements)
	}
}

func assertResolvedLifecycleSource(t *testing.T, arguments []any) {
	t.Helper()
	if len(arguments) < 2 || arguments[0] != core.SourceAdapter("github") ||
		arguments[1] != core.SourceIdentifier("request-3") {
		t.Fatalf("resolved source arguments = %#v", arguments)
	}
}

func TestLifecycleObservationRejectsMismatchedSourceAndJobIDAtomically(t *testing.T) {
	t.Parallel()
	executions, commits, rollbacks := 0, 0, 0
	transaction := mismatchedLifecycleTransaction(&executions, &commits, &rollbacks)
	identity := core.JobIdentity{Source: core.SourceReference{Adapter: "github", ID: "different"}, JobID: testJobID}
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).ObserveRunnerAcceptance(
		context.Background(), identity,
	)
	if !errors.Is(err, core.ErrJobIdentityMismatch) || result.Outcome != "" {
		t.Fatalf("mismatched observation = %#v, %v", result, err)
	}
	if executions != 0 || commits != 0 || rollbacks != 1 {
		t.Fatalf("mismatched effects: executions=%d commits=%d rollbacks=%d", executions, commits, rollbacks)
	}
}

func mismatchedLifecycleTransaction(executions, commits, rollbacks *int) transactionStub {
	return transactionStub{
		queryRow: func(context.Context, string, ...any) rowStub {
			return rowFromValues([]any{"github", "request-3"})
		},
		execute: func(context.Context, string, ...any) (resultStub, error) {
			*executions++
			return successfulResult(), nil
		},
		commit:   func(context.Context) error { *commits++; return nil },
		rollback: func(context.Context) error { *rollbacks++; return nil },
	}
}

func TestLifecycleObservationsByMissingJobIDAreUnmatched(t *testing.T) {
	t.Parallel()
	assertMissingJobObservation(t, "accepted", func(store *Store) (core.ObservationResult, error) {
		return store.ObserveRunnerAcceptance(context.Background(), core.JobIdentity{JobID: testJobID})
	})
	assertMissingJobObservation(t, "terminal", func(store *Store) (core.ObservationResult, error) {
		return store.Terminalize(context.Background(), core.JobIdentity{JobID: testJobID})
	})
}

func assertMissingJobObservation(
	t *testing.T,
	name string,
	observe func(*Store) (core.ObservationResult, error),
) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		transaction := missingLifecycleJobTransaction(successfulCompletion)
		result, err := observe(newStore(executorStub{begin: transactionBeginning(transaction)}))
		assertObservationOutcome(t, result, err, core.ObservationUnmatched)
	})
}

func missingLifecycleJobTransaction(commit func(context.Context) error) transactionStub {
	return transactionStub{
		queryRow: func(context.Context, string, ...any) rowStub {
			return rowStub{scan: func(...any) error { return sql.ErrNoRows }}
		},
		execute: func(context.Context, string, ...any) (resultStub, error) {
			return resultStub{}, errors.New("unmatched job observation mutated state")
		},
		commit: commit, rollback: successfulCompletion,
	}
}

func TestRunnerAcceptedObservationByJobIDUsesTheDurableSource(t *testing.T) {
	t.Parallel()
	mutationArguments := [][]any{}
	transaction := acceptedLifecycleTransaction(&mutationArguments)
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).ObserveRunnerAcceptance(
		context.Background(), core.JobIdentity{JobID: testJobID},
	)
	assertObservationOutcome(t, result, err, core.ObservationApplied)
	assertResolvedLifecycleSource(t, mutationArguments[0])
}

func TestRunnerAcceptedObservationGuardsTheExactAttempt(t *testing.T) {
	t.Parallel()
	attemptID := core.AttemptID{1}
	statements, arguments := []string{}, [][]any{}
	transaction := recordingLifecycleTransaction(&statements, &arguments)
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).ObserveRunnerAcceptance(
		context.Background(), core.JobIdentity{JobID: testJobID, AttemptID: attemptID},
	)
	assertObservationOutcome(t, result, err, core.ObservationApplied)
	wantRunnerName := core.AttemptRunnerName(testJobID, attemptID)
	if !strings.Contains(statements[1], "runner_name") || len(arguments[1]) < 5 || arguments[1][2] != wantRunnerName {
		t.Fatalf("acceptance statement = %q, arguments = %#v", statements[1], arguments[1])
	}
}

func TestRunnerAcceptedObservationForAnotherAttemptIsUnmatched(t *testing.T) {
	t.Parallel()
	statements := make([]string, 0, 1)
	rows := []rowStub{
		rowFromValues([]any{"github", "request-3"}),
		rowFromValues([]any{false}),
		rowFromValues([]any{false}),
	}
	zeroResult := resultStub{rowsAffected: func() (int64, error) { return 0, nil }}
	transaction := transactionStub{
		execute: func(_ context.Context, statement string, _ ...any) (resultStub, error) {
			statements = append(statements, statement)
			return zeroResult, nil
		},
		queryRow: func(context.Context, string, ...any) rowStub {
			row := rows[0]
			rows = rows[1:]
			return row
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).ObserveRunnerAcceptance(
		context.Background(), core.JobIdentity{JobID: testJobID, AttemptID: core.AttemptID{2}},
	)
	assertObservationOutcome(t, result, err, core.ObservationUnmatched)
	if len(statements) != 1 || !strings.Contains(statements[0], "UPDATE runnerfoundry_processing_jobs") ||
		strings.Contains(statements[0], "INSERT INTO runnerfoundry_source_tombstones") ||
		strings.Contains(statements[0], "DELETE FROM runnerfoundry_queued_jobs") {
		t.Fatalf("stale attempt mutations = %#v", statements)
	}
}

func TestTerminalObservationForAnotherAttemptDoesNotTerminalizeQueuedRetry(t *testing.T) {
	t.Parallel()
	statements := make([]string, 0)
	rows := []rowStub{
		rowFromValues([]any{"github", "request-3"}),
		rowFromValues([]any{false}),
		rowFromValues([]any{false}),
	}
	transaction := transactionStub{
		queryRow: func(context.Context, string, ...any) rowStub {
			row := rows[0]
			rows = rows[1:]
			return row
		},
		execute: func(_ context.Context, statement string, _ ...any) (resultStub, error) {
			statements = append(statements, statement)
			return successfulResult(), nil
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).Terminalize(
		context.Background(), core.JobIdentity{JobID: testJobID, AttemptID: core.AttemptID{2}},
	)
	assertObservationOutcome(t, result, err, core.ObservationUnmatched)
	if len(statements) != 0 {
		t.Fatalf("stale terminal attempt mutations = %#v", statements)
	}
}

func TestTerminalObservationForExactAttemptTerminalizesSource(t *testing.T) {
	t.Parallel()
	statements := make([]string, 0, 2)
	arguments := make([][]any, 0, 2)
	rows := []rowStub{
		rowFromValues([]any{"github", "request-3"}),
		rowFromValues([]any{true}),
	}
	transaction := transactionStub{
		queryRow: func(context.Context, string, ...any) rowStub {
			row := rows[0]
			rows = rows[1:]
			return row
		},
		execute: func(_ context.Context, statement string, supplied ...any) (resultStub, error) {
			statements = append(statements, statement)
			arguments = append(arguments, supplied)
			return successfulResult(), nil
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
	attemptID := core.AttemptID{2}
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).Terminalize(
		context.Background(), core.JobIdentity{JobID: testJobID, AttemptID: attemptID},
	)
	assertObservationOutcome(t, result, err, core.ObservationApplied)
	if len(statements) != 2 || len(arguments) != 2 {
		t.Fatalf("terminal attempt mutations = %#v %#v", statements, arguments)
	}
}

func TestTerminalObservationForCompletedAttemptIsReplay(t *testing.T) {
	t.Parallel()
	rows := []rowStub{
		rowStub{scan: func(...any) error { return sql.ErrNoRows }},
		rowFromValues([]any{false}),
		rowFromValues([]any{true}),
	}
	transaction := transactionStub{
		queryRow: func(context.Context, string, ...any) rowStub {
			row := rows[0]
			rows = rows[1:]
			return row
		},
		execute: func(context.Context, string, ...any) (resultStub, error) {
			return resultStub{}, errors.New("terminal replay mutated state")
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).Terminalize(
		context.Background(), core.JobIdentity{Source: sqliteLifecycleSource, JobID: testJobID, AttemptID: core.AttemptID{2}},
	)
	assertObservationOutcome(t, result, err, core.ObservationAlreadyApplied)
}

func TestAttemptQualifiedTerminalObservationReportsClassificationFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for name, rows := range map[string][]rowStub{
		"attempt lookup": {
			rowFromValues([]any{"github", "request-3"}),
			rowFromError(protocolErr),
		},
		"tombstone lookup": {
			rowFromValues([]any{"github", "request-3"}),
			rowFromValues([]any{false}),
			rowFromError(protocolErr),
		},
	} {
		t.Run(name, func(t *testing.T) {
			transaction := transactionStub{
				queryRow: func(context.Context, string, ...any) rowStub {
					row := rows[0]
					rows = rows[1:]
					return row
				},
				execute: func(context.Context, string, ...any) (resultStub, error) {
					return resultStub{}, errors.New("classification failure mutated state")
				},
				rollback: successfulCompletion,
			}
			if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).Terminalize(
				context.Background(), core.JobIdentity{JobID: testJobID, AttemptID: core.AttemptID{2}},
			); !errors.Is(err, protocolErr) {
				t.Fatalf("Terminalize() error = %v, want %v", err, protocolErr)
			}
		})
	}
}

func acceptedLifecycleTransaction(arguments *[][]any) transactionStub {
	return transactionStub{
		queryRow: func(context.Context, string, ...any) rowStub {
			return rowFromValues([]any{"github", "request-3"})
		},
		execute: func(_ context.Context, _ string, supplied ...any) (resultStub, error) {
			*arguments = append(*arguments, supplied)
			return successfulResult(), nil
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
}

func TestRunnerAcceptedObservationDistinguishesReplayFromMissingState(t *testing.T) {
	t.Parallel()
	assertUnchangedAcceptanceOutcome(t, "missing", []rowStub{
		rowFromValues([]any{false}), rowFromValues([]any{false}),
	}, core.ObservationUnmatched)
	assertUnchangedAcceptanceOutcome(t, "terminal replay", []rowStub{
		rowFromValues([]any{true}),
	}, core.ObservationAlreadyApplied)
	assertUnchangedAcceptanceOutcome(t, "accepted processing replay", []rowStub{
		rowFromValues([]any{false}), rowFromValues([]any{true}),
	}, core.ObservationAlreadyApplied)
}

func assertUnchangedAcceptanceOutcome(
	t *testing.T,
	name string,
	classifications []rowStub,
	want core.ObservationOutcome,
) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		transaction := unchangedAcceptanceTransaction(classifications, successfulCompletion)
		result, err := observeAcceptedSource(transaction)
		assertObservationOutcome(t, result, err, want)
	})
}

func unchangedAcceptanceTransaction(classifications []rowStub, commit func(context.Context) error) transactionStub {
	zeroResult := resultStub{rowsAffected: func() (int64, error) { return 0, nil }}
	return transactionStub{
		execute: func(context.Context, string, ...any) (resultStub, error) { return zeroResult, nil },
		queryRow: func(context.Context, string, ...any) rowStub {
			classification := classifications[0]
			classifications = classifications[1:]
			return classification
		},
		commit: commit, rollback: successfulCompletion,
	}
}

func TestTerminalObservationRejectsAnInvalidSelector(t *testing.T) {
	t.Parallel()
	assertTerminalObservationFails(t, transactionStub{rollback: successfulCompletion}, core.JobIdentity{})
}

func TestTerminalObservationReportsJobLookupFailure(t *testing.T) {
	t.Parallel()
	transaction := lifecycleQueryFailureTransaction(errors.New("protocol"))
	assertTerminalObservationFails(t, transaction, core.JobIdentity{JobID: testJobID})
}

func TestTerminalObservationReportsUnmatchedCommitFailure(t *testing.T) {
	t.Parallel()
	transaction := missingLifecycleJobTransaction(failingCompletion(errors.New("protocol")))
	assertTerminalObservationFails(t, transaction, core.JobIdentity{JobID: testJobID})
}

func TestTerminalObservationReportsTombstoneResultFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	transaction := transactionStub{execute: failedRowsAffected(protocolErr), rollback: successfulCompletion}
	assertTerminalObservationFails(t, transaction, core.JobIdentity{Source: sqliteLifecycleSource})
}

func TestTerminalObservationClassifiesExistingTombstoneAsAlreadyApplied(t *testing.T) {
	t.Parallel()
	transaction := transactionStub{execute: zeroRowsAffected, commit: successfulCompletion, rollback: successfulCompletion}
	result, err := newStore(executorStub{begin: transactionBeginning(transaction)}).Terminalize(
		context.Background(), core.JobIdentity{Source: sqliteLifecycleSource},
	)
	assertObservationOutcome(t, result, err, core.ObservationAlreadyApplied)
}

func assertTerminalObservationFails(t *testing.T, transaction transactionStub, identity core.JobIdentity) {
	t.Helper()
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		Terminalize(context.Background(), identity); err == nil {
		t.Fatal("expected terminal observation failure")
	}
}

func TestRunnerAcceptedObservationReportsMutationResultFailures(t *testing.T) {
	t.Parallel()
	assertAcceptedMutationResultFails(t, "accepted result", 1)
	assertAcceptedMutationResultFails(t, "delayed-acceptance tombstone result", 2)
	assertAcceptedMutationResultFails(t, "queued removal result", 3)
}

func assertAcceptedMutationResultFails(t *testing.T, name string, failureCall int) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		transaction := acceptedResultFailureTransaction(failureCall, errors.New("protocol"))
		assertRunnerAcceptedObservationFails(t, transaction, core.JobIdentity{Source: sqliteLifecycleSource})
	})
}

func acceptedResultFailureTransaction(failureCall int, protocolErr error) transactionStub {
	executeCalls := 0
	return transactionStub{
		execute: func(ctx context.Context, statement string, arguments ...any) (resultStub, error) {
			executeCalls++
			if executeCalls == failureCall {
				return failedRowsAffected(protocolErr)(ctx, statement, arguments...)
			}
			return successfulResult(), nil
		},
		rollback: successfulCompletion,
	}
}

func zeroRowsAffected(context.Context, string, ...any) (resultStub, error) {
	return resultStub{rowsAffected: func() (int64, error) { return 0, nil }}, nil
}

func TestRunnerAcceptedObservationReportsCommitFailure(t *testing.T) {
	t.Parallel()
	transaction := unchangedAcceptanceTransaction(
		[]rowStub{rowFromValues([]any{false}), rowFromValues([]any{false})},
		failingCompletion(errors.New("protocol")),
	)
	assertRunnerAcceptedObservationFails(t, transaction, core.JobIdentity{Source: sqliteLifecycleSource})
}

func TestRunnerAcceptedObservationReportsClassificationFailure(t *testing.T) {
	t.Parallel()
	transaction := unchangedAcceptanceTransaction([]rowStub{rowFromError(errors.New("protocol"))}, successfulCompletion)
	assertRunnerAcceptedObservationFails(t, transaction, core.JobIdentity{Source: sqliteLifecycleSource})
}

func TestRunnerAcceptedObservationReportsAcceptedProcessingLookupFailure(t *testing.T) {
	t.Parallel()
	transaction := unchangedAcceptanceTransaction(
		[]rowStub{rowFromValues([]any{false}), rowFromError(errors.New("protocol"))},
		successfulCompletion,
	)
	assertRunnerAcceptedObservationFails(t, transaction, core.JobIdentity{Source: sqliteLifecycleSource})
}

func TestRunnerAcceptedObservationReportsUnmatchedCommitFailure(t *testing.T) {
	t.Parallel()
	transaction := missingLifecycleJobTransaction(failingCompletion(errors.New("protocol")))
	assertRunnerAcceptedObservationFails(t, transaction, core.JobIdentity{JobID: testJobID})
}

func assertRunnerAcceptedObservationFails(t *testing.T, transaction transactionStub, identity core.JobIdentity) {
	t.Helper()
	if _, err := newStore(executorStub{begin: transactionBeginning(transaction)}).
		ObserveRunnerAcceptance(context.Background(), identity); err == nil {
		t.Fatal("expected runner-acceptance observation failure")
	}
}

func lifecycleQueryFailureTransaction(protocolErr error) transactionStub {
	return transactionStub{queryRow: func(context.Context, string, ...any) rowStub {
		return rowFromError(protocolErr)
	}, rollback: successfulCompletion}
}

func rowFromError(protocolErr error) rowStub {
	return rowStub{scan: func(...any) error { return protocolErr }}
}

func failedRowsAffected(protocolErr error) func(context.Context, string, ...any) (resultStub, error) {
	return func(context.Context, string, ...any) (resultStub, error) {
		return resultStub{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
	}
}

func observeAcceptedSource(transaction transactionStub) (core.ObservationResult, error) {
	store := newStore(executorStub{begin: transactionBeginning(transaction)})
	return store.ObserveRunnerAcceptance(context.Background(), core.JobIdentity{Source: sqliteLifecycleSource})
}

func assertObservationOutcome(
	t *testing.T,
	result core.ObservationResult,
	err error,
	want core.ObservationOutcome,
) {
	t.Helper()
	if err != nil || result.Outcome != want {
		t.Fatalf("observation = %#v, %v; want %q", result, err, want)
	}
}
