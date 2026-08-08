package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
)

var sqliteLifecycleSource = core.SourceReference{Adapter: "github", ID: "request-3"}

func TestTerminalObservationByJobIDUsesTheDurableSource(t *testing.T) {
	t.Parallel()
	statements, arguments := []string{}, [][]any{}
	transaction := recordingLifecycleTransaction(&statements, &arguments)
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).Terminalize(
		context.Background(), core.JobIdentity{JobID: testJobID},
	)
	assertObservationOutcome(t, result, err, core.ObservationApplied)
	assertTerminalLifecycleStatements(t, statements)
	assertResolvedLifecycleSource(t, arguments[1])
}

func recordingLifecycleTransaction(statements *[]string, arguments *[][]any) Transaction {
	return Transaction{
		queryRow: func(_ context.Context, statement string, supplied ...any) Row {
			*statements = append(*statements, statement)
			*arguments = append(*arguments, supplied)
			return rowFromValues([]any{"github", "request-3"})
		},
		execute: func(_ context.Context, statement string, supplied ...any) (Result, error) {
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
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).MarkRunnerStarted(
		context.Background(), identity,
	)
	if !errors.Is(err, core.ErrJobIdentityMismatch) || result.Outcome != "" {
		t.Fatalf("mismatched observation = %#v, %v", result, err)
	}
	if executions != 0 || commits != 0 || rollbacks != 1 {
		t.Fatalf("mismatched effects: executions=%d commits=%d rollbacks=%d", executions, commits, rollbacks)
	}
}

func mismatchedLifecycleTransaction(executions, commits, rollbacks *int) Transaction {
	return Transaction{
		queryRow: func(context.Context, string, ...any) Row {
			return rowFromValues([]any{"github", "request-3"})
		},
		execute: func(context.Context, string, ...any) (Result, error) {
			*executions++
			return successfulResult(), nil
		},
		commit:   func(context.Context) error { *commits++; return nil },
		rollback: func(context.Context) error { *rollbacks++; return nil },
	}
}

func TestLifecycleObservationsByMissingJobIDAreUnmatched(t *testing.T) {
	t.Parallel()
	assertMissingJobObservation(t, "started", func(store *Store) (core.ObservationResult, error) {
		return store.MarkRunnerStarted(context.Background(), core.JobIdentity{JobID: testJobID})
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
		result, err := observe(NewStore(Executor{begin: transactionBeginning(transaction)}))
		assertObservationOutcome(t, result, err, core.ObservationUnmatched)
	})
}

func missingLifecycleJobTransaction(commit func(context.Context) error) Transaction {
	return Transaction{
		queryRow: func(context.Context, string, ...any) Row {
			return Row{scan: func(...any) error { return sql.ErrNoRows }}
		},
		execute: func(context.Context, string, ...any) (Result, error) {
			return Result{}, errors.New("unmatched job observation mutated state")
		},
		commit: commit, rollback: successfulCompletion,
	}
}

func TestRunnerStartedObservationByJobIDUsesTheDurableSource(t *testing.T) {
	t.Parallel()
	mutationArguments := [][]any{}
	transaction := startedLifecycleTransaction(&mutationArguments)
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).MarkRunnerStarted(
		context.Background(), core.JobIdentity{JobID: testJobID},
	)
	assertObservationOutcome(t, result, err, core.ObservationApplied)
	assertResolvedLifecycleSource(t, mutationArguments[0])
}

func startedLifecycleTransaction(arguments *[][]any) Transaction {
	return Transaction{
		queryRow: func(context.Context, string, ...any) Row {
			return rowFromValues([]any{"github", "request-3"})
		},
		execute: func(_ context.Context, _ string, supplied ...any) (Result, error) {
			*arguments = append(*arguments, supplied)
			return successfulResult(), nil
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
}

func TestRunnerStartedObservationDistinguishesReplayFromMissingState(t *testing.T) {
	t.Parallel()
	assertUnchangedStartedOutcome(t, "missing", false, core.ObservationUnmatched)
	assertUnchangedStartedOutcome(t, "terminal replay", true, core.ObservationAlreadyApplied)
}

func assertUnchangedStartedOutcome(
	t *testing.T,
	name string,
	tombstoned bool,
	want core.ObservationOutcome,
) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		transaction := unchangedStartedTransaction(rowFromValues([]any{tombstoned}), successfulCompletion)
		result, err := observeStartedSource(transaction)
		assertObservationOutcome(t, result, err, want)
	})
}

func unchangedStartedTransaction(classification Row, commit func(context.Context) error) Transaction {
	zeroResult := Result{rowsAffected: func() (int64, error) { return 0, nil }}
	return Transaction{
		execute:  func(context.Context, string, ...any) (Result, error) { return zeroResult, nil },
		queryRow: func(context.Context, string, ...any) Row { return classification },
		commit:   commit, rollback: successfulCompletion,
	}
}

func TestTerminalObservationRejectsAnInvalidSelector(t *testing.T) {
	t.Parallel()
	assertTerminalObservationFails(t, Transaction{rollback: successfulCompletion}, core.JobIdentity{})
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
	transaction := Transaction{execute: failedRowsAffected(protocolErr), rollback: successfulCompletion}
	assertTerminalObservationFails(t, transaction, core.JobIdentity{Source: sqliteLifecycleSource})
}

func TestTerminalObservationClassifiesExistingTombstoneAsAlreadyApplied(t *testing.T) {
	t.Parallel()
	transaction := Transaction{execute: zeroRowsAffected, commit: successfulCompletion, rollback: successfulCompletion}
	result, err := NewStore(Executor{begin: transactionBeginning(transaction)}).Terminalize(
		context.Background(), core.JobIdentity{Source: sqliteLifecycleSource},
	)
	assertObservationOutcome(t, result, err, core.ObservationAlreadyApplied)
}

func assertTerminalObservationFails(t *testing.T, transaction Transaction, identity core.JobIdentity) {
	t.Helper()
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		Terminalize(context.Background(), identity); err == nil {
		t.Fatal("expected terminal observation failure")
	}
}

func TestRunnerStartedObservationReportsMutationResultFailures(t *testing.T) {
	t.Parallel()
	assertStartedMutationResultFails(t, "started result", 1)
	assertStartedMutationResultFails(t, "delayed-start tombstone result", 2)
	assertStartedMutationResultFails(t, "queued removal result", 3)
}

func assertStartedMutationResultFails(t *testing.T, name string, failureCall int) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		transaction := startedResultFailureTransaction(failureCall, errors.New("protocol"))
		assertRunnerStartedObservationFails(t, transaction, core.JobIdentity{Source: sqliteLifecycleSource})
	})
}

func startedResultFailureTransaction(failureCall int, protocolErr error) Transaction {
	executeCalls := 0
	return Transaction{
		execute: func(ctx context.Context, statement string, arguments ...any) (Result, error) {
			executeCalls++
			if executeCalls == failureCall {
				return failedRowsAffected(protocolErr)(ctx, statement, arguments...)
			}
			return successfulResult(), nil
		},
		rollback: successfulCompletion,
	}
}

func zeroRowsAffected(context.Context, string, ...any) (Result, error) {
	return Result{rowsAffected: func() (int64, error) { return 0, nil }}, nil
}

func TestRunnerStartedObservationReportsCommitFailure(t *testing.T) {
	t.Parallel()
	transaction := unchangedStartedTransaction(rowFromValues([]any{false}), failingCompletion(errors.New("protocol")))
	assertRunnerStartedObservationFails(t, transaction, core.JobIdentity{Source: sqliteLifecycleSource})
}

func TestRunnerStartedObservationReportsClassificationFailure(t *testing.T) {
	t.Parallel()
	transaction := unchangedStartedTransaction(rowFromError(errors.New("protocol")), successfulCompletion)
	assertRunnerStartedObservationFails(t, transaction, core.JobIdentity{Source: sqliteLifecycleSource})
}

func TestRunnerStartedObservationReportsUnmatchedCommitFailure(t *testing.T) {
	t.Parallel()
	transaction := missingLifecycleJobTransaction(failingCompletion(errors.New("protocol")))
	assertRunnerStartedObservationFails(t, transaction, core.JobIdentity{JobID: testJobID})
}

func assertRunnerStartedObservationFails(t *testing.T, transaction Transaction, identity core.JobIdentity) {
	t.Helper()
	if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		MarkRunnerStarted(context.Background(), identity); err == nil {
		t.Fatal("expected runner-start observation failure")
	}
}

func lifecycleQueryFailureTransaction(protocolErr error) Transaction {
	return Transaction{queryRow: func(context.Context, string, ...any) Row {
		return rowFromError(protocolErr)
	}, rollback: successfulCompletion}
}

func rowFromError(protocolErr error) Row {
	return Row{scan: func(...any) error { return protocolErr }}
}

func failedRowsAffected(protocolErr error) func(context.Context, string, ...any) (Result, error) {
	return func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
	}
}

func observeStartedSource(transaction Transaction) (core.ObservationResult, error) {
	store := NewStore(Executor{begin: transactionBeginning(transaction)})
	return store.MarkRunnerStarted(context.Background(), core.JobIdentity{Source: sqliteLifecycleSource})
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
