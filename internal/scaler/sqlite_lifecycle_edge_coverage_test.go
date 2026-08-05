package scaler

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRunnerLifecycleConsistencyErrorDescribesConflictingRunner(t *testing.T) {
	err := &RunnerLifecycleConsistencyError{
		ScaleSet: "linux", Repository: "owner/repo", RunnerName: "linux-42", Reason: "provider conflict",
	}
	const wanted = "runner lifecycle consistency for linux/owner/repo/linux-42: provider conflict"
	if err.Error() != wanted {
		t.Fatalf("Error() = %q, want %q", err.Error(), wanted)
	}
}

func TestSQLiteMirrorReapReportsAffectedRowFailures(t *testing.T) {
	identity := RunnerIdentity{RequestID: 42}
	for name, result := range map[string]sql.Result{
		"unavailable count": failingSQLResult{},
		"duplicate match":   sqlmock.NewResult(0, 2),
	} {
		t.Run(name, func(t *testing.T) {
			fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
			fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnResult(result)
			err := mirrorSQLiteRunnerReapRequest(
				context.Background(), transaction, "linux", "owner/repo", identity,
			)
			if err == nil {
				t.Fatal("expected mirror reap failure")
			}
			rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
		})
	}
}

func TestSQLiteCompletedMirrorRejectsDuplicateMatch(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 2))
	err := mirrorSQLiteCompletedRunner(context.Background(), transaction, JobCompletion{
		ScaleSet: "linux", Repository: "owner/repo", RunnerIdentity: RunnerIdentity{RequestID: 42},
	})
	if !errors.Is(err, ErrRunnerIdentityInvariant) {
		t.Fatalf("completion mirror error = %v, want identity invariant", err)
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteTerminalAuthorityRejectsInvalidIdentity(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	_, err := authoritativeSQLiteTerminalRunner(context.Background(), transaction, RunnerLifecycle{
		ScaleSet: "linux", Repository: "owner/repo",
	})
	if !errors.Is(err, ErrInvalidRunnerIdentity) {
		t.Fatalf("terminal authority error = %v, want invalid identity", err)
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteMirroredTerminalReapReportsDeleteFailure(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	fixture.mock.ExpectExec("DELETE FROM libvirt_runner_lifecycle").WillReturnError(errors.New("delete failed"))
	err := confirmSQLiteMirroredRunnerReaped(context.Background(), transaction, RunnerLifecycle{
		ScaleSet: "linux", Repository: "owner/repo", RequestID: 42, RunnerName: "linux-42",
	})
	if err == nil {
		t.Fatal("expected mirrored terminal delete failure")
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteAtMostOneIdentityChangeAcceptsNoMatch(t *testing.T) {
	if err := requireAtMostOneSQLiteIdentityChange(
		sqlmock.NewResult(0, 0), RunnerIdentity{RequestID: 42}, "change",
	); err != nil {
		t.Fatalf("zero affected rows error = %v", err)
	}
}
