package scaler

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLiteIdentityClassificationBranches(t *testing.T) {
	identity := RunnerIdentity{RequestID: 42, RunnerName: "runner"}
	failure := errors.New("count failed")
	requestFailure := func(RunnerIdentity) (int64, error) { return 0, failure }
	if err := classifyMissingSQLiteIdentity(identity, requestFailure); !errors.Is(err, failure) {
		t.Fatalf("request classification error = %v", err)
	}
	call := 0
	nameFailure := func(RunnerIdentity) (int64, error) {
		call++
		if call == 2 {
			return 0, failure
		}
		return 0, nil
	}
	if err := classifyMissingSQLiteIdentity(identity, nameFailure); !errors.Is(err, failure) {
		t.Fatalf("name classification error = %v", err)
	}

	assertSQLiteIdentityClassification(t, identity)
}

func assertSQLiteIdentityClassification(t *testing.T, identity RunnerIdentity) {
	t.Helper()
	assertSQLiteIdentityFreeClassification(t, identity)
}

func assertSQLiteIdentityFreeClassification(t *testing.T, identity RunnerIdentity) {
	t.Helper()
	for name, test := range map[string]struct {
		counts sqliteBindingCounts
		want   error
	}{
		"invariant": {counts: sqliteBindingCounts{request: 2}, want: ErrRunnerIdentityInvariant},
		"mismatch":  {counts: sqliteBindingCounts{request: 1}, want: ErrRunnerIdentityMismatch},
		"not found": {counts: sqliteBindingCounts{}, want: ErrRunnerIdentityNotFound},
	} {
		t.Run("identity-free "+name, func(t *testing.T) {
			if err := classifySQLiteIdentityFreeBinding(identity, test.counts); !errors.Is(err, test.want) {
				t.Fatalf("classification error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestSQLiteIdentityResultAndExecutionFailures(t *testing.T) {
	identity := RunnerIdentity{RequestID: 42}
	if err := requireSingleSQLiteIdentityChange(sqlmock.NewResult(0, 2), identity, "change"); !errors.Is(err, ErrRunnerIdentityInvariant) {
		t.Fatalf("single change error = %v", err)
	}
	if err := requireAtMostOneSQLiteIdentityChange(failingSQLResult{}, identity, "change"); err == nil {
		t.Fatal("expected affected-row error")
	}
	if err := requireAtMostOneSQLiteIdentityChange(sqlmock.NewResult(0, 2), identity, "change"); !errors.Is(err, ErrRunnerIdentityInvariant) {
		t.Fatalf("at-most-one error = %v", err)
	}
	if err := requireAtMostOneSQLiteIdentityChange(sqlmock.NewResult(0, 1), identity, "change"); err != nil {
		t.Fatalf("single change error = %v", err)
	}
}

func TestSQLiteIdentityStatementExecutionFailures(t *testing.T) {
	identity := RunnerIdentity{RequestID: 42}
	fixture := mockDemandStore(t)
	fixture.mock.ExpectBegin()
	transaction, err := fixture.store.database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = executeSQLiteIdentityStatement(context.Background(), transaction, RunnerIdentity{}, nil,
		"SELECT 1", "SELECT 1", "SELECT 1", "execute"); !errors.Is(err, ErrInvalidRunnerIdentity) {
		t.Fatalf("invalid execution error = %v", err)
	}
	fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("execute failed"))
	if _, err = executeSQLiteIdentityStatement(context.Background(), transaction, identity, nil,
		"UPDATE runner_lifecycle SET phase = 'busy' WHERE request_id = ?", "", "", "execute"); err == nil {
		t.Fatal("expected execution failure")
	}
	fixture.mock.ExpectRollback()
	if err = transaction.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteIdentityBindingCandidateFailures(t *testing.T) {
	identity := RunnerIdentity{RequestID: 42, RunnerName: "runner"}
	for _, failureIndex := range []int{0, 1, 2} {
		t.Run("runner candidate", func(t *testing.T) {
			fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
			expectSQLiteCandidateFailure(fixture.mock, failureIndex)
			if _, err := sqliteBindingCandidateCounts(context.Background(), transaction, "set", "repo", identity); err == nil {
				t.Fatal("expected candidate count failure")
			}
			rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
		})
	}
}

func expectSQLiteCandidateFailure(mock sqlmock.Sqlmock, failureIndex int) {
	for queryIndex := 0; queryIndex <= failureIndex; queryIndex++ {
		expectation := mock.ExpectQuery("SELECT COUNT\\(\\*\\)")
		if queryIndex == failureIndex {
			expectation.WillReturnError(errors.New("count failed"))
		} else {
			expectation.WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		}
	}
}

func TestSQLiteBindingAndMarkBoundaryBranches(t *testing.T) {
	fixture := mockDemandStore(t)
	fixture.mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
	if err := fixture.store.MarkRunnerBusy(context.Background(), "set", "repo",
		RunnerIdentity{RequestID: 42}, 1, time.Now()); err == nil {
		t.Fatal("expected begin failure")
	}
	if err := fixture.store.BindRunnerIdentity(context.Background(), "set", "repo",
		RunnerIdentity{RequestID: 42}, 1, time.Now()); !errors.Is(err, ErrInvalidRunnerIdentity) {
		t.Fatalf("invalid binding error = %v", err)
	}

	fixture = mockDemandStore(t)
	fixture.mock.ExpectBegin()
	expectSQLiteIdentityMatch(fixture.mock)
	fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
	fixture.mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
	if err := fixture.store.MarkRunnerBusy(context.Background(), "set", "repo",
		RunnerIdentity{RequestID: 42}, 1, time.Now()); err == nil {
		t.Fatal("expected commit failure")
	}
}

func TestSQLiteCompletionWithoutRequestDemand(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	if err := removeSQLiteCompletedRequestDemand(context.Background(), transaction, JobCompletion{}); err != nil {
		t.Fatal(err)
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteIdentityFreeBindingMutationFailures(t *testing.T) {
	identity := RunnerIdentity{RequestID: 42, RunnerName: "runner"}
	for name, operation := range map[string]func(demandStoreMock, *sql.Tx) error{
		"candidate count": func(fixture demandStoreMock, transaction *sql.Tx) error {
			fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\)").WillReturnError(errors.New("count failed"))
			return bindSQLiteIdentityFreeRunner(context.Background(), transaction, "set", "repo", identity, 1, time.Now())
		},
		"identity-free update": func(fixture demandStoreMock, transaction *sql.Tx) error {
			expectSQLiteCoverageCounts(fixture.mock, 0, 1, 1)
			fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("update failed"))
			return bindSQLiteIdentityFreeRunner(context.Background(), transaction, "set", "repo", identity, 1, time.Now())
		},
		"identity-free affected rows": func(fixture demandStoreMock, transaction *sql.Tx) error {
			expectSQLiteCoverageCounts(fixture.mock, 0, 1, 1)
			fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 0))
			return bindSQLiteIdentityFreeRunner(context.Background(), transaction, "set", "repo", identity, 1, time.Now())
		},
	} {
		assertSQLiteBindingOperationFails(t, name, operation)
	}
}

func assertSQLiteBindingOperationFails(
	t *testing.T,
	name string,
	operation func(demandStoreMock, *sql.Tx) error,
) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
		if err := operation(fixture, transaction); err == nil {
			t.Fatal("expected binding failure")
		}
		rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
	})
}

func TestSQLiteRunnerBindingRejectsDuplicateResolution(t *testing.T) {
	identity := RunnerIdentity{RequestID: 42, RunnerName: "runner"}
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\)").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	if err := bindSQLiteRunnerIdentity(context.Background(), transaction, "set", "repo", identity, 1, time.Now()); !errors.Is(err, ErrRunnerIdentityInvariant) {
		t.Fatalf("resolution error = %v", err)
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func TestSQLiteBusyUpdateRejectsInvalidIdentity(t *testing.T) {
	fixture, transaction := beginSQLiteIdentityCoverageTransaction(t)
	if err := updateSQLiteRunnerBusy(context.Background(), transaction, "set", "repo", RunnerIdentity{}, 1, time.Now()); !errors.Is(err, ErrInvalidRunnerIdentity) {
		t.Fatalf("invalid busy identity error = %v", err)
	}
	rollbackSQLiteIdentityCoverageTransaction(t, fixture, transaction)
}

func expectSQLiteCoverageCounts(mock sqlmock.Sqlmock, counts ...int64) {
	for _, count := range counts {
		mock.ExpectQuery("SELECT COUNT\\(\\*\\)").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
	}
}

func beginSQLiteIdentityCoverageTransaction(t *testing.T) (demandStoreMock, *sql.Tx) {
	t.Helper()
	fixture := mockDemandStore(t)
	fixture.mock.ExpectBegin()
	transaction, err := fixture.store.database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	return fixture, transaction
}

func rollbackSQLiteIdentityCoverageTransaction(t *testing.T, fixture demandStoreMock, transaction *sql.Tx) {
	t.Helper()
	fixture.mock.ExpectRollback()
	if err := transaction.Rollback(); err != nil {
		t.Fatal(err)
	}
}
