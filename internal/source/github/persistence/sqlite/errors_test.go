package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	sqlitestate "github.com/stateshifters/runnerfoundry/internal/state/sqlite"
)

func TestRegistrationAndDeclarationMutationsReportTransactionFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	record := githubsource.Registration{GitHubAppID: 7, RepositoryURL: "repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	declaration := []githubsource.RunnerPoolDeclaration{{RunnerPool: "linux", RunnerGroup: "default"}}
	mutationFailures := []func() sqlitestate.Executor{
		func() sqlitestate.Executor { return failingSQLiteBegin(expectedErr) },
		func() sqlitestate.Executor { return sqliteExecuteFailure(1, expectedErr) },
		func() sqlitestate.Executor { return sqliteExecuteFailure(2, expectedErr) },
		func() sqlitestate.Executor { return sqliteCommitFailure(expectedErr) },
		func() sqlitestate.Executor { return sqliteExecuteAndRollbackFailure(expectedErr) },
	}
	for _, executor := range mutationFailures {
		store := NewStore(executor())
		if err := store.UpsertRegistration(context.Background(), record); !errors.Is(err, expectedErr) {
			t.Fatalf("registration error = %v", err)
		}
		if err := NewStore(executor()).ReplaceGitHubRunnerPoolDeclarations(context.Background(), "member", declaration); !errors.Is(err, expectedErr) {
			t.Fatalf("declaration error = %v", err)
		}
	}
}

func TestRegistrationAndDeclarationListsReportTransactionFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	listFailures := []func() sqlitestate.Executor{
		func() sqlitestate.Executor { return failingSQLiteBegin(expectedErr) },
		func() sqlitestate.Executor { return sqliteListFailure(expectedErr, nil, nil, nil) },
		func() sqlitestate.Executor { return sqliteListFailure(nil, expectedErr, nil, nil) },
		func() sqlitestate.Executor { return sqliteListFailure(nil, nil, expectedErr, nil) },
		func() sqlitestate.Executor { return sqliteListFailure(nil, nil, nil, expectedErr) },
		func() sqlitestate.Executor { return sqliteListCommitFailure(expectedErr) },
	}
	for _, executor := range listFailures {
		store := NewStore(executor())
		if _, err := store.Registrations(context.Background()); !errors.Is(err, expectedErr) {
			t.Fatalf("registration list error = %v", err)
		}
		if _, err := NewStore(executor()).LiveGitHubRunnerPoolDeclarations(context.Background(), time.Time{}); !errors.Is(err, expectedErr) {
			t.Fatalf("declaration list error = %v", err)
		}
	}
}

func TestAssignedDemandReportsBoundaryFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	demand := githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "observation", DesiredJobs: 0, ObservedAt: time.Unix(1_700_000_000, 0).UTC()}
	for _, testCase := range []struct {
		name     string
		executor sqlitestate.Executor
	}{
		{name: "begin", executor: failingSQLiteBegin(expectedErr)},
		{name: "query", executor: sqliteDemandExecutor(expectedErr, sqlitestate.Rows{}, nil, nil)},
		{name: "scan", executor: sqliteDemandExecutor(nil,
			sqlitestate.NewRows(func() error { return nil }, func() error { return nil }, func() bool { return true },
				func(...any) error { return expectedErr }), nil, nil)},
		{name: "iteration", executor: sqliteDemandExecutor(nil,
			sqlitestate.NewRows(func() error { return nil }, func() error { return expectedErr }, func() bool { return false },
				func(...any) error { return nil }), nil, nil)},
		{name: "close", executor: sqliteDemandExecutor(nil,
			sqlitestate.NewRows(func() error { return expectedErr }, func() error { return nil }, func() bool { return false },
				func(...any) error { return nil }), nil, nil)},
		{name: "commit", executor: sqliteDemandExecutor(nil, sqliteRows(nil), expectedErr, nil)},
		{name: "rollback", executor: sqliteDemandExecutor(expectedErr, sqlitestate.Rows{}, nil, expectedErr)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(testCase.executor).ReconcileAssignedDemand(context.Background(), demand); !errors.Is(err, expectedErr) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestAssignedDemandReportsQueueFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	demand := sqliteFailureDemand(1)
	for _, transaction := range []sqlitestate.Transaction{
		sqlitestate.NewTransaction(func(context.Context, string, ...any) (sqlitestate.Result, error) {
			return sqlitestate.Result{}, expectedErr
		}, func(context.Context, string, ...any) (sqlitestate.Rows, error) { return sqliteRows(nil), nil }, nil, nil,
			func(context.Context) error { return nil }),
		sqlitestate.NewTransaction(func(context.Context, string, ...any) (sqlitestate.Result, error) {
			return sqlitestate.NewResult(func() (int64, error) { return 0, expectedErr }), nil
		}, func(context.Context, string, ...any) (sqlitestate.Rows, error) { return sqliteRows(nil), nil }, nil, nil,
			func(context.Context) error { return nil }),
	} {
		store := NewStore(sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) { return transaction, nil }, nil))
		if _, err := store.ReconcileAssignedDemand(context.Background(), demand); !errors.Is(err, expectedErr) {
			t.Fatalf("queue error = %v", err)
		}
	}
}

func TestAssignedDemandReportsRemovalCountFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	demand, row := sqliteRemovalFailureInput(t)
	for _, result := range []sqlitestate.Result{
		sqlitestate.NewResult(func() (int64, error) { return 0, expectedErr }),
		sqlitestate.NewResult(func() (int64, error) { return 0, nil }),
	} {
		transaction := sqlitestate.NewTransaction(func(context.Context, string, ...any) (sqlitestate.Result, error) {
			return result, nil
		}, func(context.Context, string, ...any) (sqlitestate.Rows, error) { return sqliteRows([][]any{row}), nil }, nil,
			func(context.Context) error { return nil }, func(context.Context) error { return nil })
		store := NewStore(sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) { return transaction, nil }, nil))
		if _, err := store.ReconcileAssignedDemand(context.Background(), demand); err == nil {
			t.Fatal("expected removal error")
		}
	}
}

func TestAssignedDemandReportsRemovalDeleteFailure(t *testing.T) {
	expectedErr := errors.New("protocol")
	demand, row := sqliteRemovalFailureInput(t)
	transaction := sqlitestate.NewTransaction(func(context.Context, string, ...any) (sqlitestate.Result, error) {
		return sqlitestate.Result{}, expectedErr
	}, func(context.Context, string, ...any) (sqlitestate.Rows, error) { return sqliteRows([][]any{row}), nil }, nil,
		func(context.Context) error { return nil }, func(context.Context) error { return nil })
	store := NewStore(sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) { return transaction, nil }, nil))
	if _, err := store.ReconcileAssignedDemand(context.Background(), demand); !errors.Is(err, expectedErr) {
		t.Fatalf("delete error = %v", err)
	}
}

func sqliteRemovalFailureInput(t *testing.T) (githubsource.AssignedDemand, []any) {
	t.Helper()
	demand := sqliteFailureDemand(0)
	previous := demand
	previous.ObservationID = "previous"
	source := aggregateDemandSource(t, previous, 1)
	row := []any{"00000000-0000-0000-0000-000000000001", string(source.Adapter), string(source.ID), "linux",
		demand.ObservedAt.UnixNano(), false}
	return demand, row
}

func sqliteFailureDemand(desiredJobs int) githubsource.AssignedDemand {
	return githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "observation", DesiredJobs: desiredJobs, ObservedAt: time.Unix(1_700_000_000, 0).UTC()}
}

func TestAssignedDemandRejectsInvalidAggregateIdentity(t *testing.T) {
	if _, err := queueAggregateAssignedDemand(context.Background(), sqlitestate.Transaction{},
		githubsource.AssignedDemand{}, 0); err == nil {
		t.Fatal("expected invalid identity error")
	}
}

func TestSingleRegistrationMutationReportsFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	for _, executor := range []sqlitestate.Executor{
		failingSQLiteBegin(expectedErr), sqliteExecuteFailure(1, expectedErr),
		sqliteCommitFailure(expectedErr), sqliteExecuteAndRollbackFailure(expectedErr),
	} {
		if err := NewStore(executor).DeleteActiveRegistration(context.Background(), githubsource.Registration{}); !errors.Is(err, expectedErr) {
			t.Fatalf("error = %v", err)
		}
	}
}

func sqliteDemandExecutor(queryErr error, rows sqlitestate.Rows, commitErr, rollbackErr error) sqlitestate.Executor {
	transaction := sqlitestate.NewTransaction(nil, func(context.Context, string, ...any) (sqlitestate.Rows, error) {
		return rows, queryErr
	}, nil, func(context.Context) error { return commitErr }, func(context.Context) error { return rollbackErr })
	return sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) { return transaction, nil }, nil)
}

func failingSQLiteBegin(expectedErr error) sqlitestate.Executor {
	return sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) {
		return sqlitestate.Transaction{}, expectedErr
	}, nil)
}

func sqliteExecuteFailure(failureCall int, expectedErr error) sqlitestate.Executor {
	calls := 0
	transaction := sqlitestate.NewTransaction(func(context.Context, string, ...any) (sqlitestate.Result, error) {
		calls++
		if calls == failureCall {
			return sqlitestate.Result{}, expectedErr
		}
		return sqlitestate.NewResult(func() (int64, error) { return 1, nil }), nil
	}, nil, nil, func(context.Context) error { return nil }, func(context.Context) error { return nil })
	return sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) { return transaction, nil }, nil)
}

func sqliteCommitFailure(expectedErr error) sqlitestate.Executor {
	transaction := sqlitestate.NewTransaction(func(context.Context, string, ...any) (sqlitestate.Result, error) {
		return sqlitestate.NewResult(func() (int64, error) { return 1, nil }), nil
	}, func(context.Context, string, ...any) (sqlitestate.Rows, error) { return sqliteRows(nil), nil }, nil,
		func(context.Context) error { return expectedErr }, func(context.Context) error { return nil })
	return sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) { return transaction, nil }, nil)
}

func sqliteExecuteAndRollbackFailure(expectedErr error) sqlitestate.Executor {
	transaction := sqlitestate.NewTransaction(func(context.Context, string, ...any) (sqlitestate.Result, error) {
		return sqlitestate.Result{}, expectedErr
	}, nil, nil, nil, func(context.Context) error { return expectedErr })
	return sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) { return transaction, nil }, nil)
}

func sqliteListFailure(queryErr, scanErr, iterationErr, closeErr error) sqlitestate.Executor {
	rows := sqlitestate.NewRows(func() error { return closeErr }, func() error { return iterationErr },
		func() bool { return scanErr != nil }, func(...any) error { return scanErr })
	transaction := sqlitestate.NewTransaction(nil, func(context.Context, string, ...any) (sqlitestate.Rows, error) {
		return rows, queryErr
	}, nil, func(context.Context) error { return nil }, func(context.Context) error { return nil })
	return sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) { return transaction, nil }, nil)
}

func sqliteListCommitFailure(expectedErr error) sqlitestate.Executor {
	transaction := sqlitestate.NewTransaction(nil, func(context.Context, string, ...any) (sqlitestate.Rows, error) {
		return sqliteRows(nil), nil
	}, nil, func(context.Context) error { return expectedErr }, func(context.Context) error { return nil })
	return sqlitestate.NewExecutor(func(context.Context) (sqlitestate.Transaction, error) { return transaction, nil }, nil)
}
