package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
)

func TestRegistrationMutationsReportTransactionFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	record := githubsource.Registration{GitHubAppID: 7, RepositoryURL: "repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	for _, testCase := range []struct {
		name     string
		executor postgresstate.Executor
	}{
		{name: "begin", executor: failingPostgresBegin(expectedErr)},
		{name: "active", executor: postgresExecuteFailure(1, expectedErr)},
		{name: "ownership", executor: postgresExecuteFailure(2, expectedErr)},
		{name: "commit", executor: postgresCommitFailure(expectedErr)},
		{name: "rollback", executor: postgresExecuteAndRollbackFailure(expectedErr)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := newExecutorStore(testCase.executor).UpsertRegistration(context.Background(), record); !errors.Is(err, expectedErr) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestRegistrationSingleRecordMutationsReportFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	for _, executor := range []postgresstate.Executor{
		postgresExecuteFailure(1, expectedErr), postgresCommitFailure(expectedErr), postgresExecuteAndRollbackFailure(expectedErr),
	} {
		if err := newExecutorStore(executor).DeleteActiveRegistration(context.Background(), githubsource.Registration{}); !errors.Is(err, expectedErr) {
			t.Fatalf("error = %v", err)
		}
	}
}

func TestRegistrationListsReportTransactionFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	for _, testCase := range []struct {
		name     string
		executor postgresstate.Executor
	}{
		{name: "begin", executor: failingPostgresBegin(expectedErr)},
		{name: "query", executor: postgresListFailure(expectedErr, nil, nil, nil)},
		{name: "scan", executor: postgresListFailure(nil, expectedErr, nil, nil)},
		{name: "iteration", executor: postgresListFailure(nil, nil, expectedErr, nil)},
		{name: "close", executor: postgresListFailure(nil, nil, nil, expectedErr)},
		{name: "commit", executor: postgresListCommitFailure(expectedErr)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newExecutorStore(testCase.executor).Registrations(context.Background()); !errors.Is(err, expectedErr) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestDeclarationPersistenceReportsTransactionFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	declaration := []githubsource.RunnerPoolDeclaration{{RunnerPool: "linux", RunnerGroup: "default"}}
	for _, executor := range []postgresstate.Executor{
		failingPostgresBegin(expectedErr), postgresExecuteFailure(1, expectedErr),
		postgresExecuteFailure(2, expectedErr), postgresCommitFailure(expectedErr),
	} {
		if err := newExecutorStore(executor).ReplaceGitHubRunnerPoolDeclarations(context.Background(), "member", declaration); !errors.Is(err, expectedErr) {
			t.Fatalf("error = %v", err)
		}
	}
	for _, executor := range []postgresstate.Executor{
		failingPostgresBegin(expectedErr), postgresListFailure(expectedErr, nil, nil, nil),
		postgresListFailure(nil, expectedErr, nil, nil), postgresListFailure(nil, nil, expectedErr, nil),
		postgresListFailure(nil, nil, nil, expectedErr), postgresListCommitFailure(expectedErr),
	} {
		if _, err := newExecutorStore(executor).LiveGitHubRunnerPoolDeclarations(context.Background(), time.Time{}); !errors.Is(err, expectedErr) {
			t.Fatalf("error = %v", err)
		}
	}
}

func TestAssignedDemandReportsBoundaryFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	demand := githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "observation", DesiredJobs: 0, ObservedAt: time.Unix(1_700_000_000, 0).UTC()}
	for _, testCase := range []struct {
		name     string
		executor postgresstate.Executor
	}{
		{name: "begin", executor: failingPostgresBegin(expectedErr)},
		{name: "scope lock", executor: postgresDemandExecutor(expectedErr, nil, postgresRows(nil), nil)},
		{name: "query", executor: postgresDemandExecutor(nil, expectedErr, postgresstate.Rows{}, nil)},
		{name: "scan", executor: postgresDemandExecutor(nil, nil,
			postgresstate.NewRows(func() error { return nil }, func() error { return nil }, func() bool { return true },
				func(...any) error { return expectedErr }), nil)},
		{name: "iteration", executor: postgresDemandExecutor(nil, nil,
			postgresstate.NewRows(func() error { return nil }, func() error { return expectedErr }, func() bool { return false },
				func(...any) error { return nil }), nil)},
		{name: "close", executor: postgresDemandExecutor(nil, nil,
			postgresstate.NewRows(func() error { return expectedErr }, func() error { return nil }, func() bool { return false },
				func(...any) error { return nil }), nil)},
		{name: "commit", executor: postgresDemandExecutor(nil, nil, postgresRows(nil), expectedErr)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newExecutorStore(testCase.executor).ReconcileAssignedDemand(context.Background(), demand); !errors.Is(err, expectedErr) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestAssignedDemandReportsMutationFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	demand := githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "observation", DesiredJobs: 1, ObservedAt: time.Unix(1_700_000_000, 0).UTC()}
	executeCalls := 0
	transaction := postgresstate.NewTransaction(func(context.Context, string, ...any) (postgresstate.Result, error) {
		executeCalls++
		if executeCalls == 2 {
			return postgresstate.Result{}, expectedErr
		}
		return postgresstate.NewResult(func() (int64, error) { return 1, nil }), nil
	}, func(context.Context, string, ...any) (postgresstate.Rows, error) { return postgresRows(nil), nil },
		func(context.Context, string, ...any) postgresstate.Row {
			return postgresstate.NewRow(func(...any) error { return expectedErr })
		}, func(context.Context) error { return nil }, func(context.Context) error { return expectedErr })
	store := newExecutorStore(postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) {
		return transaction, nil
	}, nil))
	if _, err := store.ReconcileAssignedDemand(context.Background(), demand); !errors.Is(err, expectedErr) {
		t.Fatalf("error = %v", err)
	}
	if _, err := addAggregateDemandJob(context.Background(), postgresstate.Transaction{}, githubsource.AssignedDemand{}, 0); err == nil {
		t.Fatal("expected identity error")
	}
}

func TestAssignedDemandDoesNotRecreateTerminalAggregateJob(t *testing.T) {
	demand := githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "terminal", DesiredJobs: 1, ObservedAt: time.Unix(1_700_000_000, 0).UTC()}
	rowCalls := 0
	transaction := postgresstate.NewTransaction(func(context.Context, string, ...any) (postgresstate.Result, error) {
		return postgresstate.NewResult(func() (int64, error) { return 1, nil }), nil
	}, func(context.Context, string, ...any) (postgresstate.Rows, error) { return postgresRows(nil), nil },
		func(context.Context, string, ...any) postgresstate.Row {
			rowCalls++
			if rowCalls <= 2 {
				return postgresstate.NewRow(func(...any) error { return sql.ErrNoRows })
			}
			return postgresRow([]any{string(core.QueueSourceTerminal)})
		}, func(context.Context) error { return nil }, func(context.Context) error { return nil })
	store := newExecutorStore(postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) { return transaction, nil }, nil))
	result, err := store.ReconcileAssignedDemand(context.Background(), demand)
	if err != nil || result != (githubsource.AssignedDemandResult{}) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
}

func TestAssignedDemandReportsQueueFailure(t *testing.T) {
	expectedErr := errors.New("queue")
	demand := githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "observation", DesiredJobs: 1, ObservedAt: time.Unix(1_700_000_000, 0).UTC()}
	transaction := postgresstate.NewTransaction(func(context.Context, string, ...any) (postgresstate.Result, error) {
		return postgresstate.NewResult(func() (int64, error) { return 1, nil }), nil
	}, func(context.Context, string, ...any) (postgresstate.Rows, error) { return postgresRows(nil), nil },
		func(context.Context, string, ...any) postgresstate.Row {
			return postgresstate.NewRow(func(...any) error { return expectedErr })
		}, func(context.Context) error { return nil }, func(context.Context) error { return nil })
	store := newExecutorStore(postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) { return transaction, nil }, nil))
	if _, err := store.ReconcileAssignedDemand(context.Background(), demand); !errors.Is(err, expectedErr) {
		t.Fatalf("error = %v", err)
	}
}

func TestAssignedDemandReportsRemovalFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	demand := githubsource.AssignedDemand{Repository: "owner/repo", RunnerPool: "linux",
		ObservationID: "current", DesiredJobs: 0, ObservedAt: time.Unix(1_700_000_000, 0).UTC()}
	previous := demand
	previous.ObservationID = "previous"
	source := aggregateDemandSource(t, previous, 1)
	row := []any{uuid.MustParse("00000000-0000-0000-0000-000000000001"), string(source.Adapter), string(source.ID), "queued", demand.ObservedAt}
	for _, failureCall := range []int{2, 3} {
		assertPostgresRemovalExecuteFailure(t, demand, row, failureCall, expectedErr)
	}
	assertPostgresRemovalCountFailure(t, demand, row, expectedErr)
}

func assertPostgresRemovalExecuteFailure(
	t *testing.T,
	demand githubsource.AssignedDemand,
	row []any,
	failureCall int,
	expectedErr error,
) {
	t.Helper()
	executeCalls := 0
	transaction := postgresstate.NewTransaction(func(context.Context, string, ...any) (postgresstate.Result, error) {
		executeCalls++
		if executeCalls == failureCall {
			return postgresstate.Result{}, expectedErr
		}
		return postgresstate.NewResult(func() (int64, error) { return 1, nil }), nil
	}, func(context.Context, string, ...any) (postgresstate.Rows, error) {
		return postgresRows([][]any{row}), nil
	}, nil, func(context.Context) error { return nil }, func(context.Context) error { return nil })
	store := newExecutorStore(postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) {
		return transaction, nil
	}, nil))
	if _, err := store.ReconcileAssignedDemand(context.Background(), demand); !errors.Is(err, expectedErr) {
		t.Fatalf("failure call %d error = %v", failureCall, err)
	}
}

func assertPostgresRemovalCountFailure(t *testing.T, demand githubsource.AssignedDemand, row []any, expectedErr error) {
	t.Helper()
	transaction := postgresstate.NewTransaction(func(context.Context, string, ...any) (postgresstate.Result, error) {
		return postgresstate.NewResult(func() (int64, error) { return 0, expectedErr }), nil
	}, func(context.Context, string, ...any) (postgresstate.Rows, error) {
		return postgresRows([][]any{row}), nil
	}, nil,
		func(context.Context) error { return nil }, func(context.Context) error { return nil })
	store := newExecutorStore(postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) { return transaction, nil }, nil))
	if _, err := store.ReconcileAssignedDemand(context.Background(), demand); !errors.Is(err, expectedErr) {
		t.Fatalf("rows affected error = %v", err)
	}
}

func postgresDemandExecutor(
	executeErr error,
	queryErr error,
	rows postgresstate.Rows,
	commitErr error,
) postgresstate.Executor {
	transaction := postgresstate.NewTransaction(func(context.Context, string, ...any) (postgresstate.Result, error) {
		if executeErr != nil {
			return postgresstate.Result{}, executeErr
		}
		return postgresstate.NewResult(func() (int64, error) { return 1, nil }), nil
	}, func(context.Context, string, ...any) (postgresstate.Rows, error) { return rows, queryErr }, nil,
		func(context.Context) error { return commitErr }, func(context.Context) error { return nil })
	return postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) { return transaction, nil }, nil)
}

func failingPostgresBegin(expectedErr error) postgresstate.Executor {
	return postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) {
		return postgresstate.Transaction{}, expectedErr
	}, nil)
}

func postgresExecuteFailure(failureCall int, expectedErr error) postgresstate.Executor {
	calls := 0
	transaction := postgresstate.NewTransaction(func(context.Context, string, ...any) (postgresstate.Result, error) {
		calls++
		if calls == failureCall {
			return postgresstate.Result{}, expectedErr
		}
		return postgresstate.NewResult(func() (int64, error) { return 1, nil }), nil
	}, nil, nil, func(context.Context) error { return nil }, func(context.Context) error { return nil })
	return postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) { return transaction, nil }, nil)
}

func postgresCommitFailure(expectedErr error) postgresstate.Executor {
	transaction := postgresstate.NewTransaction(func(context.Context, string, ...any) (postgresstate.Result, error) {
		return postgresstate.NewResult(func() (int64, error) { return 1, nil }), nil
	}, func(context.Context, string, ...any) (postgresstate.Rows, error) { return postgresRows(nil), nil }, nil,
		func(context.Context) error { return expectedErr }, func(context.Context) error { return nil })
	return postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) { return transaction, nil }, nil)
}

func postgresExecuteAndRollbackFailure(expectedErr error) postgresstate.Executor {
	transaction := postgresstate.NewTransaction(func(context.Context, string, ...any) (postgresstate.Result, error) {
		return postgresstate.Result{}, expectedErr
	}, nil, nil, nil, func(context.Context) error { return expectedErr })
	return postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) { return transaction, nil }, nil)
}

func postgresListFailure(queryErr, scanErr, iterationErr, closeErr error) postgresstate.Executor {
	rows := postgresstate.NewRows(func() error { return closeErr }, func() error { return iterationErr },
		func() bool { return scanErr != nil }, func(...any) error { return scanErr })
	transaction := postgresstate.NewTransaction(nil, func(context.Context, string, ...any) (postgresstate.Rows, error) {
		return rows, queryErr
	}, nil, func(context.Context) error { return nil }, func(context.Context) error { return nil })
	return postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) { return transaction, nil }, nil)
}

func postgresListCommitFailure(expectedErr error) postgresstate.Executor {
	transaction := postgresstate.NewTransaction(nil, func(context.Context, string, ...any) (postgresstate.Rows, error) {
		return postgresRows(nil), nil
	}, nil, func(context.Context) error { return expectedErr }, func(context.Context) error { return nil })
	return postgresstate.NewExecutor(func(context.Context) (postgresstate.Transaction, error) { return transaction, nil }, nil)
}
