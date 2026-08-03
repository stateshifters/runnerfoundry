package scaler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func genericSQLiteRunner() RunnerLifecycle {
	return RunnerLifecycle{ScaleSet: "linux", Repository: "repo", Provider: RunnerProviderDocker, RunnerName: "linux-42", RequestID: 42}
}

var sqliteRunnerCompletion = JobCompletion{ScaleSet: "linux", Repository: "repo", RequestID: 42}

var sqliteRunnerCompletionFailureOperations = []func(demandStoreMock) error{
	func(f demandStoreMock) error {
		f.mock.ExpectBegin().WillReturnError(errors.New("failed"))
		return f.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("failed"))
		f.mock.ExpectRollback()
		return f.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		f.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
		f.mock.ExpectRollback()
		return f.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, time.Now())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("DELETE FROM demand").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, time.Now())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("DELETE FROM demand").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, time.Now())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectCommit().WillReturnError(errors.New("failed"))
		return fixture.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, time.Now())
	},
}

func TestSQLiteRunnerLifecycleCompletionReportsBoundaryFailures(t *testing.T) {
	assertSQLiteLifecycleFailures(t, sqliteRunnerCompletionFailureOperations)
}

func TestSQLiteRunnerRegistrationReportsBoundaryFailures(t *testing.T) {
	runner := genericSQLiteRunner()
	operations := []func(demandStoreMock) error{
		func(f demandStoreMock) error {
			f.mock.ExpectExec("INSERT INTO runner_lifecycle").WillReturnError(errors.New("failed"))
			return f.store.RegisterRunner(context.Background(), runner)
		},
		func(fixture demandStoreMock) error {
			fixture.mock.ExpectExec("INSERT INTO runner_lifecycle").WillReturnResult(failingSQLResult{})
			return fixture.store.RegisterRunner(context.Background(), runner)
		},
		func(fixture demandStoreMock) error {
			fixture.mock.ExpectExec("INSERT INTO runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 0))
			fixture.mock.ExpectQuery("SELECT provider").WillReturnError(errors.New("failed"))
			return fixture.store.RegisterRunner(context.Background(), runner)
		},
		func(f demandStoreMock) error {
			f.mock.ExpectExec("INSERT INTO runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 0))
			f.mock.ExpectQuery("SELECT provider").WillReturnRows(sqlmock.NewRows([]string{"provider", "request_id", "assignment_id", "phase"}).AddRow("libvirt", 42, 0, "provisioning"))
			return f.store.RegisterRunner(context.Background(), runner)
		},
		func(f demandStoreMock) error {
			candidate := runner
			candidate.Provider = RunnerProviderLibvirt
			f.mock.ExpectExec("INSERT INTO runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
			f.mock.ExpectExec("INSERT INTO libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
			return f.store.RegisterRunner(context.Background(), candidate)
		},
	}
	assertSQLiteLifecycleFailures(t, operations)
}

var sqliteRunnerSimpleTransitionFailures = []func(demandStoreMock) error{
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("failed"))
		return f.store.ConfirmRunnerWaiting(context.Background(), genericSQLiteRunner(), time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(failingSQLResult{})
		return f.store.ConfirmRunnerWaiting(context.Background(), genericSQLiteRunner(), time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("failed"))
		return f.store.MarkRunnerBusy(context.Background(), "linux", "repo", 42, "runner", 1, time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(failingSQLResult{})
		return f.store.MarkRunnerBusy(context.Background(), "linux", "repo", 42, "runner", 1, time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("failed"))
		return f.store.RequestRunnerReap(context.Background(), "linux", "repo", 42, "runner", "reason", time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(failingSQLResult{})
		return f.store.RequestRunnerReap(context.Background(), "linux", "repo", 42, "runner", "reason", time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		f.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
		return f.store.RequestRunnerReap(context.Background(), "linux", "repo", 42, "runner", "reason", time.Now())
	},
}

func TestSQLiteRunnerSimpleTransitionsReportBoundaryFailures(t *testing.T) {
	assertSQLiteLifecycleFailures(t, sqliteRunnerSimpleTransitionFailures)
}

var sqliteRunnerBulkReapFailures = []func(demandStoreMock) error{
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("failed"))
		_, err := f.store.RequestExpiredRunnerReaps(context.Background(), "linux", "repo", time.Now(), time.Now())
		return err
	},
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(failingSQLResult{})
		_, err := f.store.RequestExpiredRunnerReaps(context.Background(), "linux", "repo", time.Now(), time.Now())
		return err
	},
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		f.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
		_, err := f.store.RequestExpiredRunnerReaps(context.Background(), "linux", "repo", time.Now(), time.Now())
		return err
	},
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("failed"))
		_, err := f.store.RequestExcessWaitingRunnerReaps(context.Background(), "linux", "repo", -1, "reason", time.Now())
		return err
	},
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(failingSQLResult{})
		_, err := f.store.RequestExcessWaitingRunnerReaps(context.Background(), "linux", "repo", 0, "reason", time.Now())
		return err
	},
	func(f demandStoreMock) error {
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		f.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
		_, err := f.store.RequestExcessWaitingRunnerReaps(context.Background(), "linux", "repo", 0, "reason", time.Now())
		return err
	},
}

func TestSQLiteRunnerBulkReapsReportBoundaryFailures(t *testing.T) {
	assertSQLiteLifecycleFailures(t, sqliteRunnerBulkReapFailures)
}

var sqliteRunnerLifecycleColumns = []string{"node_id", "scale_set", "repository", "provider", "runner_name", "request_id", "assignment_id", "installation_id", "github_scale_set_id", "github_runner_id", "phase", "created_at", "waiting_since", "updated_at", "reap_reason"}

var sqliteRunnerLifecycleQueryFailures = map[string]func(demandStoreMock) error{
	"pending query": func(f demandStoreMock) error {
		f.mock.ExpectQuery("SELECT node_id").WillReturnError(errors.New("failed"))
		_, err := f.store.PendingRunnerReaps(context.Background(), "linux", "repo")
		return err
	},
	"pending scan": func(f demandStoreMock) error {
		f.mock.ExpectQuery("SELECT node_id").WillReturnRows(sqlmock.NewRows(sqliteRunnerLifecycleColumns).AddRow("node", "linux", "repo", "docker", "runner", "bad", 0, 0, 0, 0, "reap_pending", 0, 0, 0, "reason"))
		_, err := f.store.PendingRunnerReaps(context.Background(), "linux", "repo")
		return err
	},
	"pending rows": func(f demandStoreMock) error {
		f.mock.ExpectQuery("SELECT node_id").WillReturnRows(sqlmock.NewRows(sqliteRunnerLifecycleColumns).AddRow("node", "linux", "repo", "docker", "runner", 1, 0, 0, 0, 0, "reap_pending", 0, 0, 0, "reason").RowError(0, errors.New("failed")))
		_, err := f.store.PendingRunnerReaps(context.Background(), "linux", "repo")
		return err
	},
	"inventory query": func(f demandStoreMock) error {
		f.mock.ExpectQuery("SELECT node_id").WillReturnError(errors.New("failed"))
		_, err := f.store.RunnerLifecycles(context.Background(), "linux", "repo")
		return err
	},
	"inventory scan": func(f demandStoreMock) error {
		f.mock.ExpectQuery("SELECT node_id").WillReturnRows(sqlmock.NewRows(sqliteRunnerLifecycleColumns).AddRow("node", "linux", "repo", "docker", "runner", "bad", 0, 0, 0, 0, "busy", 0, 0, 0, ""))
		_, err := f.store.RunnerLifecycles(context.Background(), "linux", "repo")
		return err
	},
	"inventory rows": func(f demandStoreMock) error {
		f.mock.ExpectQuery("SELECT node_id").WillReturnRows(sqlmock.NewRows(sqliteRunnerLifecycleColumns).AddRow("node", "linux", "repo", "docker", "runner", 1, 0, 0, 0, 0, "busy", 0, 0, 0, "").RowError(0, errors.New("failed")))
		_, err := f.store.RunnerLifecycles(context.Background(), "linux", "repo")
		return err
	},
}

func TestSQLiteRunnerLifecycleQueriesReportBoundaryFailures(t *testing.T) {
	for name, operation := range sqliteRunnerLifecycleQueryFailures {
		t.Run(name, func(t *testing.T) {
			if err := operation(mockDemandStore(t)); err == nil {
				t.Fatal("expected query failure")
			}
		})
	}
}

func TestSQLiteRunnerLifecycleQueriesReportCloseFailures(t *testing.T) {
	columns := []string{"node_id", "scale_set", "repository", "provider", "runner_name", "request_id", "assignment_id", "installation_id", "github_scale_set_id", "github_runner_id", "phase", "created_at", "waiting_since", "updated_at", "reap_reason"}
	for name, operation := range map[string]func(demandStoreMock) error{
		"pending": func(f demandStoreMock) error {
			f.mock.ExpectQuery("SELECT node_id").WillReturnRows(sqlmock.NewRows(columns).
				AddRow("node", "linux", "repo", "docker", "runner", 1, 0, 0, 0, 0, "reap_pending", 0, 0, 0, "reason").CloseError(errors.New("close")))
			_, err := f.store.PendingRunnerReaps(context.Background(), "linux", "repo")
			return err
		},
		"inventory": func(f demandStoreMock) error {
			f.mock.ExpectQuery("SELECT node_id").WillReturnRows(sqlmock.NewRows(columns).
				AddRow("node", "linux", "repo", "docker", "runner", 1, 0, 0, 0, 0, "busy", 0, 0, 0, "").CloseError(errors.New("close")))
			_, err := f.store.RunnerLifecycles(context.Background(), "linux", "repo")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := operation(mockDemandStore(t)); err == nil {
				t.Fatal("expected close failure")
			}
		})
	}
}

var sqliteConfirmRunnerReapedFailures = []func(demandStoreMock) error{
	func(f demandStoreMock) error {
		f.mock.ExpectBegin().WillReturnError(errors.New("failed"))
		return f.store.ConfirmRunnerReaped(context.Background(), genericSQLiteRunner())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		f.mock.ExpectExec("DELETE FROM runner_lifecycle").WillReturnError(errors.New("failed"))
		f.mock.ExpectRollback()
		return f.store.ConfirmRunnerReaped(context.Background(), genericSQLiteRunner())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		f.mock.ExpectExec("DELETE FROM runner_lifecycle").WillReturnResult(failingSQLResult{})
		f.mock.ExpectRollback()
		return f.store.ConfirmRunnerReaped(context.Background(), genericSQLiteRunner())
	},
	func(fixture demandStoreMock) error {
		candidate := genericSQLiteRunner()
		candidate.Provider = RunnerProviderLibvirt
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectExec("DELETE FROM runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("DELETE FROM libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.ConfirmRunnerReaped(context.Background(), candidate)
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectExec("DELETE FROM runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectCommit().WillReturnError(errors.New("failed"))
		return fixture.store.ConfirmRunnerReaped(context.Background(), genericSQLiteRunner())
	},
}

func TestSQLiteConfirmRunnerReapedReportsBoundaryFailures(t *testing.T) {
	assertSQLiteLifecycleFailures(t, sqliteConfirmRunnerReapedFailures)
}
