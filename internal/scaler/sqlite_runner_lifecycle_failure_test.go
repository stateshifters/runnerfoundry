package scaler

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func genericSQLiteRunner() RunnerLifecycle {
	return RunnerLifecycle{ScaleSet: "linux", Repository: "repo", Provider: RunnerProviderDocker, RunnerName: "linux-42", RequestID: 42}
}

var sqliteRunnerCompletion = JobCompletion{
	ScaleSet: "linux", Repository: "repo", RunnerIdentity: RunnerIdentity{RequestID: 42},
}

func expectSQLiteDockerMirrorReconciliation(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT provider, runner_name").WillReturnRows(sqlmock.NewRows(
		[]string{"provider", "runner_name", "request_id", "phase"},
	).AddRow("docker", "linux-42", 42, "reap_pending"))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM libvirt_runner_lifecycle").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery("SELECT request_id FROM libvirt_runner_lifecycle").WillReturnError(sql.ErrNoRows)
}

func expectSQLiteTerminalAuthoritativeDocker(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT provider, runner_name").WillReturnRows(sqlmock.NewRows(
		[]string{"provider", "runner_name", "request_id", "phase"},
	).AddRow("docker", "linux-42", 42, "reap_pending"))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM libvirt_runner_lifecycle").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery("SELECT request_id FROM libvirt_runner_lifecycle").WillReturnError(sql.ErrNoRows)
}

var sqliteRunnerCompletionFailureOperations = []func(demandStoreMock) error{
	func(f demandStoreMock) error {
		f.mock.ExpectBegin().WillReturnError(errors.New("failed"))
		return f.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, time.Now())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runner_lifecycle").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		expectSQLiteIdentityMatch(f.mock, "runner_lifecycle")
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("failed"))
		f.mock.ExpectRollback()
		return f.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, time.Now())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "runner_lifecycle")
		fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, time.Now())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "runner_lifecycle")
		fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		expectSQLiteDockerMirrorReconciliation(fixture.mock)
		fixture.mock.ExpectExec("DELETE FROM demand").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, time.Now())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "runner_lifecycle")
		fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		expectSQLiteDockerMirrorReconciliation(fixture.mock)
		fixture.mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("DELETE FROM demand").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, time.Now())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "runner_lifecycle")
		fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		expectSQLiteDockerMirrorReconciliation(fixture.mock)
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
	assertSQLiteLifecycleFailures(t, sqliteRunnerRegistrationFailureOperations)
}

var sqliteRunnerRegistrationFailureOperations = []func(demandStoreMock) error{
	func(fixture demandStoreMock) error {
		runner := genericSQLiteRunner()
		fixture.mock.ExpectBegin().WillReturnError(errors.New("failed"))
		return fixture.store.RegisterRunner(context.Background(), runner)
	},
	func(fixture demandStoreMock) error {
		runner := genericSQLiteRunner()
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectQuery("SELECT provider").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.RegisterRunner(context.Background(), runner)
	},
	func(fixture demandStoreMock) error {
		runner := genericSQLiteRunner()
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectQuery("SELECT provider").WillReturnError(sql.ErrNoRows)
		fixture.mock.ExpectQuery("SELECT request_id, reap_requested").WillReturnError(sql.ErrNoRows)
		fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM libvirt_runner_lifecycle").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runner_lifecycle").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.RegisterRunner(context.Background(), runner)
	},
	func(fixture demandStoreMock) error {
		runner := genericSQLiteRunner()
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectQuery("SELECT provider").WillReturnError(sql.ErrNoRows)
		fixture.mock.ExpectQuery("SELECT request_id, reap_requested").WillReturnError(sql.ErrNoRows)
		fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM libvirt_runner_lifecycle").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runner_lifecycle").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		fixture.mock.ExpectExec("INSERT INTO runner_lifecycle").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.RegisterRunner(context.Background(), runner)
	},
	func(fixture demandStoreMock) error {
		candidate := genericSQLiteRunner()
		candidate.Provider = RunnerProviderLibvirt
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectQuery("SELECT provider").WillReturnRows(sqlmock.NewRows(
			[]string{"provider", "request_id", "assignment_id", "phase"},
		).AddRow("libvirt", 42, 0, "provisioning"))
		fixture.mock.ExpectQuery("SELECT request_id, reap_requested").WillReturnError(sql.ErrNoRows)
		fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM libvirt_runner_lifecycle").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runner_lifecycle").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		fixture.mock.ExpectExec("INSERT INTO libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.RegisterRunner(context.Background(), candidate)
	},
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
		f.mock.ExpectBegin()
		f.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runner_lifecycle").WillReturnError(errors.New("failed"))
		f.mock.ExpectRollback()
		return f.store.MarkRunnerBusy(context.Background(), "linux", "repo", RunnerIdentity{RequestID: 42, RunnerName: "runner"}, 1, time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		expectSQLiteIdentityMatch(f.mock, "runner_lifecycle")
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("failed"))
		f.mock.ExpectRollback()
		return f.store.MarkRunnerBusy(context.Background(), "linux", "repo", RunnerIdentity{RequestID: 42, RunnerName: "runner"}, 1, time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		expectSQLiteIdentityMatch(f.mock, "runner_lifecycle")
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(failingSQLResult{})
		f.mock.ExpectRollback()
		return f.store.MarkRunnerBusy(context.Background(), "linux", "repo", RunnerIdentity{RequestID: 42, RunnerName: "runner"}, 1, time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		f.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runner_lifecycle").WillReturnError(errors.New("failed"))
		f.mock.ExpectRollback()
		return f.store.RequestRunnerReap(context.Background(), "linux", "repo", RunnerIdentity{RequestID: 42, RunnerName: "runner"}, "reason", time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		expectSQLiteIdentityMatch(f.mock, "runner_lifecycle")
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("failed"))
		f.mock.ExpectRollback()
		return f.store.RequestRunnerReap(context.Background(), "linux", "repo", RunnerIdentity{RequestID: 42, RunnerName: "runner"}, "reason", time.Now())
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		expectSQLiteIdentityMatch(f.mock, "runner_lifecycle")
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(failingSQLResult{})
		f.mock.ExpectRollback()
		return f.store.RequestRunnerReap(context.Background(), "linux", "repo", RunnerIdentity{RequestID: 42, RunnerName: "runner"}, "reason", time.Now())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "runner_lifecycle")
		fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.RequestRunnerReap(context.Background(), "linux", "repo", RunnerIdentity{RequestID: 42, RunnerName: "runner"}, "reason", time.Now())
	},
}

func TestSQLiteRunnerSimpleTransitionsReportBoundaryFailures(t *testing.T) {
	assertSQLiteLifecycleFailures(t, sqliteRunnerSimpleTransitionFailures)
}

var sqliteRunnerBulkReapFailures = []func(demandStoreMock) error{
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("failed"))
		f.mock.ExpectRollback()
		_, err := f.store.RequestExpiredRunnerReaps(context.Background(), "linux", "repo", time.Now(), time.Now())
		return err
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(failingSQLResult{})
		f.mock.ExpectRollback()
		_, err := f.store.RequestExpiredRunnerReaps(context.Background(), "linux", "repo", time.Now(), time.Now())
		return err
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		f.mock.ExpectQuery("SELECT repository").WillReturnError(errors.New("failed"))
		f.mock.ExpectRollback()
		_, err := f.store.RequestExpiredRunnerReaps(context.Background(), "linux", "repo", time.Now(), time.Now())
		return err
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(errors.New("failed"))
		f.mock.ExpectRollback()
		_, err := f.store.RequestExcessWaitingRunnerReaps(context.Background(), "linux", "repo", -1, "reason", time.Now())
		return err
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(failingSQLResult{})
		f.mock.ExpectRollback()
		_, err := f.store.RequestExcessWaitingRunnerReaps(context.Background(), "linux", "repo", 0, "reason", time.Now())
		return err
	},
	func(f demandStoreMock) error {
		f.mock.ExpectBegin()
		f.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		f.mock.ExpectQuery("SELECT repository").WillReturnError(errors.New("failed"))
		f.mock.ExpectRollback()
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
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin().WillReturnError(errors.New("failed"))
		return fixture.store.ConfirmRunnerReaped(context.Background(), genericSQLiteRunner())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM runner_lifecycle").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.ConfirmRunnerReaped(context.Background(), genericSQLiteRunner())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "runner_lifecycle")
		expectSQLiteTerminalAuthoritativeDocker(fixture.mock)
		fixture.mock.ExpectExec("DELETE FROM runner_lifecycle").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.ConfirmRunnerReaped(context.Background(), genericSQLiteRunner())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "runner_lifecycle")
		expectSQLiteTerminalAuthoritativeDocker(fixture.mock)
		fixture.mock.ExpectExec("DELETE FROM runner_lifecycle").WillReturnResult(failingSQLResult{})
		fixture.mock.ExpectRollback()
		return fixture.store.ConfirmRunnerReaped(context.Background(), genericSQLiteRunner())
	},
	func(fixture demandStoreMock) error {
		candidate := genericSQLiteRunner()
		candidate.Provider = RunnerProviderLibvirt
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "runner_lifecycle")
		fixture.mock.ExpectQuery("SELECT provider, runner_name").WillReturnRows(sqlmock.NewRows(
			[]string{"provider", "runner_name", "request_id", "phase"},
		).AddRow("libvirt", candidate.RunnerName, candidate.RequestID, "reap_pending"))
		fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM libvirt_runner_lifecycle").WillReturnRows(
			sqlmock.NewRows([]string{"count"}).AddRow(0))
		fixture.mock.ExpectQuery("SELECT request_id FROM libvirt_runner_lifecycle").WillReturnRows(
			sqlmock.NewRows([]string{"request_id"}).AddRow(candidate.RequestID))
		fixture.mock.ExpectExec("DELETE FROM runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("DELETE FROM libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.ConfirmRunnerReaped(context.Background(), candidate)
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "runner_lifecycle")
		expectSQLiteTerminalAuthoritativeDocker(fixture.mock)
		fixture.mock.ExpectExec("DELETE FROM runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectCommit().WillReturnError(errors.New("failed"))
		return fixture.store.ConfirmRunnerReaped(context.Background(), genericSQLiteRunner())
	},
}

func TestSQLiteConfirmRunnerReapedReportsBoundaryFailures(t *testing.T) {
	assertSQLiteLifecycleFailures(t, sqliteConfirmRunnerReapedFailures)
}
