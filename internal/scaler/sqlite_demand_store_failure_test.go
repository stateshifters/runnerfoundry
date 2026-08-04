package scaler

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLiteLibvirtRegistrationReportsBoundaryFailures(t *testing.T) {
	runner := sqliteLifecycleRunner()
	operations := []func(demandStoreMock) error{
		func(fixture demandStoreMock) error {
			fixture.mock.ExpectExec("INSERT INTO libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
			return fixture.store.RegisterLibvirtRunner(context.Background(), runner)
		},
		func(fixture demandStoreMock) error {
			fixture.mock.ExpectExec("INSERT INTO libvirt_runner_lifecycle").WillReturnResult(failingSQLResult{})
			return fixture.store.RegisterLibvirtRunner(context.Background(), runner)
		},
		func(fixture demandStoreMock) error {
			fixture.mock.ExpectExec("INSERT INTO libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 0))
			fixture.mock.ExpectQuery("SELECT request_id").WillReturnError(errors.New("failed"))
			return fixture.store.RegisterLibvirtRunner(context.Background(), runner)
		},
	}
	assertSQLiteLifecycleFailures(t, operations)
}

func TestSQLiteLibvirtRegistrationAcceptsMatchingDuplicate(t *testing.T) {
	fixture := mockDemandStore(t)
	runner := sqliteLifecycleRunner()
	fixture.mock.ExpectExec("INSERT INTO libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectQuery("SELECT request_id").WillReturnRows(sqlmock.NewRows([]string{"request_id", "reap_requested"}).AddRow(runner.RequestID, false))
	if err := fixture.store.RegisterLibvirtRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteLibvirtCompletionReportsBoundaryFailures(t *testing.T) {
	runner := sqliteLifecycleRunner()
	operations := []func(demandStoreMock) error{
		func(fixture demandStoreMock) error {
			fixture.mock.ExpectBegin().WillReturnError(errors.New("failed"))
			return fixture.store.CompleteJob(context.Background(), JobCompletion{
				ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerIdentity: runner.Identity(),
			})
		},
		func(fixture demandStoreMock) error {
			fixture.mock.ExpectBegin()
			fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
			fixture.mock.ExpectRollback()
			return fixture.store.CompleteJob(context.Background(), JobCompletion{
				ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerIdentity: runner.Identity(),
			})
		},
		func(fixture demandStoreMock) error {
			return failedSQLiteCompletion(fixture, runner, "UPDATE libvirt_runner_lifecycle")
		},
		func(fixture demandStoreMock) error {
			return failedSQLiteCompletion(fixture, runner, "DELETE FROM demand")
		},
		func(fixture demandStoreMock) error { return failedSQLiteCompletion(fixture, runner, "COMMIT") },
	}
	assertSQLiteLifecycleFailures(t, operations)
}

func TestSQLitePendingLibvirtReapsReportsBoundaryFailures(t *testing.T) {
	operations := []func(demandStoreMock) error{
		func(fixture demandStoreMock) error {
			fixture.mock.ExpectQuery("SELECT scale_set").WillReturnError(errors.New("failed"))
			_, err := fixture.store.PendingLibvirtReaps(context.Background(), "linux", "repo")
			return err
		},
		func(fixture demandStoreMock) error {
			fixture.mock.ExpectQuery("SELECT scale_set").WillReturnRows(sqlmock.NewRows([]string{"scale_set", "repository", "request_id", "runner_name"}).AddRow("linux", "repo", "bad", "linux-42"))
			_, err := fixture.store.PendingLibvirtReaps(context.Background(), "linux", "repo")
			return err
		},
		func(fixture demandStoreMock) error {
			fixture.mock.ExpectQuery("SELECT scale_set").WillReturnRows(sqlmock.NewRows([]string{"scale_set", "repository", "request_id", "runner_name"}).AddRow("linux", "repo", 42, "linux-42").RowError(0, errors.New("failed")))
			_, err := fixture.store.PendingLibvirtReaps(context.Background(), "linux", "repo")
			return err
		},
	}
	assertSQLiteLifecycleFailures(t, operations)
}

func TestSQLitePendingLibvirtReapsReportsRowCloseFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	closeErr := errors.New("close failed")
	rows := sqlmock.NewRows([]string{"scale_set", "repository", "request_id", "runner_name"}).
		AddRow("linux", "repo", 42, "linux-42").CloseError(closeErr)
	fixture.mock.ExpectQuery("SELECT scale_set").WillReturnRows(rows)
	if _, err := fixture.store.PendingLibvirtReaps(context.Background(), "linux", "repo"); !errors.Is(err, closeErr) {
		t.Fatalf("pending reap error = %v, want row close failure", err)
	}
}

func TestSQLitePendingLibvirtReapsJoinsCloseFailureAfterSuccessfulScan(t *testing.T) {
	fixture := mockDemandStore(t)
	closeErr := errors.New("close failed")
	rows := sqlmock.NewRows([]string{"scale_set", "repository", "request_id", "runner_name"}).
		AddRow("linux", "repo", 42, "linux-42").CloseError(closeErr)
	fixture.mock.ExpectQuery("SELECT scale_set").WillReturnRows(rows)
	if _, err := fixture.store.PendingLibvirtReaps(context.Background(), "linux", "repo"); !errors.Is(err, closeErr) {
		t.Fatalf("pending reap error = %v", err)
	}
}

func TestSQLiteConfirmLibvirtReapedReportsBoundaryFailures(t *testing.T) {
	assertSQLiteLifecycleFailures(t, sqliteConfirmLibvirtReapedFailureOperations)
}

var sqliteConfirmLibvirtReapedFailureOperations = []func(demandStoreMock) error{
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin().WillReturnError(errors.New("failed"))
		return fixture.store.ConfirmLibvirtReaped(context.Background(), sqliteLifecycleRunner())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.ConfirmLibvirtReaped(context.Background(), sqliteLifecycleRunner())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "libvirt_runner_lifecycle")
		fixture.mock.ExpectExec("DELETE FROM libvirt_runner_lifecycle").WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.ConfirmLibvirtReaped(context.Background(), sqliteLifecycleRunner())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "libvirt_runner_lifecycle")
		fixture.mock.ExpectExec("DELETE FROM libvirt_runner_lifecycle").WillReturnResult(failingSQLResult{})
		fixture.mock.ExpectRollback()
		return fixture.store.ConfirmLibvirtReaped(context.Background(), sqliteLifecycleRunner())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "libvirt_runner_lifecycle")
		fixture.mock.ExpectExec("DELETE FROM libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 0))
		fixture.mock.ExpectRollback()
		return fixture.store.ConfirmLibvirtReaped(context.Background(), sqliteLifecycleRunner())
	},
	func(fixture demandStoreMock) error {
		fixture.mock.ExpectBegin()
		expectSQLiteIdentityMatch(fixture.mock, "libvirt_runner_lifecycle")
		fixture.mock.ExpectExec("DELETE FROM libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectCommit().WillReturnError(errors.New("failed"))
		return fixture.store.ConfirmLibvirtReaped(context.Background(), sqliteLifecycleRunner())
	},
}

func failedSQLiteCompletion(fixture demandStoreMock, runner LibvirtRunnerLifecycle, failurePoint string) error {
	completion := JobCompletion{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerIdentity: runner.Identity(),
	}
	completion.RunnerName = ""
	fixture.mock.ExpectBegin()
	expectSQLiteIdentityMatch(fixture.mock, "libvirt_runner_lifecycle")
	if failurePoint == "UPDATE libvirt_runner_lifecycle" {
		fixture.mock.ExpectExec(failurePoint).WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.CompleteJob(context.Background(), completion)
	}
	fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
	if failurePoint == "DELETE FROM demand" {
		fixture.mock.ExpectExec(failurePoint).WillReturnError(errors.New("failed"))
		fixture.mock.ExpectRollback()
		return fixture.store.CompleteJob(context.Background(), completion)
	}
	fixture.mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
	fixture.mock.ExpectCommit().WillReturnError(errors.New("failed"))
	return fixture.store.CompleteJob(context.Background(), completion)
}

func expectSQLiteIdentityMatch(mock sqlmock.Sqlmock, table string) {
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM " + table).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
}

func assertSQLiteLifecycleFailures(t *testing.T, operations []func(demandStoreMock) error) {
	t.Helper()
	for index, operation := range operations {
		t.Run(fmt.Sprintf("failure-%d", index), func(t *testing.T) {
			if err := operation(mockDemandStore(t)); err == nil {
				t.Fatal("expected failure")
			}
		})
	}
}

func sqliteLifecycleRunner() LibvirtRunnerLifecycle {
	return LibvirtRunnerLifecycle{ScaleSet: "linux", Repository: "repo", RequestID: 42, RunnerName: "linux-42"}
}

type failingSQLResult struct{}

func (failingSQLResult) LastInsertId() (int64, error) { return 0, errors.New("failed") }
func (failingSQLResult) RowsAffected() (int64, error) { return 0, errors.New("failed") }

var _ driver.Result = failingSQLResult{}

func TestSQLiteDemandStoreReportsDatabaseConfigurationFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	fixture.mock.ExpectExec("PRAGMA journal_mode").WillReturnError(errors.New("configure failed"))
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected database configuration error")
	}
}

func TestSQLiteDemandStoreReportsSchemaVersionReadFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	fixture.mock.ExpectExec("PRAGMA journal_mode").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectQuery("PRAGMA user_version").WillReturnError(errors.New("read failed"))
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected schema read error")
	}
}

func TestSQLiteDemandStoreReportsMigrationBeginFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 0)
	fixture.mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected migration begin error")
	}
}

func TestSQLiteDemandStoreReportsSchemaWriteAndRollbackFailures(t *testing.T) {
	fixture := mockDemandStore(t)
	writeErr := errors.New("write failed")
	rollbackErr := errors.New("rollback failed")
	expectSchemaVersion(fixture.mock, 0)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("CREATE TABLE demand").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectExec("PRAGMA user_version").WillReturnError(writeErr)
	fixture.mock.ExpectRollback().WillReturnError(rollbackErr)
	err := fixture.store.migrate(context.Background())
	if !errors.Is(err, writeErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("migration error = %v, want write and rollback failures", err)
	}
}

func TestSQLiteDemandStoreReportsMigrationCommitFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 0)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("CREATE TABLE demand").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectExec("PRAGMA user_version").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
	assertMigrationCommitFailure(t, fixture.store)
}

func TestSQLiteDemandStoreReportsLegacyMigrationBeginFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 1)
	fixture.mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected legacy migration begin error")
	}
}

func TestSQLiteDemandStoreReportsLegacyDemandRemovalFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 1)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("DELETE FROM demand").WillReturnError(errors.New("delete failed"))
	fixture.mock.ExpectRollback()
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected legacy demand removal error")
	}
}

func TestSQLiteDemandStoreReportsLegacyVersionWriteFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 1)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
	fixture.mock.ExpectExec("PRAGMA user_version").WillReturnError(errors.New("write failed"))
	fixture.mock.ExpectRollback()
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected legacy version write error")
	}
}

func TestSQLiteDemandStoreReportsLegacyMigrationCommitFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 1)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
	fixture.mock.ExpectExec("PRAGMA user_version").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
	assertMigrationCommitFailure(t, fixture.store)
}

func TestSQLiteDemandStoreReportsLibvirtTableCreationFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 2)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("CREATE TABLE libvirt_runner_lifecycle").WillReturnError(errors.New("create failed"))
	fixture.mock.ExpectRollback()
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected libvirt lifecycle table creation failure")
	}
}

func TestSQLiteDemandStoreReportsLibvirtVersionWriteFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 2)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("CREATE TABLE libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectExec("PRAGMA user_version").WillReturnError(errors.New("write failed"))
	fixture.mock.ExpectRollback()
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected libvirt lifecycle version write failure")
	}
}

var genericLifecycleMigrationPatterns = []string{
	"CREATE TABLE runner_lifecycle", "CREATE INDEX runner_lifecycle_pending_reaps", "INSERT INTO runner_lifecycle",
	"CREATE TRIGGER mirror_libvirt_runner_lifecycle_insert", "CREATE TRIGGER mirror_libvirt_runner_lifecycle_update",
	"CREATE TRIGGER mirror_libvirt_runner_lifecycle_delete", "PRAGMA user_version = 4",
}

func TestSQLiteDemandStoreReportsGenericLifecycleMigrationStepFailures(t *testing.T) {
	for failedIndex := range genericLifecycleMigrationPatterns {
		t.Run(fmt.Sprintf("step-%d", failedIndex), func(t *testing.T) {
			assertGenericLifecycleMigrationStepFailure(t, failedIndex)
		})
	}
}

func TestSQLiteDemandStoreReportsRepositoryStateMigrationStepFailures(t *testing.T) {
	for failedIndex := range repositoryStateMigrationSteps {
		t.Run(fmt.Sprintf("step-%d", failedIndex), func(t *testing.T) {
			assertRepositoryStateMigrationStepFailure(t, failedIndex)
		})
	}
}

func TestSQLiteDemandStoreReportsCapacityObligationMigrationStepFailures(t *testing.T) {
	for failedIndex := range capacityObligationMigrationSteps {
		t.Run(fmt.Sprintf("step-%d", failedIndex), func(t *testing.T) {
			assertCapacityObligationMigrationStepFailure(t, failedIndex)
		})
	}
}

func assertCapacityObligationMigrationStepFailure(t *testing.T, failedIndex int) {
	t.Helper()
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 5)
	fixture.mock.ExpectBegin()
	expectCapacityObligationMigrationFailure(fixture.mock, failedIndex)
	fixture.mock.ExpectRollback()
	err := fixture.store.migrate(context.Background())
	if err == nil || !strings.Contains(err.Error(), capacityObligationMigrationSteps[failedIndex].operation) {
		t.Fatalf("capacity-obligation migration error = %v", err)
	}
}

func expectCapacityObligationMigrationFailure(mock sqlmock.Sqlmock, failedIndex int) {
	for index, step := range capacityObligationMigrationSteps {
		expectation := mock.ExpectExec(regexp.QuoteMeta(step.statement))
		if index == failedIndex {
			expectation.WillReturnError(errors.New("failed"))
			return
		}
		expectation.WillReturnResult(sqlmock.NewResult(0, 0))
	}
}

func assertRepositoryStateMigrationStepFailure(t *testing.T, failedIndex int) {
	t.Helper()
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 4)
	fixture.mock.ExpectBegin()
	for index, step := range repositoryStateMigrationSteps {
		expectation := fixture.mock.ExpectExec(regexp.QuoteMeta(step.statement))
		if index == failedIndex {
			expectation.WillReturnError(errors.New("failed"))
			break
		}
		expectation.WillReturnResult(sqlmock.NewResult(0, 0))
	}
	fixture.mock.ExpectRollback()
	err := fixture.store.migrate(context.Background())
	if err == nil || !strings.Contains(err.Error(), repositoryStateMigrationSteps[failedIndex].operation) {
		t.Fatalf("repository-state migration error = %v", err)
	}
}

func assertGenericLifecycleMigrationStepFailure(t *testing.T, failedIndex int) {
	t.Helper()
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 3)
	fixture.mock.ExpectBegin()
	for index, pattern := range genericLifecycleMigrationPatterns {
		expectation := fixture.mock.ExpectExec(pattern)
		if index == failedIndex {
			expectation.WillReturnError(errors.New("failed"))
			break
		}
		expectation.WillReturnResult(sqlmock.NewResult(0, 0))
	}
	fixture.mock.ExpectRollback()
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected generic lifecycle migration failure")
	}
}

func TestSQLiteDemandStoreReportsGenericLifecycleMigrationBeginFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 3)
	fixture.mock.ExpectBegin().WillReturnError(errors.New("failed"))
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected generic lifecycle begin failure")
	}
}

func TestSQLiteDemandStoreReportsGenericLifecycleMigrationCommitFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 3)
	fixture.mock.ExpectBegin()
	for _, pattern := range genericLifecycleMigrationPatterns {
		fixture.mock.ExpectExec(pattern).WillReturnResult(sqlmock.NewResult(0, 0))
	}
	fixture.mock.ExpectCommit().WillReturnError(errors.New("failed"))
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected generic lifecycle commit failure")
	}
}

func TestSQLiteDemandStoreReportsDemandCommitFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
	if err := fixture.store.Apply(context.Background(), "linux", nil, nil); err == nil {
		t.Fatal("expected demand commit error")
	}
}

func TestSQLiteDemandStoreReportsRowCloseFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	closeErr := errors.New("close failed")
	rows := sqlmock.NewRows([]string{"demand_id", "repository", "request_id", "queue_time", "acquired", "observation_order", "source", "assigned_observation_id", "controller_epoch", "attempt", "next_attempt_at"}).
		AddRow(1, "repo", "invalid", 0, false, 1, "job_available", 0, 0, 0, 0).
		CloseError(closeErr)
	fixture.mock.ExpectQuery("SELECT demand_id").WillReturnRows(rows)
	if _, err := fixture.store.Ordered(context.Background(), "linux"); !errors.Is(err, closeErr) {
		t.Fatalf("scan error = %v, want row close failure", err)
	}
}

func TestSQLiteDemandStoreReportsRowIterationFailure(t *testing.T) {
	fixture := mockDemandStore(t)
	rows := sqlmock.NewRows([]string{"demand_id", "repository", "request_id", "queue_time", "acquired", "observation_order", "source", "assigned_observation_id", "controller_epoch", "attempt", "next_attempt_at"}).
		AddRow(1, "repo", 1, 0, false, 1, "job_available", 0, 0, 0, 0).
		RowError(0, errors.New("iteration failed"))
	fixture.mock.ExpectQuery("SELECT demand_id").WillReturnRows(rows)
	if _, err := fixture.store.Ordered(context.Background(), "linux"); err == nil {
		t.Fatal("expected iteration error")
	}
}

type demandStoreMock struct {
	store *SQLiteDemandStore
	mock  sqlmock.Sqlmock
}

func mockDemandStore(t *testing.T) demandStoreMock {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mock.ExpectClose()
		if err := database.Close(); err != nil {
			t.Error(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	return demandStoreMock{store: &SQLiteDemandStore{database: database}, mock: mock}
}

func expectSchemaVersion(mock sqlmock.Sqlmock, version int) {
	mock.ExpectExec("PRAGMA journal_mode").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(version))
}

func assertMigrationCommitFailure(t *testing.T, store *SQLiteDemandStore) {
	t.Helper()
	if err := store.migrate(context.Background()); err == nil {
		t.Fatal("expected migration commit error")
	}
}
