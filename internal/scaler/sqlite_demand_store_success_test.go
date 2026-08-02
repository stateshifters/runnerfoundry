package scaler

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLiteStoreOpensAndMigratesWithInMemoryDependencies(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	lock := &fakeStateDatabaseLock{locked: true}
	expectSchemaVersion(mock, currentStateSchemaVersion)
	mock.ExpectClose()
	dependencies := sqliteStoreDependencies{
		newLock:      func(string) stateDatabaseLock { return lock },
		openDatabase: func(string) (*sql.DB, error) { return database, nil },
	}
	original := productionSQLiteStoreDependencies
	t.Cleanup(func() { productionSQLiteStoreDependencies = original })
	productionSQLiteStoreDependencies = dependencies
	store, err := OpenSQLiteDemandStore("state.db")
	if err != nil || store == nil {
		t.Fatalf("store = %#v, %v", store, err)
	}
	if err = store.Close(); err != nil || !lock.unlocked {
		t.Fatalf("close = %v, unlocked = %t", err, lock.unlocked)
	}
	assertSQLExpectations(t, mock)
}

func TestSQLiteStoreReportsInMemoryLockAndOpenFailures(t *testing.T) {
	failure := errors.New("failed")
	for _, lock := range []*fakeStateDatabaseLock{{err: failure}, {locked: false}} {
		dependencies := sqliteStoreDependencies{
			newLock:      func(string) stateDatabaseLock { return lock },
			openDatabase: func(string) (*sql.DB, error) { return nil, failure },
		}
		if _, err := createSQLiteDemandStore("state.db", dependencies); err == nil {
			t.Fatal("expected lock failure")
		}
	}
	lock := &fakeStateDatabaseLock{locked: true}
	dependencies := sqliteStoreDependencies{
		newLock:      func(string) stateDatabaseLock { return lock },
		openDatabase: func(string) (*sql.DB, error) { return nil, failure },
	}
	if _, err := createSQLiteDemandStore("state.db", dependencies); err == nil || !lock.unlocked {
		t.Fatalf("open error = %v, unlocked = %t", err, lock.unlocked)
	}
}

func TestSQLiteStoreRunsEveryMigrationInMemory(t *testing.T) {
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 0)
	for _, statementCount := range []int{2, 2, 2, len(runnerLifecycleMigrationSteps), len(repositoryStateMigrationSteps)} {
		fixture.mock.ExpectBegin()
		for range statementCount {
			fixture.mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		}
		fixture.mock.ExpectCommit()
	}
	if err := fixture.store.migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteDemandMutationsSucceedInMemory(t *testing.T) {
	fixture := mockDemandStore(t)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("INSERT INTO demand").WillReturnResult(sqlmock.NewResult(1, 1))
	fixture.mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
	fixture.mock.ExpectCommit()
	err := fixture.store.Apply(context.Background(), "set",
		[]JobDemand{{Repository: "repo", RequestID: 1, QueueTime: time.Unix(1, 0).UTC()}},
		[]DemandRemoval{{Repository: "old", RequestID: 2}})
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func() error{
		func() error { return fixture.store.RemoveRepository(context.Background(), "set", "repo") },
		func() error { return fixture.store.RemoveScaleSet(context.Background(), "set") },
		func() error { return fixture.store.Delete(context.Background(), "set", "repo", 1) },
	} {
		fixture.mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
		if err = operation(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSQLiteDemandRowsDecodeInMemory(t *testing.T) {
	fixture := mockDemandStore(t)
	fixture.mock.ExpectQuery("SELECT repository").WillReturnRows(sqlmock.NewRows(
		[]string{"repository", "request_id", "queue_time", "acquired", "observation_order"},
	).AddRow("repo", 1, int64(2), true, 3))
	demands, err := fixture.store.Ordered(context.Background(), "set")
	if err != nil || len(demands) != 1 || demands[0].Repository != "repo" {
		t.Fatalf("demands = %#v, %v", demands, err)
	}
}

func TestSQLiteLibvirtLifecycleSucceedsInMemory(t *testing.T) {
	fixture := mockDemandStore(t)
	runner := sqliteLifecycleRunner()
	fixture.mock.ExpectExec("INSERT INTO libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(1, 1))
	if err := fixture.store.RegisterLibvirtRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	fixture.mock.ExpectBegin()
	for _, statement := range []string{"INSERT INTO libvirt_runner_lifecycle", "UPDATE libvirt_runner_lifecycle", "DELETE FROM demand"} {
		fixture.mock.ExpectExec(statement).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	fixture.mock.ExpectCommit()
	if err := fixture.store.CompleteJob(context.Background(), JobCompletion(runner)); err != nil {
		t.Fatal(err)
	}
	fixture.mock.ExpectQuery("SELECT scale_set").WillReturnRows(sqlmock.NewRows(
		[]string{"scale_set", "repository", "request_id", "runner_name"},
	).AddRow(runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName))
	reaps, err := fixture.store.PendingLibvirtReaps(context.Background(), runner.ScaleSet, runner.Repository)
	if err != nil || len(reaps) != 1 {
		t.Fatalf("reaps = %#v, %v", reaps, err)
	}
	fixture.mock.ExpectExec("DELETE FROM libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
	if err = fixture.store.ConfirmLibvirtReaped(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteRunnerLifecycleSucceedsInMemory(t *testing.T) {
	fixture := mockDemandStore(t)
	now := time.Unix(1, 2).UTC()
	runner := genericSQLiteRunner()
	runner.CreatedAt = now
	fixture.mock.ExpectExec("INSERT INTO runner_lifecycle").WillReturnResult(sqlmock.NewResult(1, 1))
	if err := fixture.store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func() error{
		func() error { return fixture.store.ConfirmRunnerWaiting(context.Background(), runner, now) },
		func() error {
			return fixture.store.MarkRunnerBusy(context.Background(), "linux", "repo", 42, "linux-42", 7, now)
		},
	} {
		fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		if err := operation(); err != nil {
			t.Fatal(err)
		}
	}
	fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
	fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := fixture.store.RequestRunnerReap(context.Background(), "linux", "repo", 42, "linux-42", "done", now); err != nil {
		t.Fatal(err)
	}
	assertBulkRunnerReapsSucceed(t, fixture, now)
}

func TestSQLiteRunnerCompletionAndQueriesSucceedInMemory(t *testing.T) {
	fixture := mockDemandStore(t)
	now := time.Unix(1, 2).UTC()
	fixture.mock.ExpectBegin()
	for _, statement := range []string{"UPDATE runner_lifecycle", "UPDATE libvirt_runner_lifecycle", "DELETE FROM demand"} {
		fixture.mock.ExpectExec(statement).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	fixture.mock.ExpectCommit()
	if err := fixture.store.CompleteRunnerLifecycleJob(context.Background(), sqliteRunnerCompletion, now); err != nil {
		t.Fatal(err)
	}
	runner := genericSQLiteRunner()
	operations := []func(context.Context, string, string) ([]RunnerLifecycle, error){
		fixture.store.PendingRunnerReaps,
		fixture.store.RunnerLifecycles,
	}
	for _, operation := range operations {
		fixture.mock.ExpectQuery("SELECT node_id").WillReturnRows(sqliteRunnerRows(now))
		runners, err := operation(context.Background(), "linux", "repo")
		if err != nil || len(runners) != 1 {
			t.Fatalf("runners = %#v, %v", runners, err)
		}
	}
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("DELETE FROM runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
	fixture.mock.ExpectCommit()
	if err := fixture.store.ConfirmRunnerReaped(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteLibvirtRunnerRegistrationSucceedsInMemory(t *testing.T) {
	fixture := mockDemandStore(t)
	runner := genericSQLiteRunner()
	fixture.mock.ExpectExec("INSERT INTO runner_lifecycle").WillReturnResult(sqlmock.NewResult(1, 1))
	fixture.mock.ExpectExec("INSERT INTO libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(1, 1))
	runner.Provider = RunnerProviderLibvirt
	if err := fixture.store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	fixture.mock.ExpectExec("INSERT INTO runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectQuery("SELECT provider").WillReturnRows(sqlmock.NewRows(
		[]string{"provider", "request_id", "assignment_id", "phase"},
	).AddRow("libvirt", runner.RequestID, runner.AssignmentID, "waiting_for_job"))
	if err := fixture.store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteLifecycleNoOpAndIdentityFreeCompletionSucceed(t *testing.T) {
	fixture := mockDemandStore(t)
	now := time.Unix(1, 2).UTC()
	fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := fixture.store.RequestRunnerReap(context.Background(), "linux", "repo", 42, "linux-42", "done", now); err != nil {
		t.Fatal(err)
	}
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
	fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
	fixture.mock.ExpectCommit()
	if err := fixture.store.CompleteRunnerLifecycleJob(context.Background(), JobCompletion{ScaleSet: "linux", Repository: "repo"}, now); err != nil {
		t.Fatal(err)
	}
	if unixNanoOrZero(now) == 0 || timeFromUnixNano(now.UnixNano()).IsZero() {
		t.Fatal("non-zero time was lost")
	}
	if err := requireLifecycleChange(sqlmock.NewResult(0, 0), "change lifecycle"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("no-op error = %v", err)
	}
}

func TestSQLiteLibvirtRegistrationRejectsReservedIdentityInMemory(t *testing.T) {
	fixture := mockDemandStore(t)
	runner := sqliteLifecycleRunner()
	fixture.mock.ExpectExec("INSERT INTO libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 0))
	fixture.mock.ExpectQuery("SELECT request_id").WillReturnRows(sqlmock.NewRows(
		[]string{"request_id", "reap_requested"},
	).AddRow(runner.RequestID+1, false))
	if err := fixture.store.RegisterLibvirtRunner(context.Background(), runner); err == nil {
		t.Fatal("expected reserved identity error")
	}
}

func TestCoordinatorConfiguresRunnerLifecycle(t *testing.T) {
	coordinator := NewDurableCoordinator("set", 1, createMemoryDemandStore())
	reconciler := &Reconciler{}
	if err := coordinator.ConfigureRunnerLifecycle(reconciler, "repo", RunnerProviderDocker); err != nil {
		t.Fatal(err)
	}
	if reconciler.ScaleSet.LifecycleScaleSet != "set" || reconciler.ScaleSet.Repository != "repo" {
		t.Fatalf("runtime = %#v", reconciler.ScaleSet)
	}
}

func TestSQLiteStoreClosesAfterRejectedFutureSchema(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	lock := &fakeStateDatabaseLock{locked: true}
	expectSchemaVersion(mock, currentStateSchemaVersion+1)
	mock.ExpectClose()
	dependencies := sqliteStoreDependencies{
		newLock:      func(string) stateDatabaseLock { return lock },
		openDatabase: func(string) (*sql.DB, error) { return database, nil },
	}
	if _, err = createSQLiteDemandStore("state.db", dependencies); err == nil || !lock.unlocked {
		t.Fatalf("future schema error = %v, unlocked = %t", err, lock.unlocked)
	}
	assertSQLExpectations(t, mock)
}

func TestProductionSQLiteDependenciesConstructWithoutIO(t *testing.T) {
	lock := productionSQLiteStoreDependencies.newLock("state.db.lock")
	if lock == nil {
		t.Fatal("missing production lock")
	}
	database, err := productionSQLiteStoreDependencies.openDatabase(":memory:")
	if err != nil || database == nil {
		t.Fatalf("database = %#v, %v", database, err)
	}
}

func assertBulkRunnerReapsSucceed(t *testing.T, fixture demandStoreMock, now time.Time) {
	t.Helper()
	for _, operation := range []func() (int, error){
		func() (int, error) {
			return fixture.store.RequestExpiredRunnerReaps(context.Background(), "linux", "repo", now, now)
		},
		func() (int, error) {
			return fixture.store.RequestExcessWaitingRunnerReaps(context.Background(), "linux", "repo", 1, "excess", now)
		},
	} {
		fixture.mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		fixture.mock.ExpectExec("UPDATE libvirt_runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
		if changed, err := operation(); err != nil || changed != 1 {
			t.Fatalf("changed = %d, %v", changed, err)
		}
	}
}

func sqliteRunnerRows(now time.Time) *sqlmock.Rows {
	return sqlmock.NewRows(sqliteRunnerLifecycleColumns).AddRow(
		"node", "linux", "repo", "docker", "linux-42", 42, 0, 0, 0, 0,
		"reap_pending", now.UnixNano(), now.UnixNano(), now.UnixNano(), "done",
	)
}

type fakeStateDatabaseLock struct {
	locked   bool
	err      error
	unlocked bool
}

func (lock *fakeStateDatabaseLock) TryLock() (bool, error) { return lock.locked, lock.err }
func (lock *fakeStateDatabaseLock) Unlock() error {
	lock.unlocked = true
	return nil
}

func assertSQLExpectations(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
