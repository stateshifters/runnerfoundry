package scaler

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLiteMessageUpdateRejectsStaleEpochBeforeMutation(t *testing.T) {
	fixture := newSQLiteMessageUpdateFixture(t)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectQuery("SELECT 1 FROM repository_controls").WillReturnRows(sqlmock.NewRows([]string{"current"}))
	fixture.mock.ExpectRollback()

	_, err := fixture.store.ApplyMessageUpdate(context.Background(), validSQLiteMessageUpdate())
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("message update error = %v, want %v", err, ErrNotLeader)
	}
}

func TestSQLiteMessageUpdateRollsBackDemandFailure(t *testing.T) {
	assertSQLiteMessageRollback(t, sqliteMessageUpdateWithDemand(), func(mock sqlmock.Sqlmock, failure error) {
		mock.ExpectExec("INSERT INTO demand").WillReturnError(failure)
	})
}

func TestSQLiteMessageUpdateRollsBackLifecycleFailure(t *testing.T) {
	assertSQLiteMessageRollback(t, sqliteMessageUpdateWithStart(), func(mock sqlmock.Sqlmock, failure error) {
		mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(failure)
	})
}

func TestSQLiteMessageUpdateRollsBackCompletionFailure(t *testing.T) {
	assertSQLiteMessageRollback(t, sqliteMessageUpdateWithCompletion(), func(mock sqlmock.Sqlmock, failure error) {
		mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(failure)
	})
}

func TestSQLiteMessageUpdateRollsBackStatisticsWithPriorEffects(t *testing.T) {
	assertSQLiteMessageRollback(t, sqliteMessageUpdateWithDemandAndStart(), expectSQLiteMessageStatisticsFailure)
}

func expectSQLiteMessageStatisticsFailure(mock sqlmock.Sqlmock, failure error) {
	mock.ExpectExec("INSERT INTO demand").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT controller_epoch").WillReturnRows(sqlmock.NewRows([]string{"controller_epoch", "observed_at"}))
	mock.ExpectExec("INSERT INTO assigned_capacity_observations").WillReturnError(failure)
}

func assertSQLiteMessageRollback(
	t *testing.T,
	update MessageUpdate,
	expectFailure func(sqlmock.Sqlmock, error),
) {
	t.Helper()
	failure := errors.New("failed")
	fixture := newSQLiteMessageUpdateFixture(t)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectQuery("SELECT 1 FROM repository_controls").
		WillReturnRows(sqlmock.NewRows([]string{"current"}).AddRow(1))
	expectFailure(fixture.mock, failure)
	fixture.mock.ExpectRollback()

	if _, err := fixture.store.ApplyMessageUpdate(context.Background(), update); !errors.Is(err, failure) {
		t.Fatalf("message update error = %v, want %v", err, failure)
	}
}

func TestSQLiteMessageUpdateReconcilesStatisticsAgainstPostCompletionCapacity(t *testing.T) {
	ctx := context.Background()
	observedAt := time.Unix(200, 0).UTC()
	store := openSQLiteCapacityStore(t)
	controllerEpoch := seedSQLitePostCompletionState(ctx, t, store, observedAt)
	update := MessageUpdate{
		AppID: 7, Repository: "repo", ScaleSet: "linux", ControllerEpoch: controllerEpoch,
		AssignedJobs: 1, ActiveCapacity: 1, ObservedAt: observedAt,
		Completed: []RunnerMessageIdentity{{RequestID: 42, RunnerName: "linux-42"}},
	}
	result, err := store.ApplyMessageUpdate(ctx, update)
	if err != nil {
		t.Fatal(err)
	}
	if result.AssignedObligations.Create != 1 {
		t.Fatalf("post-completion obligation decision = %#v", result.AssignedObligations)
	}
	assertPostCompletionObligation(ctx, t, store, observedAt)
}

func seedSQLitePostCompletionState(
	ctx context.Context,
	t *testing.T,
	store *SQLiteDemandStore,
	observedAt time.Time,
) int64 {
	t.Helper()
	snapshot, err := store.StartController(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	registerSQLiteMessageRunner(ctx, t, store, "repo", "linux-42", 42, observedAt)
	registerSQLiteMessageRunner(ctx, t, store, "other", "other-linux-1", 1, observedAt)
	return snapshot.Control.ControllerEpoch
}

func registerSQLiteMessageRunner(
	ctx context.Context,
	t *testing.T,
	store *SQLiteDemandStore,
	repository string,
	runnerName string,
	requestID int64,
	observedAt time.Time,
) {
	t.Helper()
	if err := store.RegisterRunner(ctx, RunnerLifecycle{
		NodeID: "local", ScaleSet: "linux", Repository: repository, Provider: RunnerProviderDocker,
		RunnerName: runnerName, RequestID: requestID, Phase: RunnerBusy, CreatedAt: observedAt.Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
}

func assertPostCompletionObligation(
	ctx context.Context,
	t *testing.T,
	store *SQLiteDemandStore,
	observedAt time.Time,
) {
	t.Helper()
	ready, err := store.OrderedReady(ctx, "linux", observedAt)
	if err != nil || len(ready) != 1 || ready[0].Source != CapacityObligationAssignedCount {
		t.Fatalf("post-completion obligations = %#v, %v", ready, err)
	}
}

type sqliteMessageUpdateTestFixture struct {
	store    *SQLiteDemandStore
	database *sql.DB
	mock     sqlmock.Sqlmock
}

func newSQLiteMessageUpdateFixture(t *testing.T) *sqliteMessageUpdateTestFixture {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	fixture := &sqliteMessageUpdateTestFixture{
		store: &SQLiteDemandStore{database: database}, database: database, mock: mock,
	}
	t.Cleanup(func() {
		fixture.mock.ExpectClose()
		if closeErr := fixture.database.Close(); closeErr != nil {
			t.Errorf("close message-update database: %v", closeErr)
		}
		assertSQLExpectations(t, fixture.mock)
	})
	return fixture
}

func validSQLiteMessageUpdate() MessageUpdate {
	return MessageUpdate{
		AppID: 7, Repository: "repo", ScaleSet: "linux", ControllerEpoch: 3,
		ObservedAt: time.Unix(100, 0).UTC(),
	}
}

func sqliteMessageUpdateWithDemand() MessageUpdate {
	update := validSQLiteMessageUpdate()
	update.Available = []JobDemand{{Repository: "repo", RequestID: 42, QueueTime: update.ObservedAt}}
	return update
}

func sqliteMessageUpdateWithStart() MessageUpdate {
	update := validSQLiteMessageUpdate()
	update.Started = []RunnerMessageIdentity{{RequestID: 42, RunnerName: "linux-42"}}
	return update
}

func sqliteMessageUpdateWithCompletion() MessageUpdate {
	update := validSQLiteMessageUpdate()
	update.Completed = []RunnerMessageIdentity{{RequestID: 42, RunnerName: "linux-42"}}
	return update
}

func sqliteMessageUpdateWithDemandAndStart() MessageUpdate {
	update := sqliteMessageUpdateWithDemand()
	update.Started = []RunnerMessageIdentity{{RequestID: 42, RunnerName: "linux-42"}}
	return update
}
