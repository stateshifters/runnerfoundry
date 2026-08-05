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
		expectNoExistingSQLiteExactObligation(mock)
		mock.ExpectExec("INSERT INTO demand").WillReturnError(failure)
	})
}

func TestSQLiteMessageUpdateRollsBackLifecycleFailure(t *testing.T) {
	assertSQLiteMessageRollback(t, sqliteMessageUpdateWithStart(), func(mock sqlmock.Sqlmock, failure error) {
		mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 0))
		expectSQLiteIdentityMatch(mock, "runner_lifecycle")
		mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(failure)
	})
}

func TestSQLiteMessageUpdateRollsBackCompletionFailure(t *testing.T) {
	assertSQLiteMessageRollback(t, sqliteMessageUpdateWithCompletion(), func(mock sqlmock.Sqlmock, failure error) {
		mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 0))
		expectSQLiteIdentityMatch(mock, "runner_lifecycle")
		mock.ExpectExec("UPDATE runner_lifecycle").WillReturnError(failure)
	})
}

func TestSQLiteMessageUpdateRollsBackStatisticsWithPriorEffects(t *testing.T) {
	assertSQLiteMessageRollback(t, sqliteMessageUpdateWithDemandAndStart(), expectSQLiteMessageStatisticsFailure)
}

func TestSQLiteMessageUpdateTreatsMissingLifecycleAsIdempotent(t *testing.T) {
	for name, applyIdentity := range map[string]func(*MessageUpdate, RunnerMessageIdentity){
		"started": func(update *MessageUpdate, identity RunnerMessageIdentity) {
			update.Started = []RunnerMessageIdentity{identity}
		},
		"completed": func(update *MessageUpdate, identity RunnerMessageIdentity) {
			update.Completed = []RunnerMessageIdentity{identity}
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := openSQLiteCapacityStore(t)
			control, err := store.StartController(ctx, 7)
			if err != nil {
				t.Fatal(err)
			}
			update := validSQLiteMessageUpdate()
			update.ControllerEpoch = control.Control.ControllerEpoch
			applyIdentity(&update, RunnerMessageIdentity{
				RunnerIdentity: RunnerIdentity{RequestID: 42, RunnerName: "missing-runner"},
			})
			if _, err = store.ApplyMessageUpdate(ctx, update); err != nil {
				t.Fatalf("missing %s lifecycle error = %v", name, err)
			}
		})
	}
}

func TestSQLiteMessageUpdateRollsBackRejectedIdentity(t *testing.T) {
	tests := []struct {
		name     string
		runners  []RunnerIdentity
		identity RunnerIdentity
		want     error
	}{
		{
			name: "mismatch",
			runners: []RunnerIdentity{
				{RequestID: 42, RunnerName: "runner-one"},
				{RequestID: 43, RunnerName: "runner-two"},
			},
			identity: RunnerIdentity{RequestID: 42, RunnerName: "runner-two"},
			want:     ErrRunnerIdentityMismatch,
		},
		{
			name: "invariant",
			runners: []RunnerIdentity{
				{RequestID: 42, RunnerName: "runner-one"},
				{RequestID: 42, RunnerName: "runner-two"},
			},
			identity: RunnerIdentity{RequestID: 42},
			want:     ErrRunnerIdentityInvariant,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertSQLiteMessageRejectedIdentityRollsBack(t, test.runners, test.identity, test.want)
		})
	}
}

func assertSQLiteMessageRejectedIdentityRollsBack(
	t *testing.T,
	runnerIdentities []RunnerIdentity,
	messageIdentity RunnerIdentity,
	wantedError error,
) {
	t.Helper()
	store, controllerEpoch, observedAt := seedSQLiteRejectedIdentityMessageState(t, runnerIdentities)
	update := validSQLiteMessageUpdate()
	update.ControllerEpoch = controllerEpoch
	update.Started = []RunnerMessageIdentity{{RunnerIdentity: messageIdentity}}
	_, err := store.ApplyMessageUpdate(context.Background(), update)
	if !errors.Is(err, wantedError) {
		t.Fatalf("message identity error = %v, want %v", err, wantedError)
	}
	assertSQLiteMessageDemandUnchanged(t, store, observedAt)
	assertSQLiteMessageRunnersRemainWaiting(t, store)
}

func seedSQLiteRejectedIdentityMessageState(
	t *testing.T,
	runnerIdentities []RunnerIdentity,
) (*SQLiteDemandStore, int64, time.Time) {
	t.Helper()
	ctx := context.Background()
	store := openSQLiteCapacityStore(t)
	control, err := store.StartController(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Unix(100, 0).UTC()
	if err = store.Apply(ctx, "linux", []JobDemand{{
		Repository: "repo", RequestID: 42, QueueTime: observedAt,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	for _, identity := range runnerIdentities {
		registerSQLiteMessageIdentityRunner(t, store, identity, observedAt)
	}
	return store, control.Control.ControllerEpoch, observedAt
}

func assertSQLiteMessageDemandUnchanged(t *testing.T, store *SQLiteDemandStore, observedAt time.Time) {
	t.Helper()
	demand, err := store.Ordered(context.Background(), "linux")
	if err != nil || len(demand) != 1 || demand[0].RequestID != 42 || !demand[0].QueueTime.Equal(observedAt) {
		t.Fatalf("rolled-back demand = %#v, %v", demand, err)
	}
}

func assertSQLiteMessageRunnersRemainWaiting(t *testing.T, store *SQLiteDemandStore) {
	t.Helper()
	runners, err := store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil {
		t.Fatal(err)
	}
	for _, runner := range runners {
		if runner.Phase != RunnerWaitingForJob {
			t.Fatalf("runner mutated after rejected identity: %#v", runner)
		}
	}
}

func registerSQLiteMessageIdentityRunner(
	t *testing.T,
	store *SQLiteDemandStore,
	identity RunnerIdentity,
	createdAt time.Time,
) {
	t.Helper()
	seedSQLiteGenericLifecycle(t, store.database, RunnerLifecycle{
		ScaleSet: "linux", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: identity.RunnerName, RequestID: identity.RequestID,
		Phase: RunnerWaitingForJob, CreatedAt: createdAt,
	})
}

func expectSQLiteMessageStatisticsFailure(mock sqlmock.Sqlmock, failure error) {
	expectNoExistingSQLiteExactObligation(mock)
	mock.ExpectExec("INSERT INTO demand").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM demand").WillReturnResult(sqlmock.NewResult(0, 1))
	expectSQLiteIdentityMatch(mock, "runner_lifecycle")
	mock.ExpectExec("UPDATE runner_lifecycle").WillReturnResult(sqlmock.NewResult(0, 1))
	expectSQLiteDockerMirrorReconciliation(mock)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT controller_epoch").WillReturnRows(sqlmock.NewRows([]string{"controller_epoch", "observed_at"}))
	mock.ExpectExec("INSERT INTO assigned_capacity_observations").WillReturnError(failure)
}

func expectNoExistingSQLiteExactObligation(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM demand").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
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
		Completed: []RunnerMessageIdentity{{
			RunnerIdentity: RunnerIdentity{RequestID: 42, RunnerName: "linux-42"},
		}},
	}
	result := applySQLiteMessageUpdate(ctx, t, store, update)
	if result.AssignedObligations.Create != 1 {
		t.Fatalf("post-completion obligation decision = %#v", result.AssignedObligations)
	}
	assertPostCompletionObligation(ctx, t, store, observedAt)
}

func TestSQLiteMessageUpdateReturnsCommittedQueueSnapshot(t *testing.T) {
	ctx := context.Background()
	store := openSQLiteCapacityStore(t)
	snapshot, err := store.StartController(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Unix(300, 0).UTC()
	update := MessageUpdate{
		AppID: 7, Repository: "repo", ScaleSet: "linux", ControllerEpoch: snapshot.Control.ControllerEpoch,
		AssignedJobs: 2, ObservedAt: observedAt,
		Available: []JobDemand{
			{Repository: "repo", RequestID: 42, QueueTime: observedAt.Add(-time.Second)},
			{Repository: "repo", RequestID: 42, QueueTime: observedAt.Add(-time.Second)},
		},
	}
	result, err := store.ApplyMessageUpdate(ctx, update)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLiteQueueSnapshot(t, result.QueueSnapshot, 1, 2, 3, 0, update.Available[0].QueueTime)

	result = applySQLiteMessageUpdate(ctx, t, store, update)
	assertSQLiteQueueSnapshot(t, result.QueueSnapshot, 1, 2, 0, 0, update.Available[0].QueueTime)

	update.Available = nil
	update.AssignedJobs = 0
	update.ObservedAt = observedAt.Add(time.Second)
	result = applySQLiteMessageUpdate(ctx, t, store, update)
	assertSQLiteQueueSnapshot(t, result.QueueSnapshot, 1, 0, 0, 2, observedAt.Add(-time.Second))
}

func TestSQLiteMessageUpdateSnapshotExcludesLifecycleOwnedAggregateFromRunnableAge(t *testing.T) {
	ctx := context.Background()
	store := openSQLiteCapacityStore(t)
	observedAt := time.Unix(320, 0).UTC()
	controllerEpoch := seedLifecycleOwnedSQLiteObligation(ctx, t, store, observedAt)
	result := applySQLiteMessageUpdate(ctx, t, store, MessageUpdate{
		AppID: 7, Repository: "repo", ScaleSet: "linux", ControllerEpoch: controllerEpoch,
		AssignedJobs: 1, ObservedAt: observedAt.Add(time.Second),
	})
	if result.QueueSnapshot.AssignedCount != 1 || result.QueueSnapshot.JobAvailable != 0 ||
		result.QueueSnapshot.NewlyQueued != 0 || result.QueueSnapshot.Canceled != 0 ||
		result.QueueSnapshot.OldestRunnableAt != nil {
		t.Fatalf("lifecycle-owned queue snapshot = %#v", result.QueueSnapshot)
	}
}

func seedLifecycleOwnedSQLiteObligation(
	ctx context.Context,
	t *testing.T,
	store *SQLiteDemandStore,
	observedAt time.Time,
) int64 {
	t.Helper()
	control, err := store.StartController(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	observation := AssignedCapacityObservation{
		ScaleSet: "linux", Repository: "repo", ControllerEpoch: control.Control.ControllerEpoch,
		AssignedJobs: 1, ObservedAt: observedAt,
	}
	if _, err = store.ObserveAssignedCapacity(ctx, observation, 0); err != nil {
		t.Fatal(err)
	}
	obligations, err := store.OrderedReady(ctx, "linux", observedAt)
	if err != nil || len(obligations) != 1 {
		t.Fatalf("assigned obligation = %#v, %v", obligations, err)
	}
	if err = store.RegisterRunner(ctx, RunnerLifecycle{
		NodeID: "local", ScaleSet: "linux", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "linux-1", AssignmentID: obligations[0].ObligationID,
		Phase: RunnerWaitingForJob, CreatedAt: observedAt,
	}); err != nil {
		t.Fatal(err)
	}
	return control.Control.ControllerEpoch
}

func applySQLiteMessageUpdate(
	ctx context.Context,
	t *testing.T,
	store *SQLiteDemandStore,
	update MessageUpdate,
) MessageUpdateResult {
	t.Helper()
	result, err := store.ApplyMessageUpdate(ctx, update)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertSQLiteQueueSnapshot(
	t *testing.T,
	snapshot CapacityObligationQueueSnapshot,
	jobAvailable int,
	assignedCount int,
	newlyQueued int,
	canceled int,
	oldestRunnableAt time.Time,
) {
	t.Helper()
	if snapshot.JobAvailable != jobAvailable || snapshot.AssignedCount != assignedCount ||
		snapshot.NewlyQueued != newlyQueued || snapshot.Canceled != canceled ||
		snapshot.OldestRunnableAt == nil || !snapshot.OldestRunnableAt.Equal(oldestRunnableAt) {
		t.Fatalf("queue snapshot = %#v", snapshot)
	}
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
	update.Started = []RunnerMessageIdentity{{
		RunnerIdentity: RunnerIdentity{RequestID: 42, RunnerName: "linux-42"},
	}}
	return update
}

func sqliteMessageUpdateWithCompletion() MessageUpdate {
	update := validSQLiteMessageUpdate()
	update.Completed = []RunnerMessageIdentity{{
		RunnerIdentity: RunnerIdentity{RequestID: 42, RunnerName: "linux-42"},
	}}
	return update
}

func sqliteMessageUpdateWithDemandAndStart() MessageUpdate {
	update := sqliteMessageUpdateWithDemand()
	update.Started = []RunnerMessageIdentity{{
		RunnerIdentity: RunnerIdentity{RequestID: 42, RunnerName: "linux-42"},
	}}
	return update
}
