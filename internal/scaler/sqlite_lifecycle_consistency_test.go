package scaler

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestRunnerLifecycleConsistencyErrorSupportsTypedInspection(t *testing.T) {
	err := &RunnerLifecycleConsistencyError{
		ScaleSet: "linux", Repository: "owner/repo", RunnerName: "linux-42", Reason: "provider conflict",
	}
	if !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("errors.Is(%v, ErrRunnerLifecycleConsistency) = false", err)
	}
	var consistencyError *RunnerLifecycleConsistencyError
	if !errors.As(err, &consistencyError) || consistencyError.Reason != "provider conflict" {
		t.Fatalf("errors.As(%v) = %#v", err, consistencyError)
	}
}

func TestSQLiteLibvirtRegistrationRepairsLifecycleRepresentations(t *testing.T) {
	for _, state := range []string{"generic-only", "mirror-only", "stale-mirror", "consistent"} {
		t.Run(state, func(t *testing.T) {
			store := newInMemorySQLiteLifecycleStore(t)
			runner := consistentSQLiteLibvirtRunner()
			seedSQLiteLifecycleState(t, store.database, state, runner)

			if err := store.RegisterRunner(context.Background(), runner); err != nil {
				t.Fatalf("RegisterRunner() error = %v", err)
			}
			assertSQLiteLifecycleRepresentations(t, store.database, runner, sqliteMirrorExpectation{})
		})
	}
}

func TestSQLiteLibvirtRegistrationRejectsConflictWithoutMutation(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	seedSQLiteLifecycleState(t, store.database, "conflicting", runner)

	err := store.RegisterRunner(context.Background(), runner)
	if !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("RegisterRunner() error = %v, want lifecycle consistency error", err)
	}
	var requestID int64
	if scanErr := store.database.QueryRow(`SELECT request_id FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&requestID); scanErr != nil {
		t.Fatal(scanErr)
	}
	if requestID != 99 {
		t.Fatalf("conflicting mirror request ID = %d, want 99", requestID)
	}
}

func TestSQLiteLibvirtRegistrationRejectsProviderConflictWithoutMutation(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	seedSQLiteLifecycleState(t, store.database, "consistent", runner)
	if _, err := store.database.Exec(`UPDATE runner_lifecycle SET provider = 'docker'
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName); err != nil {
		t.Fatal(err)
	}

	err := store.RegisterRunner(context.Background(), runner)
	if !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("RegisterRunner() error = %v, want lifecycle consistency error", err)
	}
	var provider string
	var genericRequestID, mirrorRequestID int64
	if scanErr := store.database.QueryRow(`SELECT provider, request_id FROM runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&provider, &genericRequestID); scanErr != nil {
		t.Fatal(scanErr)
	}
	if scanErr := store.database.QueryRow(`SELECT request_id FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&mirrorRequestID); scanErr != nil {
		t.Fatal(scanErr)
	}
	if provider != "docker" || genericRequestID != 42 || mirrorRequestID != 42 {
		t.Fatalf("mutated conflict = provider %q generic %d mirror %d", provider, genericRequestID, mirrorRequestID)
	}
}

func TestSQLiteLibvirtRegistrationRejectsRequestOwnedByDifferentMirrorName(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	seedSQLiteLifecycleState(t, store.database, "generic-only", runner)
	if _, err := store.database.Exec(`INSERT INTO libvirt_runner_lifecycle(
		scale_set, repository, request_id, runner_name, reap_requested) VALUES(?, ?, ?, 'other-runner', 0)`,
		runner.ScaleSet, runner.Repository, runner.RequestID); err != nil {
		t.Fatal(err)
	}

	err := store.RegisterRunner(context.Background(), runner)
	if !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("RegisterRunner() error = %v, want lifecycle consistency error", err)
	}
	var expectedNameCount, otherNameCount int
	if scanErr := store.database.QueryRow(`SELECT COUNT(*) FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&expectedNameCount); scanErr != nil {
		t.Fatal(scanErr)
	}
	if scanErr := store.database.QueryRow(`SELECT COUNT(*) FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = 'other-runner'`,
		runner.ScaleSet, runner.Repository).Scan(&otherNameCount); scanErr != nil {
		t.Fatal(scanErr)
	}
	if expectedNameCount != 0 || otherNameCount != 1 {
		t.Fatalf("mirror ownership mutated: expected-name=%d other-name=%d", expectedNameCount, otherNameCount)
	}
}

func TestSQLiteConcurrentRegistrationRetriesConverge(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	seedSQLiteLifecycleState(t, store.database, "generic-only", runner)
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan error, 2)
	for attempt := 0; attempt < 2; attempt++ {
		go func() {
			ready <- struct{}{}
			<-release
			results <- store.RegisterRunner(context.Background(), runner)
		}()
	}
	<-ready
	<-ready
	close(release)
	for attempt := 0; attempt < 2; attempt++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent RegisterRunner() error = %v", err)
		}
	}
	assertSQLiteLifecycleRepresentations(t, store.database, runner, sqliteMirrorExpectation{})
}

func TestSQLiteMessageStartBindsGenericAndLegacyIdentityTogether(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	runner.RequestID = 0
	runner.Phase = RunnerWaitingForJob
	if err := store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	if _, err := store.database.Exec(`INSERT INTO repository_controls(app_id, controller_epoch) VALUES(1, 1)`); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Unix(200, 0).UTC()
	_, err := store.ApplyMessageUpdate(context.Background(), MessageUpdate{
		AppID: 1, Repository: runner.Repository, ScaleSet: runner.ScaleSet, ControllerEpoch: 1,
		ObservedAt: observedAt, Started: []RunnerMessageIdentity{{
			RunnerIdentity: RunnerIdentity{RequestID: 42, RunnerName: runner.RunnerName}, GitHubRunnerID: 84,
		}},
	})
	if err != nil {
		t.Fatalf("ApplyMessageUpdate() error = %v", err)
	}
	runner.RequestID = 42
	assertSQLiteLifecycleRepresentations(t, store.database, runner, sqliteMirrorExpectation{})
}

func TestSQLiteCompletionAndTerminalReapTransitionBothRepresentations(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	if err := store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteRunnerLifecycleJob(context.Background(), JobCompletion{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerIdentity: runner.Identity(),
	}, time.Unix(300, 0).UTC()); err != nil {
		t.Fatalf("CompleteRunnerLifecycleJob() error = %v", err)
	}
	assertSQLiteLifecycleRepresentations(t, store.database, runner, sqliteMirrorExpectation{reapRequested: true})
	runner.Phase = RunnerReapPending
	if err := store.ConfirmRunnerReaped(context.Background(), runner); err != nil {
		t.Fatalf("ConfirmRunnerReaped() error = %v", err)
	}
	assertSQLiteLifecycleAbsent(t, store.database, runner)
}

func newInMemorySQLiteLifecycleStore(t *testing.T) *SQLiteDemandStore {
	t.Helper()
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	store := &SQLiteDemandStore{database: database}
	if err = store.migrate(context.Background()); err != nil {
		if closeErr := database.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Errorf("close in-memory SQLite lifecycle store: %v", closeErr)
		}
	})
	return store
}

func consistentSQLiteLibvirtRunner() RunnerLifecycle {
	now := time.Unix(100, 0).UTC()
	return RunnerLifecycle{
		ScaleSet: "linux", Repository: "owner/repo", Provider: RunnerProviderLibvirt,
		RunnerName: "linux-42", RequestID: 42, Phase: RunnerWaitingForJob,
		CreatedAt: now, WaitingSince: now, UpdatedAt: now,
	}
}

func seedSQLiteLifecycleState(t *testing.T, database *sql.DB, state string, runner RunnerLifecycle) {
	t.Helper()
	if _, err := database.Exec(`DROP TRIGGER mirror_libvirt_runner_lifecycle_insert;
		DROP TRIGGER mirror_libvirt_runner_lifecycle_update;
		DROP TRIGGER mirror_libvirt_runner_lifecycle_delete;`); err != nil {
		t.Fatal(err)
	}
	insertGeneric := state != "mirror-only"
	insertMirror := state != "generic-only"
	if insertGeneric {
		seedSQLiteGenericLifecycle(t, database, runner)
	}
	if insertMirror {
		seedSQLiteMirrorLifecycle(t, database, state, runner)
	}
}

func seedSQLiteGenericLifecycle(t *testing.T, database *sql.DB, runner RunnerLifecycle) {
	t.Helper()
	if runner.UpdatedAt.IsZero() {
		runner.UpdatedAt = runner.CreatedAt
	}
	if runner.WaitingSince.IsZero() && (runner.Phase == RunnerWaitingForJob || runner.Phase == RunnerBusy) {
		runner.WaitingSince = runner.CreatedAt
	}
	if _, err := database.Exec(`INSERT INTO runner_lifecycle(scale_set, repository, provider, runner_name,
		request_id, phase, created_at, waiting_since, updated_at, reap_reason) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		runner.ScaleSet, runner.Repository, runner.Provider, runner.RunnerName, runner.RequestID,
		runner.Phase, runner.CreatedAt.UnixNano(), runner.WaitingSince.UnixNano(), runner.UpdatedAt.UnixNano(),
		runner.ReapReason); err != nil {
		t.Fatal(err)
	}
}

func seedSQLiteMirrorLifecycle(t *testing.T, database *sql.DB, state string, runner RunnerLifecycle) {
	t.Helper()
	requestID := runner.RequestID
	reapRequested := state == "stale-mirror"
	if state == "conflicting" {
		requestID = 99
	}
	if _, err := database.Exec(`INSERT INTO libvirt_runner_lifecycle(
		scale_set, repository, request_id, runner_name, reap_requested) VALUES(?, ?, ?, ?, ?)`,
		runner.ScaleSet, runner.Repository, requestID, runner.RunnerName, reapRequested); err != nil {
		t.Fatal(err)
	}
}

type sqliteMirrorExpectation struct {
	reapRequested bool
}

func assertSQLiteLifecycleRepresentations(
	t *testing.T,
	database *sql.DB,
	runner RunnerLifecycle,
	expectation sqliteMirrorExpectation,
) {
	t.Helper()
	var provider string
	var genericRequestID int64
	if err := database.QueryRow(`SELECT provider, request_id FROM runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&provider, &genericRequestID); err != nil {
		t.Fatal(err)
	}
	var mirrorRequestID int64
	var mirroredReap bool
	if err := database.QueryRow(`SELECT request_id, reap_requested FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&mirrorRequestID, &mirroredReap); err != nil {
		t.Fatal(err)
	}
	if provider != string(RunnerProviderLibvirt) || genericRequestID != runner.RequestID ||
		mirrorRequestID != runner.RequestID || mirroredReap != expectation.reapRequested {
		t.Fatalf("representations = provider %q generic %d mirror %d reap %t", provider, genericRequestID, mirrorRequestID, mirroredReap)
	}
}

func assertSQLiteLifecycleAbsent(t *testing.T, database *sql.DB, runner RunnerLifecycle) {
	t.Helper()
	for _, table := range []string{"runner_lifecycle", "libvirt_runner_lifecycle"} {
		var count int
		query := `SELECT COUNT(*) FROM ` + table + ` WHERE scale_set = ? AND repository = ? AND runner_name = ?`
		if err := database.QueryRow(query, runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s row count = %d, want 0", table, count)
		}
	}
}
