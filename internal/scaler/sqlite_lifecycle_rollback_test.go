package scaler

import (
	"context"
	"testing"
	"time"
)

func TestSQLiteRegistrationRollsBackWhenMirrorWriteFails(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	if _, err := store.database.Exec(`CREATE TRIGGER reject_mirror_insert BEFORE INSERT ON libvirt_runner_lifecycle
		BEGIN SELECT RAISE(ABORT, 'injected mirror failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterRunner(context.Background(), runner); err == nil {
		t.Fatal("RegisterRunner() error = nil")
	}
	assertSQLiteLifecycleAbsent(t, store.database, runner)
}

func TestSQLiteCompletionRollsBackAfterMirrorWrite(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	if err := store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	if _, err := store.database.Exec(`INSERT INTO demand(observation_order, scale_set, repository, request_id, queue_time, acquired,
		source, controller_epoch, attempt, next_attempt_at) VALUES(1, ?, ?, ?, 1, 0, 'job_available', 0, 0, 1)`,
		runner.ScaleSet, runner.Repository, runner.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.database.Exec(`CREATE TRIGGER reject_demand_delete BEFORE DELETE ON demand
		BEGIN SELECT RAISE(ABORT, 'injected post-mirror failure'); END`); err != nil {
		t.Fatal(err)
	}
	err := store.CompleteRunnerLifecycleJob(context.Background(), JobCompletion{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerIdentity: runner.Identity(),
	}, time.Unix(500, 0).UTC())
	if err == nil {
		t.Fatal("CompleteRunnerLifecycleJob() error = nil")
	}
	assertSQLiteLifecycleRepresentations(t, store.database, runner, sqliteMirrorExpectation{})
}

func TestSQLiteBulkReapRollsBackWhenMirrorWriteFails(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	if err := store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	if _, err := store.database.Exec(`CREATE TRIGGER reject_mirror_update BEFORE UPDATE ON libvirt_runner_lifecycle
		BEGIN SELECT RAISE(ABORT, 'injected mirror failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RequestExpiredRunnerReaps(context.Background(), runner.ScaleSet, runner.Repository,
		runner.WaitingSince, time.Unix(600, 0).UTC()); err == nil {
		t.Fatal("RequestExpiredRunnerReaps() error = nil")
	}
	assertSQLiteLifecycleRepresentations(t, store.database, runner, sqliteMirrorExpectation{})
}
