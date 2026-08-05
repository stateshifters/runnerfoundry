package scaler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLiteExplicitReapRepairsMissingMirror(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	seedSQLiteLifecycleState(t, store.database, "generic-only", runner)
	if err := store.RequestRunnerReap(context.Background(), runner.ScaleSet, runner.Repository,
		runner.Identity(), "requested", time.Unix(310, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	assertSQLiteLifecycleRepresentations(t, store.database, runner, sqliteMirrorExpectation{reapRequested: true})
}

func TestSQLiteCompletionRepairsMissingMirror(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	seedSQLiteLifecycleState(t, store.database, "generic-only", runner)
	if err := store.CompleteRunnerLifecycleJob(context.Background(), JobCompletion{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerIdentity: runner.Identity(),
	}, time.Unix(320, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	assertSQLiteLifecycleRepresentations(t, store.database, runner, sqliteMirrorExpectation{reapRequested: true})
}

func TestSQLiteTerminalReapRemovesStaleMirror(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	runner.Phase = RunnerReapPending
	runner.ReapReason = "requested"
	seedSQLiteLifecycleState(t, store.database, "consistent", runner)
	if err := store.ConfirmRunnerReaped(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	assertSQLiteLifecycleAbsent(t, store.database, runner)
}

func TestSQLiteTransitionRejectsRequestOwnedByDifferentMirrorNameWithoutMutation(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	seedSQLiteLifecycleState(t, store.database, "generic-only", runner)
	if _, err := store.database.Exec(`INSERT INTO libvirt_runner_lifecycle(
		scale_set, repository, request_id, runner_name, reap_requested) VALUES(?, ?, ?, 'other-runner', 0)`,
		runner.ScaleSet, runner.Repository, runner.RequestID); err != nil {
		t.Fatal(err)
	}
	err := store.RequestRunnerReap(context.Background(), runner.ScaleSet, runner.Repository,
		runner.Identity(), "requested", time.Unix(330, 0).UTC())
	if !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("RequestRunnerReap() error = %v, want lifecycle consistency error", err)
	}
	var phase RunnerLifecyclePhase
	if scanErr := store.database.QueryRow(`SELECT phase FROM runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&phase); scanErr != nil {
		t.Fatal(scanErr)
	}
	if phase != RunnerWaitingForJob {
		t.Fatalf("authoritative phase mutated to %q", phase)
	}
}

func TestSQLiteBulkReapTransitionsBothRepresentations(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	seedSQLiteLifecycleState(t, store.database, "generic-only", runner)
	changed, err := store.RequestExpiredRunnerReaps(context.Background(), runner.ScaleSet, runner.Repository,
		runner.WaitingSince, time.Unix(400, 0).UTC())
	if err != nil || changed != 1 {
		t.Fatalf("RequestExpiredRunnerReaps() = %d, %v", changed, err)
	}
	assertSQLiteLifecycleRepresentations(t, store.database, runner, sqliteMirrorExpectation{reapRequested: true})
}

func TestSQLiteExpiredBulkReapRetryRepairsAlreadyPendingGenericLifecycle(t *testing.T) {
	assertSQLiteBulkRetryRepairsMirror(t, "job_start_timeout", func(store *SQLiteDemandStore, runner RunnerLifecycle) (int, error) {
		return store.RequestExpiredRunnerReaps(context.Background(), runner.ScaleSet, runner.Repository,
			time.Unix(1, 0).UTC(), time.Unix(460, 0).UTC())
	})
}

func TestSQLiteExcessBulkReapRetryRepairsAlreadyPendingGenericLifecycle(t *testing.T) {
	assertSQLiteBulkRetryRepairsMirror(t, "excess", func(store *SQLiteDemandStore, runner RunnerLifecycle) (int, error) {
		return store.RequestExcessWaitingRunnerReaps(context.Background(), runner.ScaleSet,
			runner.Repository, 0, "excess", time.Unix(470, 0).UTC())
	})
}

func assertSQLiteBulkRetryRepairsMirror(
	t *testing.T,
	reason string,
	apply func(*SQLiteDemandStore, RunnerLifecycle) (int, error),
) {
	t.Helper()
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	runner.Phase = RunnerReapPending
	runner.ReapReason = reason
	seedSQLiteLifecycleState(t, store.database, "generic-only", runner)
	changed, err := apply(store, runner)
	if err != nil || changed != 0 {
		t.Fatalf("bulk retry = %d, %v", changed, err)
	}
	assertSQLiteLifecycleRepresentations(t, store.database, runner,
		sqliteMirrorExpectation{reapRequested: true})
}

func TestSQLiteExcessBulkReapTransitionsSelectedRepresentationPair(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	first := consistentSQLiteLibvirtRunner()
	first.RequestID = 0
	first.RunnerName = "linux-41"
	second := first
	second.RunnerName = "linux-42"
	seedSQLiteLifecycleState(t, store.database, "generic-only", first)
	seedSQLiteGenericLifecycle(t, store.database, second)
	changed, err := store.RequestExcessWaitingRunnerReaps(context.Background(), first.ScaleSet, first.Repository,
		1, "excess", time.Unix(450, 0).UTC())
	if err != nil || changed != 1 {
		t.Fatalf("RequestExcessWaitingRunnerReaps() = %d, %v", changed, err)
	}
	assertSQLiteLifecycleRepresentations(t, store.database, first, sqliteMirrorExpectation{reapRequested: true})
	var retainedPhase RunnerLifecyclePhase
	if err = store.database.QueryRow(`SELECT phase FROM runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		second.ScaleSet, second.Repository, second.RunnerName).Scan(&retainedPhase); err != nil {
		t.Fatal(err)
	}
	if retainedPhase != RunnerWaitingForJob {
		t.Fatalf("retained runner phase = %q", retainedPhase)
	}
}
