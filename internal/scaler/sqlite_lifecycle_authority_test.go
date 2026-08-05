package scaler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLiteRequestMessageStartRejectsLegacyMirrorForDockerAuthority(t *testing.T) {
	assertSQLiteMessageStartRejectsLegacyMirror(t, func(runner RunnerLifecycle) RunnerIdentity {
		return RunnerIdentity{RequestID: runner.RequestID}
	})
}

func TestSQLiteNameMessageStartRejectsLegacyMirrorForDockerAuthority(t *testing.T) {
	assertSQLiteMessageStartRejectsLegacyMirror(t, func(runner RunnerLifecycle) RunnerIdentity {
		return RunnerIdentity{RunnerName: runner.RunnerName}
	})
}

func TestSQLitePairMessageStartRejectsLegacyMirrorForDockerAuthority(t *testing.T) {
	assertSQLiteMessageStartRejectsLegacyMirror(t, func(runner RunnerLifecycle) RunnerIdentity {
		return runner.Identity()
	})
}

func assertSQLiteMessageStartRejectsLegacyMirror(
	t *testing.T,
	identityFor func(RunnerLifecycle) RunnerIdentity,
) {
	t.Helper()
	store, runner := sqliteDockerWithLegacyMirror(t, RunnerWaitingForJob, "")
	if _, err := store.database.Exec(`INSERT INTO repository_controls(app_id, controller_epoch) VALUES(1, 1)`); err != nil {
		t.Fatal(err)
	}
	_, err := store.ApplyMessageUpdate(context.Background(), MessageUpdate{
		AppID: 1, Repository: runner.Repository, ScaleSet: runner.ScaleSet, ControllerEpoch: 1,
		ObservedAt: time.Unix(700, 0).UTC(), Started: []RunnerMessageIdentity{{RunnerIdentity: identityFor(runner)}},
	})
	assertSQLiteConsistencyRollback(t, store, runner, err, RunnerWaitingForJob)
}

func TestSQLiteCompletionRejectsLegacyMirrorForDockerAuthority(t *testing.T) {
	store, runner := sqliteDockerWithLegacyMirror(t, RunnerWaitingForJob, "")
	err := store.CompleteRunnerLifecycleJob(context.Background(), JobCompletion{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerIdentity: runner.Identity(),
	}, time.Unix(710, 0).UTC())
	assertSQLiteConsistencyRollback(t, store, runner, err, RunnerWaitingForJob)
}

func TestSQLiteBulkReapRejectsLegacyMirrorForDockerAuthority(t *testing.T) {
	store, runner := sqliteDockerWithLegacyMirror(t, RunnerWaitingForJob, "")
	_, err := store.RequestExpiredRunnerReaps(context.Background(), runner.ScaleSet, runner.Repository,
		runner.WaitingSince, time.Unix(720, 0).UTC())
	assertSQLiteConsistencyRollback(t, store, runner, err, RunnerWaitingForJob)
}

func TestSQLiteTerminalReapUsesAuthoritativeProvider(t *testing.T) {
	store, runner := sqliteDockerWithLegacyMirror(t, RunnerReapPending, "requested")
	caller := runner
	caller.Provider = RunnerProviderLibvirt
	err := store.ConfirmRunnerReaped(context.Background(), caller)
	assertSQLiteConsistencyRollback(t, store, runner, err, RunnerReapPending)
}

func sqliteDockerWithLegacyMirror(
	t *testing.T,
	phase RunnerLifecyclePhase,
	reason string,
) (*SQLiteDemandStore, RunnerLifecycle) {
	t.Helper()
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	runner.Provider = RunnerProviderDocker
	runner.Phase = phase
	runner.ReapReason = reason
	seedSQLiteLifecycleState(t, store.database, "generic-only", runner)
	if _, err := store.database.Exec(`INSERT INTO libvirt_runner_lifecycle(
		scale_set, repository, request_id, runner_name, reap_requested) VALUES(?, ?, ?, ?, 0)`,
		runner.ScaleSet, runner.Repository, runner.RequestID, runner.RunnerName); err != nil {
		t.Fatal(err)
	}
	return store, runner
}

func assertSQLiteConsistencyRollback(
	t *testing.T,
	store *SQLiteDemandStore,
	runner RunnerLifecycle,
	err error,
	wantedPhase RunnerLifecyclePhase,
) {
	t.Helper()
	if !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("transition error = %v, want lifecycle consistency error", err)
	}
	var phase RunnerLifecyclePhase
	if scanErr := store.database.QueryRow(`SELECT phase FROM runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&phase); scanErr != nil {
		t.Fatal(scanErr)
	}
	if phase != wantedPhase {
		t.Fatalf("authoritative phase = %q, want %q", phase, wantedPhase)
	}
}
