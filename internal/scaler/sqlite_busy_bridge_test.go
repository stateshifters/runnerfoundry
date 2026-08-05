package scaler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLiteMarkRunnerBusyRepairsMissingLibvirtMirror(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	seedSQLiteLifecycleState(t, store.database, "generic-only", runner)

	if err := store.MarkRunnerBusy(context.Background(), runner.ScaleSet, runner.Repository,
		runner.Identity(), 84, time.Unix(200, 0).UTC()); err != nil {
		t.Fatalf("MarkRunnerBusy() error = %v", err)
	}
	runner.Phase = RunnerBusy
	assertSQLiteLifecycleRepresentations(t, store.database, runner, sqliteMirrorExpectation{})
}

func TestSQLiteMarkRunnerBusyRejectsLegacyHistoryForDockerWithoutMutation(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	runner := consistentSQLiteLibvirtRunner()
	runner.Provider = RunnerProviderDocker
	seedSQLiteLifecycleState(t, store.database, "consistent", runner)

	err := store.MarkRunnerBusy(context.Background(), runner.ScaleSet, runner.Repository,
		runner.Identity(), 84, time.Unix(200, 0).UTC())
	if !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("MarkRunnerBusy() error = %v, want lifecycle consistency error", err)
	}
	var phase string
	if scanErr := store.database.QueryRow(`SELECT phase FROM runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		runner.ScaleSet, runner.Repository, runner.RunnerName).Scan(&phase); scanErr != nil {
		t.Fatal(scanErr)
	}
	if phase != string(RunnerWaitingForJob) {
		t.Fatalf("generic phase = %q, want %q", phase, RunnerWaitingForJob)
	}
}
