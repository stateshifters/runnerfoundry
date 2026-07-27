//go:build integration

package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteLegacyLibvirtWritesMirrorIntoGenericLifecycle(t *testing.T) {
	store, err := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteDemandStore(t, store)
	if _, err = store.database.Exec(`INSERT INTO libvirt_runner_lifecycle
		(scale_set, repository, request_id, runner_name, reap_requested)
		VALUES('linux', 'repo', 42, 'linux-42', 0)`); err != nil {
		t.Fatal(err)
	}
	lifecycles, err := store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil || len(lifecycles) != 1 || lifecycles[0].Provider != RunnerProviderLibvirt {
		t.Fatalf("mirrored registration = %#v, %v", lifecycles, err)
	}
	if _, err = store.database.Exec(`UPDATE libvirt_runner_lifecycle SET reap_requested = 1
		WHERE scale_set = 'linux' AND repository = 'repo' AND runner_name = 'linux-42'`); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingRunnerReaps(context.Background(), "linux", "repo")
	if err != nil || len(pending) != 1 {
		t.Fatalf("mirrored reap = %#v, %v", pending, err)
	}
	if _, err = store.database.Exec(`DELETE FROM libvirt_runner_lifecycle
		WHERE scale_set = 'linux' AND repository = 'repo' AND runner_name = 'linux-42'`); err != nil {
		t.Fatal(err)
	}
	lifecycles, err = store.RunnerLifecycles(context.Background(), "linux", "repo")
	if err != nil || len(lifecycles) != 0 {
		t.Fatalf("mirrored confirmation remained = %#v, %v", lifecycles, err)
	}
}

func TestSQLiteAssignedDecreaseCountsBusyRunnerBeforeRetainingWaitingCapacity(t *testing.T) {
	store, err := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteDemandStore(t, store)
	ctx := context.Background()
	now := time.Now().UTC()
	for index, phase := range []RunnerLifecyclePhase{RunnerBusy, RunnerWaitingForJob} {
		runner := RunnerLifecycle{
			ScaleSet: "linux", Repository: "repo", Provider: RunnerProviderDocker,
			RunnerName: "linux-" + string(rune('1'+index)), Phase: phase, CreatedAt: now, WaitingSince: now,
		}
		if err = store.RegisterRunner(ctx, runner); err != nil {
			t.Fatal(err)
		}
	}
	changed, err := store.RequestExcessWaitingRunnerReaps(ctx, "linux", "repo", 1, "assigned_jobs_decreased", now)
	if err != nil || changed != 1 {
		t.Fatalf("excess waiting reaps = %d, %v", changed, err)
	}
}

func TestSQLiteRunnerLifecycleSupportsIdempotentTransitionsAndExactCleanup(t *testing.T) {
	store, err := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteDemandStore(t, store)
	ctx := context.Background()
	now := time.Date(2026, time.July, 27, 20, 0, 0, 0, time.UTC)
	runner := RunnerLifecycle{ScaleSet: "linux", Repository: "repo", Provider: RunnerProviderLibvirt,
		RunnerName: "linux-42", RequestID: 42, Phase: RunnerProvisioning}
	if err = store.Apply(ctx, "linux", []JobDemand{{Repository: "repo", RequestID: 42, QueueTime: now}}, nil); err != nil {
		t.Fatal(err)
	}
	if err = store.RegisterRunner(ctx, runner); err != nil {
		t.Fatal(err)
	}
	if err = store.RegisterRunner(ctx, runner); err != nil {
		t.Fatalf("matching duplicate: %v", err)
	}
	conflict := runner
	conflict.RequestID++
	if err = store.RegisterRunner(ctx, conflict); err == nil {
		t.Fatal("expected conflicting duplicate")
	}
	if err = store.ConfirmRunnerWaiting(ctx, runner, now); err != nil {
		t.Fatal(err)
	}
	if err = store.ConfirmRunnerWaiting(ctx, runner, now); err == nil {
		t.Fatal("expected duplicate waiting transition failure")
	}
	if err = store.MarkRunnerBusy(ctx, "linux", "repo", 42, "", 101, now); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkRunnerBusy(ctx, "linux", "repo", 404, "", 0, now); err == nil {
		t.Fatal("expected missing busy transition failure")
	}
	if err = store.RequestRunnerReap(ctx, "linux", "repo", 42, "", "job_completed", now); err != nil {
		t.Fatal(err)
	}
	if err = store.CompleteRunnerLifecycleJob(ctx, JobCompletion{ScaleSet: "linux", Repository: "repo", RequestID: 42}, now); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingRunnerReaps(ctx, "linux", "")
	if err != nil || len(pending) != 1 || pending[0].WaitingSince != now {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	if err = store.ConfirmRunnerReaped(ctx, pending[0]); err != nil {
		t.Fatal(err)
	}
	if err = store.ConfirmRunnerReaped(ctx, pending[0]); err == nil {
		t.Fatal("expected missing reap confirmation failure")
	}
	if err = store.RequestRunnerReap(ctx, "linux", "repo", 0, "missing", "orphan", now); err != nil {
		t.Fatal(err)
	}
	lifecycles, err := store.RunnerLifecycles(ctx, "linux", "")
	if err != nil || len(lifecycles) != 0 {
		t.Fatalf("lifecycles=%#v err=%v", lifecycles, err)
	}
}
