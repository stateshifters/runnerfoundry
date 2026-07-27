//go:build integration

package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteWaitingLeaseNeverExpiresBusyRunner(t *testing.T) {
	store, err := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteDemandStore(t, store)
	ctx := context.Background()
	startedAt := time.Date(2026, time.July, 27, 17, 0, 0, 0, time.UTC)
	runner := RunnerLifecycle{
		ScaleSet: "linux", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "linux-42", RequestID: 42, GitHubRunnerID: 7, CreatedAt: startedAt,
	}
	if err = store.RegisterRunner(ctx, runner); err != nil {
		t.Fatal(err)
	}
	if err = store.ConfirmRunnerWaiting(ctx, runner, startedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkRunnerBusy(ctx, "linux", "repo", 42, "linux-42", 7, startedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	changed, err := store.RequestExpiredRunnerReaps(ctx, "linux", "repo", startedAt.Add(time.Hour), startedAt.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingRunnerReaps(ctx, "linux", "repo")
	if err != nil || changed != 0 || len(pending) != 0 {
		t.Fatalf("expired busy runner: changed=%d pending=%#v err=%v", changed, pending, err)
	}
}

func TestSQLiteIdentityFreeCompletionRequestsExactRunnerReap(t *testing.T) {
	store, err := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteDemandStore(t, store)
	runner := RunnerLifecycle{ScaleSet: "linux", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "linux-1", Phase: RunnerWaitingForJob}
	if err = store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
	if err = store.CompleteRunnerLifecycleJob(context.Background(), JobCompletion{
		ScaleSet: "linux", Repository: "repo", RunnerName: "linux-1",
	}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingRunnerReaps(context.Background(), "linux", "repo")
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
}

func TestSQLiteExpiredAndCanceledWaitingRunnersBecomeDurableReaps(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	startedAt := time.Date(2026, time.July, 27, 17, 0, 0, 0, time.UTC)
	for index, runnerName := range []string{"linux-1", "linux-2"} {
		runner := RunnerLifecycle{
			ScaleSet: "linux", Repository: "repo", Provider: RunnerProviderDocker,
			RunnerName: runnerName, CreatedAt: startedAt.Add(time.Duration(index) * time.Second),
		}
		if err = store.RegisterRunner(ctx, runner); err != nil {
			t.Fatal(err)
		}
		if err = store.ConfirmRunnerWaiting(ctx, runner, runner.CreatedAt); err != nil {
			t.Fatal(err)
		}
	}
	changed, err := store.RequestExcessWaitingRunnerReaps(ctx, "linux", "repo", 1, "assigned_jobs_decreased", startedAt.Add(time.Minute))
	if err != nil || changed != 1 {
		t.Fatalf("request canceled reap: changed=%d err=%v", changed, err)
	}
	changed, err = store.RequestExpiredRunnerReaps(ctx, "linux", "repo", startedAt.Add(time.Hour), startedAt.Add(2*time.Hour))
	if err != nil || changed != 1 {
		t.Fatalf("request timeout reap: changed=%d err=%v", changed, err)
	}
	pending, err := store.PendingRunnerReaps(ctx, "linux", "repo")
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending reaps = %#v, %v", pending, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteDemandStore(t, reopened)
	pending, err = reopened.PendingRunnerReaps(ctx, "linux", "repo")
	if err != nil || len(pending) != 2 {
		t.Fatalf("restarted pending reaps = %#v, %v", pending, err)
	}
}
