//go:build integration

package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteLibvirtReapObligationSurvivesRestartUntilConfirmed(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	runner := LibvirtRunnerLifecycle{ScaleSet: "linux", Repository: "repo", RequestID: 42, RunnerName: "repo-linux-42"}
	if err = store.Apply(ctx, "linux", []JobDemand{{Repository: "repo", RequestID: 42, QueueTime: time.Now()}}, nil); err != nil {
		t.Fatal(err)
	}
	if err = store.RegisterLibvirtRunner(ctx, runner); err != nil {
		t.Fatal(err)
	}
	if err = store.CompleteJob(ctx, JobCompletion{ScaleSet: "linux", Repository: "repo", RequestID: 42, RunnerName: runner.RunnerName}); err != nil {
		t.Fatal(err)
	}
	if err = store.RemoveRepository(ctx, "linux", "repo"); err != nil {
		t.Fatal(err)
	}
	if err = store.RemoveScaleSet(ctx, "linux"); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteDemandStore(t, reopened)
	pending, err := reopened.PendingLibvirtReaps(ctx, "linux", "repo")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0] != runner {
		t.Fatalf("pending reaps = %#v", pending)
	}
	if err = reopened.RegisterLibvirtRunner(ctx, runner); err == nil {
		t.Fatal("expected pending runner identity to remain reserved")
	}
	if err = reopened.ConfirmLibvirtReaped(ctx, runner); err != nil {
		t.Fatal(err)
	}
	pending, err = reopened.PendingLibvirtReaps(ctx, "linux", "repo")
	if err != nil || len(pending) != 0 {
		t.Fatalf("confirmed reap remained pending: %#v, %v", pending, err)
	}
}

func TestSQLiteJobCompletionCreatesMissingReapAndRemovesDemandAtomically(t *testing.T) {
	store, err := OpenSQLiteDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	closeSQLiteDemandStore(t, store)
	ctx := context.Background()
	if err = store.Apply(ctx, "linux", []JobDemand{{Repository: "repo", RequestID: 9, QueueTime: time.Now()}}, nil); err != nil {
		t.Fatal(err)
	}
	if err = store.CompleteJob(ctx, JobCompletion{ScaleSet: "linux", Repository: "repo", RequestID: 9, RunnerName: "repo-linux-9"}); err != nil {
		t.Fatal(err)
	}
	demand, err := store.Ordered(ctx, "linux")
	if err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingLibvirtReaps(ctx, "linux", "repo")
	if err != nil {
		t.Fatal(err)
	}
	if len(demand) != 0 || len(pending) != 1 {
		t.Fatalf("demand=%#v pending=%#v", demand, pending)
	}
}
