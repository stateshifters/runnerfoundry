package scaler

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

var memoryLifecycleTestTime = time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)

func mustRegisterMemoryRunner(t *testing.T, store *MemoryDemandStore, runner RunnerLifecycle) {
	t.Helper()
	if err := store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryRunnerLifecycleRegistrationAndConfirmation(t *testing.T) {
	ctx := context.Background()
	store := createMemoryDemandStore()
	runner := RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker, RunnerName: "runner", RequestID: 1}
	mustRegisterMemoryRunner(t, store, runner)
	if err := store.RegisterRunner(ctx, runner); err != nil {
		t.Fatalf("idempotent registration: %v", err)
	}
	if err := store.ConfirmRunnerWaiting(ctx, RunnerLifecycle{ScaleSet: "set", Repository: "repo", RunnerName: "runner", GitHubRunnerID: 99}, memoryLifecycleTestTime); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfirmRunnerWaiting(ctx, runner, memoryLifecycleTestTime); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("second confirmation error = %v", err)
	}
	conflict := runner
	conflict.Provider = RunnerProviderLibvirt
	if err := store.RegisterRunner(ctx, conflict); err == nil {
		t.Fatal("expected conflicting registration")
	}
}

func TestMemoryRunnerLifecycleBusyAndExpirationTransitions(t *testing.T) {
	ctx := context.Background()
	store := createMemoryDemandStore()
	docker := RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker, RunnerName: "docker", RequestID: 1,
		Phase: RunnerWaitingForJob, WaitingSince: memoryLifecycleTestTime}
	libvirt := RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderLibvirt, RunnerName: "libvirt", RequestID: 2,
		Phase: RunnerWaitingForJob, WaitingSince: memoryLifecycleTestTime}
	mustRegisterMemoryRunner(t, store, docker)
	mustRegisterMemoryRunner(t, store, libvirt)
	if err := store.MarkRunnerBusy(ctx, "set", "repo", 0, "docker", 101, memoryLifecycleTestTime); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRunnerBusy(ctx, "set", "repo", 404, "", 0, memoryLifecycleTestTime); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing busy transition error = %v", err)
	}
	changed, err := store.RequestExpiredRunnerReaps(ctx, "set", "", memoryLifecycleTestTime, memoryLifecycleTestTime)
	if err != nil || changed != 1 {
		t.Fatalf("expired changed=%d err=%v", changed, err)
	}
}

func TestMemoryRunnerLifecyclePendingAndConfirmation(t *testing.T) {
	ctx := context.Background()
	store := createMemoryDemandStore()
	for _, runner := range []RunnerLifecycle{
		{ScaleSet: "set", Repository: "z-repo", Provider: RunnerProviderLibvirt, RunnerName: "libvirt", RequestID: 2, Phase: RunnerReapPending},
		{ScaleSet: "set", Repository: "a-repo", Provider: RunnerProviderDocker, RunnerName: "docker", RequestID: 1, Phase: RunnerReapPending},
	} {
		mustRegisterMemoryRunner(t, store, runner)
	}
	mustRegisterMemoryRunner(t, store, RunnerLifecycle{ScaleSet: "other", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "ignored", Phase: RunnerWaitingForJob})
	pending, err := store.PendingRunnerReaps(ctx, "set", "")
	if err != nil || len(pending) != 2 || pending[0].Repository != "a-repo" {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	for _, runner := range pending {
		if err = store.ConfirmRunnerReaped(ctx, runner); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.ConfirmRunnerReaped(ctx, pending[0]); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing confirmation error = %v", err)
	}
}

func TestMemoryRunnerLifecycleExactReapAndReservedIdentity(t *testing.T) {
	ctx := context.Background()
	store := createMemoryDemandStore()
	runner := RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker, RunnerName: "runner", RequestID: 1}
	mustRegisterMemoryRunner(t, store, runner)
	if err := store.RequestRunnerReap(ctx, "set", "repo", 1, "", "job_completed", memoryLifecycleTestTime); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterRunner(ctx, runner); err == nil {
		t.Fatal("expected reap-pending identity reservation")
	}
}

func TestMemoryRunnerLifecycleExcessWaitingCapacity(t *testing.T) {
	ctx := context.Background()
	store := createMemoryDemandStore()
	for index, name := range []string{"old", "new", "tie-b", "tie-a"} {
		waiting := memoryLifecycleTestTime.Add(time.Duration(min(index, 2)) * time.Minute)
		mustRegisterMemoryRunner(t, store, RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker,
			RunnerName: name, Phase: RunnerWaitingForJob, WaitingSince: waiting})
	}
	mustRegisterMemoryRunner(t, store, RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "busy", Phase: RunnerBusy})
	mustRegisterMemoryRunner(t, store, RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderLibvirt,
		RunnerName: "libvirt", Phase: RunnerWaitingForJob, WaitingSince: memoryLifecycleTestTime.Add(3 * time.Minute)})
	mustRegisterMemoryRunner(t, store, RunnerLifecycle{ScaleSet: "other", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "ignored", Phase: RunnerWaitingForJob})
	changed, err := store.RequestExcessWaitingRunnerReaps(ctx, "set", "repo", 3, "decreased", memoryLifecycleTestTime)
	if err != nil || changed != 3 {
		t.Fatalf("changed=%d err=%v", changed, err)
	}
	changed, err = store.RequestExcessWaitingRunnerReaps(ctx, "set", "repo", -1, "canceled", memoryLifecycleTestTime)
	if err != nil || changed != 2 {
		t.Fatalf("negative retain changed=%d err=%v", changed, err)
	}
	lifecycles, err := store.RunnerLifecycles(ctx, "set", "")
	if err != nil || len(lifecycles) != 6 {
		t.Fatalf("lifecycles=%#v err=%v", lifecycles, err)
	}
}

func TestMemoryRunnerLifecycleCompletionRemovesDemand(t *testing.T) {
	ctx := context.Background()
	store := createMemoryDemandStore()
	key := demandKey{Repository: "repo", RequestID: 42}
	store.demand[key] = QueuedDemand{JobDemand: JobDemand{Repository: "repo", RequestID: 42}}
	mustRegisterMemoryRunner(t, store, RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "bound", RequestID: 42})
	if err := store.CompleteRunnerLifecycleJob(ctx, JobCompletion{ScaleSet: "set", Repository: "repo", RequestID: 42}, memoryLifecycleTestTime); err != nil {
		t.Fatal(err)
	}
	if _, exists := store.demand[key]; exists {
		t.Fatal("completed demand was retained")
	}
	if err := store.CompleteRunnerLifecycleJob(ctx, JobCompletion{ScaleSet: "set", Repository: "repo", RunnerName: "missing"}, memoryLifecycleTestTime); err != nil {
		t.Fatal(err)
	}
}
