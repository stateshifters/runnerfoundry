package scaler

import (
	"context"
	"errors"
	"testing"
	"time"
)

type coordinatorLifecycleFaultStore struct {
	*MemoryDemandStore
	pendingErr  error
	excessErr   error
	busyErr     error
	completeErr error
	expireErr   error
	confirmErr  error
}

func (store *coordinatorLifecycleFaultStore) MarkRunnerBusy(ctx context.Context, scaleSet, repository string, requestID int64, runnerName string, githubRunnerID int64, startedAt time.Time) error {
	if store.busyErr != nil {
		return store.busyErr
	}
	return store.MemoryDemandStore.MarkRunnerBusy(ctx, scaleSet, repository, requestID, runnerName, githubRunnerID, startedAt)
}

func (store *coordinatorLifecycleFaultStore) CompleteRunnerLifecycleJob(ctx context.Context, completion JobCompletion, completedAt time.Time) error {
	if store.completeErr != nil {
		return store.completeErr
	}
	return store.MemoryDemandStore.CompleteRunnerLifecycleJob(ctx, completion, completedAt)
}

func (store *coordinatorLifecycleFaultStore) RequestExpiredRunnerReaps(ctx context.Context, scaleSet, repository string, deadline, requestedAt time.Time) (int, error) {
	if store.expireErr != nil {
		return 0, store.expireErr
	}
	return store.MemoryDemandStore.RequestExpiredRunnerReaps(ctx, scaleSet, repository, deadline, requestedAt)
}

func (store *coordinatorLifecycleFaultStore) PendingRunnerReaps(ctx context.Context, scaleSet, repository string) ([]RunnerLifecycle, error) {
	if store.pendingErr != nil {
		return nil, store.pendingErr
	}
	return store.MemoryDemandStore.PendingRunnerReaps(ctx, scaleSet, repository)
}

func (store *coordinatorLifecycleFaultStore) RequestExcessWaitingRunnerReaps(ctx context.Context, scaleSet, repository string, retain int, reason string, at time.Time) (int, error) {
	if store.excessErr != nil {
		return 0, store.excessErr
	}
	return store.MemoryDemandStore.RequestExcessWaitingRunnerReaps(ctx, scaleSet, repository, retain, reason, at)
}

func (store *coordinatorLifecycleFaultStore) ConfirmRunnerReaped(ctx context.Context, runner RunnerLifecycle) error {
	if store.confirmErr != nil {
		return store.confirmErr
	}
	return store.MemoryDemandStore.ConfirmRunnerReaped(ctx, runner)
}

type lifecycleIdentityWorker struct {
	running      int
	present      bool
	presentErr   error
	usabilityErr error
	usabilityRun *int
}

func (worker lifecycleIdentityWorker) Running(context.Context) (int, error) {
	return worker.running, nil
}
func (lifecycleIdentityWorker) Exists(context.Context, int64) (bool, error) { return false, nil }
func (lifecycleIdentityWorker) Acquire(context.Context, int64) error        { return nil }
func (lifecycleIdentityWorker) StartOne(context.Context, int64) error       { return nil }
func (lifecycleIdentityWorker) StartAssigned(context.Context) error         { return nil }
func (worker lifecycleIdentityWorker) RunnerPresent(context.Context, string) (bool, error) {
	return worker.present, worker.presentErr
}
func (worker lifecycleIdentityWorker) ReconcileRunnerUsability(context.Context) (int, error) {
	if worker.usabilityRun != nil {
		*worker.usabilityRun++
	}
	return 0, worker.usabilityErr
}

func TestCoordinatorRunnerLifecycleFacade(t *testing.T) {
	store := createMemoryDemandStore()
	coordinator := NewDurableCoordinator("set", 2, store)
	reconciler := &Reconciler{}
	if !coordinator.HasRunnerLifecycle() {
		t.Fatal("expected runner lifecycle support")
	}
	if err := coordinator.ConfigureRunnerLifecycle(reconciler, "repo", RunnerProviderDocker); err != nil {
		t.Fatal(err)
	}
	if reconciler.ScaleSet.Repository != "repo" || reconciler.ScaleSet.ProviderKind != RunnerProviderDocker ||
		reconciler.ScaleSet.LifecycleScaleSet != "set" || reconciler.ScaleSet.Lifecycle != store {
		t.Fatalf("runtime=%#v", reconciler.ScaleSet)
	}
}

func TestCoordinatorRunnerLifecycleTransitions(t *testing.T) {
	ctx := context.Background()
	store := createMemoryDemandStore()
	coordinator := NewDurableCoordinator("set", 2, store)
	now := time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
	runner := RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker, RunnerName: "runner",
		RequestID: 7, Phase: RunnerWaitingForJob, WaitingSince: now.Add(-time.Hour)}
	if err := store.RegisterRunner(ctx, runner); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.MarkRunnerBusy(ctx, "repo", 7, "", 11, now); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CompleteRunnerLifecycleJob(ctx, JobCompletion{Repository: "repo", RequestID: 7}, now); err != nil {
		t.Fatal(err)
	}
	pending, err := coordinator.PendingRunnerReaps(ctx, "repo")
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	if err := coordinator.ConfirmRunnerReaped(ctx, pending[0]); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorRunnerLifecycleExpiration(t *testing.T) {
	ctx := context.Background()
	store := createMemoryDemandStore()
	coordinator := NewDurableCoordinator("set", 2, store)
	now := time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
	expired := RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker, RunnerName: "expired",
		Phase: RunnerWaitingForJob, WaitingSince: now.Add(-time.Hour)}
	if err := store.RegisterRunner(ctx, expired); err != nil {
		t.Fatal(err)
	}
	if changed, expireErr := coordinator.RequestExpiredRunnerReaps(ctx, "repo", 10*time.Minute, now); expireErr != nil || changed != 1 {
		t.Fatalf("changed=%d err=%v", changed, expireErr)
	}
}

func TestCoordinatorRunnerLifecycleFacadeRejectsUnsupportedStore(t *testing.T) {
	coordinator := NewDurableCoordinator("set", 1, &failingDemandStore{})
	ctx := context.Background()
	if coordinator.HasRunnerLifecycle() {
		t.Fatal("unexpected runner lifecycle support")
	}
	checks := []error{
		coordinator.ConfigureRunnerLifecycle(&Reconciler{}, "repo", RunnerProviderDocker),
		coordinator.MarkRunnerBusy(ctx, "repo", 1, "runner", 1, time.Time{}),
		coordinator.CompleteRunnerLifecycleJob(ctx, JobCompletion{}, time.Time{}),
	}
	if _, err := coordinator.RequestExpiredRunnerReaps(ctx, "repo", time.Minute, time.Time{}); err == nil {
		t.Fatal("expected expired-reap support error")
	}
	if _, err := coordinator.PendingRunnerReaps(ctx, "repo"); err == nil {
		t.Fatal("expected pending-reap support error")
	}
	if err := coordinator.ConfirmRunnerReaped(ctx, RunnerLifecycle{}); err == nil {
		t.Fatal("expected confirmation support error")
	}
	for _, err := range checks {
		if err == nil {
			t.Fatal("expected unsupported-store error")
		}
	}
}

func TestCoordinatorRunnerLifecycleCapacityBoundaries(t *testing.T) {
	ctx := context.Background()
	store := &coordinatorLifecycleFaultStore{MemoryDemandStore: createMemoryDemandStore(), pendingErr: errors.New("pending")}
	coordinator := NewDurableCoordinator("set", 1, store)
	coordinator.AddRepository("repo", lifecycleIdentityWorker{})
	if err := coordinator.Schedule(ctx); err == nil {
		t.Fatal("expected pending capacity failure")
	}
	store.pendingErr = nil
	runner := RunnerLifecycle{ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker,
		RunnerName: "runner", Phase: RunnerReapPending}
	if err := store.RegisterRunner(ctx, runner); err != nil {
		t.Fatal(err)
	}
	coordinator.workers["repo"] = lifecycleIdentityWorker{presentErr: errors.New("inspect")}
	if err := coordinator.Schedule(ctx); err == nil {
		t.Fatal("expected identity inspection failure")
	}
	coordinator.workers["repo"] = lifecycleIdentityWorker{present: false}
	if err := coordinator.Schedule(ctx); err != nil {
		t.Fatal(err)
	}
	coordinator.workers["repo"] = lifecycleIdentityWorker{present: true}
	if err := coordinator.Schedule(ctx); err != nil {
		t.Fatal(err)
	}
	store.excessErr = errors.New("excess")
	if _, err := coordinator.ReconcileAssigned(ctx, "repo", 0); err == nil {
		t.Fatal("expected excess cleanup failure")
	}
}

func TestCoordinatorBlockedCapacitySurfacesUsabilityFailure(t *testing.T) {
	coordinator := NewCoordinator(1)
	coordinator.AddRepository("repo", lifecycleIdentityWorker{running: 1, usabilityErr: errors.New("usability")})
	if _, err := coordinator.ReconcileAssigned(context.Background(), "repo", 2); err == nil {
		t.Fatal("expected blocked usability failure")
	}
}

func TestCoordinatorBlockedCapacityReconcilesUnusableRunnersBeforeReturning(t *testing.T) {
	coordinator := NewCoordinator(1)
	usabilityRuns := 0
	coordinator.AddRepository("repo", lifecycleIdentityWorker{running: 1, usabilityRun: &usabilityRuns})
	result, err := coordinator.ReconcileAssigned(context.Background(), "repo", 2)
	if err != nil {
		t.Fatal(err)
	}
	if usabilityRuns != 1 || result.StartedRunners != 0 {
		t.Fatalf("usability reconciliations=%d started runners=%d", usabilityRuns, result.StartedRunners)
	}
}

func TestCoordinatorRunnerLifecycleFacadeWrapsStoreFailures(t *testing.T) {
	store := &coordinatorLifecycleFaultStore{MemoryDemandStore: createMemoryDemandStore(), busyErr: errors.New("busy"),
		completeErr: errors.New("complete"), expireErr: errors.New("expire"), pendingErr: errors.New("pending"), confirmErr: errors.New("confirm")}
	coordinator := NewDurableCoordinator("set", 1, store)
	ctx := context.Background()
	if err := coordinator.MarkRunnerBusy(ctx, "repo", 1, "runner", 1, time.Time{}); err == nil {
		t.Fatal("expected busy failure")
	}
	if err := coordinator.CompleteRunnerLifecycleJob(ctx, JobCompletion{}, time.Time{}); err == nil {
		t.Fatal("expected completion failure")
	}
	if _, err := coordinator.RequestExpiredRunnerReaps(ctx, "repo", time.Minute, time.Time{}); err == nil {
		t.Fatal("expected expiration failure")
	}
	if _, err := coordinator.PendingRunnerReaps(ctx, "repo"); err == nil {
		t.Fatal("expected pending failure")
	}
	if err := coordinator.ConfirmRunnerReaped(ctx, RunnerLifecycle{}); err == nil {
		t.Fatal("expected confirmation failure")
	}
}

func TestCoordinatorBlockedCapacityAllowsWorkerWithoutUsabilityInspection(t *testing.T) {
	coordinator := NewCoordinator(1)
	coordinator.AddRepository("repo", &recordingWorker{repository: "repo", running: 1})
	if _, err := coordinator.ReconcileAssigned(context.Background(), "repo", 2); err != nil {
		t.Fatal(err)
	}
}
