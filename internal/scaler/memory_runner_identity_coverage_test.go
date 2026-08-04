package scaler

import (
	"context"
	"errors"
	"testing"
	"time"
)

const memoryIdentityCoverageScaleSet = "coverage-set"

const memoryIdentityCoverageRepository = "coverage-repo"

func TestMemoryIdentityCoverageRejectsInvalidIdentities(t *testing.T) {
	store := NewMemoryDemandStore()
	if err := store.CompleteJob(context.Background(), JobCompletion{}); !errors.Is(err, ErrInvalidRunnerIdentity) {
		t.Fatalf("CompleteJob() error = %v, want invalid identity", err)
	}
	if err := store.BindRunnerIdentity(context.Background(), "set", "repo",
		RunnerIdentity{RequestID: 1}, 0, time.Time{}); !errors.Is(err, ErrInvalidRunnerIdentity) {
		t.Fatalf("BindRunnerIdentity() error = %v, want invalid identity", err)
	}
}

func TestMemoryIdentityCoverageClonesEveryStateMap(t *testing.T) {
	store := NewMemoryDemandStore()
	ctx := context.Background()
	if err := store.Apply(ctx, "set", []JobDemand{{Repository: "repo", RequestID: 1}}, nil); err != nil {
		t.Fatal(err)
	}
	registerMemoryCoverageRunner(t, store, RunnerLifecycle{
		ScaleSet: "set", Repository: "repo", Provider: RunnerProviderDocker,
		RequestID: 1, RunnerName: "generic", Phase: RunnerWaitingForJob,
	})
	if err := store.RegisterLibvirtRunner(ctx, LibvirtRunnerLifecycle{
		ScaleSet: "set", Repository: "repo", RequestID: 2, RunnerName: "libvirt",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyMessageUpdate(ctx, MessageUpdate{}); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryIdentityCoverageBindingSelectionOutcomes(t *testing.T) {
	assertMemoryBindingError(t, memoryBindingStoreWithDuplicatePair(), ErrRunnerIdentityInvariant)
	assertMemoryBindingError(t, memoryBindingStoreWithDuplicateName(), ErrRunnerIdentityInvariant)
	assertMemoryBindingError(t, memoryBindingStoreWithConflictingName(), ErrRunnerIdentityMismatch)
	assertMemoryBindingError(t, memoryBindingStoreWithoutCandidate(), ErrRunnerIdentityNotFound)
}

func TestMemoryIdentityCoverageExactBindingRejectsMirrorConflict(t *testing.T) {
	identity := RunnerIdentity{RequestID: 42, RunnerName: "runner"}
	store := NewMemoryDemandStore()
	runner := memoryCoverageRunner(identity, RunnerProviderLibvirt)
	store.runners["first"] = runner
	mirrorKey := runnerLifecycleKey(LibvirtRunnerLifecycle{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerName: runner.RunnerName,
	})
	store.libvirtRunners[mirrorKey] = memoryLibvirtLifecycle{runner: LibvirtRunnerLifecycle{
		ScaleSet: memoryIdentityCoverageScaleSet, Repository: memoryIdentityCoverageRepository,
		RunnerName: identity.RunnerName,
	}}
	store.libvirtRunners["owner"] = memoryLibvirtLifecycle{runner: LibvirtRunnerLifecycle{
		ScaleSet: memoryIdentityCoverageScaleSet, Repository: memoryIdentityCoverageRepository,
		RequestID: identity.RequestID, RunnerName: "owner",
	}}
	err := store.BindRunnerIdentity(context.Background(), memoryIdentityCoverageScaleSet,
		memoryIdentityCoverageRepository, identity, 0, time.Time{})
	if !errors.Is(err, ErrRunnerIdentityMismatch) {
		t.Fatalf("BindRunnerIdentity() error = %v, want mismatch", err)
	}
}

func TestMemoryIdentityCoverageFallbackBindingRejectsMirrorConflict(t *testing.T) {
	identity := RunnerIdentity{RequestID: 42, RunnerName: "runner"}
	store := NewMemoryDemandStore()
	runner := memoryCoverageRunner(RunnerIdentity{RunnerName: identity.RunnerName}, RunnerProviderLibvirt)
	store.runners["candidate"] = runner
	store.libvirtRunners[runnerLifecycleKey(LibvirtRunnerLifecycle{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerName: runner.RunnerName,
	})] = memoryLibvirtLifecycle{runner: LibvirtRunnerLifecycle{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerName: runner.RunnerName,
	}}
	store.libvirtRunners["owner"] = memoryLibvirtLifecycle{runner: LibvirtRunnerLifecycle{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RequestID: identity.RequestID, RunnerName: "owner",
	}}
	err := store.BindRunnerIdentity(context.Background(), runner.ScaleSet, runner.Repository, identity, 0, time.Time{})
	if !errors.Is(err, ErrRunnerIdentityMismatch) {
		t.Fatalf("BindRunnerIdentity() error = %v, want mismatch", err)
	}
}

func TestMemoryIdentityCoverageLibvirtMirrorValidation(t *testing.T) {
	identity := RunnerIdentity{RequestID: 42, RunnerName: "runner"}
	assertMemoryMirrorValidationError(t, identity, 0, 2, ErrRunnerIdentityInvariant)
	assertMemoryMirrorValidationError(t, identity, 0, 1, ErrRunnerIdentityMismatch)
	assertMemoryMirrorValidationError(t, identity, 41, 0, ErrRunnerIdentityMismatch)
	assertMemoryMirrorValidationError(t, identity, 0, 0, nil)

	store := NewMemoryDemandStore()
	runner := memoryCoverageRunner(identity, RunnerProviderLibvirt)
	if err := store.bindMemoryLibvirtMirror(runner, identity); err != nil {
		t.Fatalf("bind absent mirror: %v", err)
	}
	store.libvirtRunners["unrelated"] = memoryLibvirtLifecycle{runner: LibvirtRunnerLifecycle{
		ScaleSet: "other", Repository: "other", RequestID: identity.RequestID,
	}}
	key := runnerLifecycleKey(LibvirtRunnerLifecycle{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerName: runner.RunnerName,
	})
	store.libvirtRunners[key] = memoryLibvirtLifecycle{runner: LibvirtRunnerLifecycle{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerName: runner.RunnerName,
	}}
	if err := store.bindMemoryLibvirtMirror(runner, identity); err != nil {
		t.Fatalf("bind mirror: %v", err)
	}
}

func TestMemoryIdentityCoverageReapMirroringAndRollback(t *testing.T) {
	assertMemoryReapMirrorsLibvirt(t)
	assertMemoryReapRejectsMirrorMismatch(t)
	assertMemoryReapConfirmationRestoresGenericRunner(t)
}

func assertMemoryBindingError(t *testing.T, store *MemoryDemandStore, target error) {
	t.Helper()
	identity := RunnerIdentity{RequestID: 42, RunnerName: "runner"}
	err := store.BindRunnerIdentity(context.Background(), memoryIdentityCoverageScaleSet,
		memoryIdentityCoverageRepository, identity, 0, time.Time{})
	if !errors.Is(err, target) {
		t.Fatalf("BindRunnerIdentity() error = %v, want %v", err, target)
	}
}

func memoryBindingStoreWithDuplicatePair() *MemoryDemandStore {
	store := NewMemoryDemandStore()
	runner := memoryCoverageRunner(RunnerIdentity{RequestID: 42, RunnerName: "runner"}, RunnerProviderDocker)
	store.runners["first"] = runner
	store.runners["second"] = runner
	return store
}

func memoryBindingStoreWithDuplicateName() *MemoryDemandStore {
	store := NewMemoryDemandStore()
	runner := memoryCoverageRunner(RunnerIdentity{RunnerName: "runner"}, RunnerProviderDocker)
	store.runners["first"] = runner
	store.runners["second"] = runner
	return store
}

func memoryBindingStoreWithConflictingName() *MemoryDemandStore {
	store := NewMemoryDemandStore()
	store.runners["conflict"] = memoryCoverageRunner(
		RunnerIdentity{RequestID: 41, RunnerName: "runner"}, RunnerProviderDocker,
	)
	return store
}

func memoryBindingStoreWithoutCandidate() *MemoryDemandStore {
	store := NewMemoryDemandStore()
	store.runners["other-scope"] = RunnerLifecycle{
		ScaleSet: "other", Repository: "other", Provider: RunnerProviderDocker,
		RunnerName: "runner", Phase: RunnerWaitingForJob,
	}
	return store
}

func assertMemoryMirrorValidationError(
	t *testing.T,
	identity RunnerIdentity,
	currentRequestID int64,
	requestMatches int,
	target error,
) {
	t.Helper()
	err := validateMemoryLibvirtMirrorBinding(identity, currentRequestID, requestMatches)
	if !errors.Is(err, target) {
		t.Fatalf("validateMemoryLibvirtMirrorBinding() error = %v, want %v", err, target)
	}
}

func assertMemoryReapMirrorsLibvirt(t *testing.T) {
	t.Helper()
	store := NewMemoryDemandStore()
	runner := memoryCoverageRunner(RunnerIdentity{RequestID: 42, RunnerName: "runner"}, RunnerProviderLibvirt)
	registerMemoryCoverageRunner(t, store, runner)
	if err := store.RequestRunnerReap(context.Background(), runner.ScaleSet, runner.Repository,
		runner.Identity(), "coverage", time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingLibvirtReaps(context.Background(), runner.ScaleSet, runner.Repository)
	if err != nil || len(pending) != 1 {
		t.Fatalf("PendingLibvirtReaps() = %#v, %v", pending, err)
	}
}

func assertMemoryReapRejectsMirrorMismatch(t *testing.T) {
	t.Helper()
	store := NewMemoryDemandStore()
	runner := memoryCoverageRunner(RunnerIdentity{RequestID: 42, RunnerName: "runner"}, RunnerProviderLibvirt)
	registerMemoryCoverageRunner(t, store, runner)
	key := runnerLifecycleKey(LibvirtRunnerLifecycle{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerName: runner.RunnerName,
	})
	lifecycle := store.libvirtRunners[key]
	lifecycle.runner.RequestID = 41
	store.libvirtRunners[key] = lifecycle
	err := store.RequestRunnerReap(context.Background(), runner.ScaleSet, runner.Repository,
		runner.Identity(), "coverage", time.Unix(2, 0))
	if !errors.Is(err, ErrRunnerIdentityMismatch) {
		t.Fatalf("RequestRunnerReap() error = %v, want mismatch", err)
	}
}

func assertMemoryReapConfirmationRestoresGenericRunner(t *testing.T) {
	t.Helper()
	store := NewMemoryDemandStore()
	runner := memoryCoverageRunner(RunnerIdentity{RequestID: 42, RunnerName: "runner"}, RunnerProviderLibvirt)
	runner.Phase = RunnerReapPending
	registerMemoryCoverageRunner(t, store, runner)
	key := runnerLifecycleKey(LibvirtRunnerLifecycle{
		ScaleSet: runner.ScaleSet, Repository: runner.Repository, RunnerName: runner.RunnerName,
	})
	lifecycle := store.libvirtRunners[key]
	lifecycle.runner.RequestID = 41
	store.libvirtRunners[key] = lifecycle
	err := store.ConfirmRunnerReaped(context.Background(), runner)
	if !errors.Is(err, ErrRunnerIdentityMismatch) {
		t.Fatalf("ConfirmRunnerReaped() error = %v, want mismatch", err)
	}
	if len(store.runners) != 1 {
		t.Fatalf("generic runners = %d, want restored runner", len(store.runners))
	}
}

func registerMemoryCoverageRunner(t *testing.T, store *MemoryDemandStore, runner RunnerLifecycle) {
	t.Helper()
	if err := store.RegisterRunner(context.Background(), runner); err != nil {
		t.Fatal(err)
	}
}

func memoryCoverageRunner(identity RunnerIdentity, provider RunnerProvider) RunnerLifecycle {
	return RunnerLifecycle{
		ScaleSet: memoryIdentityCoverageScaleSet, Repository: memoryIdentityCoverageRepository,
		Provider: provider, RequestID: identity.RequestID, RunnerName: identity.RunnerName,
		Phase: RunnerWaitingForJob,
	}
}
