package scaler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

const memoryIdentityCoverageScaleSet = "coverage-set"

const memoryIdentityCoverageRepository = "coverage-repo"

func TestRunnerLifecycleConsistencyErrorIncludesIdentityAndReason(t *testing.T) {
	err := (&RunnerLifecycleConsistencyError{
		ScaleSet: "linux", Repository: "repo", RunnerName: "runner", Reason: "conflict",
	}).Error()
	for _, fragment := range []string{"linux", "repo", "runner", "conflict"} {
		if !strings.Contains(err, fragment) {
			t.Fatalf("consistency error %q omits %q", err, fragment)
		}
	}
}

func TestMemoryIdentityCoverageRejectsInvalidIdentities(t *testing.T) {
	store := NewMemoryDemandStore()
	if err := store.CompleteRunnerLifecycleJob(context.Background(), JobCompletion{}, time.Time{}); !errors.Is(err, ErrInvalidRunnerIdentity) {
		t.Fatalf("CompleteRunnerLifecycleJob() error = %v, want invalid identity", err)
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
