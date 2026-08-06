package daemon

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/core"
)

func TestAffectedRunnerPoolsIncludesOnlyRemovedAndProviderTypeChanges(t *testing.T) {
	t.Parallel()
	docker := &config.DockerConfig{}
	libvirt := &config.LibvirtConfig{}
	active := config.Config{RunnerPools: []config.RunnerPoolConfig{
		{Name: "unchanged", Docker: docker}, {Name: "resized", MaxRunners: 1, Docker: docker},
		{Name: "switched", Docker: docker}, {Name: "removed", Libvirt: libvirt},
	}}
	replacement := config.Config{RunnerPools: []config.RunnerPoolConfig{
		{Name: "unchanged", Docker: docker}, {Name: "resized", MaxRunners: 9, Docker: docker},
		{Name: "switched", Libvirt: libvirt}, {Name: "added", Docker: docker},
	}}
	got, err := affectedRunnerPools(active, replacement)
	if err != nil {
		t.Fatalf("derive affected Runner Pools: %v", err)
	}
	if want := []core.RunnerPool{"removed", "switched"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("affected Runner Pools = %#v, want %#v", got, want)
	}
}

func TestTerminatePolicyActivatesReplacementAndRetainsIncompleteFinalizers(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("cleanup failed")
	removed := &supervisorFinalizerFake{runnerPool: "removed", remaining: []int{1}, errors: []error{wantErr}}
	unchanged := &supervisorFinalizerFake{runnerPool: "unchanged"}
	active := managedWithPools(&fakeGeneration{finalizers: []runtimeFinalizer{removed, unchanged}}, "removed", "unchanged")
	replacementFake := &fakeGeneration{}
	replacement := managedWithPools(replacementFake, "unchanged")
	removed.onTerminate = func() {
		if replacementFake.activations != 1 {
			t.Fatal("old-provider termination ran before replacement activation committed")
		}
	}
	handoff := &generationHandoff{policy: TerminateActiveRunners, active: active}
	if err := replaceActiveGeneration(context.Background(), handoff, replacement); !errors.Is(err, wantErr) {
		t.Fatalf("replacement cleanup error = %v, want %v", err, wantErr)
	}
	if handoff.active != replacement || removed.calls != 1 || unchanged.calls != 0 || len(handoff.retainedFinalizers) != 1 {
		t.Fatalf("active=%p replacement=%p removed=%d unchanged=%d retained=%d", handoff.active, replacement, removed.calls, unchanged.calls, len(handoff.retainedFinalizers))
	}
	if replacementFake.activations != 1 {
		t.Fatalf("replacement activations = %d, want one despite cleanup failure", replacementFake.activations)
	}
}

func TestTerminatePolicyRetriesRetainedFinalizersOnLaterReplacement(t *testing.T) {
	t.Parallel()
	retained := &supervisorFinalizerFake{runnerPool: "removed"}
	active := managedWithPools(&fakeGeneration{}, "unchanged")
	handoff := &generationHandoff{policy: TerminateActiveRunners, active: active, retainedFinalizers: []runtimeFinalizer{retained}}
	replacement := managedWithPools(&fakeGeneration{}, "unchanged")
	if err := replaceActiveGeneration(context.Background(), handoff, replacement); err != nil {
		t.Fatalf("retry retained finalizer: %v", err)
	}
	if handoff.active != replacement || retained.calls != 1 || len(handoff.retainedFinalizers) != 0 {
		t.Fatalf("active=%p replacement=%p calls=%d retained=%d", handoff.active, replacement, retained.calls, len(handoff.retainedFinalizers))
	}
}

func TestTerminatePolicyRetainsFinalizerWhileStartedProcessingRemains(t *testing.T) {
	t.Parallel()
	finalizer := &supervisorFinalizerFake{runnerPool: "removed", remaining: []int{1}}
	active := managedWithPools(&fakeGeneration{finalizers: []runtimeFinalizer{finalizer}}, "removed")
	handoff := &generationHandoff{policy: TerminateActiveRunners, active: active}
	if err := replaceActiveGeneration(context.Background(), handoff, &managedGeneration{generation: &fakeGeneration{}}); err != nil {
		t.Fatalf("replace active generation: %v", err)
	}
	if len(handoff.retainedFinalizers) != 1 || handoff.retainedFinalizers[0] != finalizer {
		t.Fatalf("retained finalizers = %#v", handoff.retainedFinalizers)
	}
}

func TestLeavePolicyStopsWithoutTerminatingPools(t *testing.T) {
	t.Parallel()
	finalizer := &supervisorFinalizerFake{runnerPool: "removed"}
	oldGeneration := &fakeGeneration{finalizers: []runtimeFinalizer{finalizer}}
	handoff := &generationHandoff{policy: LeaveActiveRunners, active: managedWithPools(oldGeneration, "removed")}
	replacement := &fakeGeneration{onActivate: func() {
		if oldGeneration.deactivations != 1 || oldGeneration.stops != 0 {
			t.Fatal("replacement activation did not occur between old deactivation and close")
		}
	}}
	if err := replaceActiveGeneration(context.Background(), handoff, &managedGeneration{generation: replacement}); err != nil {
		t.Fatalf("replace with leave policy: %v", err)
	}
	if oldGeneration.stops != 1 || finalizer.calls != 0 || len(handoff.retainedFinalizers) != 0 {
		t.Fatalf("old stops=%d finalizer calls=%d retained=%d", oldGeneration.stops, finalizer.calls, len(handoff.retainedFinalizers))
	}
}

func TestReplacementActivationFailureRestoresOldGeneration(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("activate failed")
	oldGeneration := &fakeGeneration{}
	handoff := &generationHandoff{policy: LeaveActiveRunners, active: &managedGeneration{generation: oldGeneration}}
	replacementFake := &fakeGeneration{activateErr: wantErr}
	replacement := &managedGeneration{generation: replacementFake}
	if err := replaceActiveGeneration(context.Background(), handoff, replacement); !errors.Is(err, wantErr) {
		t.Fatalf("replacement activation error = %v", err)
	}
	if oldGeneration.stops != 0 || oldGeneration.deactivations != 1 || oldGeneration.activations != 1 ||
		replacementFake.stops != 1 || handoff.active.generation != oldGeneration {
		t.Fatalf("old=%+v replacement=%+v active=%p", oldGeneration, replacementFake, handoff.active)
	}
}

func TestReloadPublishesReadyAfterTerminationCleanupFailure(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("cleanup failed")
	finalizer := &supervisorFinalizerFake{runnerPool: "removed", errors: []error{wantErr}}
	handoff := &generationHandoff{policy: TerminateActiveRunners, active: managedWithPools(&fakeGeneration{finalizers: []runtimeFinalizer{finalizer}}, "removed")}
	replacement := &fakeGeneration{}
	notifier := &fakeSupervisorNotifier{}
	err := reloadGeneration(context.Background(), emptyConfig, generationReturning(replacement), notifier, handoff)
	if !errors.Is(err, wantErr) {
		t.Fatalf("reload error = %v, want cleanup failure", err)
	}
	if handoff.active.generation != replacement || notifier.ready != 1 || len(handoff.retainedFinalizers) != 1 {
		t.Fatalf("active=%T ready=%d retained=%d", handoff.active.generation, notifier.ready, len(handoff.retainedFinalizers))
	}
}

func TestReloadStartFailureLeavesActiveGenerationRunning(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("start failed")
	oldGeneration := &fakeGeneration{}
	handoff := &generationHandoff{policy: TerminateActiveRunners, active: &managedGeneration{generation: oldGeneration}}
	err := reloadGeneration(context.Background(), emptyConfig, generationReturning(&fakeGeneration{startErr: wantErr}), &fakeSupervisorNotifier{}, handoff)
	if !errors.Is(err, wantErr) {
		t.Fatalf("reload error = %v, want start failure", err)
	}
	if handoff.active.generation != oldGeneration || oldGeneration.stops != 0 {
		t.Fatalf("active=%T old stops=%d", handoff.active.generation, oldGeneration.stops)
	}
}

func TestFinalShutdownHonorsActiveRunnerPolicy(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		policy    ActiveRunnerPolicy
		wantCalls int
	}{{"leave", LeaveActiveRunners, 0}, {"terminate", TerminateActiveRunners, 1}} {
		t.Run(test.name, func(t *testing.T) {
			finalizer := &supervisorFinalizerFake{runnerPool: "linux"}
			active := managedWithPools(&fakeGeneration{finalizers: []runtimeFinalizer{finalizer}}, "linux")
			if err := finalizeSupervisor(context.Background(), active, test.policy, nil); err != nil {
				t.Fatalf("finalize supervisor: %v", err)
			}
			if finalizer.calls != test.wantCalls {
				t.Fatalf("finalizer calls = %d, want %d", finalizer.calls, test.wantCalls)
			}
		})
	}
}

func managedWithPools(generation generation, runnerPools ...string) *managedGeneration {
	configured := config.Config{RunnerPools: make([]config.RunnerPoolConfig, 0, len(runnerPools))}
	for _, runnerPool := range runnerPools {
		configured.RunnerPools = append(configured.RunnerPools, config.RunnerPoolConfig{Name: runnerPool, Docker: &config.DockerConfig{}})
	}
	return &managedGeneration{generation: generation, configured: configured}
}

func emptyConfig() (config.Config, error) { return config.Config{}, nil }

func generationReturning(candidate generation) generationBuilder {
	return func(context.Context, config.Config) (generation, error) { return candidate, nil }
}

type supervisorFinalizerFake struct {
	runnerPool  core.RunnerPool
	remaining   []int
	errors      []error
	onTerminate func()
	calls       int
}

func (finalizer *supervisorFinalizerFake) RunnerPool() core.RunnerPool { return finalizer.runnerPool }
func (finalizer *supervisorFinalizerFake) Terminate(context.Context) (int, error) {
	if finalizer.onTerminate != nil {
		finalizer.onTerminate()
	}
	index := finalizer.calls
	finalizer.calls++
	var remaining int
	var err error
	if index < len(finalizer.remaining) {
		remaining = finalizer.remaining[index]
	}
	if index < len(finalizer.errors) {
		err = finalizer.errors[index]
	}
	return remaining, err
}
