package scaler

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestDesiredRunnerCountCapsAssignedJobsAtMaximumCapacity(t *testing.T) {
	if got := DesiredRunnerCount(7, 3); got != 3 {
		t.Fatalf("expected cap at 3, got %d", got)
	}
	if got := DesiredRunnerCount(-2, 3); got != 0 {
		t.Fatalf("expected negative demand to become zero, got %d", got)
	}
}

func TestReconcilerCreatesOnlyMissingRunnersAndCleansCompleted(t *testing.T) {
	provider := &recordingProvider{
		instances: []RunnerInstance{
			{ID: "running", RunnerName: "running", State: RunnerStateRunning},
			{ID: "completed", RunnerName: "completed", State: RunnerStateCompleted},
		},
	}
	jitConfigs := &recordingJITGenerator{}
	reconciler := Reconciler{
		ScaleSet: ScaleSetRuntime{
			Name:       "docker-set",
			MaxRunners: 3,
			Provider:   provider,
			JITConfig:  jitConfigs,
		},
	}

	createdCount, err := reconciler.Reconcile(context.Background(), 3)
	if err != nil {
		t.Fatalf("expected reconcile success, got %v", err)
	}
	if createdCount != 3 {
		t.Fatalf("expected desired count 3, got %d", createdCount)
	}
	if len(provider.destroyed) != 1 || provider.destroyed[0] != "completed" {
		t.Fatalf("expected completed runner destroyed, got %#v", provider.destroyed)
	}
	if len(provider.created) != 2 {
		t.Fatalf("expected two created runners, got %#v", provider.created)
	}
	if len(jitConfigs.names) != 2 {
		t.Fatalf("expected two jit configs, got %#v", jitConfigs.names)
	}
}

func TestReconcilerDoesNotDuplicateRecoveredRunnerNames(t *testing.T) {
	provider := &recordingProvider{
		instances: []RunnerInstance{
			{ID: "existing", RunnerName: "docker-set-2", State: RunnerStateRunning},
		},
	}
	jitConfigs := &recordingJITGenerator{}
	reconciler := Reconciler{
		ScaleSet: ScaleSetRuntime{
			Name:       "docker-set",
			MaxRunners: 2,
			Provider:   provider,
			JITConfig:  jitConfigs,
		},
	}

	if _, err := reconciler.Reconcile(context.Background(), 2); err != nil {
		t.Fatalf("expected reconcile success, got %v", err)
	}
	if len(provider.created) != 1 {
		t.Fatalf("expected one created runner, got %#v", provider.created)
	}
	if provider.created[0].RunnerName != "docker-set-1" {
		t.Fatalf("expected recovered name to be skipped, got %#v", provider.created)
	}
}

func TestReconcilerDrainCleansCompletedAndDoesNotStartRunners(t *testing.T) {
	provider := &recordingProvider{
		instances: []RunnerInstance{
			{ID: "running", RunnerName: "docker-set-1", State: RunnerStateRunning},
			{ID: "completed", RunnerName: "docker-set-2", State: RunnerStateCompleted},
		},
	}
	jitConfigs := &recordingJITGenerator{}
	reconciler := Reconciler{
		ScaleSet: ScaleSetRuntime{
			Name:       "docker-set",
			MaxRunners: 3,
			Provider:   provider,
			JITConfig:  jitConfigs,
		},
	}

	runningCount, err := reconciler.Drain(context.Background())
	if err != nil {
		t.Fatalf("expected drain success, got %v", err)
	}
	if runningCount != 1 {
		t.Fatalf("expected one running runner, got %d", runningCount)
	}
	if len(provider.destroyed) != 1 || provider.destroyed[0] != "completed" {
		t.Fatalf("expected completed runner destroyed, got %#v", provider.destroyed)
	}
	if len(provider.created) != 0 {
		t.Fatalf("expected no runners to start during drain, got %#v", provider.created)
	}
	if len(jitConfigs.names) != 0 {
		t.Fatalf("expected no jit configs during drain, got %#v", jitConfigs.names)
	}
}

func TestReconcilerDoesNotDestroyUnknownRunnerState(t *testing.T) {
	provider := &recordingProvider{
		instances: []RunnerInstance{
			{ID: "unknown", RunnerName: "docker-set-1", State: RunnerStateUnknown},
		},
	}
	reconciler := Reconciler{
		ScaleSet: ScaleSetRuntime{
			Name:       "docker-set",
			MaxRunners: 1,
			Provider:   provider,
			JITConfig:  &recordingJITGenerator{},
		},
	}

	if _, err := reconciler.Drain(context.Background()); err != nil {
		t.Fatalf("expected drain success, got %v", err)
	}
	if len(provider.destroyed) != 0 {
		t.Fatalf("expected unknown runner to be preserved, got destroyed %#v", provider.destroyed)
	}
}

func TestReconcilerDrainSurfacesProviderFailures(t *testing.T) {
	tests := map[string]Provider{
		"list": failingProvider{failure: "list"},
		"destroy": failingProvider{
			failure:   "destroy",
			instances: []RunnerInstance{{ID: "done", RunnerName: "done", State: RunnerStateCompleted}},
		},
	}
	for testName, provider := range tests {
		t.Run(testName, func(t *testing.T) {
			reconciler := Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: provider}}
			if _, err := reconciler.Drain(context.Background()); err == nil {
				t.Fatal("expected drain failure")
			}
		})
	}
}

func TestReconcilerStopsWhenJITGenerationFails(t *testing.T) {
	provider := &recordingProvider{}
	reconciler := Reconciler{
		ScaleSet: ScaleSetRuntime{
			Name:       "docker-set",
			MaxRunners: 1,
			Provider:   provider,
			JITConfig:  failingJITGenerator{},
		},
	}

	_, err := reconciler.Reconcile(context.Background(), 1)
	if err == nil {
		t.Fatal("expected jit generation failure")
	}
	if len(provider.created) != 0 {
		t.Fatalf("expected no runner creation, got %#v", provider.created)
	}
}

func TestReconcilerSurfacesProviderFailures(t *testing.T) {
	tests := map[string]struct {
		provider Provider
	}{
		"list": {
			provider: failingProvider{failure: "list"},
		},
		"destroy": {
			provider: failingProvider{
				failure:   "destroy",
				instances: []RunnerInstance{{ID: "done", RunnerName: "done", State: RunnerStateCompleted}},
			},
		},
		"start": {
			provider: failingProvider{failure: "start"},
		},
	}
	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
				Name:       "set",
				MaxRunners: 1,
				Provider:   testCase.provider,
				JITConfig:  &recordingJITGenerator{},
			}}
			if _, err := reconciler.Reconcile(context.Background(), 1); err == nil {
				t.Fatal("expected provider failure")
			}
		})
	}
}

func TestReconcilerRemovesJITRunnerAfterProviderStartFailure(t *testing.T) {
	jitConfigs := &recordingJITGenerator{}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name:       "set",
		MaxRunners: 1,
		Provider:   failingProvider{failure: "start"},
		JITConfig:  jitConfigs,
	}}
	if _, err := reconciler.Reconcile(context.Background(), 1); err == nil {
		t.Fatal("expected provider failure")
	}
	if len(jitConfigs.removed) != 1 || jitConfigs.removed[0].Name != "set-1" {
		t.Fatalf("expected generated runner removed, got %#v", jitConfigs.removed)
	}
}

func TestReconcilerReportsJITRunnerCleanupFailure(t *testing.T) {
	jitConfigs := &recordingJITGenerator{removeErr: errors.New("remove failed")}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name:       "set",
		MaxRunners: 1,
		Provider:   failingProvider{failure: "start"},
		JITConfig:  jitConfigs,
	}}
	_, err := reconciler.Reconcile(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "remove github runner") {
		t.Fatalf("expected cleanup failure, got %v", err)
	}
}
