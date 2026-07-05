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

type recordingProvider struct {
	instances []RunnerInstance
	created   []RunnerStartRequest
	destroyed []string
}

func (provider *recordingProvider) List(ctx context.Context, scaleSetName string) ([]RunnerInstance, error) {
	return append([]RunnerInstance(nil), provider.instances...), nil
}

func (provider *recordingProvider) Start(ctx context.Context, request RunnerStartRequest) error {
	provider.created = append(provider.created, request)
	return nil
}

func (provider *recordingProvider) Destroy(ctx context.Context, instance RunnerInstance) error {
	provider.destroyed = append(provider.destroyed, instance.ID)
	return nil
}

type recordingJITGenerator struct {
	names     []string
	removed   []JITRunner
	removeErr error
}

func (generator *recordingJITGenerator) Generate(ctx context.Context, runnerName string) (JITRunnerConfig, error) {
	generator.names = append(generator.names, runnerName)
	return JITRunnerConfig{
		Runner:    JITRunner{ID: int64(len(generator.names)), Name: runnerName},
		JITConfig: "jit-" + runnerName,
	}, nil
}

func (generator *recordingJITGenerator) Remove(ctx context.Context, runner JITRunner) error {
	if generator.removeErr != nil {
		return generator.removeErr
	}
	generator.removed = append(generator.removed, runner)
	return nil
}

type failingJITGenerator struct{}

func (generator failingJITGenerator) Generate(ctx context.Context, runnerName string) (JITRunnerConfig, error) {
	return JITRunnerConfig{}, errors.New("jit failed")
}

func (generator failingJITGenerator) Remove(ctx context.Context, runner JITRunner) error {
	return nil
}

type failingProvider struct {
	failure   string
	instances []RunnerInstance
}

func (provider failingProvider) List(ctx context.Context, scaleSetName string) ([]RunnerInstance, error) {
	if provider.failure == "list" {
		return nil, errors.New("list failed")
	}
	return append([]RunnerInstance(nil), provider.instances...), nil
}

func (provider failingProvider) Start(ctx context.Context, request RunnerStartRequest) error {
	if provider.failure == "start" {
		return errors.New("start failed")
	}
	return nil
}

func (provider failingProvider) Destroy(ctx context.Context, instance RunnerInstance) error {
	if provider.failure == "destroy" {
		return errors.New("destroy failed")
	}
	return nil
}
