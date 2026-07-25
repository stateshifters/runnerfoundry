package scaler

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestReconcilerRepositoryWorkerCountsRecoveredRunnersAndStartsOne(t *testing.T) {
	provider := &recordingProvider{instances: []RunnerInstance{{RunnerName: "set-1", State: RunnerStateRunning}}}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name:       "set",
		MaxRunners: 3,
		Provider:   provider,
		JITConfig:  &recordingJITGenerator{},
	}}
	runningCount, err := reconciler.Running(context.Background())
	if err != nil || runningCount != 1 {
		t.Fatalf("expected one recovered runner, got %d, %v", runningCount, err)
	}
	if err := reconciler.StartOne(context.Background()); err != nil {
		t.Fatalf("expected one runner to start, got %v", err)
	}
	if len(provider.created) != 1 {
		t.Fatalf("expected one new runner, got %#v", provider.created)
	}
}

func TestReconcilerStartsAndFindsNamedRunner(t *testing.T) {
	provider := &recordingProvider{instances: []RunnerInstance{{RunnerName: "linux-42", State: RunnerStateRunning}}}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{Name: "linux", MaxRunners: 2, Provider: provider, JITConfig: &recordingJITGenerator{}}}
	exists, err := reconciler.Exists(context.Background(), 42)
	if err != nil || !exists {
		t.Fatalf("expected named runner, got %t, %v", exists, err)
	}
	exists, err = reconciler.Exists(context.Background(), 43)
	if err != nil || exists {
		t.Fatalf("expected missing runner, got %t, %v", exists, err)
	}
	provider.instances = nil
	if err := reconciler.StartNamed(context.Background(), 42); err != nil {
		t.Fatal(err)
	}
	if len(provider.created) != 1 || provider.created[0].RunnerName != "linux-42" {
		t.Fatalf("expected stable runner name, got %#v", provider.created)
	}
	if _, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "linux", Provider: failingProvider{failure: "list"}}}).Exists(context.Background(), 42); err == nil {
		t.Fatal("expected exists failure")
	}
}

func TestReconcilerCompactsNamedRunnerForStartAndRecovery(t *testing.T) {
	baseName := "github-com-meza-mezas-conventional-changelog-meza-ubuntu-latest"
	expectedRunnerName := "github-com-meza-mezas-conventional-changelog-ee540047238cb46a-42"
	provider := &recordingProvider{instances: []RunnerInstance{{RunnerName: expectedRunnerName, State: RunnerStateRunning}}}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name:       baseName,
		MaxRunners: 2,
		Provider:   provider,
		JITConfig:  &recordingJITGenerator{},
	}}

	exists, err := reconciler.Exists(context.Background(), 42)
	if err != nil || !exists {
		t.Fatalf("expected compacted runner to be recovered, got exists=%t err=%v", exists, err)
	}

	provider.instances = nil
	if err := reconciler.StartNamed(context.Background(), 42); err != nil {
		t.Fatal(err)
	}
	if len(provider.created) != 1 || provider.created[0].RunnerName != expectedRunnerName {
		t.Fatalf("expected compacted runner name, got %#v", provider.created)
	}
}

func TestReconcilerCompactsAutomaticallyAllocatedRunnerName(t *testing.T) {
	baseName := "github-com-meza-mezas-conventional-changelog-meza-ubuntu-latest"
	expectedRunnerName := "github-com-meza-mezas-conventional-changelog-ee540047238cb46a-1"
	provider := &recordingProvider{}
	jitGenerator := &recordingJITGenerator{}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name:       baseName,
		MaxRunners: 1,
		Provider:   provider,
		JITConfig:  jitGenerator,
	}}

	if _, err := reconciler.Reconcile(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if len(jitGenerator.names) != 1 || jitGenerator.names[0] != expectedRunnerName {
		t.Fatalf("expected compacted JIT runner name, got %#v", jitGenerator.names)
	}
	if len(provider.created) != 1 || provider.created[0].RunnerName != expectedRunnerName {
		t.Fatalf("expected compacted provider runner name, got %#v", provider.created)
	}
}

func TestReconcilerDestroysActiveRunnersOnImmediateRemoval(t *testing.T) {
	provider := &recordingProvider{instances: []RunnerInstance{
		{RunnerName: "running", State: RunnerStateRunning},
		{RunnerName: "completed", State: RunnerStateCompleted},
		{RunnerName: "unknown", State: RunnerStateUnknown},
	}}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: provider}}
	if err := reconciler.DestroyActive(context.Background()); err != nil {
		t.Fatalf("expected active cleanup, got %v", err)
	}
	if len(provider.destroyed) != 2 {
		t.Fatalf("expected running and completed runners destroyed, got %#v", provider.destroyed)
	}
}

func TestReconcilerRunningReportsProviderInspectionFailure(t *testing.T) {
	if _, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: failingProvider{failure: "list"}}}).Running(context.Background()); err == nil {
		t.Fatal("expected running list failure")
	}
}

func TestReconcilerStartReportsProviderInspectionFailure(t *testing.T) {
	if err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: failingProvider{failure: "list"}}}).StartOne(context.Background()); err == nil {
		t.Fatal("expected start list failure")
	}
}

func TestReconcilerStartReportsCompletedRunnerCleanupFailure(t *testing.T) {
	if err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: failingProvider{
		failure: "destroy", instances: []RunnerInstance{{RunnerName: "done", State: RunnerStateCompleted}},
	}}}).StartOne(context.Background()); err == nil {
		t.Fatal("expected completed cleanup failure")
	}
}

func TestReconcilerStartHonorsLocalRunnerLimit(t *testing.T) {
	provider := &recordingProvider{instances: []RunnerInstance{{RunnerName: "running", State: RunnerStateRunning}}}
	if err := (Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "set", MaxRunners: 1, Provider: provider, JITConfig: &recordingJITGenerator{},
	}}).StartOne(context.Background()); err != nil || len(provider.created) != 0 {
		t.Fatalf("expected local limit to prevent start, created=%#v error=%v", provider.created, err)
	}
}

func TestReconcilerDestroyActiveReportsProviderInspectionFailure(t *testing.T) {
	if err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: failingProvider{failure: "list"}}}).DestroyActive(context.Background()); err == nil {
		t.Fatal("expected destroy list failure")
	}
}

func TestReconcilerDestroyActiveReportsProviderDestroyFailure(t *testing.T) {
	if err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: failingProvider{
		failure: "destroy", instances: []RunnerInstance{{RunnerName: "running", State: RunnerStateRunning}},
	}}}).DestroyActive(context.Background()); err == nil {
		t.Fatal("expected active destroy failure")
	}
}

func TestReconcilerRegistersRunnerLifecycleBeforeProviderStart(t *testing.T) {
	events := make([]string, 0, 2)
	provider := &orderedStartProvider{events: &events}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "set", MaxRunners: 1, Provider: provider, JITConfig: &recordingJITGenerator{},
		RegisterRunner: func(_ context.Context, runnerName string, _ int64) error {
			events = append(events, "register:"+runnerName)
			return nil
		},
	}}

	if err := reconciler.StartNamed(context.Background(), 42); err != nil {
		t.Fatal(err)
	}
	if strings.Join(events, ",") != "register:set-42,start:set-42" {
		t.Fatalf("events = %v", events)
	}
}

func TestReconcilerRegistrationFailureRemovesJITAndDoesNotStart(t *testing.T) {
	events := make([]string, 0)
	provider := &orderedStartProvider{events: &events}
	jit := &recordingJITGenerator{}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "set", MaxRunners: 1, Provider: provider, JITConfig: jit,
		RegisterRunner: func(context.Context, string, int64) error { return errors.New("reserved") },
	}}

	if err := reconciler.StartNamed(context.Background(), 42); err == nil {
		t.Fatal("expected lifecycle registration failure")
	}
	if len(events) != 0 || len(jit.removed) != 1 {
		t.Fatalf("provider events=%#v removed JIT=%#v", events, jit.removed)
	}
}

type orderedStartProvider struct{ events *[]string }

func (*orderedStartProvider) List(context.Context, string) ([]RunnerInstance, error) { return nil, nil }
func (provider *orderedStartProvider) Start(_ context.Context, request RunnerStartRequest) error {
	*provider.events = append(*provider.events, "start:"+request.RunnerName)
	return nil
}
func (*orderedStartProvider) Destroy(context.Context, RunnerInstance) error { return nil }

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
