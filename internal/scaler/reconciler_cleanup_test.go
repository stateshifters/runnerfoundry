package scaler

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestReapRunnerLifecycleConfirmsProviderAndGitHubAbsence(t *testing.T) {
	provider := &lifecycleProvider{instances: []RunnerInstance{{ID: "owned", RunnerName: "runner", State: RunnerStateRunning}}}
	registry := &lifecycleRegistry{runners: map[string]JITRunner{"runner": {ID: 7, Name: "runner"}}}
	reconciler := Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: provider, JITConfig: registry}}
	reaped, err := reconciler.ReapRunnerLifecycle(context.Background(), RunnerLifecycle{RunnerName: "runner", GitHubRunnerID: 7})
	if err != nil || !reaped || len(provider.instances) != 0 || len(registry.runners) != 0 {
		t.Fatalf("reaped=%t err=%v provider=%#v github=%#v", reaped, err, provider.instances, registry.runners)
	}
	reaped, err = reconciler.ReapRunnerLifecycle(context.Background(), RunnerLifecycle{RunnerName: "already-absent"})
	if err != nil || !reaped {
		t.Fatalf("idempotent reap=%t err=%v", reaped, err)
	}
}

type reapRunnerLifecycleCase struct {
	name       string
	provider   *lifecycleProvider
	registry   JITConfigGenerator
	runner     RunnerLifecycle
	wantError  string
	wantReaped bool
}

var reapRunnerLifecycleCases = []reapRunnerLifecycleCase{
	{name: "provider list", provider: &lifecycleProvider{listErr: errors.New("list")}, registry: &lifecycleRegistry{runners: map[string]JITRunner{}}, runner: RunnerLifecycle{RunnerName: "runner"}, wantError: "list runner instances"},
	{name: "provider destroy", provider: &lifecycleProvider{instances: []RunnerInstance{{ID: "runner", RunnerName: "runner"}}, destroyErr: errors.New("destroy")}, registry: &lifecycleRegistry{runners: map[string]JITRunner{}}, runner: RunnerLifecycle{RunnerName: "runner"}, wantError: "destroy runner"},
	{name: "provider still present", provider: &lifecycleProvider{instances: []RunnerInstance{{ID: "runner", RunnerName: "runner"}}, retainOnDestroy: true}, registry: &lifecycleRegistry{runners: map[string]JITRunner{}}, runner: RunnerLifecycle{RunnerName: "runner"}},
	{name: "github inspect", provider: &lifecycleProvider{}, registry: &lifecycleRegistry{runners: map[string]JITRunner{}, findErr: errors.New("find")}, runner: RunnerLifecycle{RunnerName: "runner"}, wantError: "inspect github runner"},
	{name: "github remove", provider: &lifecycleProvider{}, registry: &lifecycleRegistry{runners: map[string]JITRunner{"runner": {ID: 1, Name: "runner"}}, removeErr: errors.New("remove")}, runner: RunnerLifecycle{RunnerName: "runner"}, wantError: "remove github runner"},
	{name: "github confirm", provider: &lifecycleProvider{}, registry: &lifecycleRegistry{runners: map[string]JITRunner{"runner": {ID: 1, Name: "runner"}}, confirmErr: errors.New("confirm")}, runner: RunnerLifecycle{RunnerName: "runner"}, wantError: "confirm github runner"},
	{name: "github still present", provider: &lifecycleProvider{}, registry: &lifecycleRegistry{runners: map[string]JITRunner{"runner": {ID: 1, Name: "runner"}}, retainAfterRemove: true}, runner: RunnerLifecycle{RunnerName: "runner"}},
	{name: "uninspectable without id", provider: &lifecycleProvider{}, registry: &recordingJITGenerator{}, runner: RunnerLifecycle{RunnerName: "runner"}, wantError: "absence cannot be confirmed"},
	{name: "uninspectable remove", provider: &lifecycleProvider{}, registry: &recordingJITGenerator{removeErr: errors.New("remove")}, runner: RunnerLifecycle{RunnerName: "runner", GitHubRunnerID: 1}, wantError: "remove github runner"},
	{name: "uninspectable success", provider: &lifecycleProvider{}, registry: &recordingJITGenerator{}, runner: RunnerLifecycle{RunnerName: "runner", GitHubRunnerID: 1}, wantReaped: true},
}

func TestReapRunnerLifecycleRetainsIncompleteCleanup(t *testing.T) {
	for _, test := range reapRunnerLifecycleCases {
		t.Run(test.name, func(t *testing.T) {
			assertReapRunnerLifecycleCase(t, test)
		})
	}
}

func TestReapRunnerLifecycleDefersBusyGitHubRegistration(t *testing.T) {
	registry := &lifecycleRegistry{
		runners:   map[string]JITRunner{"runner": {ID: 1, Name: "runner"}},
		removeErr: ErrRunnerStillBusy,
	}
	reaped, err := (Reconciler{ScaleSet: ScaleSetRuntime{
		Name: "set", Provider: &lifecycleProvider{}, JITConfig: registry,
	}}).ReapRunnerLifecycle(context.Background(), RunnerLifecycle{RunnerName: "runner", GitHubRunnerID: 1})
	if reaped || !errors.Is(err, ErrRunnerStillBusy) {
		t.Fatalf("reaped=%t err=%v", reaped, err)
	}
	if _, found := registry.runners["runner"]; !found {
		t.Fatal("expected busy registration to remain")
	}
}

func assertReapRunnerLifecycleCase(t *testing.T, test reapRunnerLifecycleCase) {
	t.Helper()
	reaped, err := (Reconciler{ScaleSet: ScaleSetRuntime{Name: "set", Provider: test.provider, JITConfig: test.registry}}).
		ReapRunnerLifecycle(context.Background(), test.runner)
	if reaped != test.wantReaped {
		t.Fatalf("reaped=%t", reaped)
	}
	if test.wantError == "" && err != nil {
		t.Fatal(err)
	}
	if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
		t.Fatalf("error=%v", err)
	}
}
