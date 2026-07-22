package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestCoordinatedRepositoryWorkerWrapsRunnerStartFailure(t *testing.T) {
	worker := coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "local", MaxRunners: 1, Provider: failingStartProvider{}, JITConfig: fakeJITConfigGenerator{},
	}}}
	if err := worker.StartOne(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "start repository runner") {
		t.Fatalf("expected wrapped runner start failure, got %v", err)
	}
	worker.MessageClient = scaleSetMessageClient{Client: &fakeMessageClient{acquireErr: errors.New("acquire failed")}}
	if err := worker.Acquire(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "acquire failed") {
		t.Fatalf("expected acquisition failure, got %v", err)
	}
	worker.Reconciler.ScaleSet.Provider = &fakeScalerProvider{listErr: errors.New("list failed")}
	if _, err := worker.Exists(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "inspect runner") {
		t.Fatalf("expected wrapped runner inspection failure, got %v", err)
	}
}

func TestCoordinatedRepositoryWorkerUsesRepositoryQualifiedRunnerName(t *testing.T) {
	provider := &fakeScalerProvider{instances: []scaler.RunnerInstance{{
		RunnerName: "github-com-meza-repo-a-self-hosted-42",
		State:      scaler.RunnerStateRunning,
	}}}
	worker := coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: "github-com-meza-repo-a-self-hosted", MaxRunners: 2, Provider: provider, JITConfig: fakeJITConfigGenerator{},
	}}}

	if err := worker.StartOne(context.Background(), 42); err != nil {
		t.Fatalf("expected runner start success, got %v", err)
	}
	if len(provider.started) != 1 || provider.started[0].RunnerName != "github-com-meza-repo-a-self-hosted-42" {
		t.Fatalf("expected repository-qualified runner name, got %#v", provider.started)
	}
	exists, err := worker.Exists(context.Background(), 42)
	if err != nil || !exists {
		t.Fatalf("expected repository-qualified runner lookup to find the runner, got exists=%t err=%v", exists, err)
	}
}

func TestCoordinatedRepositoryWorkerUsesCompactedRunnerNameForStartAndRecovery(t *testing.T) {
	baseName := "github-com-meza-mezas-conventional-changelog-meza-ubuntu-latest"
	expectedRunnerName := "github-com-meza-mezas-conventional-changelog-ee540047238cb46a-42"
	provider := &fakeScalerProvider{instances: []scaler.RunnerInstance{{
		RunnerName: expectedRunnerName,
		State:      scaler.RunnerStateRunning,
	}}}
	jitGenerator := &recordingRunnerNameJITGenerator{}
	worker := coordinatedRepositoryWorker{Reconciler: scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
		Name: baseName, MaxRunners: 2, Provider: provider, JITConfig: jitGenerator,
	}}}

	if err := worker.StartOne(context.Background(), 42); err != nil {
		t.Fatalf("expected runner start success, got %v", err)
	}
	if len(provider.started) != 1 || provider.started[0].RunnerName != expectedRunnerName {
		t.Fatalf("expected compacted runner name, got %#v", provider.started)
	}
	if len(jitGenerator.runnerNames) != 1 || jitGenerator.runnerNames[0] != expectedRunnerName {
		t.Fatalf("expected compacted JIT runner name, got %#v", jitGenerator.runnerNames)
	}
	exists, err := worker.Exists(context.Background(), 42)
	if err != nil || !exists {
		t.Fatalf("expected compacted runner lookup to find the runner, got exists=%t err=%v", exists, err)
	}
}
