package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/scaler"
)

func TestJITGeneratorRemovesStaleRunnerAfterNameConflict(t *testing.T) {
	client := &fakeScaleSetClient{
		jitRunnerAlreadyExists: true,
		jitConfig:              "encoded",
		jitRunner:              &upstream.RunnerReference{ID: 100, Name: "linux-1"},
		runnerByName:           &upstream.RunnerReference{ID: 99, Name: "linux-1"},
	}
	generator := scalesetJITGenerator{Client: client, ScaleSetID: 12}
	jitConfig, err := generator.Generate(context.Background(), "linux-1")
	if err != nil {
		t.Fatalf("expected jit success after stale runner cleanup, got %v", err)
	}
	if jitConfig.Runner.ID != 100 || jitConfig.JITConfig != "encoded" {
		t.Fatalf("expected retried jit config, got %#v", jitConfig)
	}
	if len(client.runnerNameLookups) != 1 || client.runnerNameLookups[0] != "linux-1" {
		t.Fatalf("expected runner name lookup, got %#v", client.runnerNameLookups)
	}
	if len(client.removedRunnerIDs) != 1 || client.removedRunnerIDs[0] != 99 {
		t.Fatalf("expected stale runner removal, got %#v", client.removedRunnerIDs)
	}
	if len(client.generatedRunnerNames) != 2 {
		t.Fatalf("expected one retry after stale runner cleanup, got %#v", client.generatedRunnerNames)
	}
}

func TestJITGeneratorSurfacesStaleRunnerLookupFailure(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{
		jitRunnerAlreadyExists: true,
		runnerByNameErr:        errors.New("lookup failed"),
		runnerByName:           &upstream.RunnerReference{ID: 99, Name: "linux-1"},
		removeRunnerErr:        nil,
		removedRunnerIDs:       nil,
	}}
	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected lookup error")
	}
}

func TestJITGeneratorSurfacesMissingStaleRunner(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{
		jitRunnerAlreadyExists: true,
	}}
	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected missing stale runner error")
	}
}

func TestJITGeneratorSurfacesStaleRunnerRemovalFailure(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{
		jitRunnerAlreadyExists: true,
		runnerByName:           &upstream.RunnerReference{ID: 99, Name: "linux-1"},
		removeRunnerErr:        errors.New("remove failed"),
	}}
	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected remove error")
	}
}

func TestJITGeneratorSurfacesRetryFailureAfterStaleRunnerCleanup(t *testing.T) {
	client := &fakeScaleSetClient{
		jitRunnerAlreadyExists: true,
		jitRetryErr:            errors.New("retry failed"),
		runnerByName:           &upstream.RunnerReference{ID: 99, Name: "linux-1"},
	}
	generator := scalesetJITGenerator{Client: client}

	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected retry error")
	}
	if len(client.removedRunnerIDs) != 1 || client.removedRunnerIDs[0] != 99 {
		t.Fatalf("expected stale runner removal, got %#v", client.removedRunnerIDs)
	}
	if len(client.generatedRunnerNames) != 2 {
		t.Fatalf("expected one retry after stale runner cleanup, got %#v", client.generatedRunnerNames)
	}
}

func TestJITGeneratorHandlesMissingRunnerReference(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{jitConfig: "encoded"}, ScaleSetID: 12}
	jitConfig, err := generator.Generate(context.Background(), "linux-1")
	if err != nil {
		t.Fatalf("expected jit success, got %v", err)
	}
	if jitConfig.Runner.ID != 0 || jitConfig.Runner.Name != "" {
		t.Fatalf("expected empty runner reference, got %#v", jitConfig.Runner)
	}
}

func TestJITGeneratorRemovesGeneratedRunner(t *testing.T) {
	client := &fakeScaleSetClient{}
	generator := scalesetJITGenerator{Client: client}
	if err := generator.Remove(context.Background(), scaler.JITRunner{ID: 99, Name: "linux-1"}); err != nil {
		t.Fatalf("expected remove success, got %v", err)
	}
	if len(client.removedRunnerIDs) != 1 || client.removedRunnerIDs[0] != 99 {
		t.Fatalf("expected runner removal, got %#v", client.removedRunnerIDs)
	}
}

func TestJITGeneratorIgnoresMissingRunnerReference(t *testing.T) {
	client := &fakeScaleSetClient{}
	generator := scalesetJITGenerator{Client: client}
	if err := generator.Remove(context.Background(), scaler.JITRunner{}); err != nil {
		t.Fatalf("expected missing runner reference success, got %v", err)
	}
	if len(client.removedRunnerIDs) != 0 {
		t.Fatalf("expected no runner removal, got %#v", client.removedRunnerIDs)
	}
}

func TestJITGeneratorSurfacesRemoveFailure(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{removeRunnerErr: errors.New("remove failed")}}
	if err := generator.Remove(context.Background(), scaler.JITRunner{ID: 99, Name: "linux-1"}); err == nil {
		t.Fatal("expected remove error")
	}
}

func waitForPoll(t *testing.T, messageClient *fakeMessageClient) {
	t.Helper()
	select {
	case <-messageClient.polled:
	case <-time.After(time.Second):
		t.Fatal("expected poll")
	}
}

func assertScaleSetLabels(t *testing.T, labels []upstream.Label, expectedNames []string) {
	t.Helper()
	if len(labels) != len(expectedNames) {
		t.Fatalf("expected labels %v, got %#v", expectedNames, labels)
	}
	for labelIndex, expectedName := range expectedNames {
		if labels[labelIndex].Name != expectedName || labels[labelIndex].Type != "System" {
			t.Fatalf("expected label %q at index %d, got %#v", expectedName, labelIndex, labels[labelIndex])
		}
	}
}
