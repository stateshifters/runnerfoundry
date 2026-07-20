package daemon

import (
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
)

func TestJITGeneratorUsesScaleSetClient(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{
		jitConfig: "encoded",
		jitRunner: &upstream.RunnerReference{ID: 99, Name: "linux-1"},
	}, ScaleSetID: 12}
	jitConfig, err := generator.Generate(context.Background(), "linux-1")
	if err != nil {
		t.Fatalf("expected jit success, got %v", err)
	}
	if jitConfig.JITConfig != "encoded" {
		t.Fatalf("expected encoded jit, got %q", jitConfig.JITConfig)
	}
	if jitConfig.Runner.ID != 99 || jitConfig.Runner.Name != "linux-1" {
		t.Fatalf("expected runner reference, got %#v", jitConfig.Runner)
	}
}

func TestJITGeneratorSurfacesClientFailure(t *testing.T) {
	generator := scalesetJITGenerator{Client: &fakeScaleSetClient{jitErr: errors.New("jit failed")}}
	if _, err := generator.Generate(context.Background(), "linux-1"); err == nil {
		t.Fatal("expected jit error")
	}
}
