package daemon

import "testing"

func TestLocalScaleSetNameQualifiesScaleSetByRepository(t *testing.T) {
	runtimeName := localScaleSetName("https://github.com/meza/agent-playground", "linux")
	if runtimeName != "github-com-meza-agent-playground-linux" {
		t.Fatalf("unexpected runtime name %q", runtimeName)
	}
}

func TestLocalScaleSetNameHandlesNonURLRepositoryTargets(t *testing.T) {
	runtimeName := localScaleSetName("github.com:meza/agent-playground", "linux")
	if runtimeName != "github-com-meza-agent-playground-linux" {
		t.Fatalf("unexpected runtime name %q", runtimeName)
	}
}
