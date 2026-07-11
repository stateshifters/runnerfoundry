package daemon

import (
	"testing"

	"github.com/meza/github-scaler/internal/config"
)

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

func TestValidateLocalScaleSetNamesRejectsCollisions(t *testing.T) {
	err := validateLocalScaleSetNames(
		[]string{
			"https://github.com/meza/repo.a",
			"https://github.com/meza/repo-a",
		},
		[]config.ScaleSetConfig{{Name: "linux"}},
	)
	if err == nil {
		t.Fatal("expected local scale set name collision")
	}
}
