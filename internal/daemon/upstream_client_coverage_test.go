//go:build coverage

package daemon

import (
	"context"
	"testing"

	upstream "github.com/actions/scaleset"
)

func TestCoverageGitHubClientRequiresToken(t *testing.T) {
	if _, err := newGitHubClient("https://github.com/meza/repo-a", ""); err == nil {
		t.Fatal("expected token error")
	}
}

func TestCoverageGitHubClientRequiresRepositoryURL(t *testing.T) {
	if _, err := newGitHubClient("", "secret"); err == nil {
		t.Fatal("expected repository error")
	}
}

func TestCoverageScaleSetClientMethodsReturnBoundaryErrors(t *testing.T) {
	client := coverageScaleSetClient{}
	if _, err := client.GetRunnerGroupByName(context.Background(), "default"); err == nil {
		t.Fatal("expected group error")
	}
	if _, err := client.GetRunnerScaleSet(context.Background(), 1, "linux"); err == nil {
		t.Fatal("expected scale-set error")
	}
	if _, err := client.CreateRunnerScaleSet(context.Background(), &upstream.RunnerScaleSet{}); err == nil {
		t.Fatal("expected create error")
	}
	if _, err := client.MessageSessionClient(context.Background(), 1, "runner"); err == nil {
		t.Fatal("expected message error")
	}
	if _, err := client.GenerateJitRunnerConfig(context.Background(), &upstream.RunnerScaleSetJitRunnerSetting{}, 1); err == nil {
		t.Fatal("expected jit error")
	}
}
