package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
)

func TestReconcileDiscoveryRetainsStateOnDiscoveryFailure(t *testing.T) {
	activeConfig := testActiveConfiguration()
	activeConfig.Discoverer = staticDiscoverer{err: errors.New("unavailable")}
	reconcileDiscovery(context.Background(), standaloneDaemon{}, &activeConfig)
	if !activeConfig.ReconciliationPending {
		t.Fatal("expected failed discovery to request a short reconciliation retry")
	}
	activeConfig.Discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{
		Repositories:         []githubapp.Repository{{InstallationID: 1, URL: "https://github.com/acme/repo"}},
		InstallationFailures: map[githubapp.InstallationID]error{},
	}}
	reconcileDiscovery(context.Background(), standaloneDaemon{newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errors.New("start failed")
	}}, &activeConfig)
	if !activeConfig.ReconciliationPending {
		t.Fatal("expected failed repository startup to request a short reconciliation retry")
	}
	activeConfig.Discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}}
	reconcileDiscovery(context.Background(), standaloneDaemon{}, &activeConfig)
	if activeConfig.ReconciliationPending {
		t.Fatal("expected successful reconciliation to restore the discovery interval")
	}
}
