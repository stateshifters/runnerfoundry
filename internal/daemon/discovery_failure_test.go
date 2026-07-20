package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
		InstallationFailures: map[int64]error{},
	}}
	reconcileDiscovery(context.Background(), standaloneDaemon{newGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errors.New("start failed")
	}}, &activeConfig)
	if !activeConfig.ReconciliationPending {
		t.Fatal("expected failed repository startup to request a short reconciliation retry")
	}
	activeConfig.Discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}
	reconcileDiscovery(context.Background(), standaloneDaemon{}, &activeConfig)
	if activeConfig.ReconciliationPending {
		t.Fatal("expected successful reconciliation to restore the discovery interval")
	}
}

func TestRunReportsMissingConfig(t *testing.T) {
	if err := Run(context.Background(), Options{ConfigPath: filepath.Join(t.TempDir(), "missing")}); err == nil {
		t.Fatal("expected missing config failure")
	}
}

func TestRunReportsStandaloneGitHubPreflightFailure(t *testing.T) {
	configPath := writeRunConfig(t, "state:\n  path: state.db\n")
	if err := Run(context.Background(), Options{ConfigPath: configPath}); err == nil {
		t.Fatal("expected invalid GitHub private key to fail standalone startup")
	}
}

func TestRunReportsInvalidClusteredDatabaseConfiguration(t *testing.T) {
	configPath := writeRunConfig(t, "state:\n  postgres:\n    role: github_scaler\n    port: invalid\n")
	if err := Run(context.Background(), Options{ConfigPath: configPath}); err == nil {
		t.Fatal("expected invalid PostgreSQL configuration to fail clustered startup")
	}
}

func TestRunStandaloneReportsCancellationBeforeReadiness(t *testing.T) {
	directory := t.TempDir()
	configPath := writeValidStandaloneRunConfig(t, filepath.Join(directory, "state.db"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, Options{ConfigPath: configPath}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestRunStandaloneReportsSQLiteOpenFailure(t *testing.T) {
	configPath := writeValidStandaloneRunConfig(t, t.TempDir())
	if err := Run(context.Background(), Options{ConfigPath: configPath}); err == nil {
		t.Fatal("expected SQLite open failure")
	}
}

func writeValidStandaloneRunConfig(t *testing.T, statePath string) string {
	t.Helper()
	directory := t.TempDir()
	credentials := testAppCredentials(t)
	privateKey := filepath.Join(directory, "app.pem")
	if err := os.WriteFile(privateKey, []byte(credentials.PrivateKey), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "config.yaml")
	configured := "github:\n  app:\n    app_id: 1\n    private_key_file: " + privateKey +
		"\nstate:\n  path: " + statePath +
		"\nscale_sets:\n  - name: linux\n    max_runners: 1\n    docker:\n      compose_file: compose.yaml\n      service: runner\n"
	if err := os.WriteFile(configPath, []byte(configured), 0o600); err != nil {
		t.Fatal(err)
	}
	return configPath
}

func writeRunConfig(t *testing.T, state string) string {
	t.Helper()
	directory := t.TempDir()
	privateKey := filepath.Join(directory, "app.pem")
	if err := os.WriteFile(privateKey, []byte("invalid-private-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "config.yaml")
	configured := "github:\n  app:\n    app_id: 1\n    private_key_file: " + privateKey +
		"\n" + state + "scale_sets:\n  - name: linux\n    max_runners: 1\n    docker:\n      compose_file: compose.yaml\n      service: runner\n"
	if err := os.WriteFile(configPath, []byte(configured), 0o600); err != nil {
		t.Fatal(err)
	}
	return configPath
}
