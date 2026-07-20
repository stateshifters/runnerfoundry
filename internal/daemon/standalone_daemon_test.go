package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	"github.com/meza/github-scaler/internal/scaler"
)

func TestStandaloneDefaultsSupplyNotifierAndDrainInterval(t *testing.T) {
	defaults := withStandaloneDefaults(standaloneDaemon{})
	if defaults.notifier == nil || defaults.drainInterval == 0 {
		t.Fatalf("expected standaloneDaemon defaults, got %#v", defaults)
	}
}

func TestLoadConfigurationReturnsConfigLoadFailure(t *testing.T) {
	if _, err := loadConfiguration("config", standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return config.Config{}, errors.New("load failed") },
	}); err == nil {
		t.Fatal("expected load failure")
	}
}

func TestRunDaemonDiscoversStartsAndShutsDownRepositories(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stopped := false
	shutdown := false
	loadedConfig := runnableConfig(t)
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{
				Repositories:         []githubapp.Repository{{InstallationID: 7, URL: "https://github.com/acme/repo"}},
				InstallationFailures: map[int64]error{},
			}}, nil
		},
		newGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		notifier:  cancellingNotifier{cancel: cancel},
		startScaleSet: func(
			context.Context,
			*slog.Logger,
			scaleSetClient,
			string,
			config.ScaleSetConfig,
			*scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			return scaleSetLifecycle{
				StopPolling:   func() { stopped = true },
				Drain:         func(context.Context) (int, error) { return 0, nil },
				DestroyActive: func(context.Context) error { return nil },
				Shutdown:      func(context.Context) error { shutdown = true; return nil },
			}, nil
		},
	}
	if err := runDaemon(ctx, Options{}, daemonUnderTest); err != nil {
		t.Fatalf("expected clean shutdown, got %v", err)
	}
	if !stopped || !shutdown {
		t.Fatalf("expected lifecycle shutdown, stopped=%t shutdown=%t", stopped, shutdown)
	}
}

func TestDaemonLogsLifecycleActions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "debug")
	discoverer := &recordingDiscoverer{
		result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}},
		afterDiscover: func(int) {
			cancel()
		},
	}
	loadedConfig := runnableConfig(t)
	daemonUnderTest := standaloneDaemon{
		loadConfig:    func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return discoverer, nil },
		newLogger:     func(string, string) *slog.Logger { return logger },
		notifier:      noopNotifier{},
	}
	if err := runDaemon(ctx, Options{ConfigPath: "config.yaml"}, daemonUnderTest); err != nil {
		t.Fatalf("expected clean daemon shutdown, got %v", err)
	}
	for _, expected := range []struct {
		action string
		phase  string
	}{
		{action: "configuration", phase: "loaded"},
		{action: "daemon", phase: "ready"},
		{action: "discovery", phase: "started"},
		{action: "discovery", phase: "completed"},
		{action: "shutdown", phase: "started"},
		{action: "shutdown", phase: "completed"},
	} {
		assertStructuredAction(t, logs.Bytes(), expected.action, expected.phase)
	}
}

func TestLoadConfigurationSurfacesLocalPreflightFailures(t *testing.T) {
	valid := runnableConfig(t)
	tests := map[string]standaloneDaemon{
		"credentials": {
			loadConfig: func(string) (config.Config, error) {
				invalid := valid
				invalid.GitHub.App.PrivateKeyFile = filepath.Join(t.TempDir(), "missing")
				return invalid, nil
			},
		},
		"discoverer": {
			loadConfig: func(string) (config.Config, error) { return valid, nil },
			newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
				return nil, errors.New("factory failed")
			},
		},
	}
	for testName, daemonUnderTest := range tests {
		t.Run(testName, func(t *testing.T) {
			if _, err := loadConfiguration("config", daemonUnderTest); err == nil {
				t.Fatal("expected startup failure")
			}
		})
	}
}

func TestLoadConfigurationDoesNotContactGitHub(t *testing.T) {
	valid := runnableConfig(t)
	discoverer := &recordingDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}
	activeConfig, err := loadConfiguration("config", standaloneDaemon{
		loadConfig:    func(string) (config.Config, error) { return valid, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return discoverer, nil },
		newLogger:     func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
	})
	if err != nil {
		t.Fatalf("expected local preflight success, got %v", err)
	}
	if discoverer.calls != 0 || len(activeConfig.Repositories) != 0 {
		t.Fatalf("expected no GitHub work before readiness, calls=%d repositories=%#v", discoverer.calls, activeConfig.Repositories)
	}
}
