package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestLoadConfigurationReturnsConfigLoadFailure(t *testing.T) {
	if _, err := loadConfiguration("config", standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return config.Config{}, errors.New("load failed") },
	}); err == nil {
		t.Fatal("expected load failure")
	}
}

func TestRunDaemonDiscoversStartsAndShutsDownRepositories(t *testing.T) {
	ctx, fixture := standaloneLifecycleFixture(t)
	if err := runDaemon(ctx, Options{}, fixture.daemon); err != nil {
		t.Fatalf("expected clean shutdown, got %v", err)
	}
	if !fixture.pollingStopped || !fixture.shutdown {
		t.Fatalf("expected lifecycle shutdown, stopped=%t shutdown=%t", fixture.pollingStopped, fixture.shutdown)
	}
}

type standaloneLifecycleObservation struct {
	daemon         standaloneDaemon
	pollingStopped bool
	shutdown       bool
}

func standaloneLifecycleFixture(t *testing.T) (context.Context, *standaloneLifecycleObservation) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	fixture := &standaloneLifecycleObservation{}
	loadedConfig := runnableConfig(t)
	fixture.daemon = standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return fixture.discovery(), nil
		},
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return successfulRegistrationReconciliationClient(), nil
		},
		newLogger:     func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		notifier:      cancellingNotifier{cancel: cancel},
		startScaleSet: fixture.startScaleSet,
	}
	return ctx, fixture
}

func (*standaloneLifecycleObservation) discovery() staticDiscoverer {
	return staticDiscoverer{result: githubapp.DiscoveryResult{
		Repositories:         []githubapp.Repository{{InstallationID: 7, URL: "https://github.com/acme/repo"}},
		InstallationFailures: map[githubapp.InstallationID]error{},
	}}
}

func (fixture *standaloneLifecycleObservation) startScaleSet(
	context.Context,
	*slog.Logger,
	scaleSetClient,
	string,
	config.ScaleSetConfig,
	*scaler.Coordinator,
) (scaleSetLifecycle, error) {
	return scaleSetLifecycle{
		StopPolling:   func() { fixture.pollingStopped = true },
		Drain:         func(context.Context) (int, error) { return 0, nil },
		DestroyActive: func(context.Context) error { return nil },
		Shutdown:      func(context.Context) error { fixture.shutdown = true; return nil },
	}, nil
}

func TestDaemonLogsLifecycleActions(t *testing.T) {
	ctx, fixture := standaloneLoggingFixture(t)
	if err := runDaemon(ctx, Options{ConfigPath: "config.yaml"}, fixture.daemon); err != nil {
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
		assertStructuredAction(t, fixture.logs.Bytes(), expected.action, expected.phase)
	}
}

type standaloneLogFixture struct {
	logs   *bytes.Buffer
	daemon standaloneDaemon
}

func standaloneLoggingFixture(t *testing.T) (context.Context, standaloneLogFixture) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "debug")
	discoverer := &recordingDiscoverer{
		result:        githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}},
		afterDiscover: func(int) { cancel() },
	}
	loadedConfig := runnableConfig(t)
	return ctx, standaloneLogFixture{logs: logs, daemon: standaloneDaemon{
		loadConfig:    func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return discoverer, nil },
		newLogger:     func(string, string) *slog.Logger { return logger },
		notifier:      noopNotifier{},
	}}
}

func TestLoadConfigurationSurfacesCredentialPreflightFailure(t *testing.T) {
	expectedErr := errors.New("credentials failed")
	daemonUnderTest := localPreflightTestDaemon()
	daemonUnderTest.loadCredentials = func(config.GitHubConfig) (config.GitHubAppCredentials, error) {
		return config.GitHubAppCredentials{}, expectedErr
	}
	if _, err := loadConfiguration("config", daemonUnderTest); !errors.Is(err, expectedErr) {
		t.Fatalf("startup failure = %v, want %v", err, expectedErr)
	}
}

func TestLoadConfigurationSurfacesDiscovererPreflightFailure(t *testing.T) {
	expectedErr := errors.New("discoverer failed")
	daemonUnderTest := localPreflightTestDaemon()
	daemonUnderTest.newDiscoverer = func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
		return nil, expectedErr
	}
	if _, err := loadConfiguration("config", daemonUnderTest); !errors.Is(err, expectedErr) {
		t.Fatalf("startup failure = %v, want %v", err, expectedErr)
	}
}

func localPreflightTestDaemon() standaloneDaemon {
	return standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return config.Config{}, nil },
		loadCredentials: func(config.GitHubConfig) (config.GitHubAppCredentials, error) {
			return config.GitHubAppCredentials{}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
	}
}

func TestLoadConfigurationDoesNotContactGitHub(t *testing.T) {
	valid := runnableConfig(t)
	discoverer := &recordingDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}}
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
