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
)

func TestRunDaemonReconcilesPeriodicDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	discoverer := &recordingDiscoverer{result: githubapp.DiscoveryResult{
		InstallationFailures: map[githubapp.InstallationID]error{},
	}, afterDiscover: func(call int) {
		if call == 2 {
			cancel()
		}
	}}
	loadedConfig := runnableConfig(t)
	loadedConfig.GitHub.DiscoveryInterval = "1ns"
	daemonUnderTest := standaloneDaemon{
		loadConfig:    func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return discoverer, nil },
		newLogger:     func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
	}
	if err := runDaemon(ctx, Options{}, daemonUnderTest); err != nil {
		t.Fatalf("expected periodic reconciliation shutdown, got %v", err)
	}
	if discoverer.calls < 2 {
		t.Fatalf("expected repeated discovery, got %d calls", discoverer.calls)
	}
}

func TestRunDaemonSurfacesReadyFailure(t *testing.T) {
	loadedConfig := runnableConfig(t)
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		notifier:  failingNotifier{readyErr: errors.New("ready failed")},
	}
	if err := runDaemon(context.Background(), Options{}, daemonUnderTest); err == nil {
		t.Fatal("expected ready notification failure")
	}
}

func TestRunDaemonSignalsReadyBeforeGitHubDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ready := false
	loadedConfig := runnableConfig(t)
	discoverer := readinessCheckingDiscoverer{ready: &ready, cancel: cancel}
	daemonUnderTest := standaloneDaemon{
		loadConfig:    func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return discoverer, nil },
		newLogger:     func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		notifier:      readyRecordingNotifier{ready: &ready},
	}
	if err := runDaemon(ctx, Options{}, daemonUnderTest); err != nil {
		t.Fatalf("expected discovery failure to remain retryable, got %v", err)
	}
	if !ready {
		t.Fatal("expected readiness notification")
	}
}

func TestRunDaemonRejectsUnvalidatedDiscoveryInterval(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.GitHub.DiscoveryInterval = "invalid"
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
	}
	if err := runDaemon(context.Background(), Options{}, daemonUnderTest); err == nil {
		t.Fatal("expected invalid discovery interval error")
	}
}

func TestRunDaemonAppliesReloadRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	loadedConfig := runnableConfig(t)
	notifier := &reloadCancellingNotifier{cancel: cancel}
	reloads := make(chan struct{}, 1)
	reloads <- struct{}{}
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		notifier:  notifier,
	}
	if err := runDaemon(ctx, Options{ReloadRequests: reloads}, daemonUnderTest); err != nil {
		t.Fatalf("expected reload and shutdown, got %v", err)
	}
	if notifier.readyCalls != 2 || notifier.reloadCalls != 1 {
		t.Fatalf("expected ready/reload notifications, got %#v", notifier)
	}
}

func TestRunDaemonRetainsActiveConfigurationAfterReloadFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	loadedConfig := runnableConfig(t)
	logs := &bytes.Buffer{}
	reloads := make(chan struct{}, 2)
	reloads <- struct{}{}
	reloads <- struct{}{}
	notifier := &reloadFailureRetainingNotifier{cancel: cancel}
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}}, nil
		},
		newLogger: func(string, string) *slog.Logger { return newLoggerForWriter(logs, "json", "debug") },
		notifier:  notifier,
	}
	if err := runDaemon(ctx, Options{ReloadRequests: reloads}, daemonUnderTest); err != nil {
		t.Fatalf("expected failed reload to retain active daemon, got %v", err)
	}
	if notifier.readyCalls != 3 || notifier.reloadCalls != 2 {
		t.Fatalf("expected completed reload notification, got %#v", notifier)
	}
	assertStructuredAction(t, logs.Bytes(), "reload", "preflight_rejected")
}

func TestRunDaemonDoesNotWaitForReplacementDiscoveryBeforeReadyNotification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	loadedConfig := runnableConfig(t)
	reloads := make(chan struct{}, 1)
	reloads <- struct{}{}
	discoverer := &recordingDiscoverer{result: githubapp.DiscoveryResult{
		InstallationFailures: map[githubapp.InstallationID]error{},
	}, afterDiscover: func(int) {}}
	notifier := &replacementReadyNotifier{cancel: cancel, discoverer: discoverer}
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return discoverer, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		notifier:  notifier,
	}

	if err := runDaemon(ctx, Options{ReloadRequests: reloads}, daemonUnderTest); err != nil {
		t.Fatalf("expected ready notification failure to preserve replacement, got %v", err)
	}
	if notifier.discoveryCallsAtReloadReady != 2 {
		t.Fatalf("ready waited for replacement discovery: calls=%d, want 2", notifier.discoveryCallsAtReloadReady)
	}
}

type replacementReadyNotifier struct {
	cancel                      context.CancelFunc
	discoverer                  *recordingDiscoverer
	readyCalls                  int
	discoveryCallsAtReloadReady int
}

func (notifier *replacementReadyNotifier) Ready(context.Context) error {
	notifier.readyCalls++
	if notifier.readyCalls != 2 {
		return nil
	}
	notifier.discoveryCallsAtReloadReady = notifier.discoverer.calls
	notifier.cancel()
	return errors.New("ready failed")
}

func (*replacementReadyNotifier) Reloading(context.Context) error { return nil }

type reloadFailureRetainingNotifier struct {
	cancel      context.CancelFunc
	readyCalls  int
	reloadCalls int
}

func (notifier *reloadFailureRetainingNotifier) Ready(context.Context) error {
	notifier.readyCalls++
	if notifier.readyCalls == 3 {
		notifier.cancel()
	}
	return nil
}

func (notifier *reloadFailureRetainingNotifier) Reloading(context.Context) error {
	notifier.reloadCalls++
	if notifier.reloadCalls == 1 {
		return errors.New("reload failed")
	}
	return nil
}
