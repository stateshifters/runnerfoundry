package daemon

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
)

func TestRunDaemonReconcilesPeriodicDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	discoverer := &recordingDiscoverer{result: githubapp.DiscoveryResult{
		InstallationFailures: map[int64]error{},
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
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}, nil
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
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}, nil
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
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		notifier:  notifier, drainInterval: time.Millisecond,
	}
	if err := runDaemon(ctx, Options{ReloadRequests: reloads}, daemonUnderTest); err != nil {
		t.Fatalf("expected reload and shutdown, got %v", err)
	}
	if notifier.readyCalls != 2 || notifier.reloadCalls != 1 {
		t.Fatalf("expected ready/reload notifications, got %#v", notifier)
	}
}

func TestRunDaemonSurfacesReloadFailure(t *testing.T) {
	loadedConfig := runnableConfig(t)
	reloads := make(chan struct{}, 1)
	reloads <- struct{}{}
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		notifier:  failingNotifier{reloadErr: errors.New("reload failed")},
	}
	if err := runDaemon(context.Background(), Options{ReloadRequests: reloads}, daemonUnderTest); err == nil {
		t.Fatal("expected reload failure")
	}
}
