//go:build integration

package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

func TestDaemonReloadRetriesPersistedDemandWithReplacementWorker(t *testing.T) {
	fixture, ctx := newReloadRecoveryFixture(t)
	if err := runDaemon(ctx, Options{ReloadRequests: fixture.reloadRequests}, fixture.daemon()); err != nil {
		t.Fatalf("expected reload recovery success, got %v", err)
	}
	fixture.assertRecovered(t)
}

func runRestartRecovery(t *testing.T, demand []scaler.JobDemand, worker scaler.RepositoryWorker) {
	t.Helper()
	fixture, ctx := newRestartRecoveryFixture(t, demand, worker)
	if err := runDaemon(ctx, Options{}, fixture.daemon()); err != nil {
		t.Fatalf("expected restart recovery success, got %v", err)
	}
	assertPersistedDemand(t, fixture.databasePath, "linux", demand[0].RequestID)
}

const recoveryRepositoryURL = "https://github.com/acme/repo"

type reloadRecoveryFixture struct {
	t                         *testing.T
	databasePath              string
	cancel                    context.CancelFunc
	reloadRequests            chan struct{}
	loadedConfig              config.Config
	discovererCount           int
	replacementDiscoveryCalls int
	scaleSetStarts            int
	oldPollingStopped         bool
	oldLifecycleShutdown      bool
	replacementWorker         *recordingRecoveryWorker
}

func newReloadRecoveryFixture(t *testing.T) (*reloadRecoveryFixture, context.Context) {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "state.db")
	seedRecoveryDemand(t, databasePath, []scaler.JobDemand{{
		RequestID: 7, Repository: recoveryRepositoryURL, QueueTime: time.Now(), Acquired: true,
	}})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	loadedConfig := runnableConfig(t)
	loadedConfig.State.Path = databasePath
	return &reloadRecoveryFixture{
		t: t, databasePath: databasePath, cancel: cancel,
		reloadRequests: make(chan struct{}, 1), loadedConfig: loadedConfig,
		replacementWorker: &recordingRecoveryWorker{existsErr: errors.New("provider unavailable")},
	}, ctx
}

func (fixture *reloadRecoveryFixture) daemon() standaloneDaemon {
	return standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return fixture.loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return fixture.nextDiscoverer(), nil
		},
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return successfulRegistrationReconciliationClient(), nil
		},
		newLogger:                func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		openDemandStore:          openRecoveryStore("reload"),
		notifier:                 noopNotifier{},
		drainInterval:            time.Nanosecond,
		reconciliationRetryDelay: time.Nanosecond,
		startScaleSet:            fixture.startScaleSet,
	}
}

func (fixture *reloadRecoveryFixture) nextDiscoverer() repositoryDiscovererFunc {
	fixture.discovererCount++
	if fixture.discovererCount == 1 {
		return func(context.Context) (githubapp.DiscoveryResult, error) {
			fixture.reloadRequests <- struct{}{}
			return recoveryDiscovery(), nil
		}
	}
	return func(context.Context) (githubapp.DiscoveryResult, error) {
		fixture.replacementDiscoveryCalls++
		if fixture.replacementDiscoveryCalls > 1 {
			fixture.replacementWorker.existsErr = nil
		}
		return recoveryDiscovery(), nil
	}
}

func (fixture *reloadRecoveryFixture) startScaleSet(
	_ context.Context, _ *slog.Logger, _ scaleSetClient, repositoryURL string,
	_ config.ScaleSetConfig, coordinator *scaler.Coordinator,
) (scaleSetLifecycle, error) {
	fixture.scaleSetStarts++
	if fixture.scaleSetStarts == 1 {
		return scaleSetLifecycle{
			StopPolling: func() { fixture.oldPollingStopped = true },
			CloseSession: func(context.Context) error {
				fixture.oldLifecycleShutdown = true
				return nil
			},
			Drain:    func(context.Context) (int, error) { return 0, nil },
			Shutdown: func(context.Context) error { fixture.t.Fatal("reload deleted a confirmed registration"); return nil },
		}, nil
	}
	if !fixture.oldPollingStopped || !fixture.oldLifecycleShutdown {
		fixture.t.Fatal("replacement worker started before the old lifecycle stopped")
	}
	coordinator.AddRepository(repositoryURL, &completionSignallingWorker{
		RepositoryWorker: fixture.replacementWorker, afterRecovery: fixture.cancel,
	})
	return inertLifecycle(), nil
}

func (fixture *reloadRecoveryFixture) assertRecovered(t *testing.T) {
	t.Helper()
	if fixture.replacementDiscoveryCalls < 2 || fixture.scaleSetStarts != 2 {
		t.Fatalf("expected replacement retry after reload, discoveries=%d starts=%d", fixture.replacementDiscoveryCalls, fixture.scaleSetStarts)
	}
	if len(fixture.replacementWorker.started) != 1 || fixture.replacementWorker.started[0] != 7 {
		t.Fatalf("expected replacement worker to provision request 7 once, got %#v", fixture.replacementWorker.started)
	}
	assertPersistedDemand(t, fixture.databasePath, "linux", 7)
}

type restartRecoveryFixture struct {
	databasePath string
	cancel       context.CancelFunc
	loadedConfig config.Config
	worker       scaler.RepositoryWorker
}

func newRestartRecoveryFixture(
	t *testing.T, demand []scaler.JobDemand, worker scaler.RepositoryWorker,
) (restartRecoveryFixture, context.Context) {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "state.db")
	seedRecoveryDemand(t, databasePath, demand)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	loadedConfig := runnableConfig(t)
	loadedConfig.State.Path = databasePath
	return restartRecoveryFixture{
		databasePath: databasePath, cancel: cancel, loadedConfig: loadedConfig, worker: worker,
	}, ctx
}

func (fixture restartRecoveryFixture) daemon() standaloneDaemon {
	return standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return fixture.loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: recoveryDiscovery()}, nil
		},
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return successfulRegistrationReconciliationClient(), nil
		},
		newLogger:       func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		openDemandStore: openRecoveryStore("restart"), notifier: noopNotifier{},
		startScaleSet: func(_ context.Context, _ *slog.Logger, _ scaleSetClient, repositoryURL string,
			_ config.ScaleSetConfig, coordinator *scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			coordinator.AddRepository(repositoryURL, &completionSignallingWorker{
				RepositoryWorker: fixture.worker, afterRecovery: fixture.cancel,
			})
			return inertLifecycle(), nil
		},
	}
}

func seedRecoveryDemand(t *testing.T, databasePath string, demand []scaler.JobDemand) {
	t.Helper()
	store, err := scaler.OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(context.Background(), "linux", demand, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func recoveryDiscovery() githubapp.DiscoveryResult {
	return githubapp.DiscoveryResult{
		Repositories:         []githubapp.Repository{{InstallationID: 1, URL: recoveryRepositoryURL}},
		InstallationFailures: map[githubapp.InstallationID]error{},
	}
}

func openRecoveryStore(operation string) func(string) (scaler.DemandStore, io.Closer, error) {
	return func(path string) (scaler.DemandStore, io.Closer, error) {
		store, err := scaler.OpenSQLiteDemandStore(path)
		if err != nil {
			return nil, nil, fmt.Errorf("open %s recovery store: %w", operation, err)
		}
		return store, store, nil
	}
}

type completionSignallingWorker struct {
	scaler.RepositoryWorker
	afterRecovery func()
	signalOnce    sync.Once
}

type repositoryDiscovererFunc func(context.Context) (githubapp.DiscoveryResult, error)

func (discover repositoryDiscovererFunc) Discover(ctx context.Context) (githubapp.DiscoveryResult, error) {
	return discover(ctx)
}

func (worker *completionSignallingWorker) Exists(ctx context.Context, requestID int64) (bool, error) {
	exists, err := worker.RepositoryWorker.Exists(ctx, requestID)
	if err != nil {
		return false, fmt.Errorf("inspect recovery runner: %w", err)
	}
	if exists {
		worker.signalOnce.Do(worker.afterRecovery)
	}
	return exists, nil
}

func (worker *completionSignallingWorker) StartOne(ctx context.Context, requestID int64) error {
	if err := worker.RepositoryWorker.StartOne(ctx, requestID); err != nil {
		return fmt.Errorf("start recovery runner: %w", err)
	}
	worker.signalOnce.Do(worker.afterRecovery)
	return nil
}

func assertPersistedDemand(t *testing.T, databasePath string, scaleSetName string, requestID int64) {
	t.Helper()
	store, err := scaler.OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close persisted demand store: %v", closeErr)
		}
	})
	demand, err := store.Ordered(context.Background(), scaleSetName)
	if err != nil {
		t.Fatal(err)
	}
	if len(demand) != 1 || demand[0].RequestID != requestID {
		t.Fatalf("expected request %d to remain persisted, got %#v", requestID, demand)
	}
}
