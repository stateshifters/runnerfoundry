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

	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	"github.com/meza/github-scaler/internal/scaler"
)

func TestDaemonReloadRetriesPersistedDemandWithReplacementWorker(t *testing.T) {
	const repositoryURL = "https://github.com/acme/repo"
	databasePath := filepath.Join(t.TempDir(), "state.db")
	seedStore, err := scaler.OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := seedStore.Apply(context.Background(), "linux", []scaler.JobDemand{{
		RequestID: 7, Repository: repositoryURL, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := seedStore.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	reloadRequests := make(chan struct{}, 1)
	loadedConfig := runnableConfig(t)
	loadedConfig.State.Path = databasePath
	discovererCount := 0
	replacementDiscoveryCalls := 0
	scaleSetStarts := 0
	oldPollingStopped := false
	oldLifecycleShutdown := false
	replacementWorker := &recordingRecoveryWorker{existsErr: errors.New("provider unavailable")}
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			discovererCount++
			if discovererCount == 1 {
				return repositoryDiscovererFunc(func(context.Context) (githubapp.DiscoveryResult, error) {
					reloadRequests <- struct{}{}
					return githubapp.DiscoveryResult{
						Repositories:         []githubapp.Repository{{InstallationID: 1, URL: repositoryURL}},
						InstallationFailures: map[int64]error{},
					}, nil
				}), nil
			}
			return repositoryDiscovererFunc(func(context.Context) (githubapp.DiscoveryResult, error) {
				replacementDiscoveryCalls++
				if replacementDiscoveryCalls > 1 {
					replacementWorker.existsErr = nil
				}
				return githubapp.DiscoveryResult{
					Repositories:         []githubapp.Repository{{InstallationID: 1, URL: repositoryURL}},
					InstallationFailures: map[int64]error{},
				}, nil
			}), nil
		},
		newGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		openDemandStore: func(path string) (scaler.DemandStore, io.Closer, error) {
			store, openErr := scaler.OpenSQLiteDemandStore(path)
			if openErr != nil {
				return nil, nil, fmt.Errorf("open reload recovery store: %w", openErr)
			}
			return store, store, nil
		},
		notifier:                 noopNotifier{},
		drainInterval:            time.Nanosecond,
		reconciliationRetryDelay: time.Nanosecond,
		startScaleSet: func(
			_ context.Context,
			_ *slog.Logger,
			_ scaleSetClient,
			startedRepositoryURL string,
			_ config.ScaleSetConfig,
			coordinator *scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			scaleSetStarts++
			if scaleSetStarts == 1 {
				return scaleSetLifecycle{
					StopPolling: func() { oldPollingStopped = true },
					Drain:       func(context.Context) (int, error) { return 0, nil },
					Shutdown:    func(context.Context) error { oldLifecycleShutdown = true; return nil },
				}, nil
			}
			if !oldPollingStopped || !oldLifecycleShutdown {
				t.Fatal("replacement worker started before the old lifecycle stopped")
			}
			coordinator.AddRepository(startedRepositoryURL, &completionSignallingWorker{
				RepositoryWorker: replacementWorker,
				afterRecovery:    cancel,
			})
			return inertLifecycle(), nil
		},
	}
	if err := runDaemon(ctx, Options{ReloadRequests: reloadRequests}, daemonUnderTest); err != nil {
		t.Fatalf("expected reload recovery success, got %v", err)
	}
	if replacementDiscoveryCalls < 2 || scaleSetStarts != 2 || len(replacementWorker.started) != 1 || replacementWorker.started[0] != 7 {
		t.Fatalf("expected replacement retry to provision request 7 once, discoveries=%d starts=%d worker=%#v", replacementDiscoveryCalls, scaleSetStarts, replacementWorker.started)
	}
	assertPersistedDemand(t, databasePath, "linux", 7)
}

func runRestartRecovery(t *testing.T, demand []scaler.JobDemand, worker scaler.RepositoryWorker) {
	t.Helper()
	const repositoryURL = "https://github.com/acme/repo"
	databasePath := filepath.Join(t.TempDir(), "state.db")
	seedStore, err := scaler.OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := seedStore.Apply(context.Background(), "linux", demand, nil); err != nil {
		t.Fatal(err)
	}
	if err := seedStore.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	loadedConfig := runnableConfig(t)
	loadedConfig.State.Path = databasePath
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{
				Repositories:         []githubapp.Repository{{InstallationID: 1, URL: repositoryURL}},
				InstallationFailures: map[int64]error{},
			}}, nil
		},
		newGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		openDemandStore: func(path string) (scaler.DemandStore, io.Closer, error) {
			store, openErr := scaler.OpenSQLiteDemandStore(path)
			if openErr != nil {
				return nil, nil, fmt.Errorf("open restart recovery store: %w", openErr)
			}
			return store, store, nil
		},
		notifier: noopNotifier{},
		startScaleSet: func(
			_ context.Context,
			_ *slog.Logger,
			_ scaleSetClient,
			startedRepositoryURL string,
			_ config.ScaleSetConfig,
			coordinator *scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			coordinator.AddRepository(startedRepositoryURL, &completionSignallingWorker{
				RepositoryWorker: worker,
				afterRecovery:    cancel,
			})
			return inertLifecycle(), nil
		},
	}
	if err := runDaemon(ctx, Options{}, daemonUnderTest); err != nil {
		t.Fatalf("expected restart recovery success, got %v", err)
	}
	assertPersistedDemand(t, databasePath, "linux", demand[0].RequestID)
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
	defer store.Close()
	demand, err := store.Ordered(context.Background(), scaleSetName)
	if err != nil {
		t.Fatal(err)
	}
	if len(demand) != 1 || demand[0].RequestID != requestID {
		t.Fatalf("expected request %d to remain persisted, got %#v", requestID, demand)
	}
}
