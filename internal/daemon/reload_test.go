package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	"github.com/meza/github-scaler/internal/scaler"
)

func TestReloadConfigurationDrainsOldAndStartsReplacement(t *testing.T) {
	loadedConfig := runnableConfig(t)
	activeConfig := testActiveConfiguration()
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": inertLifecycle()}}
	daemonUnderTest := successfulReloadDaemon(loadedConfig)
	reloaded, err := reloadConfiguration(context.Background(), "config", daemonUnderTest, activeConfig)
	if err != nil {
		t.Fatalf("expected reload success, got %v", err)
	}
	if reloaded.Config.GitHub.App.AppID != 1 {
		t.Fatalf("expected replacement config, got %#v", reloaded.Config)
	}
}

func TestReloadConfigurationReportsReloadNotificationFailure(t *testing.T) {
	activeConfig := testActiveConfiguration()
	if _, err := reloadConfiguration(context.Background(), "config", standaloneDaemon{notifier: failingNotifier{reloadErr: errors.New("reload failed")}}, activeConfig); err == nil {
		t.Fatal("expected reload notification failure")
	}
}

func TestReloadLogsEveryLifecycleStage(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "debug")
	loadedConfig := runnableConfig(t)
	activeConfig := activeWithLifecycle(inertLifecycle())
	activeConfig.Config.State.Path = loadedConfig.State.Path
	activeConfig.Logger = logger
	activeConfig.DemandStore = scaler.NewMemoryDemandStore()
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}}, nil
		},
		newLogger:     func(string, string) *slog.Logger { return logger },
		notifier:      noopNotifier{},
		drainInterval: time.Millisecond,
	}

	if _, _, err := reloadAndReadInterval(context.Background(), "config", daemonUnderTest, activeConfig); err != nil {
		t.Fatalf("expected reload success, got %v", err)
	}
	for _, phase := range []string{
		"requested",
		"replacement_validated",
		"polling_stopped",
		"drain_started",
		"drain_completed",
		"old_configuration_stopped",
		"state_reconciled",
		"replacement_reconciliation_started",
		"completed",
	} {
		assertStructuredAction(t, logs.Bytes(), "reload", phase)
	}
	for _, action := range []string{"polling_stop", "runner_drain", "scale_set_shutdown", "state_cleanup"} {
		assertStructuredAction(t, logs.Bytes(), action, "started")
		assertStructuredAction(t, logs.Bytes(), action, "completed")
	}
}

func TestReloadRejectsAChangedSQLiteStatePath(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.State.Path = "replacement.db"
	if _, err := reloadConfiguration(context.Background(), "config", successfulReloadDaemon(loadedConfig), testActiveConfiguration()); err == nil {
		t.Fatal("expected reload state path rejection")
	}
}

func TestLoadConfigurationReportsSQLiteOpenFailure(t *testing.T) {
	loadedConfig := runnableConfig(t)
	baseRuntime := successfulReloadDaemon(loadedConfig)
	openFailureRuntime := baseRuntime
	openFailureRuntime.openDemandStore = func(string) (scaler.DemandStore, io.Closer, error) { return nil, nil, errors.New("open failed") }
	if _, err := loadConfiguration("config", openFailureRuntime); err == nil {
		t.Fatal("expected durable state open failure")
	}
}

func TestImmediateRepositoryRemovalReportsDurableStateFailure(t *testing.T) {
	storeFailure := &recordingAppDemandStore{removeRepositoryErr: errors.New("remove failed")}
	active := testActiveConfiguration()
	active.Coordinators["linux"] = scaler.NewDurableCoordinator("linux", 1, storeFailure)
	active.Coordinators["linux"].AddRepository("repo", coordinatedRepositoryWorker{})
	if err := removeRepositoryImmediately(context.Background(), &active, "repo", managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": inertLifecycle()}}); err == nil {
		t.Fatal("expected repository state removal failure")
	}
}

func TestReloadPurgesRemovedScaleSetDemand(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.ScaleSets[0].Name = "replacement"
	store := &recordingAppDemandStore{}
	active := testActiveConfiguration()
	active.DemandStore = store
	active.Config.State.Path = loadedConfig.State.Path
	baseRuntime := successfulReloadDaemon(loadedConfig)
	if _, err := reloadConfiguration(context.Background(), "config", baseRuntime, active); err != nil {
		t.Fatal(err)
	}
	if store.removedScaleSet != "linux" {
		t.Fatalf("expected old scale set purge, got %q", store.removedScaleSet)
	}
}

func TestReloadReportsRemovedScaleSetDemandPurgeFailure(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.ScaleSets[0].Name = "replacement"
	store := &recordingAppDemandStore{removeScaleSetErr: errors.New("remove failed")}
	active := testActiveConfiguration()
	active.DemandStore = store
	active.Config.State.Path = loadedConfig.State.Path
	if _, err := reloadConfiguration(
		context.Background(), "config", successfulReloadDaemon(loadedConfig), active,
	); err == nil {
		t.Fatal("expected scale set purge failure")
	}
}

type recordingAppDemandStore struct {
	removedScaleSet     string
	removeScaleSetErr   error
	removeRepositoryErr error
}

func (store *recordingAppDemandStore) Apply(context.Context, string, []scaler.JobDemand, []scaler.DemandRemoval) error {
	return nil
}
func (store *recordingAppDemandStore) RemoveRepository(context.Context, string, string) error {
	return store.removeRepositoryErr
}
func (store *recordingAppDemandStore) RemoveScaleSet(_ context.Context, scaleSet string) error {
	store.removedScaleSet = scaleSet
	return store.removeScaleSetErr
}
func (store *recordingAppDemandStore) Ordered(context.Context, string) ([]scaler.QueuedDemand, error) {
	return nil, nil
}
func (store *recordingAppDemandStore) Delete(context.Context, string, string, int64) error {
	return nil
}

func TestReloadConfigurationReportsDrainFailure(t *testing.T) {
	loadedConfig := runnableConfig(t)
	daemonUnderTest := successfulReloadDaemon(loadedConfig)
	active := activeWithLifecycle(scaleSetLifecycle{Drain: func(context.Context) (int, error) {
		return 0, errors.New("drain failed")
	}})
	if _, err := reloadConfiguration(context.Background(), "config", daemonUnderTest, active); err == nil {
		t.Fatal("expected drain failure")
	}
}

func TestReloadConfigurationReportsShutdownFailure(t *testing.T) {
	loadedConfig := runnableConfig(t)
	active := activeWithLifecycle(scaleSetLifecycle{
		Drain: func(context.Context) (int, error) { return 0, nil }, StopPolling: func() {},
		Shutdown: func(context.Context) error { return errors.New("shutdown failed") },
	})
	if _, err := reloadConfiguration(context.Background(), "config", successfulReloadDaemon(loadedConfig), active); err == nil {
		t.Fatal("expected shutdown failure")
	}
}

func TestReloadConfigurationReportsReplacementLoadFailure(t *testing.T) {
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return config.Config{}, errors.New("replacement failed") },
		notifier:   noopNotifier{}, drainInterval: time.Millisecond,
	}
	if _, err := reloadConfiguration(context.Background(), "config", daemonUnderTest, activeWithLifecycle(inertLifecycle())); err == nil {
		t.Fatal("expected replacement load failure")
	}
}

func TestReloadConfigurationReportsReplacementReadyFailure(t *testing.T) {
	loadedConfig := runnableConfig(t)
	daemonUnderTest := successfulReloadDaemon(loadedConfig)
	daemonUnderTest.notifier = failingNotifier{readyErr: errors.New("ready failed")}
	if _, err := reloadConfiguration(context.Background(), "config", daemonUnderTest, activeWithLifecycle(inertLifecycle())); err == nil {
		t.Fatal("expected replacement readiness failure")
	}
}

func successfulReloadDaemon(loadedConfig config.Config) standaloneDaemon {
	return standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		notifier:  noopNotifier{}, drainInterval: time.Millisecond,
	}
}

func TestReloadAndReadIntervalRejectsUnvalidatedReplacementInterval(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.GitHub.DiscoveryInterval = "invalid"
	activeConfig := activeWithLifecycle(inertLifecycle())
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		notifier:  noopNotifier{}, drainInterval: time.Millisecond,
	}
	if _, _, err := reloadAndReadInterval(context.Background(), "config", daemonUnderTest, activeConfig); err == nil {
		t.Fatal("expected invalid reloaded interval error")
	}
}
