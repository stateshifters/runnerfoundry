package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	"github.com/meza/github-scaler/internal/scaler"
)

func TestApplyDiscoveryAddsRepositoriesAndRetainsFailedInstallations(t *testing.T) {
	started := []string{}
	daemonRuntime := runtime{
		NewGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		StartScaleSet: func(
			_ context.Context,
			_ *slog.Logger,
			_ scaleSetClient,
			repositoryURL string,
			scaleSetConfig config.ScaleSetConfig,
			_ *scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			started = append(started, repositoryURL+"/"+scaleSetConfig.Name)
			return inertLifecycle(), nil
		},
	}
	activeConfig := testActiveConfiguration()
	discovery := githubapp.DiscoveryResult{
		Repositories:         []githubapp.Repository{{InstallationID: 1, URL: "https://github.com/acme/one"}},
		InstallationFailures: map[int64]error{2: errors.New("unavailable")},
	}
	activeConfig.Repositories["https://github.com/kept/two"] = managedRepository{
		InstallationID: 2,
		Lifecycles:     map[string]scaleSetLifecycle{"linux": inertLifecycle()},
	}
	if err := applyDiscovery(context.Background(), daemonRuntime, &activeConfig, discovery); err != nil {
		t.Fatalf("expected discovery application, got %v", err)
	}
	if len(started) != 1 || len(activeConfig.Repositories) != 2 {
		t.Fatalf("expected new and retained repositories, started=%#v repositories=%#v", started, activeConfig.Repositories)
	}
}

func TestApplyDiscoveryImmediatelyRemovesRevokedRepository(t *testing.T) {
	stopped := false
	destroyed := false
	shutdown := false
	activeConfig := testActiveConfiguration()
	activeConfig.Repositories["https://github.com/acme/removed"] = managedRepository{
		InstallationID: 1,
		Lifecycles: map[string]scaleSetLifecycle{"linux": {
			StopPolling:   func() { stopped = true },
			DestroyActive: func(context.Context) error { destroyed = true; return nil },
			Shutdown:      func(context.Context) error { shutdown = true; return nil },
		}},
	}
	if err := applyDiscovery(context.Background(), runtime{}, &activeConfig, githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}); err != nil {
		t.Fatalf("expected removal, got %v", err)
	}
	if !stopped || !destroyed || !shutdown || len(activeConfig.Repositories) != 0 {
		t.Fatalf("expected immediate teardown, stopped=%t destroyed=%t shutdown=%t", stopped, destroyed, shutdown)
	}
}

func TestApplyDiscoveryIsolatesRepositoryStartupFailureAndLogsItsIdentity(t *testing.T) {
	logs := &bytes.Buffer{}
	activeConfig := testActiveConfiguration()
	activeConfig.Logger = slog.New(slog.NewTextHandler(logs, nil))
	daemonRuntime := runtime{
		NewGitHubClient: func(repositoryURL string, _ int64, _ config.GitHubAppCredentials) (scaleSetClient, error) {
			if repositoryURL == "https://github.com/acme/failing" {
				return nil, errors.New("client failed")
			}
			return &fakeScaleSetClient{}, nil
		},
		StartScaleSet: func(context.Context, *slog.Logger, scaleSetClient, string, config.ScaleSetConfig, *scaler.Coordinator) (scaleSetLifecycle, error) {
			return inertLifecycle(), nil
		},
	}
	err := applyDiscovery(context.Background(), daemonRuntime, &activeConfig, githubapp.DiscoveryResult{
		Repositories: []githubapp.Repository{
			{InstallationID: 1, URL: "https://github.com/acme/failing"},
			{InstallationID: 2, URL: "https://github.com/acme/healthy"},
		},
		InstallationFailures: map[int64]error{},
	})
	if err == nil {
		t.Fatal("expected repository startup failure")
	}
	if _, exists := activeConfig.Repositories["https://github.com/acme/healthy"]; !exists {
		t.Fatal("expected healthy repository to start despite another repository failure")
	}
	for _, expected := range []string{"repository startup failed", "repository=https://github.com/acme/failing", "installationID=1", "client failed"} {
		if !strings.Contains(logs.String(), expected) {
			t.Fatalf("expected %q in repository failure log %q", expected, logs.String())
		}
	}
}

func TestStartRepositoryRollsBackEarlierScaleSetsWhenLaterStartupFails(t *testing.T) {
	stopped := false
	shutdown := false
	activeConfig := testActiveConfiguration()
	activeConfig.Config.ScaleSets = append(activeConfig.Config.ScaleSets, config.ScaleSetConfig{Name: "windows", MaxRunners: 1})
	activeConfig.Coordinators["windows"] = scaler.NewCoordinator(1)
	startCalls := 0
	daemonRuntime := runtime{
		NewGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		StartScaleSet: func(
			context.Context,
			*slog.Logger,
			scaleSetClient,
			string,
			config.ScaleSetConfig,
			*scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			startCalls++
			if startCalls == 2 {
				return scaleSetLifecycle{}, errors.New("second failed")
			}
			return scaleSetLifecycle{
				StopPolling:   func() { stopped = true },
				DestroyActive: func(context.Context) error { return nil },
				Shutdown:      func(context.Context) error { shutdown = true; return nil },
			}, nil
		},
	}
	_, err := startRepository(context.Background(), daemonRuntime, &activeConfig, githubapp.Repository{
		InstallationID: 1, URL: "https://github.com/acme/repo",
	})
	if err == nil {
		t.Fatal("expected later startup failure")
	}
	if !stopped || !shutdown {
		t.Fatalf("expected earlier scale set rollback, stopped=%t shutdown=%t", stopped, shutdown)
	}
}

func TestWithRuntimeDefaultsSuppliesNotifierAndDrainInterval(t *testing.T) {
	defaults := withRuntimeDefaults(runtime{})
	if defaults.Notifier == nil || defaults.DrainInterval == 0 {
		t.Fatalf("expected runtime defaults, got %#v", defaults)
	}
}

func TestRunDaemonDiscoversStartsAndShutsDownRepositories(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stopped := false
	shutdown := false
	loadedConfig := runnableConfig(t)
	daemonRuntime := runtime{
		LoadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{
				Repositories:         []githubapp.Repository{{InstallationID: 7, URL: "https://github.com/acme/repo"}},
				InstallationFailures: map[int64]error{},
			}}, nil
		},
		NewGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		NewLogger: func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		Notifier:  cancellingNotifier{cancel: cancel},
		StartScaleSet: func(
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
	if err := runDaemon(ctx, Options{}, daemonRuntime); err != nil {
		t.Fatalf("expected clean shutdown, got %v", err)
	}
	if !stopped || !shutdown {
		t.Fatalf("expected lifecycle shutdown, stopped=%t shutdown=%t", stopped, shutdown)
	}
}

func TestLoadConfigurationSurfacesLocalPreflightFailures(t *testing.T) {
	valid := runnableConfig(t)
	tests := map[string]runtime{
		"credentials": {
			LoadConfig: func(string) (config.Config, error) {
				invalid := valid
				invalid.GitHub.App.PrivateKeyFile = filepath.Join(t.TempDir(), "missing")
				return invalid, nil
			},
		},
		"discoverer": {
			LoadConfig: func(string) (config.Config, error) { return valid, nil },
			NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
				return nil, errors.New("factory failed")
			},
		},
	}
	for testName, daemonRuntime := range tests {
		t.Run(testName, func(t *testing.T) {
			if _, err := loadConfiguration("config", daemonRuntime); err == nil {
				t.Fatal("expected startup failure")
			}
		})
	}
}

func TestLoadConfigurationDoesNotContactGitHub(t *testing.T) {
	valid := runnableConfig(t)
	discoverer := &sequenceDiscoverer{results: []githubapp.DiscoveryResult{{InstallationFailures: map[int64]error{}}}}
	activeConfig, err := loadConfiguration("config", runtime{
		LoadConfig:    func(string) (config.Config, error) { return valid, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return discoverer, nil },
		NewLogger:     func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
	})
	if err != nil {
		t.Fatalf("expected local preflight success, got %v", err)
	}
	if discoverer.calls != 0 || len(activeConfig.Repositories) != 0 {
		t.Fatalf("expected no GitHub work before readiness, calls=%d repositories=%#v", discoverer.calls, activeConfig.Repositories)
	}
}

func TestDrainConfigurationWaitsForActiveRunnerAndSurfacesFailures(t *testing.T) {
	drainCalls := 0
	activeConfig := testActiveConfiguration()
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		Drain: func(context.Context) (int, error) {
			drainCalls++
			if drainCalls == 1 {
				return 1, nil
			}
			return 0, nil
		},
	}}}
	if err := drainConfiguration(context.Background(), activeConfig, time.Millisecond); err != nil || drainCalls != 2 {
		t.Fatalf("expected drain retry, calls=%d error=%v", drainCalls, err)
	}
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		Drain: func(context.Context) (int, error) { return 0, errors.New("drain failed") },
	}}}
	if err := drainConfiguration(context.Background(), activeConfig, time.Millisecond); err == nil {
		t.Fatal("expected drain failure")
	}
}

func TestLoadConfigReadsValidatedFileAndLoggerFormats(t *testing.T) {
	loadedConfig := runnableConfig(t)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	configText := "github:\n  app:\n    app_id: 1\n    private_key_file: " + loadedConfig.GitHub.App.PrivateKeyFile + "\nstate:\n  path: state.db\nscale_sets:\n  - name: linux\n    max_runners: 1\n    docker:\n      compose_file: compose.yaml\n      service: runner\n"
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(configPath); err != nil {
		t.Fatalf("expected config file to load, got %v", err)
	}
	if _, err := loadConfig(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("expected missing config error")
	}
	if _, err := readConfigFile(filepath.Join(t.TempDir(), "missing", "config.yaml")); err == nil {
		t.Fatal("expected missing config directory error")
	}
	if _, err := readConfigFile(t.TempDir()); err == nil {
		t.Fatal("expected directory read error")
	}
	if newLogger("json") == nil || newLogger("text") == nil {
		t.Fatal("expected both logger formats")
	}
}

func TestPruneRepositoryDeletesOnlyUnconfiguredScaleSets(t *testing.T) {
	client := &fakeScaleSetClient{
		runnerGroup:     &upstream.RunnerGroup{ID: 5},
		runnerScaleSets: []upstream.RunnerScaleSet{{ID: 1, Name: "linux"}, {ID: 2, Name: "stale"}},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := pruneRepository(context.Background(), logger, client, "https://github.com/acme/repo", []config.ScaleSetConfig{{
		Name: "linux", RunnerGroup: "default",
	}}); err != nil {
		t.Fatalf("expected prune success, got %v", err)
	}
	if len(client.deletedScaleSetIDs) != 1 || client.deletedScaleSetIDs[0] != 2 {
		t.Fatalf("expected stale scale set deletion, got %#v", client.deletedScaleSetIDs)
	}
}

func TestPruneRepositorySurfacesGitHubFailures(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := map[string]*fakeScaleSetClient{
		"runner group": {runnerGroupErr: errors.New("group failed")},
		"list":         {runnerGroup: &upstream.RunnerGroup{ID: 1}, listScaleSetsErr: errors.New("list failed")},
		"delete": {
			runnerGroup:       &upstream.RunnerGroup{ID: 1},
			runnerScaleSets:   []upstream.RunnerScaleSet{{ID: 2, Name: "stale"}},
			deleteScaleSetErr: errors.New("delete failed"),
		},
	}
	for testName, client := range tests {
		t.Run(testName, func(t *testing.T) {
			if err := pruneRepository(context.Background(), logger, client, "repo", []config.ScaleSetConfig{{Name: "linux", RunnerGroup: "default"}}); err == nil {
				t.Fatal("expected prune failure")
			}
		})
	}
}

func TestReconcileDiscoveryRetainsStateOnDiscoveryFailure(t *testing.T) {
	activeConfig := testActiveConfiguration()
	activeConfig.Discoverer = staticDiscoverer{err: errors.New("unavailable")}
	reconcileDiscovery(context.Background(), runtime{}, &activeConfig)
	if !activeConfig.ReconciliationPending {
		t.Fatal("expected failed discovery to request a short reconciliation retry")
	}
	activeConfig.Discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{
		Repositories:         []githubapp.Repository{{InstallationID: 1, URL: "https://github.com/acme/repo"}},
		InstallationFailures: map[int64]error{},
	}}
	reconcileDiscovery(context.Background(), runtime{NewGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errors.New("start failed")
	}}, &activeConfig)
	if !activeConfig.ReconciliationPending {
		t.Fatal("expected failed repository startup to request a short reconciliation retry")
	}
	activeConfig.Discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}
	reconcileDiscovery(context.Background(), runtime{}, &activeConfig)
	if activeConfig.ReconciliationPending {
		t.Fatal("expected successful reconciliation to restore the discovery interval")
	}
}

func TestRunUsesProductionRuntimeAndRejectsMissingConfig(t *testing.T) {
	if err := Run(context.Background(), Options{ConfigPath: filepath.Join(t.TempDir(), "missing")}); err == nil {
		t.Fatal("expected missing config failure")
	}
}

func TestProductionRuntimeBuildsAppBoundaries(t *testing.T) {
	production := productionRuntime()
	if _, err := production.NewDiscoverer(config.GitHubAppCredentials{AppID: 1, PrivateKey: "invalid"}); err == nil {
		t.Fatal("expected discoverer key validation")
	}
	if _, err := production.NewGitHubClient("://invalid", 1, config.GitHubAppCredentials{AppID: 1, PrivateKey: "invalid"}); err == nil {
		t.Fatal("expected scale set client validation")
	}
	if _, err := production.StartScaleSet(
		context.Background(), slog.Default(), &fakeScaleSetClient{runnerGroupErr: errors.New("group failed")},
		"https://github.com/acme/repo", config.ScaleSetConfig{Name: "linux", RunnerGroup: "default"}, scaler.NewCoordinator(1),
	); err == nil {
		t.Fatal("expected coordinated scale set startup failure")
	}
	if production.NewLogger("text") == nil || production.Notifier == nil {
		t.Fatal("expected production logger and notifier")
	}
}

func TestRunDaemonReconcilesPeriodicDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	discoverer := &sequenceDiscoverer{results: []githubapp.DiscoveryResult{
		{InstallationFailures: map[int64]error{}},
		{InstallationFailures: map[int64]error{}},
	}, afterDiscover: func(call int) {
		if call == 2 {
			cancel()
		}
	}}
	loadedConfig := runnableConfig(t)
	loadedConfig.GitHub.DiscoveryInterval = "1ns"
	daemonRuntime := runtime{
		LoadConfig:    func(string) (config.Config, error) { return loadedConfig, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return discoverer, nil },
		NewLogger:     func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
	}
	if err := runDaemon(ctx, Options{}, daemonRuntime); err != nil {
		t.Fatalf("expected periodic reconciliation shutdown, got %v", err)
	}
	if discoverer.calls < 2 {
		t.Fatalf("expected repeated discovery, got %d calls", discoverer.calls)
	}
}

func TestRunDaemonSurfacesReadyFailure(t *testing.T) {
	loadedConfig := runnableConfig(t)
	daemonRuntime := runtime{
		LoadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}, nil
		},
		NewLogger: func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		Notifier:  failingNotifier{readyErr: errors.New("ready failed")},
	}
	if err := runDaemon(context.Background(), Options{}, daemonRuntime); err == nil {
		t.Fatal("expected ready notification failure")
	}
}

func TestRunDaemonSignalsReadyBeforeGitHubDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ready := false
	loadedConfig := runnableConfig(t)
	discoverer := readinessCheckingDiscoverer{ready: &ready, cancel: cancel}
	daemonRuntime := runtime{
		LoadConfig:    func(string) (config.Config, error) { return loadedConfig, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return discoverer, nil },
		NewLogger:     func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		Notifier:      readyRecordingNotifier{ready: &ready},
	}
	if err := runDaemon(ctx, Options{}, daemonRuntime); err != nil {
		t.Fatalf("expected discovery failure to remain retryable, got %v", err)
	}
	if !ready {
		t.Fatal("expected readiness notification")
	}
}

func TestRunDaemonRejectsUnvalidatedDiscoveryInterval(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.GitHub.DiscoveryInterval = "invalid"
	daemonRuntime := runtime{
		LoadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}, nil
		},
		NewLogger: func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
	}
	if err := runDaemon(context.Background(), Options{}, daemonRuntime); err == nil {
		t.Fatal("expected invalid discovery interval error")
	}
}

func TestRunDaemonAppliesReloadRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	loadedConfig := runnableConfig(t)
	notifier := &reloadCancellingNotifier{cancel: cancel}
	reloads := make(chan struct{}, 1)
	reloads <- struct{}{}
	daemonRuntime := runtime{
		LoadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}, nil
		},
		NewLogger: func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		Notifier:  notifier, DrainInterval: time.Millisecond,
	}
	if err := runDaemon(ctx, Options{ReloadRequests: reloads}, daemonRuntime); err != nil {
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
	daemonRuntime := runtime{
		LoadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}, nil
		},
		NewLogger: func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		Notifier:  failingNotifier{reloadErr: errors.New("reload failed")},
	}
	if err := runDaemon(context.Background(), Options{ReloadRequests: reloads}, daemonRuntime); err == nil {
		t.Fatal("expected reload failure")
	}
}

func TestReloadConfigurationDrainsOldAndStartsReplacement(t *testing.T) {
	loadedConfig := runnableConfig(t)
	activeConfig := testActiveConfiguration()
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": inertLifecycle()}}
	daemonRuntime := runtime{
		LoadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}, nil
		},
		NewLogger: func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		Notifier:  noopNotifier{}, DrainInterval: time.Millisecond,
	}
	reloaded, err := reloadConfiguration(context.Background(), "config", daemonRuntime, activeConfig)
	if err != nil {
		t.Fatalf("expected reload success, got %v", err)
	}
	if reloaded.Config.GitHub.App.AppID != 1 {
		t.Fatalf("expected replacement config, got %#v", reloaded.Config)
	}
	if _, err := reloadConfiguration(context.Background(), "config", runtime{Notifier: failingNotifier{reloadErr: errors.New("reload failed")}}, activeConfig); err == nil {
		t.Fatal("expected reload notification failure")
	}
}

func TestDurableStateLifecycleFailuresRemainExplicit(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.State.Path = "replacement.db"
	baseRuntime := runtime{
		LoadConfig:    func(string) (config.Config, error) { return loadedConfig, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return staticDiscoverer{}, nil },
		NewLogger:     func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		Notifier:      noopNotifier{}, DrainInterval: time.Millisecond,
	}
	if _, err := reloadConfiguration(context.Background(), "config", baseRuntime, testActiveConfiguration()); err == nil {
		t.Fatal("expected reload state path rejection")
	}
	openFailureRuntime := baseRuntime
	openFailureRuntime.OpenDemandStore = func(string) (scaler.DemandStore, io.Closer, error) { return nil, nil, errors.New("open failed") }
	if _, err := loadConfiguration("config", openFailureRuntime); err == nil {
		t.Fatal("expected durable state open failure")
	}

	production := productionRuntime()
	store, closer, err := production.OpenDemandStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil || store == nil {
		t.Fatalf("expected production state store, got %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
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
	baseRuntime := runtime{
		LoadConfig:    func(string) (config.Config, error) { return loadedConfig, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) { return staticDiscoverer{}, nil },
		NewLogger:     func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		Notifier:      noopNotifier{}, DrainInterval: time.Millisecond,
	}
	if _, err := reloadConfiguration(context.Background(), "config", baseRuntime, active); err != nil {
		t.Fatal(err)
	}
	if store.removedScaleSet != "linux" {
		t.Fatalf("expected old scale set purge, got %q", store.removedScaleSet)
	}
	store.removeScaleSetErr = errors.New("remove failed")
	if _, err := reloadConfiguration(context.Background(), "config", baseRuntime, active); err == nil {
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

func TestReloadConfigurationSurfacesDrainShutdownReplacementAndReadyFailures(t *testing.T) {
	loadedConfig := runnableConfig(t)
	baseRuntime := runtime{
		LoadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}, nil
		},
		NewLogger: func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		Notifier:  noopNotifier{}, DrainInterval: time.Millisecond,
	}
	tests := map[string]struct {
		active  activeConfiguration
		runtime runtime
	}{
		"drain": {
			active:  activeWithLifecycle(scaleSetLifecycle{Drain: func(context.Context) (int, error) { return 0, errors.New("drain failed") }}),
			runtime: baseRuntime,
		},
		"shutdown": {
			active: activeWithLifecycle(scaleSetLifecycle{
				Drain: func(context.Context) (int, error) { return 0, nil }, StopPolling: func() {},
				Shutdown: func(context.Context) error { return errors.New("shutdown failed") },
			}),
			runtime: baseRuntime,
		},
		"replacement": {
			active: activeWithLifecycle(inertLifecycle()),
			runtime: runtime{
				LoadConfig: func(string) (config.Config, error) { return config.Config{}, errors.New("replacement failed") },
				Notifier:   noopNotifier{}, DrainInterval: time.Millisecond,
			},
		},
		"ready": {
			active: activeWithLifecycle(inertLifecycle()),
			runtime: runtime{
				LoadConfig: baseRuntime.LoadConfig, NewDiscoverer: baseRuntime.NewDiscoverer, NewLogger: baseRuntime.NewLogger,
				Notifier: failingNotifier{readyErr: errors.New("ready failed")}, DrainInterval: time.Millisecond,
			},
		},
	}
	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			if _, err := reloadConfiguration(context.Background(), "config", testCase.runtime, testCase.active); err == nil {
				t.Fatal("expected reload failure")
			}
		})
	}
}

func TestReloadAndReadIntervalRejectsUnvalidatedReplacementInterval(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.GitHub.DiscoveryInterval = "invalid"
	activeConfig := activeWithLifecycle(inertLifecycle())
	daemonRuntime := runtime{
		LoadConfig: func(string) (config.Config, error) { return loadedConfig, nil },
		NewDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}}, nil
		},
		NewLogger: func(string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		Notifier:  noopNotifier{}, DrainInterval: time.Millisecond,
	}
	if _, _, err := reloadAndReadInterval(context.Background(), "config", daemonRuntime, activeConfig); err == nil {
		t.Fatal("expected invalid reloaded interval error")
	}
}

func TestLifecycleFailurePathsRemainExplicit(t *testing.T) {
	if _, err := loadConfiguration("config", runtime{
		LoadConfig: func(string) (config.Config, error) { return config.Config{}, errors.New("load failed") },
	}); err == nil {
		t.Fatal("expected load failure")
	}
	activeConfig := testActiveConfiguration()
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		StopPolling:   func() {},
		DestroyActive: func(context.Context) error { return errors.New("destroy failed") },
		Shutdown:      func(context.Context) error { return errors.New("shutdown failed") },
	}}}
	if err := removeRepositoryImmediately(context.Background(), &activeConfig, "repo", activeConfig.Repositories["repo"]); err == nil {
		t.Fatal("expected immediate removal failure")
	}
	if err := shutdownWithTimeout(context.Background(), activeConfig, immediateShutdown); err == nil {
		t.Fatal("expected forced shutdown failure")
	}
}

func TestShutdownCleansLifecyclesConcurrentlyWithIndependentContexts(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	activeConfig := testActiveConfiguration()
	for _, repositoryURL := range []string{"repo-a", "repo-b"} {
		activeConfig.Repositories[repositoryURL] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
			StopPolling: func() {},
			Shutdown: func(ctx context.Context) error {
				started <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}}}
	}
	finished := make(chan error, 1)
	go func() { finished <- shutdownWithTimeout(context.Background(), activeConfig, drainShutdown) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("expected lifecycle cleanup to run concurrently")
		}
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatalf("expected concurrent cleanup success, got %v", err)
	}
}

func TestTerminationReportsCleanupFailuresAsWarnings(t *testing.T) {
	logs := &bytes.Buffer{}
	activeConfig := activeWithLifecycle(scaleSetLifecycle{
		StopPolling: func() {},
		Shutdown:    func(context.Context) error { return errors.New("close failed") },
	})
	activeConfig.Logger = slog.New(slog.NewTextHandler(logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runActiveConfiguration(ctx, Options{}, runtime{}, activeConfig, time.Hour); err != nil {
		t.Fatalf("expected termination cleanup failure not to fail the process, got %v", err)
	}
	if !strings.Contains(logs.String(), "shutdown cleanup incomplete") || !strings.Contains(logs.String(), "close failed") {
		t.Fatalf("expected actionable shutdown warning, got %q", logs.String())
	}
}

func TestApplyDiscoverySurfacesRemovalFailure(t *testing.T) {
	activeConfig := testActiveConfiguration()
	activeConfig.Repositories["repo"] = managedRepository{InstallationID: 1, Lifecycles: map[string]scaleSetLifecycle{"linux": {
		StopPolling:   func() {},
		DestroyActive: func(context.Context) error { return errors.New("destroy failed") },
		Shutdown:      func(context.Context) error { return nil },
	}}}
	if err := applyDiscovery(context.Background(), runtime{}, &activeConfig, githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}); err == nil {
		t.Fatal("expected removal failure")
	}
}

func TestStartRepositorySurfacesPruneFailure(t *testing.T) {
	activeConfig := testActiveConfiguration()
	activeConfig.Config.Prune = true
	daemonRuntime := runtime{NewGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
		return &fakeScaleSetClient{runnerGroupErr: errors.New("group failed")}, nil
	}}
	if _, err := startRepository(context.Background(), daemonRuntime, &activeConfig, githubapp.Repository{InstallationID: 1, URL: "repo"}); err == nil {
		t.Fatal("expected prune failure")
	}
}

func TestValidationAndPruneFailurePaths(t *testing.T) {
	activeConfig := testActiveConfiguration()
	discovery := githubapp.DiscoveryResult{
		Repositories: []githubapp.Repository{
			{InstallationID: 1, URL: "https://github.com/acme/repo.a"},
			{InstallationID: 1, URL: "https://github.com/acme/repo-a"},
		},
		InstallationFailures: map[int64]error{},
	}
	if err := applyDiscovery(context.Background(), runtime{}, &activeConfig, discovery); err == nil {
		t.Fatal("expected local name collision")
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := pruneRepository(context.Background(), logger, nonPruningScaleSetClient{}, "repo", []config.ScaleSetConfig{{Name: "linux"}}); err == nil {
		t.Fatal("expected unsupported prune client error")
	}
	invalidConfigPath := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(invalidConfigPath, []byte("github: ["), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(invalidConfigPath); err == nil {
		t.Fatal("expected invalid config error")
	}
}

func TestDrainCancellationAndRollbackFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	activeConfig := testActiveConfiguration()
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		Drain: func(context.Context) (int, error) { return 1, nil },
	}}}
	if err := drainConfiguration(ctx, activeConfig, time.Hour); err == nil {
		t.Fatal("expected cancelled drain")
	}
	rollbackConfig := testActiveConfiguration()
	rollback := managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		StopPolling:   func() {},
		DestroyActive: func(context.Context) error { return errors.New("destroy failed") },
		Shutdown:      func(context.Context) error { return errors.New("shutdown failed") },
	}}}
	if err := rollbackRepositoryStartup(context.Background(), &rollbackConfig, "repo", rollback); err == nil {
		t.Fatal("expected rollback failure")
	}
}

func testActiveConfiguration() activeConfiguration {
	scaleSetConfig := config.ScaleSetConfig{Name: "linux", MaxRunners: 1}
	return activeConfiguration{
		Config:       config.Config{ScaleSets: []config.ScaleSetConfig{scaleSetConfig}},
		Coordinators: map[string]*scaler.Coordinator{"linux": scaler.NewCoordinator(1)},
		Repositories: map[string]managedRepository{},
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func inertLifecycle() scaleSetLifecycle {
	return scaleSetLifecycle{
		StopPolling:   func() {},
		Drain:         func(context.Context) (int, error) { return 0, nil },
		DestroyActive: func(context.Context) error { return nil },
		Shutdown:      func(context.Context) error { return nil },
	}
}

func runnableConfig(t *testing.T) config.Config {
	t.Helper()
	privateKeyPath := filepath.Join(t.TempDir(), "app.pem")
	if err := os.WriteFile(privateKeyPath, []byte("private-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	return config.Config{
		GitHub: config.GitHubConfig{
			App:               config.GitHubAppConfig{AppID: 1, PrivateKeyFile: privateKeyPath},
			DiscoveryInterval: "5m",
		},
		Log: config.LogConfig{Format: "text"},
		ScaleSets: []config.ScaleSetConfig{{
			Name: "linux", MaxRunners: 1,
			Docker: &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
		}},
	}
}

func activeWithLifecycle(lifecycle scaleSetLifecycle) activeConfiguration {
	if lifecycle.StopPolling == nil {
		lifecycle.StopPolling = func() {}
	}
	if lifecycle.Drain == nil {
		lifecycle.Drain = func(context.Context) (int, error) { return 0, nil }
	}
	if lifecycle.Shutdown == nil {
		lifecycle.Shutdown = func(context.Context) error { return nil }
	}
	active := testActiveConfiguration()
	active.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": lifecycle}}
	return active
}

type staticDiscoverer struct {
	result githubapp.DiscoveryResult
	err    error
}

func (discoverer staticDiscoverer) Discover(context.Context) (githubapp.DiscoveryResult, error) {
	return discoverer.result, discoverer.err
}

type cancellingNotifier struct {
	cancel context.CancelFunc
}

func (notifier cancellingNotifier) Ready(context.Context) error {
	notifier.cancel()
	return nil
}

func (cancellingNotifier) Reloading(context.Context) error { return nil }

type failingNotifier struct {
	readyErr  error
	reloadErr error
}

type readyRecordingNotifier struct {
	ready *bool
}

func (notifier readyRecordingNotifier) Ready(context.Context) error {
	*notifier.ready = true
	return nil
}

func (readyRecordingNotifier) Reloading(context.Context) error { return nil }

type readinessCheckingDiscoverer struct {
	ready  *bool
	cancel context.CancelFunc
}

func (discoverer readinessCheckingDiscoverer) Discover(context.Context) (githubapp.DiscoveryResult, error) {
	if !*discoverer.ready {
		return githubapp.DiscoveryResult{}, errors.New("discovery ran before readiness")
	}
	discoverer.cancel()
	return githubapp.DiscoveryResult{}, errors.New("GitHub unavailable")
}

func (notifier failingNotifier) Ready(context.Context) error     { return notifier.readyErr }
func (notifier failingNotifier) Reloading(context.Context) error { return notifier.reloadErr }

type sequenceDiscoverer struct {
	results       []githubapp.DiscoveryResult
	calls         int
	afterDiscover func(call int)
}

type reloadCancellingNotifier struct {
	cancel      context.CancelFunc
	readyCalls  int
	reloadCalls int
}

func (notifier *reloadCancellingNotifier) Ready(context.Context) error {
	notifier.readyCalls++
	if notifier.readyCalls == 2 {
		notifier.cancel()
	}
	return nil
}

func (notifier *reloadCancellingNotifier) Reloading(context.Context) error {
	notifier.reloadCalls++
	return nil
}

type nonPruningScaleSetClient struct{}

func (nonPruningScaleSetClient) GetRunnerGroupByName(context.Context, string) (*upstream.RunnerGroup, error) {
	return nil, errors.New("not implemented")
}

func (nonPruningScaleSetClient) GetRunnerScaleSet(context.Context, int, string) (*upstream.RunnerScaleSet, error) {
	return nil, errors.New("not implemented")
}

func (nonPruningScaleSetClient) CreateRunnerScaleSet(context.Context, *upstream.RunnerScaleSet) (*upstream.RunnerScaleSet, error) {
	return nil, errors.New("not implemented")
}

func (nonPruningScaleSetClient) DeleteRunnerScaleSet(context.Context, int) error { return nil }

func (nonPruningScaleSetClient) MessageSessionClient(context.Context, int, string) (scaleSetMessageClient, error) {
	return scaleSetMessageClient{}, nil
}

func (nonPruningScaleSetClient) GenerateJitRunnerConfig(context.Context, *upstream.RunnerScaleSetJitRunnerSetting, int) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
	return nil, errors.New("not implemented")
}

func (nonPruningScaleSetClient) GetRunnerByName(context.Context, string) (*upstream.RunnerReference, error) {
	return nil, errors.New("not implemented")
}

func (nonPruningScaleSetClient) RemoveRunner(context.Context, int64) error { return nil }

func (discoverer *sequenceDiscoverer) Discover(context.Context) (githubapp.DiscoveryResult, error) {
	resultIndex := min(discoverer.calls, len(discoverer.results)-1)
	result := discoverer.results[resultIndex]
	discoverer.calls++
	discoverer.afterDiscover(discoverer.calls)
	return result, nil
}
