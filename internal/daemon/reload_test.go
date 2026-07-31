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

func TestReloadConfigurationHandsOverOldAndStartsReplacement(t *testing.T) {
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

func TestReloadConfigurationDoesNotWaitForExistingRunner(t *testing.T) {
	loadedConfig := runnableConfig(t)
	observation := &reloadHandoverObservation{}
	activeConfig := activeRunnerReloadConfiguration(t, loadedConfig, observation)
	daemonUnderTest := replacementStartingReloadDaemon(loadedConfig, observation)

	if _, _, err := reloadAndReadInterval(context.Background(), "config", daemonUnderTest, activeConfig); err != nil {
		t.Fatalf("expected active runner to survive reload, got %v", err)
	}
	if !observation.stopped || !observation.closed || observation.destroyed || !observation.replacementStarted {
		t.Fatalf("unexpected handover observation: %#v", observation)
	}
}

type reloadHandoverObservation struct {
	stopped            bool
	closed             bool
	destroyed          bool
	replacementStarted bool
}

func activeRunnerReloadConfiguration(
	t *testing.T,
	loadedConfig config.Config,
	observation *reloadHandoverObservation,
) activeConfiguration {
	t.Helper()
	activeConfig := activeWithLifecycle(scaleSetLifecycle{
		StopPolling: func() { observation.stopped = true },
		CloseSession: func(context.Context) error {
			observation.closed = true
			return nil
		},
		Running: func(context.Context) (int, error) {
			t.Fatal("ordinary reload must not inspect active runners")
			return 0, nil
		},
		DestroyActive: func(context.Context) error {
			observation.destroyed = true
			return nil
		},
	})
	activeConfig.Config = loadedConfig
	activeConfig.DemandStore = scaler.NewMemoryDemandStore()
	return activeConfig
}

func replacementStartingReloadDaemon(
	loadedConfig config.Config,
	observation *reloadHandoverObservation,
) standaloneDaemon {
	daemonUnderTest := successfulReloadDaemon(loadedConfig)
	daemonUnderTest.newDiscoverer = func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
		return staticDiscoverer{result: githubapp.DiscoveryResult{
			Repositories:         []githubapp.Repository{{InstallationID: 1, URL: "https://github.com/acme/repo"}},
			InstallationFailures: map[githubapp.InstallationID]error{},
		}}, nil
	}
	daemonUnderTest.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return successfulRegistrationReconciliationClient(), nil
	}
	daemonUnderTest.startScaleSet = func(
		_ context.Context,
		_ *slog.Logger,
		_ scaleSetClient,
		repositoryURL string,
		_ config.ScaleSetConfig,
		coordinator *scaler.Coordinator,
	) (scaleSetLifecycle, error) {
		observation.replacementStarted = true
		coordinator.AddRepository(repositoryURL, &recordingRecoveryWorker{})
		return inertLifecycle(), nil
	}
	return daemonUnderTest
}

func TestReloadConfigurationRejectsActiveProviderChange(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.ScaleSets[0].Docker.Service = "replacement"
	stopped := false
	closed := false
	activeConfig := activeWithLifecycle(scaleSetLifecycle{
		StopPolling: func() { stopped = true },
		CloseSession: func(context.Context) error {
			closed = true
			return nil
		},
		Running: func(context.Context) (int, error) { return 1, nil },
	})
	activeConfig.Config = runnableConfig(t)

	if _, err := reloadConfiguration(
		context.Background(), "config", successfulReloadDaemon(loadedConfig), activeConfig,
	); err == nil {
		t.Fatal("expected active provider change rejection")
	}
	if stopped || closed {
		t.Fatalf("old generation changed before provider validation: stopped=%t closed=%t", stopped, closed)
	}
}

func TestReloadConfigurationAllowsIdleProviderChange(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.ScaleSets[0].Docker.Service = "replacement"
	inspected := false
	activeConfig := activeWithLifecycle(scaleSetLifecycle{
		CloseSession: func(context.Context) error { return nil },
		Running: func(context.Context) (int, error) {
			inspected = true
			return 0, nil
		},
	})
	activeConfig.Config = runnableConfig(t)

	if _, err := reloadConfiguration(
		context.Background(), "config", successfulReloadDaemon(loadedConfig), activeConfig,
	); err != nil {
		t.Fatalf("expected idle provider change success, got %v", err)
	}
	if !inspected {
		t.Fatal("expected old provider inspection")
	}
}

func TestReloadConfigurationRejectsProviderChangeWhenInspectionFails(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.ScaleSets[0].Docker.Service = "replacement"
	stopped := false
	activeConfig := activeWithLifecycle(scaleSetLifecycle{
		StopPolling: func() { stopped = true },
		Running: func(context.Context) (int, error) {
			return 0, errors.New("provider unavailable")
		},
	})
	activeConfig.Config = runnableConfig(t)

	if _, err := reloadConfiguration(
		context.Background(), "config", successfulReloadDaemon(loadedConfig), activeConfig,
	); err == nil {
		t.Fatal("expected provider inspection failure")
	}
	if stopped {
		t.Fatal("old generation stopped after provider inspection failure")
	}
}

func TestReloadConfigurationRejectsProviderChangeWithoutActiveCoordinator(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.ScaleSets[0].Docker.Service = "replacement"
	stopped := false
	activeConfig := activeWithLifecycle(scaleSetLifecycle{
		StopPolling: func() { stopped = true },
		Running:     func(context.Context) (int, error) { return 0, nil },
	})
	activeConfig.Config = runnableConfig(t)
	delete(activeConfig.Coordinators, "linux")

	if _, err := reloadConfiguration(
		context.Background(), "config", successfulReloadDaemon(loadedConfig), activeConfig,
	); err == nil {
		t.Fatal("expected unavailable coordinator rejection")
	}
	if stopped {
		t.Fatal("old generation stopped after unavailable coordinator rejection")
	}
}

func TestReloadProviderChangeFencesOldProvisioningAfterInspection(t *testing.T) {
	fixture := newProviderChangeRaceFixture(t)
	if _, err := reloadConfiguration(
		context.Background(), "config", fixture.daemon, fixture.activeConfig,
	); err != nil {
		t.Fatalf("expected idle provider change success, got %v", err)
	}
	if fixture.scheduleErr != nil {
		t.Fatalf("schedule during handover: %v", fixture.scheduleErr)
	}
	if len(fixture.worker.started) != 0 {
		t.Fatalf("old provider started runners after inspection: %#v", fixture.worker.started)
	}
}

type providerChangeRaceFixture struct {
	activeConfig activeConfiguration
	daemon       standaloneDaemon
	worker       *recordingRecoveryWorker
	scheduleErr  error
}

func newProviderChangeRaceFixture(t *testing.T) *providerChangeRaceFixture {
	t.Helper()
	loadedConfig := runnableConfig(t)
	loadedConfig.ScaleSets[0].Docker.Service = "replacement"
	coordinator := scaler.NewCoordinator(1)
	worker := &recordingRecoveryWorker{}
	coordinator.AddRepository("repo", worker)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		Repository: "repo", RequestID: 42,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	fixture := &providerChangeRaceFixture{worker: worker}
	activeConfig := activeWithLifecycle(scaleSetLifecycle{
		Running: func(context.Context) (int, error) { return 0, nil },
		StopPolling: func() {
			fixture.scheduleErr = coordinator.Schedule(context.Background())
		},
		CloseSession: func(context.Context) error { return nil },
	})
	activeConfig.Config = runnableConfig(t)
	activeConfig.Coordinators["linux"] = coordinator
	fixture.activeConfig = activeConfig
	fixture.daemon = successfulReloadDaemon(loadedConfig)
	return fixture
}

func TestValidateReloadProviderChangesIgnoresUnchangedLibvirtProvider(t *testing.T) {
	provider := &config.LibvirtConfig{ConnectionURI: "qemu:///system", DomainXMLTemplate: "runner.xml"}
	activeConfig := testActiveConfiguration()
	activeConfig.Config.ScaleSets = []config.ScaleSetConfig{{Name: "linux", Libvirt: provider}}
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		Running: func(context.Context) (int, error) {
			t.Fatal("unchanged provider must not be inspected")
			return 0, nil
		},
	}}}
	reloadedConfig := activeConfig.Config

	if _, err := validateReloadProviderChanges(context.Background(), activeConfig, reloadedConfig); err != nil {
		t.Fatalf("expected unchanged libvirt provider to pass validation, got %v", err)
	}
}

func TestValidateReloadProviderChangesIgnoresRepositoryWithoutChangedScaleSet(t *testing.T) {
	activeConfig := testActiveConfiguration()
	activeConfig.Config = runnableConfig(t)
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{}}
	reloadedConfig := runnableConfig(t)
	reloadedConfig.ScaleSets[0].Docker.Service = "replacement"

	if _, err := validateReloadProviderChanges(context.Background(), activeConfig, reloadedConfig); err != nil {
		t.Fatalf("expected repository without changed scale set to pass validation, got %v", err)
	}
}

func TestReloadKeepsOldSessionsWhenReplacementRegistrationsCannotBeConfirmed(t *testing.T) {
	loadedConfig := runnableConfig(t)
	stopped := false
	activeConfig := testActiveConfiguration()
	activeConfig.Config.State.Path = loadedConfig.State.Path
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		StopPolling: func() { stopped = true },
		Running:     func(context.Context) (int, error) { return 0, nil },
		Shutdown:    func(context.Context) error { return nil },
	}}}
	daemonUnderTest := successfulReloadDaemon(loadedConfig)
	daemonUnderTest.newDiscoverer = func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
		return staticDiscoverer{result: githubapp.DiscoveryResult{Repositories: []githubapp.Repository{{
			InstallationID: 1, URL: "https://github.com/acme/repo",
		}}, InstallationFailures: map[githubapp.InstallationID]error{}}}, nil
	}
	registry := successfulRegistrationReconciliationClient()
	registry.getErr = errors.New("audit unavailable")
	daemonUnderTest.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return registry, nil
	}
	if _, err := reloadConfiguration(context.Background(), "config", daemonUnderTest, activeConfig); err == nil {
		t.Fatal("expected replacement registration audit failure")
	}
	if stopped {
		t.Fatal("old session stopped before replacement registrations were confirmed")
	}
}

func TestConfirmReloadRegistrationsReportsDiscoveryFailure(t *testing.T) {
	reloaded := testActiveConfiguration()
	reloaded.Discoverer = staticDiscoverer{err: errors.New("discovery failed")}
	if err := confirmReloadRegistrations(context.Background(), standaloneDaemon{}, &reloaded); err == nil {
		t.Fatal("expected discovery failure")
	}
}

func TestConfirmReloadRegistrationsRejectsRepositoryNameCollision(t *testing.T) {
	reloaded := testActiveConfiguration()
	reloaded.Discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{Repositories: []githubapp.Repository{
		{URL: "https://github.com/acme/repo.a"}, {URL: "https://github.com/acme/repo-a"},
	}}}
	if err := confirmReloadRegistrations(context.Background(), standaloneDaemon{}, &reloaded); err == nil {
		t.Fatal("expected repository name collision")
	}
}

func TestConfirmReloadRegistrationsReportsClientCreationFailure(t *testing.T) {
	reloaded := reloadRegistrationConfiguration()
	daemon := standaloneDaemon{newGitHubClient: func(
		string, githubapp.InstallationID, config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		return nil, errors.New("client failed")
	}}
	if err := confirmReloadRegistrations(context.Background(), daemon, &reloaded); err == nil {
		t.Fatal("expected client creation failure")
	}
}

func TestConfirmReloadRegistrationsReportsRegistrationClientFailure(t *testing.T) {
	reloaded := reloadRegistrationConfiguration()
	daemon := standaloneDaemon{newGitHubClient: func(
		string, githubapp.InstallationID, config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		return nonPruningScaleSetClient{}, nil
	}}
	if err := confirmReloadRegistrations(context.Background(), daemon, &reloaded); err == nil {
		t.Fatal("expected registration client failure")
	}
}

func TestConfirmReloadRegistrationsRetainsConfirmedClient(t *testing.T) {
	reloaded := reloadRegistrationConfiguration()
	client := successfulRegistrationReconciliationClient()
	daemon := standaloneDaemon{newGitHubClient: func(
		string, githubapp.InstallationID, config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		return client, nil
	}}
	if err := confirmReloadRegistrations(context.Background(), daemon, &reloaded); err != nil {
		t.Fatal(err)
	}
	if reloaded.Repositories["https://github.com/acme/repo"].Client != client {
		t.Fatal("confirmed client was not retained for activation")
	}
}

func reloadRegistrationConfiguration() activeConfiguration {
	reloaded := testActiveConfiguration()
	reloaded.Discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{Repositories: []githubapp.Repository{{
		InstallationID: 1, URL: "https://github.com/acme/repo",
	}}}}
	return reloaded
}

func TestReloadConfigurationReportsReloadNotificationFailure(t *testing.T) {
	activeConfig := testActiveConfiguration()
	if _, err := reloadConfiguration(context.Background(), "config", standaloneDaemon{notifier: failingNotifier{reloadErr: errors.New("reload failed")}}, activeConfig); err == nil {
		t.Fatal("expected reload notification failure")
	}
}

func TestReloadLogsEveryLifecycleStage(t *testing.T) {
	fixture := newReloadLoggingFixture(t)
	if _, _, err := reloadAndReadInterval(context.Background(), "config", fixture.daemon, fixture.active); err != nil {
		t.Fatalf("expected reload success, got %v", err)
	}
	for _, phase := range []string{
		"requested",
		"replacement_validated",
		"handover_started",
		"polling_stopped",
		"old_configuration_stopped",
		"state_reconciled",
		"replacement_reconciliation_started",
		"completed",
	} {
		assertStructuredAction(t, fixture.logs.Bytes(), "reload", phase)
	}
	for _, action := range []string{"polling_stop", "scale_set_shutdown", "state_cleanup"} {
		assertStructuredAction(t, fixture.logs.Bytes(), action, "started")
		assertStructuredAction(t, fixture.logs.Bytes(), action, "completed")
	}
}

type reloadLoggingFixture struct {
	logs   *bytes.Buffer
	daemon standaloneDaemon
	active activeConfiguration
}

func newReloadLoggingFixture(t *testing.T) reloadLoggingFixture {
	t.Helper()
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "debug")
	loadedConfig := runnableConfig(t)
	active := activeWithLifecycle(inertLifecycle())
	active.Config.State.Path = loadedConfig.State.Path
	active.Logger = logger
	active.DemandStore = scaler.NewMemoryDemandStore()
	daemon := successfulReloadDaemon(loadedConfig)
	daemon.newLogger = func(string, string) *slog.Logger { return logger }
	return reloadLoggingFixture{logs: logs, daemon: daemon, active: active}
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

func TestReloadContinuesAfterRemovedScaleSetDemandPurgeFailure(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.ScaleSets[0].Name = "replacement"
	store := &recordingAppDemandStore{removeScaleSetErr: errors.New("remove failed")}
	active := testActiveConfiguration()
	active.DemandStore = store
	active.Config.State.Path = loadedConfig.State.Path
	if _, err := reloadConfiguration(
		context.Background(), "config", successfulReloadDaemon(loadedConfig), active,
	); err != nil {
		t.Fatalf("expected replacement after scale set purge failure, got %v", err)
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

func TestReloadConfigurationContinuesAfterOldSessionCloseFailure(t *testing.T) {
	loadedConfig := runnableConfig(t)
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "debug")
	active := activeWithLifecycle(scaleSetLifecycle{
		Running: func(context.Context) (int, error) { return 0, nil }, StopPolling: func() {},
		CloseSession: func(context.Context) error { return errors.New("shutdown failed") },
	})
	active.Config = loadedConfig
	active.Logger = logger
	daemonUnderTest := successfulReloadDaemon(loadedConfig)
	daemonUnderTest.newLogger = func(string, string) *slog.Logger { return logger }
	if _, err := reloadConfiguration(context.Background(), "config", daemonUnderTest, active); err != nil {
		t.Fatalf("expected replacement after old session close failure, got %v", err)
	}
	assertStructuredAction(t, logs.Bytes(), "reload", "cleanup_incomplete")
}

func TestReloadConfigurationReportsReplacementLoadFailure(t *testing.T) {
	daemonUnderTest := standaloneDaemon{
		loadConfig: func(string) (config.Config, error) { return config.Config{}, errors.New("replacement failed") },
		notifier:   noopNotifier{},
	}
	if _, err := reloadConfiguration(context.Background(), "config", daemonUnderTest, activeWithLifecycle(inertLifecycle())); err == nil {
		t.Fatal("expected replacement load failure")
	}
}

func successfulReloadDaemon(loadedConfig config.Config) standaloneDaemon {
	return standaloneDaemon{
		loadConfig:      func(string) (config.Config, error) { return loadedConfig, nil },
		loadCredentials: loadReloadTestCredentials,
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		notifier:  noopNotifier{},
	}
}

func TestReloadAndReadIntervalRejectsUnvalidatedReplacementInterval(t *testing.T) {
	loadedConfig := runnableConfig(t)
	loadedConfig.GitHub.DiscoveryInterval = "invalid"
	stopped := false
	activeConfig := activeWithLifecycle(scaleSetLifecycle{
		StopPolling:  func() { stopped = true },
		CloseSession: func(context.Context) error { return nil },
		Running:      func(context.Context) (int, error) { return 0, nil },
	})
	daemonUnderTest := standaloneDaemon{
		loadConfig:      func(string) (config.Config, error) { return loadedConfig, nil },
		loadCredentials: loadReloadTestCredentials,
		newDiscoverer: func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return staticDiscoverer{result: githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}}, nil
		},
		newLogger: func(string, string) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) },
		notifier:  noopNotifier{},
	}
	if _, _, err := reloadAndReadInterval(context.Background(), "config", daemonUnderTest, activeConfig); err == nil {
		t.Fatal("expected invalid reloaded interval error")
	}
	if stopped {
		t.Fatal("old generation stopped before replacement interval validation")
	}
}

func loadReloadTestCredentials(config.GitHubConfig) (config.GitHubAppCredentials, error) {
	return config.GitHubAppCredentials{}, nil
}
