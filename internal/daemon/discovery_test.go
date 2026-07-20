package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	"github.com/meza/github-scaler/internal/scaler"
)

func TestApplyDiscoveryAddsRepositoriesAndRetainsFailedInstallations(t *testing.T) {
	started := []string{}
	daemonUnderTest := standaloneDaemon{
		newGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		startScaleSet: func(
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
	if err := applyDiscovery(context.Background(), daemonUnderTest, &activeConfig, discovery); err != nil {
		t.Fatalf("expected discovery application, got %v", err)
	}
	if len(started) != 1 || len(activeConfig.Repositories) != 2 {
		t.Fatalf("expected new and retained repositories, started=%#v repositories=%#v", started, activeConfig.Repositories)
	}
}

func TestRepositoryReconciliationLogsDetailedActionsAtDebugLevel(t *testing.T) {
	logs := &bytes.Buffer{}
	activeConfig := testActiveConfiguration()
	activeConfig.Logger = newLoggerForWriter(logs, "json", "debug")
	daemonUnderTest := standaloneDaemon{
		newGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		startScaleSet: func(
			context.Context,
			*slog.Logger,
			scaleSetClient,
			string,
			config.ScaleSetConfig,
			*scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			return inertLifecycle(), nil
		},
	}
	discovery := githubapp.DiscoveryResult{
		Repositories:         []githubapp.Repository{{InstallationID: 7, URL: "https://github.com/acme/repo"}},
		InstallationFailures: map[int64]error{},
	}
	if err := applyDiscovery(context.Background(), daemonUnderTest, &activeConfig, discovery); err != nil {
		t.Fatalf("expected repository reconciliation, got %v", err)
	}
	for _, expected := range []struct {
		action string
		phase  string
	}{
		{action: "repository_start", phase: "started"},
		{action: "scale_set_start", phase: "started"},
		{action: "scale_set_start", phase: "completed"},
		{action: "repository_start", phase: "completed"},
		{action: "demand_recovery", phase: "completed"},
	} {
		assertStructuredAction(t, logs.Bytes(), expected.action, expected.phase)
	}
}

func TestApplyDiscoveryResumesPersistedAcquiredDemandWithoutGitHubMessage(t *testing.T) {
	const repositoryURL = "https://github.com/acme/repo"
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		RequestID: 42, Repository: repositoryURL, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	worker := &recordingRecoveryWorker{}
	activeConfig := testActiveConfiguration()
	activeConfig.Coordinators["linux"] = coordinator
	daemonUnderTest := recoveryDaemonForTest(worker)

	err := applyDiscovery(context.Background(), daemonUnderTest, &activeConfig, githubapp.DiscoveryResult{
		Repositories:         []githubapp.Repository{{InstallationID: 1, URL: repositoryURL}},
		InstallationFailures: map[int64]error{},
	})
	if err != nil {
		t.Fatalf("expected startup recovery, got %v", err)
	}
	if len(worker.acquired) != 0 || len(worker.started) != 1 || worker.started[0] != 42 {
		t.Fatalf("expected promised runner 42 to start without reacquisition, acquired=%#v started=%#v", worker.acquired, worker.started)
	}
}

func recoveryDaemonForTest(worker scaler.RepositoryWorker) standaloneDaemon {
	return standaloneDaemon{
		newGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		startScaleSet: func(
			_ context.Context,
			_ *slog.Logger,
			_ scaleSetClient,
			startedRepositoryURL string,
			_ config.ScaleSetConfig,
			startedCoordinator *scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			startedCoordinator.AddRepository(startedRepositoryURL, worker)
			return inertLifecycle(), nil
		},
	}
}

func TestApplyDiscoveryAcquiresPersistedAvailableDemandBeforeProvisioning(t *testing.T) {
	const repositoryURL = "https://github.com/acme/repo"
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		RequestID: 43, Repository: repositoryURL, QueueTime: time.Now(),
	}}, nil); err != nil {
		t.Fatal(err)
	}
	worker := &recordingRecoveryWorker{}
	activeConfig := testActiveConfiguration()
	activeConfig.Coordinators["linux"] = coordinator
	daemonUnderTest := recoveryDaemonForTest(worker)

	err := applyDiscovery(context.Background(), daemonUnderTest, &activeConfig, githubapp.DiscoveryResult{
		Repositories:         []githubapp.Repository{{InstallationID: 1, URL: repositoryURL}},
		InstallationFailures: map[int64]error{},
	})
	if err != nil {
		t.Fatalf("expected startup recovery, got %v", err)
	}
	if len(worker.acquired) != 1 || worker.acquired[0] != 43 || len(worker.started) != 1 || worker.started[0] != 43 {
		t.Fatalf("expected available demand 43 to be acquired before start, acquired=%#v started=%#v", worker.acquired, worker.started)
	}
}

func TestApplyDiscoveryReprovisionsProviderVisiblePromiseThatDisappearsBeforeJobStarted(t *testing.T) {
	const repositoryURL = "https://github.com/acme/repo"
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		RequestID: 44, Repository: repositoryURL, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	worker := &recordingRecoveryWorker{existing: map[int64]bool{44: true}}
	activeConfig := testActiveConfiguration()
	activeConfig.Coordinators["linux"] = coordinator

	err := applyDiscovery(context.Background(), recoveryDaemonForTest(worker), &activeConfig, githubapp.DiscoveryResult{
		Repositories:         []githubapp.Repository{{InstallationID: 1, URL: repositoryURL}},
		InstallationFailures: map[int64]error{},
	})
	if err != nil {
		t.Fatalf("expected provider-visible recovery, got %v", err)
	}
	worker.existing[44] = false
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(worker.acquired) != 0 || len(worker.started) != 1 || worker.started[0] != 44 {
		t.Fatalf("expected disappeared promise to reprovision without reacquisition, acquired=%#v started=%#v", worker.acquired, worker.started)
	}
}

func TestApplyDiscoveryResumesAvailableRepositoriesAndRetainsUnavailableDemand(t *testing.T) {
	const healthyRepository = "https://github.com/acme/healthy"
	const unavailableRepository = "https://github.com/acme/unavailable"
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 2, store)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{
		{RequestID: 45, Repository: unavailableRepository, QueueTime: time.Now(), Acquired: true},
		{RequestID: 46, Repository: healthyRepository, QueueTime: time.Now().Add(time.Second), Acquired: true},
	}, nil); err != nil {
		t.Fatal(err)
	}
	healthyWorker := &recordingRecoveryWorker{}
	activeConfig := testActiveConfiguration()
	activeConfig.Config.ScaleSets[0].MaxRunners = 2
	activeConfig.Coordinators["linux"] = coordinator
	daemonUnderTest := recoveryDaemonForTest(healthyWorker)
	daemonUnderTest.newGitHubClient = func(repositoryURL string, _ int64, _ config.GitHubAppCredentials) (scaleSetClient, error) {
		if repositoryURL == unavailableRepository {
			return nil, errors.New("unavailable")
		}
		return &fakeScaleSetClient{}, nil
	}

	err := applyDiscovery(context.Background(), daemonUnderTest, &activeConfig, githubapp.DiscoveryResult{
		Repositories: []githubapp.Repository{
			{InstallationID: 1, URL: unavailableRepository},
			{InstallationID: 2, URL: healthyRepository},
		},
		InstallationFailures: map[int64]error{},
	})
	if err == nil {
		t.Fatal("expected unavailable repository startup failure")
	}
	if len(healthyWorker.started) != 1 || healthyWorker.started[0] != 46 {
		t.Fatalf("expected healthy promised runner to resume, got %#v", healthyWorker.started)
	}
	unavailableWorker := &recordingRecoveryWorker{}
	coordinator.AddRepository(unavailableRepository, unavailableWorker)
	if err := coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(unavailableWorker.started) != 1 || unavailableWorker.started[0] != 45 {
		t.Fatalf("expected unavailable demand to remain queued for retry, got %#v", unavailableWorker.started)
	}
}

func TestReconcileDiscoveryRetriesPersistedDemandAfterRecoveryFailure(t *testing.T) {
	const repositoryURL = "https://github.com/acme/repo"
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 1, store)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{{
		RequestID: 47, Repository: repositoryURL, QueueTime: time.Now(), Acquired: true,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	worker := &recordingRecoveryWorker{existsErr: errors.New("provider unavailable")}
	activeConfig := testActiveConfiguration()
	activeConfig.Coordinators["linux"] = coordinator
	activeConfig.Discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{
		Repositories:         []githubapp.Repository{{InstallationID: 1, URL: repositoryURL}},
		InstallationFailures: map[int64]error{},
	}}
	daemonUnderTest := recoveryDaemonForTest(worker)

	reconcileDiscovery(context.Background(), daemonUnderTest, &activeConfig)
	if !activeConfig.ReconciliationPending {
		t.Fatal("expected failed recovery to request a short reconciliation retry")
	}
	worker.existsErr = nil
	reconcileDiscovery(context.Background(), daemonUnderTest, &activeConfig)
	if activeConfig.ReconciliationPending {
		t.Fatal("expected successful recovery retry to restore the discovery interval")
	}
	if len(worker.started) != 1 || worker.started[0] != 47 {
		t.Fatalf("expected failed recovery to retain promised demand, got %#v", worker.started)
	}
}

func TestApplyDiscoveryImmediatelyRemovesRevokedRepository(t *testing.T) {
	stopped := false
	destroyed := false
	shutdown := false
	logs := &bytes.Buffer{}
	activeConfig := testActiveConfiguration()
	activeConfig.Logger = newLoggerForWriter(logs, "json", "debug")
	activeConfig.Repositories["https://github.com/acme/removed"] = managedRepository{
		InstallationID: 1,
		Lifecycles: map[string]scaleSetLifecycle{"linux": {
			StopPolling:   func() { stopped = true },
			DestroyActive: func(context.Context) error { destroyed = true; return nil },
			Shutdown:      func(context.Context) error { shutdown = true; return nil },
		}},
	}
	if err := applyDiscovery(context.Background(), standaloneDaemon{}, &activeConfig, githubapp.DiscoveryResult{InstallationFailures: map[int64]error{}}); err != nil {
		t.Fatalf("expected removal, got %v", err)
	}
	if !stopped || !destroyed || !shutdown || len(activeConfig.Repositories) != 0 {
		t.Fatalf("expected immediate teardown, stopped=%t destroyed=%t shutdown=%t", stopped, destroyed, shutdown)
	}
	for _, action := range []string{"repository_remove", "demand_remove", "runner_destroy", "scale_set_shutdown"} {
		assertStructuredAction(t, logs.Bytes(), action, "started")
		assertStructuredAction(t, logs.Bytes(), action, "completed")
	}
}

func TestApplyDiscoveryIsolatesRepositoryStartupFailureAndLogsItsIdentity(t *testing.T) {
	logs := &bytes.Buffer{}
	activeConfig := testActiveConfiguration()
	activeConfig.Logger = slog.New(slog.NewTextHandler(logs, nil))
	daemonUnderTest := standaloneDaemon{
		newGitHubClient: func(repositoryURL string, _ int64, _ config.GitHubAppCredentials) (scaleSetClient, error) {
			if repositoryURL == "https://github.com/acme/failing" {
				return nil, errors.New("client failed")
			}
			return &fakeScaleSetClient{}, nil
		},
		startScaleSet: func(context.Context, *slog.Logger, scaleSetClient, string, config.ScaleSetConfig, *scaler.Coordinator) (scaleSetLifecycle, error) {
			return inertLifecycle(), nil
		},
	}
	err := applyDiscovery(context.Background(), daemonUnderTest, &activeConfig, githubapp.DiscoveryResult{
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
	daemonUnderTest := standaloneDaemon{
		newGitHubClient: func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		startScaleSet: func(
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
	_, err := startRepository(context.Background(), daemonUnderTest, &activeConfig, githubapp.Repository{
		InstallationID: 1, URL: "https://github.com/acme/repo",
	})
	if err == nil {
		t.Fatal("expected later startup failure")
	}
	if !stopped || !shutdown {
		t.Fatalf("expected earlier scale set rollback, stopped=%t shutdown=%t", stopped, shutdown)
	}
}
