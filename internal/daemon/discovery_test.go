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
	fixture := newDiscoveryStartFixture()
	discovery := githubapp.DiscoveryResult{
		Repositories:         []githubapp.Repository{{InstallationID: 1, URL: "https://github.com/acme/one"}},
		InstallationFailures: map[githubapp.InstallationID]error{2: errors.New("unavailable")},
	}
	fixture.active.Repositories["https://github.com/kept/two"] = managedRepository{
		InstallationID: 2,
		Lifecycles:     map[string]scaleSetLifecycle{"linux": inertLifecycle()},
	}
	if err := applyDiscovery(context.Background(), fixture.daemon, &fixture.active, discovery); err != nil {
		t.Fatalf("expected discovery application, got %v", err)
	}
	if len(fixture.started) != 1 || len(fixture.active.Repositories) != 2 {
		t.Fatalf("expected new and retained repositories, started=%#v repositories=%#v", fixture.started, fixture.active.Repositories)
	}
}

func TestRepositoryReconciliationLogsDetailedActionsAtDebugLevel(t *testing.T) {
	fixture := newDiscoveryStartFixture()
	logs := &bytes.Buffer{}
	fixture.active.Logger = newLoggerForWriter(logs, "json", "debug")
	discovery := githubapp.DiscoveryResult{
		Repositories:         []githubapp.Repository{{InstallationID: 7, URL: "https://github.com/acme/repo"}},
		InstallationFailures: map[githubapp.InstallationID]error{},
	}
	if err := applyDiscovery(context.Background(), fixture.daemon, &fixture.active, discovery); err != nil {
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

type discoveryStartFixture struct {
	daemon  standaloneDaemon
	active  activeConfiguration
	started []string
}

func newDiscoveryStartFixture() *discoveryStartFixture {
	fixture := &discoveryStartFixture{active: testActiveConfiguration()}
	fixture.daemon = standaloneDaemon{
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		startScaleSet: fixture.startScaleSet,
	}
	return fixture
}

func (fixture *discoveryStartFixture) startScaleSet(
	_ context.Context,
	_ *slog.Logger,
	_ scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
	_ *scaler.Coordinator,
) (scaleSetLifecycle, error) {
	fixture.started = append(fixture.started, repositoryURL+"/"+scaleSetConfig.Name)
	return inertLifecycle(), nil
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
		InstallationFailures: map[githubapp.InstallationID]error{},
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
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
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
		InstallationFailures: map[githubapp.InstallationID]error{},
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
		InstallationFailures: map[githubapp.InstallationID]error{},
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
	fixture := newPartialRecoveryFixture(t)
	if err := applyDiscovery(context.Background(), fixture.daemon, &fixture.active, fixture.discovery); err == nil {
		t.Fatal("expected unavailable repository startup failure")
	}
	if len(fixture.healthyWorker.started) != 1 || fixture.healthyWorker.started[0] != 46 {
		t.Fatalf("expected healthy promised runner to resume, got %#v", fixture.healthyWorker.started)
	}
}

func TestApplyDiscoveryRetainsUnavailableRepositoryDemandForRetry(t *testing.T) {
	fixture := newPartialRecoveryFixture(t)
	if err := applyDiscovery(context.Background(), fixture.daemon, &fixture.active, fixture.discovery); err == nil {
		t.Fatal("expected unavailable repository startup failure")
	}
	unavailableWorker := &recordingRecoveryWorker{}
	fixture.coordinator.AddRepository(unavailableRepositoryURL, unavailableWorker)
	if err := fixture.coordinator.Schedule(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(unavailableWorker.started) != 1 || unavailableWorker.started[0] != 45 {
		t.Fatalf("expected unavailable demand to remain queued for retry, got %#v", unavailableWorker.started)
	}
}

const healthyRepositoryURL = "https://github.com/acme/healthy"
const unavailableRepositoryURL = "https://github.com/acme/unavailable"

type partialRecoveryFixture struct {
	coordinator   *scaler.Coordinator
	healthyWorker *recordingRecoveryWorker
	active        activeConfiguration
	daemon        standaloneDaemon
	discovery     githubapp.DiscoveryResult
}

func newPartialRecoveryFixture(t *testing.T) partialRecoveryFixture {
	t.Helper()
	store := scaler.NewMemoryDemandStore()
	coordinator := scaler.NewDurableCoordinator("linux", 2, store)
	if err := coordinator.Apply(context.Background(), []scaler.JobDemand{
		{RequestID: 45, Repository: unavailableRepositoryURL, QueueTime: time.Now(), Acquired: true},
		{RequestID: 46, Repository: healthyRepositoryURL, QueueTime: time.Now().Add(time.Second), Acquired: true},
	}, nil); err != nil {
		t.Fatal(err)
	}
	healthyWorker := &recordingRecoveryWorker{}
	activeConfig := testActiveConfiguration()
	activeConfig.Config.ScaleSets[0].MaxRunners = 2
	activeConfig.Coordinators["linux"] = coordinator
	daemonUnderTest := recoveryDaemonForTest(healthyWorker)
	daemonUnderTest.newGitHubClient = func(repositoryURL string, _ githubapp.InstallationID, _ config.GitHubAppCredentials) (scaleSetClient, error) {
		if repositoryURL == unavailableRepositoryURL {
			return nil, errors.New("unavailable")
		}
		return &fakeScaleSetClient{}, nil
	}

	return partialRecoveryFixture{
		coordinator: coordinator, healthyWorker: healthyWorker, active: activeConfig, daemon: daemonUnderTest,
		discovery: githubapp.DiscoveryResult{
			Repositories: []githubapp.Repository{
				{InstallationID: 1, URL: unavailableRepositoryURL},
				{InstallationID: 2, URL: healthyRepositoryURL},
			},
			InstallationFailures: map[githubapp.InstallationID]error{},
		},
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
		InstallationFailures: map[githubapp.InstallationID]error{},
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
	if err := applyDiscovery(context.Background(), standaloneDaemon{}, &activeConfig, githubapp.DiscoveryResult{InstallationFailures: map[githubapp.InstallationID]error{}}); err != nil {
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
	fixture := repositoryStartupFailureFixture()
	err := applyDiscovery(context.Background(), fixture.daemon, &fixture.active, fixture.discovery)
	if err == nil {
		t.Fatal("expected repository startup failure")
	}
	if _, exists := fixture.active.Repositories["https://github.com/acme/healthy"]; !exists {
		t.Fatal("expected healthy repository to start despite another repository failure")
	}
	for _, expected := range []string{"repository startup failed", "repository=https://github.com/acme/failing", "installationID=1", "client failed"} {
		if !strings.Contains(fixture.logs.String(), expected) {
			t.Fatalf("expected %q in repository failure log %q", expected, fixture.logs.String())
		}
	}
}

type repositoryStartupFailureTestFixture struct {
	logs      *bytes.Buffer
	active    activeConfiguration
	daemon    standaloneDaemon
	discovery githubapp.DiscoveryResult
}

func repositoryStartupFailureFixture() repositoryStartupFailureTestFixture {
	logs := &bytes.Buffer{}
	active := testActiveConfiguration()
	active.Logger = slog.New(slog.NewTextHandler(logs, nil))
	daemon := standaloneDaemon{
		newGitHubClient: func(repositoryURL string, _ githubapp.InstallationID, _ config.GitHubAppCredentials) (scaleSetClient, error) {
			if repositoryURL == "https://github.com/acme/failing" {
				return nil, errors.New("client failed")
			}
			return &fakeScaleSetClient{}, nil
		},
		startScaleSet: successfulDiscoveryScaleSetStart,
	}
	return repositoryStartupFailureTestFixture{logs: logs, active: active, daemon: daemon, discovery: githubapp.DiscoveryResult{
		Repositories: []githubapp.Repository{
			{InstallationID: 1, URL: "https://github.com/acme/failing"},
			{InstallationID: 2, URL: "https://github.com/acme/healthy"},
		},
		InstallationFailures: map[githubapp.InstallationID]error{},
	}}
}

func successfulDiscoveryScaleSetStart(
	context.Context,
	*slog.Logger,
	scaleSetClient,
	string,
	config.ScaleSetConfig,
	*scaler.Coordinator,
) (scaleSetLifecycle, error) {
	return inertLifecycle(), nil
}

func TestStartRepositoryRollsBackEarlierScaleSetsWhenLaterStartupFails(t *testing.T) {
	fixture := repositoryRollbackFixture()
	_, err := startRepository(context.Background(), fixture.daemon, &fixture.active, githubapp.Repository{
		InstallationID: 1, URL: "https://github.com/acme/repo",
	})
	if err == nil {
		t.Fatal("expected later startup failure")
	}
	if !fixture.stopped || !fixture.shutdown {
		t.Fatalf("expected earlier scale set rollback, stopped=%t shutdown=%t", fixture.stopped, fixture.shutdown)
	}
}

type repositoryRollbackTestFixture struct {
	active     activeConfiguration
	daemon     standaloneDaemon
	startCalls int
	stopped    bool
	shutdown   bool
}

func repositoryRollbackFixture() *repositoryRollbackTestFixture {
	fixture := &repositoryRollbackTestFixture{active: testActiveConfiguration()}
	fixture.active.Config.ScaleSets = append(fixture.active.Config.ScaleSets, config.ScaleSetConfig{Name: "windows", MaxRunners: 1})
	fixture.active.Coordinators["windows"] = scaler.NewCoordinator(1)
	fixture.daemon = standaloneDaemon{
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		startScaleSet: fixture.startScaleSet,
	}
	return fixture
}

func (fixture *repositoryRollbackTestFixture) startScaleSet(
	context.Context,
	*slog.Logger,
	scaleSetClient,
	string,
	config.ScaleSetConfig,
	*scaler.Coordinator,
) (scaleSetLifecycle, error) {
	fixture.startCalls++
	if fixture.startCalls == 2 {
		return scaleSetLifecycle{}, errors.New("second failed")
	}
	return scaleSetLifecycle{
		StopPolling: func() { fixture.stopped = true }, DestroyActive: func(context.Context) error { return nil },
		Shutdown: func(context.Context) error { fixture.shutdown = true; return nil },
	}, nil
}

func TestApplyDiscoveryRejectsLocalScaleSetNameCollision(t *testing.T) {
	activeConfig := testActiveConfiguration()
	discovery := githubapp.DiscoveryResult{
		Repositories: []githubapp.Repository{
			{InstallationID: 1, URL: "https://github.com/acme/repo.a"},
			{InstallationID: 1, URL: "https://github.com/acme/repo-a"},
		},
		InstallationFailures: map[githubapp.InstallationID]error{},
	}
	if err := applyDiscovery(context.Background(), standaloneDaemon{}, &activeConfig, discovery); err == nil {
		t.Fatal("expected local name collision")
	}
}

func TestRemoveRepositoryImmediatelyReportsLifecycleCleanupFailure(t *testing.T) {
	activeConfig := testActiveConfiguration()
	repository := managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		StopPolling:   func() {},
		DestroyActive: func(context.Context) error { return errors.New("destroy failed") },
		Shutdown:      func(context.Context) error { return errors.New("shutdown failed") },
	}}}
	activeConfig.Repositories["repo"] = repository
	if err := removeRepositoryImmediately(context.Background(), &activeConfig, "repo", repository); err == nil {
		t.Fatal("expected immediate removal failure")
	}
}

func TestRollbackRepositoryStartupReportsLifecycleCleanupFailure(t *testing.T) {
	activeConfig := testActiveConfiguration()
	repository := managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		StopPolling:   func() {},
		DestroyActive: func(context.Context) error { return errors.New("destroy failed") },
		Shutdown:      func(context.Context) error { return errors.New("shutdown failed") },
	}}}
	if err := rollbackRepositoryStartup(context.Background(), &activeConfig, "repo", repository); err == nil {
		t.Fatal("expected rollback failure")
	}
}
