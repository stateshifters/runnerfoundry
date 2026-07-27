package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
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

func TestApplyDiscoveryRetainsPartialRegistrationsUntilAtomicActivationSucceeds(t *testing.T) {
	fixture := newPartialRegistrationTestFixture()
	if err := fixture.reconcile(); err == nil {
		t.Fatal("expected incomplete registration reconciliation")
	}
	assertPartialRegistrationReconciliation(t, fixture.active, fixture.repositoryURL, fixture.started, fixture.registry)
	fixture.registry.failWindows = false
	if err := fixture.reconcile(); err != nil {
		t.Fatal(err)
	}
	assertCompletedRegistrationReconciliation(t, fixture.active, fixture.repositoryURL, fixture.started, fixture.registry)
}

type partialRegistrationTestFixture struct {
	repositoryURL string
	active        activeConfiguration
	registry      *registrationReconciliationClient
	started       []string
	daemon        standaloneDaemon
	discovery     githubapp.DiscoveryResult
}

func newPartialRegistrationTestFixture() *partialRegistrationTestFixture {
	fixture := &partialRegistrationTestFixture{
		repositoryURL: "https://github.com/acme/repo", active: testActiveConfiguration(),
		registry: newRegistrationReconciliationClient(),
	}
	fixture.active.Config.ScaleSets = append(fixture.active.Config.ScaleSets, config.ScaleSetConfig{
		Name: "windows", RunnerGroup: fixture.active.Config.ScaleSets[0].RunnerGroup, MaxRunners: 1,
	})
	fixture.active.Coordinators["windows"] = scaler.NewCoordinator(1)
	fixture.daemon = standaloneDaemon{
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return fixture.registry, nil
		},
		startScaleSet: func(
			_ context.Context, _ *slog.Logger, _ scaleSetClient, _ string,
			scaleSetConfig config.ScaleSetConfig, _ *scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			fixture.started = append(fixture.started, scaleSetConfig.Name)
			return inertLifecycle(), nil
		},
	}
	fixture.discovery = githubapp.DiscoveryResult{Repositories: []githubapp.Repository{{
		InstallationID: 1, URL: fixture.repositoryURL,
	}}, InstallationFailures: map[githubapp.InstallationID]error{}}
	return fixture
}

func (fixture *partialRegistrationTestFixture) reconcile() error {
	return applyDiscovery(context.Background(), fixture.daemon, &fixture.active, fixture.discovery)
}

func assertPartialRegistrationReconciliation(
	t *testing.T,
	active activeConfiguration,
	repositoryURL string,
	started []string,
	registry *registrationReconciliationClient,
) {
	t.Helper()
	managed, retained := active.Repositories[repositoryURL]
	if !retained || len(managed.Lifecycles) != 0 || len(started) != 0 ||
		managed.RegistrationStatus != repositoryRegistrationDegraded {
		t.Fatalf("retained=%t lifecycles=%d started=%#v status=%q",
			retained, len(managed.Lifecycles), started, managed.RegistrationStatus)
	}
	if registry.createCalls["linux"] != 1 || registry.createCalls["windows"] != 1 || registry.registrations["linux"] == nil {
		t.Fatalf("create calls=%#v registrations=%#v", registry.createCalls, registry.registrations)
	}
}

func assertCompletedRegistrationReconciliation(
	t *testing.T,
	active activeConfiguration,
	repositoryURL string,
	started []string,
	registry *registrationReconciliationClient,
) {
	t.Helper()
	managed := active.Repositories[repositoryURL]
	if len(managed.Lifecycles) != 2 || len(started) != 2 ||
		managed.RegistrationStatus != repositoryRegistrationOperational {
		t.Fatalf("lifecycles=%d started=%#v status=%q", len(managed.Lifecycles), started, managed.RegistrationStatus)
	}
	if registry.createCalls["linux"] != 1 || registry.createCalls["windows"] != 2 || registry.listCalls != 2 {
		t.Fatalf("create calls=%#v grouped audit calls=%d", registry.createCalls, registry.listCalls)
	}
}

func TestApplyDiscoveryStartsSessionFromTheAuditedRegistrationIdentity(t *testing.T) {
	activeConfig := testActiveConfiguration()
	registry := successfulRegistrationReconciliationClient()
	confirmedID, legacyStarts := 0, 0
	daemon := standaloneDaemon{
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return registry, nil
		},
		startScaleSet: func(
			context.Context, *slog.Logger, scaleSetClient, string, config.ScaleSetConfig, *scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			legacyStarts++
			return inertLifecycle(), nil
		},
		startConfirmedScaleSet: func(
			_ context.Context, _ *slog.Logger, _ scaleSetClient, _ string, _ config.ScaleSetConfig,
			_ *scaler.Coordinator, registration *upstream.RunnerScaleSet,
		) (scaleSetLifecycle, error) {
			confirmedID = registration.ID
			return inertLifecycle(), nil
		},
	}
	if err := applyDiscovery(context.Background(), daemon, &activeConfig, githubapp.DiscoveryResult{
		Repositories: []githubapp.Repository{{InstallationID: 1, URL: "repo"}},
	}); err != nil {
		t.Fatal(err)
	}
	if confirmedID != registry.registrations["linux"].ID || legacyStarts != 0 {
		t.Fatalf("confirmedID=%d legacyStarts=%d registrations=%#v", confirmedID, legacyStarts, registry.registrations)
	}
}

func TestApplyDiscoveryRepairsRegistrationDriftByReplacingTheCompleteSessionGroup(t *testing.T) {
	fixture := newRegistrationDriftTestFixture()
	if err := fixture.reconcile(); err != nil {
		t.Fatal(err)
	}
	oldLinuxID := fixture.active.Repositories[fixture.repositoryURL].Lifecycles["linux"].RunnerScaleSetID
	delete(fixture.registry.registrations, "linux")
	if err := fixture.reconcile(); err != nil {
		t.Fatal(err)
	}
	fixture.assertRepaired(t, oldLinuxID)
}

type registrationDriftTestFixture struct {
	repositoryURL            string
	active                   activeConfiguration
	registry                 *registrationReconciliationClient
	daemon                   standaloneDaemon
	discovery                githubapp.DiscoveryResult
	started, stopped, closed int
}

func newRegistrationDriftTestFixture() *registrationDriftTestFixture {
	fixture := &registrationDriftTestFixture{
		repositoryURL: "https://github.com/acme/repo", active: testActiveConfiguration(),
		registry: successfulRegistrationReconciliationClient(),
	}
	fixture.active.Config.ScaleSets = append(fixture.active.Config.ScaleSets, config.ScaleSetConfig{
		Name: "windows", RunnerGroup: fixture.active.Config.ScaleSets[0].RunnerGroup, MaxRunners: 1,
	})
	fixture.active.Coordinators["windows"] = scaler.NewCoordinator(1)
	fixture.daemon = standaloneDaemon{
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return fixture.registry, nil
		},
		startScaleSet: func(
			context.Context, *slog.Logger, scaleSetClient, string, config.ScaleSetConfig, *scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			fixture.started++
			return scaleSetLifecycle{StopPolling: func() { fixture.stopped++ }, CloseSession: func(context.Context) error {
				fixture.closed++
				return nil
			}}, nil
		},
	}
	fixture.discovery = githubapp.DiscoveryResult{Repositories: []githubapp.Repository{{
		InstallationID: 1, URL: fixture.repositoryURL,
	}}, InstallationFailures: map[githubapp.InstallationID]error{}}
	return fixture
}

func (fixture *registrationDriftTestFixture) reconcile() error {
	return applyDiscovery(context.Background(), fixture.daemon, &fixture.active, fixture.discovery)
}

func (fixture *registrationDriftTestFixture) assertRepaired(t *testing.T, oldLinuxID int) {
	t.Helper()
	managed := fixture.active.Repositories[fixture.repositoryURL]
	if fixture.started != 4 || fixture.stopped != 2 || fixture.closed != 2 || len(managed.Lifecycles) != 2 {
		t.Fatalf("started=%d stopped=%d closed=%d lifecycles=%d",
			fixture.started, fixture.stopped, fixture.closed, len(managed.Lifecycles))
	}
	if managed.Lifecycles["linux"].RunnerScaleSetID == oldLinuxID {
		t.Fatalf("linux registration ID was not replaced: %d", oldLinuxID)
	}
	if fixture.registry.createCalls["linux"] != 2 || fixture.registry.createCalls["windows"] != 1 {
		t.Fatalf("create calls=%#v", fixture.registry.createCalls)
	}
}

func TestApplyDiscoveryRetainsHealthySessionsDuringRegistrationAuditFailure(t *testing.T) {
	fixture := activeRegistrationAuditFixture(t)
	fixture.registry.listErr = errors.New("audit timeout")
	if err := applyDiscovery(context.Background(), fixture.daemon, &fixture.active, fixture.discovery); err == nil {
		t.Fatal("expected degraded audit")
	}
	if fixture.started != 1 || fixture.closed != 0 || len(fixture.active.Repositories[fixture.repositoryURL].Lifecycles) != 1 {
		t.Fatalf("started=%d closed=%d lifecycles=%d", fixture.started, fixture.closed,
			len(fixture.active.Repositories[fixture.repositoryURL].Lifecycles))
	}
}

func TestRepositoryReconciliationReportsSessionSuspensionFailure(t *testing.T) {
	active := testActiveConfiguration()
	registry := successfulRegistrationReconciliationClient()
	registry.registrations["linux"] = &upstream.RunnerScaleSet{ID: 2, Name: "linux"}
	managed := managedRepository{Client: registry, Lifecycles: map[string]scaleSetLifecycle{"linux": {
		RunnerScaleSetID: 1, StopPolling: func() {},
		CloseSession: func(context.Context) error { return errors.New("close failed") },
	}}}
	if err := reconcileRepositoryState(
		context.Background(), standaloneDaemon{}, &active, githubapp.Repository{URL: "repo"}, &managed,
	); err == nil {
		t.Fatal("expected session suspension failure")
	}
}

func TestRepositoryRegistrationAuditRejectsUnsupportedClient(t *testing.T) {
	active := testActiveConfiguration()
	_, err := auditRepositoryRegistrations(context.Background(), &active, "repo", managedRepository{
		Client: nonPruningScaleSetClient{},
	})
	if err == nil {
		t.Fatal("expected unsupported audit client")
	}
}

func TestRepositorySessionStartupRequiresEveryConfirmedRegistration(t *testing.T) {
	active := testActiveConfiguration()
	managed := managedRepository{Lifecycles: map[string]scaleSetLifecycle{}}
	if err := startRepositoryScaleSets(
		context.Background(), standaloneDaemon{}, &active, "repo", &managed, nonPruningScaleSetClient{}, nil,
	); err == nil {
		t.Fatal("expected missing registration")
	}
}

func TestPruneRepositoryWhenEnabledRunsSupportedClient(t *testing.T) {
	active := testActiveConfiguration()
	active.Config.Prune = true
	client := successfulRegistrationReconciliationClient()
	client.deleteRunnerScaleSet = func(context.Context, int) error { return nil }
	if err := pruneRepositoryWhenEnabled(context.Background(), &active, client, "repo"); err != nil {
		t.Fatal(err)
	}
}

func TestRegistrationHTTPAuditFailuresRetainHealthySessions(t *testing.T) {
	t.Run("http failure", func(t *testing.T) {
		assertRegistrationHTTPAuditFailure(t, func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusServiceUnavailable)
		})
	})
	t.Run("timeout", func(t *testing.T) {
		assertRegistrationHTTPAuditFailure(t, func(_ http.ResponseWriter, request *http.Request) {
			<-request.Context().Done()
		})
	})
}

func assertRegistrationHTTPAuditFailure(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	server := httptest.NewServer(handler)
	defer server.Close()
	fixture := activeRegistrationAuditFixture(t)
	fixture.registry.listRunnerScaleSets = func(ctx context.Context, _ int) ([]upstream.RunnerScaleSet, error) {
		return listRegistrationsFromHTTP(ctx, server.Client(), server.URL)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := applyDiscovery(ctx, fixture.daemon, &fixture.active, fixture.discovery); err == nil {
		t.Fatal("expected degraded HTTP audit")
	}
	if fixture.started != 1 || fixture.closed != 0 || len(fixture.active.Repositories[fixture.repositoryURL].Lifecycles) != 1 {
		t.Fatalf("started=%d closed=%d lifecycles=%d", fixture.started, fixture.closed,
			len(fixture.active.Repositories[fixture.repositoryURL].Lifecycles))
	}
}

func listRegistrationsFromHTTP(
	ctx context.Context,
	client *http.Client,
	url string,
) ([]upstream.RunnerScaleSet, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("create registration audit request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("perform registration audit request: %w", err)
	}
	statusErr := error(nil)
	if response.StatusCode != http.StatusOK {
		statusErr = errors.New(response.Status)
	}
	return nil, errors.Join(statusErr, response.Body.Close())
}

type activeRegistrationAuditTestFixture struct {
	repositoryURL string
	active        activeConfiguration
	daemon        standaloneDaemon
	registry      *registrationReconciliationClient
	discovery     githubapp.DiscoveryResult
	started       int
	closed        int
}

func activeRegistrationAuditFixture(t *testing.T) *activeRegistrationAuditTestFixture {
	t.Helper()
	const repositoryURL = "https://github.com/acme/repo"
	fixture := &activeRegistrationAuditTestFixture{
		repositoryURL: repositoryURL, active: testActiveConfiguration(), registry: successfulRegistrationReconciliationClient(),
	}
	fixture.daemon = standaloneDaemon{
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return fixture.registry, nil
		},
		startScaleSet: func(
			context.Context, *slog.Logger, scaleSetClient, string, config.ScaleSetConfig, *scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			fixture.started++
			return scaleSetLifecycle{StopPolling: func() {}, CloseSession: func(context.Context) error {
				fixture.closed++
				return nil
			}}, nil
		},
	}
	fixture.discovery = githubapp.DiscoveryResult{Repositories: []githubapp.Repository{{
		InstallationID: 1, URL: repositoryURL,
	}}, InstallationFailures: map[githubapp.InstallationID]error{}}
	if err := applyDiscovery(context.Background(), fixture.daemon, &fixture.active, fixture.discovery); err != nil {
		t.Fatal(err)
	}
	return fixture
}

type registrationReconciliationClient struct {
	fakeScaleSetClient
	registrations map[string]*upstream.RunnerScaleSet
	createCalls   map[string]int
	listCalls     int
	failWindows   bool
	listErr       error
	nextID        int
}

func newRegistrationReconciliationClient() *registrationReconciliationClient {
	client := &registrationReconciliationClient{
		registrations: map[string]*upstream.RunnerScaleSet{}, createCalls: map[string]int{}, failWindows: true, nextID: 1,
	}
	client.getRunnerGroupByName = func(context.Context, string) (*upstream.RunnerGroup, error) {
		return &upstream.RunnerGroup{ID: 1}, nil
	}
	client.listRunnerScaleSets = client.listRegistrations
	client.getRunnerScaleSet = client.getRegistration
	client.createRunnerScaleSet = client.createRegistration
	return client
}

func (client *registrationReconciliationClient) listRegistrations(context.Context, int) ([]upstream.RunnerScaleSet, error) {
	client.listCalls++
	if client.listErr != nil {
		return nil, client.listErr
	}
	result := make([]upstream.RunnerScaleSet, 0, len(client.registrations))
	for _, registration := range client.registrations {
		result = append(result, *registration)
	}
	return result, nil
}

func (client *registrationReconciliationClient) getRegistration(_ context.Context, _ int, name string) (*upstream.RunnerScaleSet, error) {
	return client.registrations[name], nil
}

func (client *registrationReconciliationClient) createRegistration(
	_ context.Context,
	registration *upstream.RunnerScaleSet,
) (*upstream.RunnerScaleSet, error) {
	client.createCalls[registration.Name]++
	if registration.Name == "windows" && client.failWindows {
		return nil, errors.New("windows registration failed")
	}
	created := *registration
	created.ID = client.nextID
	client.nextID++
	client.registrations[registration.Name] = &created
	return &created, nil
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
			return successfulRegistrationReconciliationClient(), nil
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
			return successfulRegistrationReconciliationClient(), nil
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
		return successfulRegistrationReconciliationClient(), nil
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

func successfulRegistrationReconciliationClient() *registrationReconciliationClient {
	client := newRegistrationReconciliationClient()
	client.failWindows = false
	return client
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

func TestStartRepositorySuspendsEarlierScaleSetsWithoutDeletingStateWhenLaterStartupFails(t *testing.T) {
	fixture := repositoryRollbackFixture()
	managed, err := startRepository(context.Background(), fixture.daemon, &fixture.active, githubapp.Repository{
		InstallationID: 1, URL: "https://github.com/acme/repo",
	})
	if err == nil {
		t.Fatal("expected later startup failure")
	}
	if !fixture.stopped || !fixture.closed || fixture.destroyed || fixture.shutdown {
		t.Fatalf("stopped=%t closed=%t destroyed=%t shutdown=%t", fixture.stopped, fixture.closed, fixture.destroyed, fixture.shutdown)
	}
	if len(managed.Lifecycles) != 0 {
		t.Fatalf("partial lifecycles=%#v", managed.Lifecycles)
	}
}

type repositoryRollbackTestFixture struct {
	active     activeConfiguration
	daemon     standaloneDaemon
	startCalls int
	stopped    bool
	closed     bool
	destroyed  bool
	shutdown   bool
}

func repositoryRollbackFixture() *repositoryRollbackTestFixture {
	fixture := &repositoryRollbackTestFixture{active: testActiveConfiguration()}
	fixture.active.Config.ScaleSets = append(fixture.active.Config.ScaleSets, config.ScaleSetConfig{Name: "windows", MaxRunners: 1})
	fixture.active.Coordinators["windows"] = scaler.NewCoordinator(1)
	registry := newRegistrationReconciliationClient()
	registry.failWindows = false
	fixture.daemon = standaloneDaemon{
		newGitHubClient: func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
			return registry, nil
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
		StopPolling: func() { fixture.stopped = true },
		CloseSession: func(context.Context) error {
			fixture.closed = true
			return nil
		},
		DestroyActive: func(context.Context) error { fixture.destroyed = true; return nil },
		Shutdown:      func(context.Context) error { fixture.shutdown = true; return nil },
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

func TestSuspendRepositoryActivationReportsSessionCloseFailure(t *testing.T) {
	activeConfig := testActiveConfiguration()
	repository := managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		StopPolling:  func() {},
		CloseSession: func(context.Context) error { return errors.New("close failed") },
	}}}
	if err := suspendRepositoryActivation(context.Background(), &activeConfig, "repo", &repository); err == nil {
		t.Fatal("expected suspension failure")
	}
}
