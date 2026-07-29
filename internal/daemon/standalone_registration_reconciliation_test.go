package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

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
	if registry.createCalls["linux"] != 1 || registry.createCalls["windows"] != 2 || registry.listCalls != 0 {
		t.Fatalf("create calls=%#v registration list calls=%d", registry.createCalls, registry.listCalls)
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
	fixture.registry.getErr = errors.New("audit timeout")
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
	getCalls      map[string]int
	listCalls     int
	failWindows   bool
	getErr        error
	listErr       error
	nextID        int
}

func newRegistrationReconciliationClient() *registrationReconciliationClient {
	client := &registrationReconciliationClient{
		registrations: map[string]*upstream.RunnerScaleSet{}, createCalls: map[string]int{}, getCalls: map[string]int{},
		failWindows: true, nextID: 1,
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
	client.getCalls[name]++
	if client.getErr != nil {
		return nil, client.getErr
	}
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
