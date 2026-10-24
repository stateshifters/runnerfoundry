package supervisor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestGitHubSourceRuntimeReservesSessionAdmissionForEachApp(t *testing.T) {
	records := make([]githubsource.Registration, 0, githubRegistrationSessionConcurrency+1)
	for index := 0; index < githubRegistrationSessionConcurrency; index++ {
		records = append(records, githubsource.Registration{GitHubAppID: 11,
			RepositoryURL: fmt.Sprintf("https://github.example/a-%02d", index), RunnerPool: "linux",
			InstallationID: 1, RunnerGroupID: 1, ScaleSetID: transport.ScaleSetID(index + 1)})
	}
	records = append(records, githubsource.Registration{GitHubAppID: 22,
		RepositoryURL: "https://github.example/z-healthy", RunnerPool: "linux",
		InstallationID: 2, RunnerGroupID: 1, ScaleSetID: 100})
	manager := &multiAppSessionAdmissionManager{records: records, healthyStarted: make(chan struct{}, 1)}
	source := newGitHubSourceRuntime(manager, &sourceCapacityProviderFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Now)
	source.sessionOpenTimeout = time.Second

	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open leader: %v", err)
	}
	select {
	case <-manager.healthyStarted:
	case <-time.After(time.Second):
		t.Fatal("healthy App could not acquire a session slot while another App was blocked")
	}
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimeActivatesHealthyAppFromDegradedCoordinatorPass(t *testing.T) {
	wantErr := errors.New("first App discovery failed")
	repository := &multiAppRuntimeRepository{}
	first := newMultiAppRuntimeManager(t, repository, 11, multiAppRuntimeDiscoverer{err: wantErr})
	second := newMultiAppRuntimeManager(t, repository, 22, multiAppRuntimeDiscoverer{repositories: []transport.Repository{{
		InstallationID: 2, URL: "https://github.example/healthy",
	}}})
	coordinator, err := githubsource.NewMultiAppCoordinator(repository, first, second)
	if err != nil {
		t.Fatalf("create multi-App coordinator: %v", err)
	}
	configured := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
		RunnerPool: "linux", RunnerGroup: "default",
	}}}
	source := newGitHubSourceRuntime(coordinator, &sourceCapacityProviderFake{}, "member", eventHandlerFake{},
		configured, time.Now)

	if err = source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open multi-App source leader: %v", err)
	}
	awaitGitHubDiscoveryError(t, source, wantErr)
	awaitInstalledGitHubAdapters(t, source, 1)
	if len(source.desired) != 1 {
		t.Fatalf("healthy App desired routes = %#v, want one", source.desired)
	}
	mustCloseGitHubSource(t, source)
}

type multiAppSessionAdmissionManager struct {
	records        []githubsource.Registration
	healthyStarted chan struct{}
}

func newMultiAppRuntimeManager(
	t *testing.T,
	repository githubsource.RegistrationRepository,
	appID githubsource.AppID,
	discoverer multiAppRuntimeDiscoverer,
) *githubsource.Manager {
	t.Helper()
	scoped, err := githubsource.ScopeRegistrationRepository(repository, appID)
	if err != nil {
		t.Fatalf("scope App %d registration state: %v", appID, err)
	}
	manager, err := githubsource.NewManager(discoverer, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{
			EnsureRunnerPool: func(context.Context, string, string) (transport.Registration, error) {
				return transport.Registration{ID: transport.ScaleSetID(appID), RunnerGroupID: 1}, nil
			},
			OpenSession: func(context.Context, transport.Registration, string) (githubsource.ManagedSession, error) {
				return &termSessionFake{}, nil
			},
			ResolveRunnerGroup: func(context.Context, string) (transport.RunnerGroupID, error) { return 1, nil },
		}, nil
	}, scoped, githubsource.ManagerObserver{})
	if err != nil {
		t.Fatalf("create App %d manager: %v", appID, err)
	}
	return manager
}

type multiAppRuntimeDiscoverer struct {
	repositories []transport.Repository
	err          error
}

func (discoverer multiAppRuntimeDiscoverer) Discover(context.Context) (transport.DiscoveryResult, error) {
	return transport.DiscoveryResult{Repositories: discoverer.repositories}, discoverer.err
}

type multiAppRuntimeRepository struct {
	mutex  sync.Mutex
	active []githubsource.Registration
	owned  []githubsource.Registration
}

func (repository *multiAppRuntimeRepository) Registrations(context.Context) ([]githubsource.Registration, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	return append([]githubsource.Registration(nil), repository.active...), nil
}

func (repository *multiAppRuntimeRepository) OwnedRegistrations(context.Context) ([]githubsource.Registration, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	return append([]githubsource.Registration(nil), repository.owned...), nil
}

func (repository *multiAppRuntimeRepository) UpsertRegistration(
	_ context.Context,
	record githubsource.Registration,
) error {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	repository.active = upsertMultiAppRuntimeActiveRecord(repository.active, record)
	repository.owned = upsertMultiAppRuntimeOwnershipRecord(repository.owned, record)
	return nil
}

func (repository *multiAppRuntimeRepository) DeleteActiveRegistration(
	_ context.Context,
	record githubsource.Registration,
) error {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	repository.active = deleteMultiAppRuntimeRecord(repository.active, record)
	return nil
}

func (repository *multiAppRuntimeRepository) DeleteOwnedRegistration(
	_ context.Context,
	record githubsource.Registration,
) error {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	repository.owned = deleteMultiAppRuntimeRecord(repository.owned, record)
	return nil
}

func (repository *multiAppRuntimeRepository) DeleteRegistrationState(
	ctx context.Context,
	record githubsource.Registration,
) error {
	if err := repository.DeleteActiveRegistration(ctx, record); err != nil {
		return err
	}
	return repository.DeleteOwnedRegistration(ctx, record)
}

func upsertMultiAppRuntimeActiveRecord(
	records []githubsource.Registration,
	record githubsource.Registration,
) []githubsource.Registration {
	return upsertMultiAppRuntimeRecord(records, record, func(candidate githubsource.Registration) bool {
		return candidate.RepositoryURL == record.RepositoryURL && candidate.RunnerPool == record.RunnerPool
	})
}

func upsertMultiAppRuntimeOwnershipRecord(
	records []githubsource.Registration,
	record githubsource.Registration,
) []githubsource.Registration {
	return upsertMultiAppRuntimeRecord(records, record, func(candidate githubsource.Registration) bool {
		return candidate.RepositoryURL == record.RepositoryURL && candidate.ScaleSetID == record.ScaleSetID
	})
}

func upsertMultiAppRuntimeRecord(
	records []githubsource.Registration,
	record githubsource.Registration,
	matches func(githubsource.Registration) bool,
) []githubsource.Registration {
	for index, candidate := range records {
		if matches(candidate) {
			records[index] = record
			return records
		}
	}
	return append(records, record)
}

func deleteMultiAppRuntimeRecord(
	records []githubsource.Registration,
	record githubsource.Registration,
) []githubsource.Registration {
	retained := records[:0]
	for _, candidate := range records {
		if candidate != record {
			retained = append(retained, candidate)
		}
	}
	return retained
}

func (manager *multiAppSessionAdmissionManager) GitHubAppIDs() []githubsource.AppID {
	return []githubsource.AppID{11, 22}
}

func (manager *multiAppSessionAdmissionManager) ActiveRegistrations(context.Context) ([]githubsource.Registration, error) {
	return append([]githubsource.Registration(nil), manager.records...), nil
}

func (manager *multiAppSessionAdmissionManager) Reconcile(
	context.Context,
	githubsource.ManagerConfig,
) ([]githubsource.Registration, error) {
	return append([]githubsource.Registration(nil), manager.records...), nil
}

func (*multiAppSessionAdmissionManager) RefreshMembers(context.Context, githubsource.ManagerConfig) error {
	return nil
}

func (manager *multiAppSessionAdmissionManager) OpenSourceRegistrationSession(
	ctx context.Context,
	record githubsource.Registration,
	_ githubsource.RunnerPoolConfig,
	_ string,
	_ core.SourceEventHandler,
	_ githubsource.AssignedDemandHandler,
) (*githubsource.RegistrationSession, error) {
	if record.GitHubAppID == 22 {
		manager.healthyStarted <- struct{}{}
		return nil, errors.New("healthy App test session completed")
	}
	<-ctx.Done()
	return nil, fmt.Errorf("blocked App session: %w", ctx.Err())
}
