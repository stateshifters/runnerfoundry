package github

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestMultiAppCoordinatorRemovalPreservesOwnershipForReAdd(t *testing.T) {
	removed := Registration{GitHubAppID: 22, RepositoryURL: "https://github.com/example/second", RunnerPool: "linux",
		InstallationID: 2, RunnerGroupID: 1, ScaleSetID: 2202}
	store := &coordinatorRepositoryFake{active: []Registration{removed}, owned: []Registration{removed}}
	first := coordinatorManager(t, store, 11, nil)
	withoutRemoved, err := NewMultiAppCoordinator(store, first)
	if err != nil {
		t.Fatalf("create coordinator without removed App: %v", err)
	}
	active, err := withoutRemoved.ActiveRegistrations(context.Background())
	if err != nil || len(active) != 0 || len(store.owned) != 1 {
		t.Fatalf("state after App removal = active %#v owned %#v error %v", active, store.owned, err)
	}
	second := coordinatorManager(t, store, 22,
		[]transport.Repository{{InstallationID: 2, URL: removed.RepositoryURL}})
	withReadded, err := NewMultiAppCoordinator(store, first, second)
	if err != nil {
		t.Fatalf("create coordinator with re-added App: %v", err)
	}
	active, err = withReadded.Reconcile(context.Background(), coordinatorManagerConfig())
	if err != nil || len(active) != 1 || active[0].GitHubAppID != 22 {
		t.Fatalf("state after App re-add = %#v, %v", active, err)
	}
}

func TestMultiAppCoordinatorRejectsInvalidRuntimeComposition(t *testing.T) {
	store := &coordinatorRepositoryFake{}
	unscoped, err := NewManager(coordinatorDiscoverer{}, func(context.Context, transport.Repository) (ManagerClient, error) {
		return ManagerClient{}, nil
	}, store, ManagerObserver{})
	if err != nil {
		t.Fatalf("create unscoped manager: %v", err)
	}
	valid := coordinatorManager(t, store, 11, nil)
	for name, build := range map[string]func() error{
		"missing repository": func() error { _, buildErr := NewMultiAppCoordinator(nil, valid); return buildErr },
		"missing managers":   func() error { _, buildErr := NewMultiAppCoordinator(store); return buildErr },
		"nil manager":        func() error { _, buildErr := NewMultiAppCoordinator(store, nil); return buildErr },
		"unscoped manager":   func() error { _, buildErr := NewMultiAppCoordinator(store, unscoped); return buildErr },
		"duplicate App":      func() error { _, buildErr := NewMultiAppCoordinator(store, valid, valid); return buildErr },
	} {
		t.Run(name, func(t *testing.T) {
			if build() == nil {
				t.Fatal("invalid coordinator was accepted")
			}
		})
	}
}

func TestMultiAppCoordinatorRejectsInvalidReconciliationConfiguration(t *testing.T) {
	store := &coordinatorRepositoryFake{}
	manager := coordinatorManager(t, store, 11, nil)
	coordinator, err := NewMultiAppCoordinator(store, manager)
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	if _, err = coordinator.Reconcile(context.Background(), ManagerConfig{RunnerPools: []RunnerPoolConfig{{
		RunnerPool: "linux",
	}}}); err == nil {
		t.Fatal("invalid reconciliation configuration was accepted")
	}
}

func TestManagerReportsCancellationWhileWaitingForSharedRepositorySlot(t *testing.T) {
	store := &coordinatorRepositoryFake{}
	manager := coordinatorManager(t, store, 11, []transport.Repository{{InstallationID: 1,
		URL: "https://github.com/example/repository"}})
	for index := 0; index < cap(manager.repositorySlots); index++ {
		manager.repositorySlots <- struct{}{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.Reconcile(ctx, coordinatorManagerConfig()); !errors.Is(err, context.Canceled) {
		t.Fatalf("repository-slot cancellation error = %v", err)
	}
}

func TestMultiAppCoordinatorRoutesSessionsByApp(t *testing.T) {
	store := &coordinatorRepositoryFake{}
	valid := coordinatorManager(t, store, 11, nil)
	coordinator, err := NewMultiAppCoordinator(store, valid)
	if err != nil {
		t.Fatalf("create valid coordinator: %v", err)
	}
	if _, err = coordinator.OpenRegistrationSession(context.Background(), Registration{GitHubAppID: 22}, RunnerPoolConfig{}, "", nil); err == nil {
		t.Fatal("unconfigured App session was accepted")
	}
	if _, err = coordinator.OpenRegistrationSession(context.Background(), Registration{GitHubAppID: 11}, RunnerPoolConfig{}, "", nil); err == nil {
		t.Fatal("invalid configured-App session was accepted")
	}
}

func TestMultiAppCoordinatorForwardsDeclarationsAndReportsStateFailures(t *testing.T) {
	store := &coordinatorRepositoryFake{}
	manager := coordinatorManager(t, store, 11, nil)
	coordinator, err := NewMultiAppCoordinator(store, manager)
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	if !coordinator.UsesSharedRunnerPoolDeclarations() {
		t.Fatal("shared declarations were not exposed")
	}
	config := coordinatorManagerConfig()
	if err = coordinator.PublishRunnerPoolDeclarations(context.Background(), "member", config); err != nil {
		t.Fatalf("publish declarations: %v", err)
	}
	snapshot, err := coordinator.ClusterRunnerPools(context.Background(), time.Time{})
	if err != nil || len(snapshot.RunnerPools) != 1 {
		t.Fatalf("declaration snapshot = %#v, %v", snapshot, err)
	}
	store.listErr = errors.New("list unavailable")
	if _, err = coordinator.ActiveRegistrations(context.Background()); !errors.Is(err, store.listErr) {
		t.Fatalf("active registration error = %v", err)
	}
}

func TestMultiAppCoordinatorReportsAbsentAppCleanupFailure(t *testing.T) {
	absent := Registration{GitHubAppID: 22, RepositoryURL: "absent", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 1, ScaleSetID: 1}
	store := &coordinatorRepositoryFake{active: []Registration{absent}, deleteErr: errors.New("delete unavailable")}
	manager := coordinatorManager(t, store, 11, nil)
	coordinator, err := NewMultiAppCoordinator(store, manager)
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	if _, err = coordinator.ActiveRegistrations(context.Background()); !errors.Is(err, store.deleteErr) {
		t.Fatalf("cleanup error = %v", err)
	}
}

func TestMultiAppCoordinatorReportsRouteRefreshFailures(t *testing.T) {
	store := &coordinatorRepositoryFake{active: []Registration{{GitHubAppID: 11, RepositoryURL: "broken", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 1, ScaleSetID: 1}}}
	manager := coordinatorManager(t, store, 11, nil)
	coordinator, err := NewMultiAppCoordinator(store, manager)
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	manager.factory = func(context.Context, transport.Repository) (ManagerClient, error) {
		return ManagerClient{}, errors.New("client unavailable")
	}
	if err = coordinator.RefreshMembers(context.Background(), coordinatorManagerConfig()); err == nil {
		t.Fatal("route client failure was ignored")
	}
	manager.factory = coordinatorClientFactory(11)
	store.active[0].ScaleSetID = 0
	if err = coordinator.RefreshMembers(context.Background(), coordinatorManagerConfig()); err == nil {
		t.Fatal("invalid route replacement was ignored")
	}
	store.listErr = errors.New("list unavailable")
	if err = coordinator.RefreshMembers(context.Background(), coordinatorManagerConfig()); !errors.Is(err, store.listErr) {
		t.Fatalf("route list error = %v", err)
	}
}

func coordinatorManager(t *testing.T, store RegistrationRepository, appID AppID,
	repositories []transport.Repository,
) *Manager {
	t.Helper()
	scoped, err := ScopeRegistrationRepository(store, appID)
	if err != nil {
		t.Fatalf("scope registrations: %v", err)
	}
	manager, err := NewManager(coordinatorDiscoverer{repositories: repositories}, coordinatorClientFactory(appID), scoped, ManagerObserver{})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	return manager
}

func coordinatorClientFactory(appID AppID) ManagerClientFactoryFunc {
	return func(_ context.Context, repository transport.Repository) (ManagerClient, error) {
		return ManagerClient{EnsureRunnerPool: func(context.Context, string, string) (transport.Registration, error) {
			return transport.Registration{ID: transport.ScaleSetID(int64(appID)*100 + int64(repository.InstallationID)), RunnerGroupID: 1}, nil
		}, GenerateBootstrap: func(context.Context, transport.Registration, string) ([]byte, error) {
			return []byte(fmt.Sprintf("app-%d", appID)), nil
		}}, nil
	}
}

func coordinatorManagerConfig() ManagerConfig {
	return ManagerConfig{RunnerPools: []RunnerPoolConfig{{RunnerPool: "linux", RunnerGroup: "default"}}}
}

type coordinatorDiscoverer struct {
	repositories []transport.Repository
	err          error
}

func (discoverer coordinatorDiscoverer) Discover(context.Context) (transport.DiscoveryResult, error) {
	return transport.DiscoveryResult{Repositories: discoverer.repositories}, discoverer.err
}

type coordinatorRepositoryFake struct {
	mutex        sync.Mutex
	active       []Registration
	owned        []Registration
	declarations []MemberRunnerPoolDeclaration
	listErr      error
	deleteErr    error
}

func (repository *coordinatorRepositoryFake) Registrations(context.Context) ([]Registration, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	return append([]Registration(nil), repository.active...), repository.listErr
}
func (repository *coordinatorRepositoryFake) OwnedRegistrations(context.Context) ([]Registration, error) {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	return append([]Registration(nil), repository.owned...), repository.listErr
}
func (repository *coordinatorRepositoryFake) UpsertRegistration(_ context.Context, record Registration) error {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	repository.active = coordinatorUpsert(repository.active, record, func(candidate Registration) bool {
		return candidate.RepositoryURL == record.RepositoryURL && candidate.RunnerPool == record.RunnerPool
	})
	repository.owned = coordinatorUpsert(repository.owned, record, func(candidate Registration) bool {
		return candidate.RepositoryURL == record.RepositoryURL && candidate.ScaleSetID == record.ScaleSetID
	})
	return nil
}
func (repository *coordinatorRepositoryFake) DeleteActiveRegistration(_ context.Context, record Registration) error {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	repository.active = coordinatorDelete(repository.active, record)
	return repository.deleteErr
}
func (repository *coordinatorRepositoryFake) DeleteOwnedRegistration(_ context.Context, record Registration) error {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	repository.owned = coordinatorDelete(repository.owned, record)
	return repository.deleteErr
}
func (repository *coordinatorRepositoryFake) DeleteRegistrationState(ctx context.Context, record Registration) error {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	repository.active = coordinatorDelete(repository.active, record)
	repository.owned = coordinatorDelete(repository.owned, record)
	return repository.deleteErr
}

func (repository *coordinatorRepositoryFake) ReplaceGitHubRunnerPoolDeclarations(
	_ context.Context,
	memberID core.MemberID,
	declarations []RunnerPoolDeclaration,
) error {
	repository.declarations = nil
	for _, declaration := range declarations {
		repository.declarations = append(repository.declarations, MemberRunnerPoolDeclaration{
			MemberID: memberID, RunnerPool: declaration.RunnerPool, RunnerGroup: declaration.RunnerGroup,
		})
	}
	return nil
}

func (repository *coordinatorRepositoryFake) LiveGitHubRunnerPoolDeclarations(
	context.Context,
	time.Time,
) ([]MemberRunnerPoolDeclaration, error) {
	return repository.declarations, nil
}

func coordinatorUpsert(records []Registration, record Registration, matches func(Registration) bool) []Registration {
	for index, candidate := range records {
		if matches(candidate) {
			records[index] = record
			return records
		}
	}
	return append(records, record)
}

func coordinatorDelete(records []Registration, record Registration) []Registration {
	retained := records[:0]
	for _, candidate := range records {
		matches := candidate == record
		if matches {
			continue
		}
		retained = append(retained, candidate)
	}
	return retained
}
