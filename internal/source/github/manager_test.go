package github_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestManagerRetainsUndiscoveredRepositoryOwnership(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/missing", 17)
	repository := &snapshotRepositoryFake{snapshot: githubsource.RegistrationSnapshot{
		Active: []githubsource.Registration{record}, Owned: []githubsource.Registration{record},
	}}
	manager := newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client, repository)
	if _, err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{Prune: true}); err != nil {
		t.Fatalf("reconcile undiscovered repository: %v", err)
	}
	if len(repository.snapshot.Active) != 0 || len(repository.snapshot.Owned) != 1 || repository.snapshot.Owned[0] != record {
		t.Fatalf("snapshot = %#v, want inactive retained ownership", repository.snapshot)
	}
}

func TestManagerPreservesSnapshotAfterPartialPruneFailureAndRetries(t *testing.T) {
	t.Parallel()

	fixture := newPartialPruneRetryFixture(t)
	_, err := fixture.manager.Reconcile(context.Background(), githubsource.ManagerConfig{Prune: true})
	assertPartialPruneFailure(t, fixture, err)
	if _, err = fixture.manager.Reconcile(context.Background(), githubsource.ManagerConfig{Prune: true}); err != nil {
		t.Fatalf("retry prune: %v", err)
	}
	if len(fixture.repository.snapshot.Owned) != 0 {
		t.Fatalf("retry snapshot = %#v", fixture.repository.snapshot)
	}
}

type partialPruneRetryFixture struct {
	manager    *githubsource.Manager
	repository *snapshotRepositoryFake
	previous   githubsource.RegistrationSnapshot
	wantErr    error
}

func newPartialPruneRetryFixture(t *testing.T) partialPruneRetryFixture {
	t.Helper()
	first := managerRecord("https://github.com/octo/repo", 11)
	first.RunnerGroupID = 17
	second := first
	second.RunnerPool = "windows"
	second.ScaleSetID = 12
	previous := githubsource.RegistrationSnapshot{Active: []githubsource.Registration{first}, Owned: []githubsource.Registration{first, second}}
	repository := &snapshotRepositoryFake{snapshot: previous}
	discoverer := &managerDiscovererFake{repositories: []transport.Repository{{InstallationID: 1, URL: first.RepositoryURL}}}
	factory := newManagerFactoryFake(discoverer.repositories)
	wantErr := errors.New("partial delete failed")
	attempt := 0
	factory.clients[first.RepositoryURL].pruneFunc = func(_ transport.RunnerGroupID, _ []transport.OwnedRunnerPool, owned []transport.OwnedRunnerPool) (transport.PruneResult, error) {
		attempt++
		if attempt == 1 {
			return transport.PruneResult{Removed: owned[:1]}, wantErr
		}
		return transport.PruneResult{Removed: owned}, nil
	}
	manager := newManagerFixture(t, discoverer, factory.Client, repository)
	return partialPruneRetryFixture{manager: manager, repository: repository, previous: previous, wantErr: wantErr}
}

func assertPartialPruneFailure(t *testing.T, fixture partialPruneRetryFixture, err error) {
	t.Helper()
	if !errors.Is(err, fixture.wantErr) {
		t.Fatalf("first reconcile error = %v, want %v", err, fixture.wantErr)
	}
	if len(fixture.repository.snapshot.Active) != 0 ||
		!reflect.DeepEqual(fixture.repository.snapshot.Owned, fixture.previous.Owned) {
		t.Fatalf("registration state after partial failure: %#v", fixture.repository.snapshot)
	}
}

func TestManagerReportsPruneClientCreationFailure(t *testing.T) {
	t.Parallel()

	repositoryURL := "https://github.com/octo/repo"
	discoverer := &managerDiscovererFake{repositories: []transport.Repository{{InstallationID: 1, URL: repositoryURL}}}
	wantErr := errors.New("client failed")
	manager := newManagerFixture(t, discoverer, func(context.Context, transport.Repository) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{}, wantErr
	}, &snapshotRepositoryFake{})
	if _, err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{Prune: true}); !errors.Is(err, wantErr) {
		t.Fatalf("reconcile error = %v, want %v", err, wantErr)
	}
}

type snapshotRepositoryFake struct {
	snapshot     githubsource.RegistrationSnapshot
	replaceCalls int
}

func (repository *snapshotRepositoryFake) RegistrationSnapshot(context.Context) (
	githubsource.RegistrationSnapshot,
	error,
) {
	return repository.snapshot, nil
}

func (repository *snapshotRepositoryFake) ReplaceRegistrationSnapshot(
	_ context.Context,
	snapshot githubsource.RegistrationSnapshot,
) error {
	repository.replaceCalls++
	repository.snapshot = snapshot
	return nil
}

func (repository *snapshotRepositoryFake) Registrations(context.Context) ([]githubsource.Registration, error) {
	return append([]githubsource.Registration(nil), repository.snapshot.Active...), nil
}

func (repository *snapshotRepositoryFake) OwnedRegistrations(context.Context) ([]githubsource.Registration, error) {
	return append([]githubsource.Registration(nil), repository.snapshot.Owned...), nil
}

func (repository *snapshotRepositoryFake) UpsertRegistration(
	_ context.Context,
	record githubsource.Registration,
) error {
	repository.snapshot.Active = upsertActiveRecord(repository.snapshot.Active, record)
	repository.snapshot.Owned = upsertOwnedRecord(repository.snapshot.Owned, record)
	return nil
}

func (repository *snapshotRepositoryFake) DeleteActiveRegistration(
	_ context.Context,
	record githubsource.Registration,
) error {
	repository.snapshot.Active = removeExactRecord(repository.snapshot.Active, record)
	return nil
}

func (repository *snapshotRepositoryFake) DeleteOwnedRegistration(
	_ context.Context,
	record githubsource.Registration,
) error {
	repository.snapshot.Owned = removeExactRecord(repository.snapshot.Owned, record)
	return nil
}

func (repository *snapshotRepositoryFake) DeleteRegistrationState(
	ctx context.Context,
	record githubsource.Registration,
) error {
	if err := repository.DeleteActiveRegistration(ctx, record); err != nil {
		return err
	}
	return repository.DeleteOwnedRegistration(ctx, record)
}

func (repository *snapshotRepositoryFake) DeleteRepositoryRegistrationState(
	_ context.Context,
	repositoryURL string,
	installationID transport.InstallationID,
) error {
	repository.snapshot.Active = removeRepositoryRecords(repository.snapshot.Active, repositoryURL, installationID)
	repository.snapshot.Owned = removeRepositoryRecords(repository.snapshot.Owned, repositoryURL, installationID)
	return nil
}

func TestManagerRefreshesMemberRoutes(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &registrationRepositoryFake{records: []githubsource.Registration{record}}
	transportRepository := managerTransportRepository(record)
	factory := newManagerFactoryFake([]transport.Repository{transportRepository})
	factory.clients[record.RepositoryURL].bootstrap = []byte("opaque")
	manager := newManagerFixture(t, &managerDiscovererFake{}, factory.Client, repository)

	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); err != nil {
		t.Fatalf("refresh members: %v", err)
	}
	job := sourceJob(t, record.RepositoryURL, record.RunnerPool, 61)
	if _, err := manager.Source().GenerateBootstrap(context.Background(), job, "runner-61"); err != nil {
		t.Fatalf("generate bootstrap: %v", err)
	}
	if err := manager.Source().CleanupBootstrap(context.Background(), job, "runner-61"); err != nil {
		t.Fatalf("cleanup bootstrap: %v", err)
	}
}

func TestManagerSharesRepositoryClientAcrossRunnerPools(t *testing.T) {
	t.Parallel()

	linux := managerRecord("https://github.com/octo/repo", 17)
	windows := linux
	windows.RunnerPool = "windows"
	windows.ScaleSetID = 18
	repository := &registrationRepositoryFake{records: []githubsource.Registration{linux, windows}}
	factoryCalls := 0
	manager := newManagerFixture(t, &managerDiscovererFake{}, func(
		context.Context, transport.Repository,
	) (githubsource.ManagerClient, error) {
		factoryCalls++
		return githubsource.ManagerClient{}, nil
	}, repository)
	config := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{
		{RunnerPool: "linux", RunnerGroup: "default"},
		{RunnerPool: "windows", RunnerGroup: "default"},
	}}

	if err := manager.RefreshMembers(context.Background(), config); err != nil {
		t.Fatalf("RefreshMembers() error = %v", err)
	}
	if factoryCalls != 1 {
		t.Fatalf("repository client constructions = %d, want one", factoryCalls)
	}
}

func TestManagerReturnsDefensiveCommittedRegistrationSnapshot(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &snapshotRepositoryFake{snapshot: githubsource.RegistrationSnapshot{
		Active: []githubsource.Registration{record}, Owned: []githubsource.Registration{record},
	}}
	manager := newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client, repository)

	snapshot, err := manager.RegistrationSnapshot(context.Background())
	if err != nil {
		t.Fatalf("RegistrationSnapshot() error = %v", err)
	}
	snapshot.Active[0] = githubsource.Registration{}
	if repository.snapshot.Active[0] != record {
		t.Fatal("RegistrationSnapshot() exposed repository-owned state")
	}
}

func TestManagerReturnsCommittedRegistrationsInCanonicalOrder(t *testing.T) {
	t.Parallel()

	first := managerRecord("https://github.com/octo/first", 17)
	second := managerRecord("https://github.com/octo/second", 18)
	repository := &snapshotRepositoryFake{snapshot: githubsource.RegistrationSnapshot{
		Active: []githubsource.Registration{second, first},
		Owned:  []githubsource.Registration{second, first},
	}}
	manager := newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client, repository)

	snapshot, err := manager.RegistrationSnapshot(context.Background())
	if err != nil {
		t.Fatalf("RegistrationSnapshot() error = %v", err)
	}
	want := []githubsource.Registration{first, second}
	if !reflect.DeepEqual(snapshot.Active, want) || !reflect.DeepEqual(snapshot.Owned, want) {
		t.Fatalf("RegistrationSnapshot() = %#v, want canonical %#v", snapshot, want)
	}
}

func TestManagerRejectsDuplicateRepositoryRunnerPoolRegistrations(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repository", 17)
	repository := &snapshotRepositoryFake{snapshot: githubsource.RegistrationSnapshot{
		Active: []githubsource.Registration{record, record},
	}}
	manager := newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client, repository)

	_, err := manager.RegistrationSnapshot(context.Background())
	if err == nil || !strings.Contains(err.Error(), "duplicate GitHub registration") {
		t.Fatalf("RegistrationSnapshot() error = %v, want duplicate registration rejection", err)
	}
}

func TestManagerRejectsDuplicateOwnershipIdentityDespiteOtherFactDifferences(t *testing.T) {
	t.Parallel()

	base := managerRecord("https://github.com/octo/repository", 17)
	byScaleSet := base
	byScaleSet.ScaleSetID++
	byPool := base
	byPool.RunnerPool = "windows"
	byInstallation := base
	byInstallation.InstallationID++
	byRunnerGroup := base
	byRunnerGroup.RunnerGroupID++
	byRepository := base
	byRepository.RepositoryURL += "-second"
	repository := &snapshotRepositoryFake{snapshot: githubsource.RegistrationSnapshot{Owned: []githubsource.Registration{
		byRepository, byRunnerGroup, byInstallation, byPool, byScaleSet, base,
	}}}
	manager := newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client, repository)

	_, err := manager.RegistrationSnapshot(context.Background())
	if err == nil || !strings.Contains(err.Error(), "duplicate GitHub ownership") {
		t.Fatalf("RegistrationSnapshot() error = %v, want duplicate ownership rejection", err)
	}
}

func TestManagerRejectsDuplicateDiscoveredRepositoryBeforePersistence(t *testing.T) {
	t.Parallel()

	repository := transport.Repository{InstallationID: 1, URL: "https://github.com/octo/repository"}
	manager := newManagerFixture(t,
		&managerDiscovererFake{repositories: []transport.Repository{repository, repository}},
		func(context.Context, transport.Repository) (githubsource.ManagerClient, error) {
			return githubsource.ManagerClient{EnsureRunnerPool: func(
				context.Context, string, string,
			) (transport.Registration, error) {
				return transport.Registration{ID: 17, RunnerGroupID: 11}, nil
			}}, nil
		}, &registrationRepositoryFake{})

	_, err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
		RunnerPool: "linux", RunnerGroup: "default",
	}}})
	if err == nil || !strings.Contains(err.Error(), "duplicate GitHub registration") {
		t.Fatalf("Reconcile() error = %v, want duplicate registration rejection", err)
	}
}

func TestManagerReportsMemberRouteReplacement(t *testing.T) {
	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &registrationRepositoryFake{records: []githubsource.Registration{record}}
	transportRepository := managerTransportRepository(record)
	factory := newManagerFactoryFake([]transport.Repository{transportRepository})
	manager, activities := newObservedManager(t, &managerDiscovererFake{}, factory.Client, repository)
	if err := manager.RefreshMembers(context.Background(), managerConfigForTests()); err != nil {
		t.Fatalf("refresh members: %v", err)
	}
	completed := findManagerActivity(*activities, "replace_routes", "completed")
	route := findManagerActivity(*activities, "configure_route", "completed")
	if completed.Count != 1 {
		t.Fatalf("completed route activity = %#v", completed)
	}
	if route.Repository != record.RepositoryURL || route.RunnerPool != record.RunnerPool {
		t.Fatalf("debug route activity = %#v", route)
	}
}

func managerTransportRepository(record githubsource.Registration) transport.Repository {
	return transport.Repository{
		InstallationID: record.InstallationID, URL: record.RepositoryURL,
	}
}

func newManagerFixture(
	t *testing.T,
	discoverer githubsource.RepositoryDiscoverer,
	factory githubsource.ManagerClientFactoryFunc,
	repository githubsource.RegistrationRepository,
) *githubsource.Manager {
	t.Helper()
	manager, err := githubsource.NewManager(discoverer, factory, repository, githubsource.ManagerObserver{})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	return manager
}

func managerRecord(repositoryURL string, registrationID int) githubsource.Registration {
	return githubsource.Registration{
		RepositoryURL: repositoryURL, InstallationID: 1, RunnerPool: core.RunnerPool("linux"),
		RunnerGroupID: 1, ScaleSetID: transport.ScaleSetID(registrationID),
	}
}

func managerConfigForTests() githubsource.ManagerConfig {
	return githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{RunnerPool: "linux", RunnerGroup: "default"}}}
}

type managerDiscovererFake struct {
	repositories         []transport.Repository
	installationFailures map[transport.InstallationID]error
	err                  error
}

func (discoverer *managerDiscovererFake) Discover(context.Context) (transport.DiscoveryResult, error) {
	return transport.DiscoveryResult{
		Repositories: discoverer.repositories, InstallationFailures: discoverer.installationFailures,
	}, discoverer.err
}

type registrationRepositoryFake struct {
	mutex          sync.Mutex
	records        []githubsource.Registration
	owned          []githubsource.Registration
	listErr        error
	ownedErr       error
	upsertErr      error
	deleteErr      error
	declarations   []githubsource.MemberRunnerPoolDeclaration
	declarationErr error
}

func (repository *registrationRepositoryFake) ReplaceGitHubRunnerPoolDeclarations(
	_ context.Context,
	memberID core.MemberID,
	declarations []githubsource.RunnerPoolDeclaration,
) error {
	if repository.declarationErr != nil {
		return repository.declarationErr
	}
	retained := repository.declarations[:0]
	for _, declaration := range repository.declarations {
		if declaration.MemberID != memberID {
			retained = append(retained, declaration)
		}
	}
	for _, declaration := range declarations {
		retained = append(retained, githubsource.MemberRunnerPoolDeclaration{MemberID: memberID,
			RunnerPool: declaration.RunnerPool, RunnerGroup: declaration.RunnerGroup})
	}
	repository.declarations = retained
	return nil
}

func (repository *registrationRepositoryFake) LiveGitHubRunnerPoolDeclarations(
	context.Context,
	time.Time,
) ([]githubsource.MemberRunnerPoolDeclaration, error) {
	return append([]githubsource.MemberRunnerPoolDeclaration(nil), repository.declarations...), repository.declarationErr
}

func (repository *registrationRepositoryFake) ReplaceRegistrations(_ context.Context, records []githubsource.Registration) error {
	if repository.listErr != nil {
		return repository.listErr
	}
	if repository.upsertErr != nil {
		return repository.upsertErr
	}
	repository.records = append([]githubsource.Registration(nil), records...)
	return repository.deleteErr
}

func (repository *registrationRepositoryFake) ReplaceRegistrationSnapshot(
	ctx context.Context,
	snapshot githubsource.RegistrationSnapshot,
) error {
	return repository.ReplaceRegistrations(ctx, snapshot.Active)
}

func (repository *registrationRepositoryFake) RegistrationSnapshot(context.Context) (
	githubsource.RegistrationSnapshot,
	error,
) {
	records := append([]githubsource.Registration(nil), repository.records...)
	return githubsource.RegistrationSnapshot{Active: records, Owned: append([]githubsource.Registration(nil), records...)}, repository.listErr
}

func (repository *registrationRepositoryFake) UpsertRegistration(
	_ context.Context,
	record githubsource.Registration,
) error {
	repository.mutex.Lock()
	defer repository.mutex.Unlock()
	if repository.upsertErr != nil {
		return repository.upsertErr
	}
	repository.records = upsertActiveRecord(repository.records, record)
	repository.owned = upsertOwnedRecord(repository.owned, record)
	return nil
}

func (repository *registrationRepositoryFake) Registration(
	_ context.Context,
	repositoryURL string,
	runnerPool core.RunnerPool,
) (githubsource.Registration, bool, error) {
	for _, record := range repository.records {
		if record.RepositoryURL == repositoryURL && record.RunnerPool == runnerPool {
			return record, true, repository.listErr
		}
	}
	return githubsource.Registration{}, false, repository.listErr
}

func (repository *registrationRepositoryFake) Registrations(context.Context) ([]githubsource.Registration, error) {
	return append([]githubsource.Registration(nil), repository.records...), repository.listErr
}

func (repository *registrationRepositoryFake) OwnedRegistrations(context.Context) ([]githubsource.Registration, error) {
	owned := repository.owned
	if owned == nil {
		owned = repository.records
	}
	return append([]githubsource.Registration(nil), owned...), repository.ownedErr
}

func (repository *registrationRepositoryFake) DeleteActiveRegistration(
	_ context.Context,
	record githubsource.Registration,
) error {
	repository.records = removeExactRecord(repository.records, record)
	return repository.deleteErr
}

func (repository *registrationRepositoryFake) DeleteOwnedRegistration(
	_ context.Context,
	record githubsource.Registration,
) error {
	repository.owned = removeExactRecord(repository.owned, record)
	return repository.deleteErr
}

func (repository *registrationRepositoryFake) DeleteRegistrationState(
	ctx context.Context,
	record githubsource.Registration,
) error {
	if err := repository.DeleteActiveRegistration(ctx, record); err != nil {
		return err
	}
	return repository.DeleteOwnedRegistration(ctx, record)
}

func (repository *registrationRepositoryFake) DeleteRepositoryRegistrationState(
	_ context.Context,
	repositoryURL string,
	installationID transport.InstallationID,
) error {
	repository.records = removeRepositoryRecords(repository.records, repositoryURL, installationID)
	repository.owned = removeRepositoryRecords(repository.owned, repositoryURL, installationID)
	return repository.deleteErr
}

func (repository *registrationRepositoryFake) DeleteRegistration(
	_ context.Context,
	repositoryURL string,
	runnerPool core.RunnerPool,
) error {
	for index, record := range repository.records {
		if record.RepositoryURL == repositoryURL && record.RunnerPool == runnerPool {
			repository.records = append(repository.records[:index], repository.records[index+1:]...)
			break
		}
	}
	return repository.deleteErr
}

type managerFactoryFake struct {
	clients map[string]*managerClientFake
	err     error
}

func newManagerFactoryFake(repositories []transport.Repository) *managerFactoryFake {
	factory := &managerFactoryFake{clients: make(map[string]*managerClientFake, len(repositories))}
	for index, repository := range repositories {
		factory.clients[repository.URL] = &managerClientFake{nextID: index + 11}
	}
	return factory
}

func (factory *managerFactoryFake) Client(
	_ context.Context,
	repository transport.Repository,
) (githubsource.ManagerClient, error) {
	if factory.err != nil {
		return githubsource.ManagerClient{}, factory.err
	}
	client := factory.clients[repository.URL]
	if client == nil {
		return githubsource.ManagerClient{}, errors.New("client not found")
	}
	return githubsource.ManagerClient{
		EnsureRunnerPool: client.EnsureRunnerPool,
		ResolveRunnerGroup: func(context.Context, string) (transport.RunnerGroupID, error) {
			runnerGroupID := client.runnerGroupID
			if runnerGroupID == 0 {
				runnerGroupID = 1
			}
			return transport.RunnerGroupID(runnerGroupID), nil
		},
		OpenSession: func(context.Context, transport.Registration, string) (githubsource.ManagedSession, error) {
			if client.session == nil {
				return nil, client.openErr
			}
			return client.session, client.openErr
		},
		GenerateBootstrap: client.GenerateBootstrap,
		CleanupRunner:     func(context.Context, string) error { return nil },
		PruneRunnerPools:  client.PruneRunnerPools,
	}, nil
}

type managerClientFake struct {
	nextID        int
	ensured       []string
	prunes        int
	bootstrap     []byte
	session       *managerSessionFake
	ensureErr     error
	pruneErr      error
	openErr       error
	bootstrapErr  error
	runnerGroupID int
	pruneFunc     func(transport.RunnerGroupID, []transport.OwnedRunnerPool, []transport.OwnedRunnerPool) (transport.PruneResult, error)
}

func (client *managerClientFake) EnsureRunnerPool(
	_ context.Context,
	runnerPoolName string,
	_ string,
) (transport.Registration, error) {
	client.ensured = append(client.ensured, runnerPoolName)
	runnerGroupID := client.runnerGroupID
	if runnerGroupID == 0 {
		runnerGroupID = 1
	}
	return transport.Registration{ID: transport.ScaleSetID(client.nextID), Name: runnerPoolName,
		RunnerGroupID: transport.RunnerGroupID(runnerGroupID)}, client.ensureErr
}

func (client *managerClientFake) GenerateBootstrap(
	context.Context,
	transport.Registration,
	string,
) ([]byte, error) {
	return client.bootstrap, client.bootstrapErr
}

func (client *managerClientFake) PruneRunnerPools(
	_ context.Context,
	runnerGroupID transport.RunnerGroupID,
	current []transport.OwnedRunnerPool,
	owned []transport.OwnedRunnerPool,
) (transport.PruneResult, error) {
	client.prunes++
	if client.pruneFunc != nil {
		return client.pruneFunc(runnerGroupID, current, owned)
	}
	return transport.PruneResult{}, client.pruneErr
}

type managerSessionFake struct {
	key      transport.SessionKey
	messages []*upstream.RunnerScaleSetMessage
	closed   bool
	closes   int
	closeErr error
}

func (session *managerSessionFake) Key() transport.SessionKey { return session.key }

func (session *managerSessionFake) GetMessage(context.Context, int, int) (*upstream.RunnerScaleSetMessage, error) {
	if len(session.messages) == 0 {
		return nil, githubsource.ErrNoMessage
	}
	message := session.messages[0]
	session.messages = session.messages[1:]
	return message, nil
}

func (*managerSessionFake) DeleteMessage(context.Context, int) error { return nil }
func (*managerSessionFake) AcquireRequests(context.Context, []int64) ([]int64, error) {
	return nil, nil
}
func (session *managerSessionFake) Close(context.Context) error {
	session.closes++
	session.closed = true
	return session.closeErr
}
