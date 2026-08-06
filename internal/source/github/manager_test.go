package github_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestManagerReconcilesRegistrationsAndPrunes(t *testing.T) {
	t.Parallel()

	discoverer := &managerDiscovererFake{repositories: []transport.Repository{
		{InstallationID: 1, URL: "https://github.com/octo/first"},
		{InstallationID: 2, URL: "https://github.com/octo/second"},
	}}
	factory := newManagerFactoryFake(discoverer.repositories)
	repository := &registrationRepositoryFake{}
	manager := newManagerFixture(t, discoverer, factory.Client, repository)
	config := githubsource.ManagerConfig{Prune: true, RunnerPools: []githubsource.RunnerPoolConfig{{
		RunnerPool: core.RunnerPool("linux"), RunnerGroup: "default",
	}}}

	if err := manager.Reconcile(context.Background(), config); err != nil {
		t.Fatalf("reconcile registrations: %v", err)
	}
	if len(repository.records) != 2 {
		t.Fatalf("persisted registrations = %#v, want two", repository.records)
	}
	for _, client := range factory.clients {
		if len(client.ensured) != 1 || client.ensured[0] != "linux" || client.prunes != 1 {
			t.Fatalf("client state: ensured=%#v prunes=%d", client.ensured, client.prunes)
		}
	}
}

func TestManagerRetainsOwnershipWhenConfigurationIsRemovedWithoutPrune(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &snapshotRepositoryFake{snapshot: githubsource.RegistrationSnapshot{
		Active: []githubsource.Registration{record},
		Owned:  []githubsource.Registration{record},
	}}
	discoverer := &managerDiscovererFake{repositories: []transport.Repository{{
		InstallationID: 1, URL: record.RepositoryURL,
	}}}
	factory := newManagerFactoryFake(discoverer.repositories)
	manager := newManagerFixture(t, discoverer, factory.Client, repository)

	if err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{}); err != nil {
		t.Fatalf("reconcile removed configuration: %v", err)
	}
	if len(repository.snapshot.Active) != 0 {
		t.Fatalf("active registrations = %#v, want none", repository.snapshot.Active)
	}
	if len(repository.snapshot.Owned) != 1 || repository.snapshot.Owned[0] != record {
		t.Fatalf("owned registrations = %#v, want retained %#v", repository.snapshot.Owned, record)
	}
}

func TestManagerRetainsUndiscoveredRepositoryOwnership(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/missing", 17)
	repository := &snapshotRepositoryFake{snapshot: githubsource.RegistrationSnapshot{
		Active: []githubsource.Registration{record}, Owned: []githubsource.Registration{record},
	}}
	manager := newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client, repository)
	if err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{Prune: true}); err != nil {
		t.Fatalf("reconcile undiscovered repository: %v", err)
	}
	if len(repository.snapshot.Active) != 0 || len(repository.snapshot.Owned) != 1 || repository.snapshot.Owned[0] != record {
		t.Fatalf("snapshot = %#v, want inactive retained ownership", repository.snapshot)
	}
}

func TestManagerPreservesSnapshotAfterPartialPruneFailureAndRetries(t *testing.T) {
	t.Parallel()

	fixture := newPartialPruneRetryFixture(t)
	err := fixture.manager.Reconcile(context.Background(), githubsource.ManagerConfig{Prune: true})
	assertPartialPruneFailure(t, fixture, err)
	if err = fixture.manager.Reconcile(context.Background(), githubsource.ManagerConfig{Prune: true}); err != nil {
		t.Fatalf("retry prune: %v", err)
	}
	if fixture.repository.replaceCalls != 1 || len(fixture.repository.snapshot.Owned) != 0 {
		t.Fatalf("retry snapshot = %#v replace calls=%d", fixture.repository.snapshot, fixture.repository.replaceCalls)
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
	if fixture.repository.replaceCalls != 0 || !reflect.DeepEqual(fixture.repository.snapshot, fixture.previous) {
		t.Fatalf("snapshot changed after partial failure: %#v", fixture.repository.snapshot)
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
	if err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{Prune: true}); !errors.Is(err, wantErr) {
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

func TestManagerRefreshesMemberRoutes(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &registrationRepositoryFake{records: []githubsource.Registration{record}}
	transportRepository := managerTransportRepository(record)
	factory := newManagerFactoryFake([]transport.Repository{transportRepository})
	factory.clients[record.RepositoryURL].bootstrap = []byte("opaque")
	manager := newManagerFixture(t, &managerDiscovererFake{}, factory.Client, repository)

	if err := manager.RefreshMembers(context.Background()); err != nil {
		t.Fatalf("refresh members: %v", err)
	}
	job := sourceJob(t, record.RepositoryURL, record.RunnerPool, 61)
	if _, err := manager.Source().GenerateBootstrap(context.Background(), job, "runner-61"); err != nil {
		t.Fatalf("generate bootstrap: %v", err)
	}
}

func TestManagerOpensPollAdaptersAndClosesLeaderSessions(t *testing.T) {
	t.Parallel()

	fixture := openLeaderManagerFixture(t)
	adapters, err := fixture.manager.OpenLeader(context.Background(), "member-1", fixture.handler)
	if err != nil {
		t.Fatalf("open leader: %v", err)
	}
	if len(adapters) != 1 {
		t.Fatalf("poll adapters = %#v, want one", adapters)
	}
	if _, err := adapters[0].PollOnce(context.Background(), 0); err != nil {
		t.Fatalf("poll opened session: %v", err)
	}
	if err := fixture.manager.CloseLeader(context.Background()); err != nil {
		t.Fatalf("close leader: %v", err)
	}
	if !fixture.session.closed {
		t.Fatal("leader session was not closed")
	}
}

func TestManagerRetriesFailedLeaderSessionClosure(t *testing.T) {
	t.Parallel()

	fixture := openLeaderManagerFixture(t)
	if _, err := fixture.manager.OpenLeader(context.Background(), "member-1", fixture.handler); err != nil {
		t.Fatalf("open leader: %v", err)
	}
	wantErr := errors.New("close failed")
	fixture.session.closeErr = wantErr
	if err := fixture.manager.CloseLeader(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("first close error = %v, want %v", err, wantErr)
	}
	fixture.session.closeErr = nil
	if err := fixture.manager.CloseLeader(context.Background()); err != nil {
		t.Fatalf("retry close: %v", err)
	}
	if fixture.session.closes != 2 {
		t.Fatalf("session close attempts = %d, want 2", fixture.session.closes)
	}
}

type leaderManagerFixture struct {
	manager *githubsource.Manager
	session *managerSessionFake
	record  githubsource.Registration
	handler *recordingEventHandler
}

func openLeaderManagerFixture(t *testing.T) leaderManagerFixture {
	t.Helper()
	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &registrationRepositoryFake{records: []githubsource.Registration{record}}
	transportRepository := managerTransportRepository(record)
	factory := newManagerFactoryFake([]transport.Repository{transportRepository})
	session := &managerSessionFake{
		key:      transport.SessionKey{Repository: record.RepositoryURL, RunnerPool: string(record.RunnerPool)},
		messages: []*upstream.RunnerScaleSetMessage{{MessageID: 31}},
	}
	factory.clients[record.RepositoryURL].session = session
	manager := newManagerFixture(t, &managerDiscovererFake{}, factory.Client, repository)
	handler := &recordingEventHandler{events: &session.events}
	return leaderManagerFixture{manager: manager, session: session, record: record, handler: handler}
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
	manager, err := githubsource.NewManager(discoverer, factory, repository)
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

type managerDiscovererFake struct {
	repositories []transport.Repository
	err          error
}

func (discoverer *managerDiscovererFake) Discover(context.Context) (transport.DiscoveryResult, error) {
	return transport.DiscoveryResult{Repositories: discoverer.repositories}, discoverer.err
}

type registrationRepositoryFake struct {
	records   []githubsource.Registration
	listErr   error
	upsertErr error
	deleteErr error
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
	if repository.upsertErr != nil {
		return repository.upsertErr
	}
	for index := range repository.records {
		if repository.records[index].RepositoryURL == record.RepositoryURL &&
			repository.records[index].RunnerPool == record.RunnerPool {
			repository.records[index] = record
			return nil
		}
	}
	repository.records = append(repository.records, record)
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
		OpenSession: func(context.Context, transport.Registration, string) (githubsource.ManagedSession, error) {
			if client.session == nil {
				return nil, client.openErr
			}
			return client.session, client.openErr
		},
		GenerateBootstrap: client.GenerateBootstrap,
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
	events   []string
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
