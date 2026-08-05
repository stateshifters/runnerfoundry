package github_test

import (
	"context"
	"errors"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
	statecontract "github.com/meza/RunnerFoundry/internal/state"
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

func TestManagerRefreshesMemberRoutes(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &registrationRepositoryFake{records: []statecontract.Registration{record}}
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
	if err := adapters[0].PollOnce(context.Background()); err != nil {
		t.Fatalf("poll opened session: %v", err)
	}
	if err := fixture.manager.Source().Acquire(
		context.Background(), sourceJob(t, fixture.record.RepositoryURL, fixture.record.RunnerPool, 62),
	); err != nil {
		t.Fatalf("acquire opened route: %v", err)
	}
	if err := fixture.manager.CloseLeader(context.Background()); err != nil {
		t.Fatalf("close leader: %v", err)
	}
	if !fixture.session.closed {
		t.Fatal("leader session was not closed")
	}
}

type leaderManagerFixture struct {
	manager *githubsource.Manager
	session *managerSessionFake
	record  statecontract.Registration
	handler *recordingEventHandler
}

func openLeaderManagerFixture(t *testing.T) leaderManagerFixture {
	t.Helper()
	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &registrationRepositoryFake{records: []statecontract.Registration{record}}
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

func managerTransportRepository(record statecontract.Registration) transport.Repository {
	return transport.Repository{
		InstallationID: transport.InstallationID(record.InstallationID), URL: record.RepositoryURL,
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

func managerRecord(repositoryURL string, registrationID int) statecontract.Registration {
	return statecontract.Registration{
		RepositoryURL: repositoryURL, InstallationID: 1, RunnerPool: core.RunnerPool("linux"),
		RunnerGroupID: 1, ScaleSetID: int64(registrationID),
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
	records   []statecontract.Registration
	listErr   error
	upsertErr error
	deleteErr error
}

func (repository *registrationRepositoryFake) UpsertRegistration(
	_ context.Context,
	record statecontract.Registration,
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
) (statecontract.Registration, bool, error) {
	for _, record := range repository.records {
		if record.RepositoryURL == repositoryURL && record.RunnerPool == runnerPool {
			return record, true, repository.listErr
		}
	}
	return statecontract.Registration{}, false, repository.listErr
}

func (repository *registrationRepositoryFake) Registrations(context.Context) ([]statecontract.Registration, error) {
	return append([]statecontract.Registration(nil), repository.records...), repository.listErr
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
	nextID       int
	ensured      []string
	prunes       int
	bootstrap    []byte
	session      *managerSessionFake
	ensureErr    error
	pruneErr     error
	openErr      error
	bootstrapErr error
}

func (client *managerClientFake) EnsureRunnerPool(
	_ context.Context,
	runnerPoolName string,
	_ string,
) (transport.Registration, error) {
	client.ensured = append(client.ensured, runnerPoolName)
	return transport.Registration{ID: client.nextID, Name: runnerPoolName}, client.ensureErr
}

func (client *managerClientFake) GenerateBootstrap(
	context.Context,
	transport.Registration,
	string,
) ([]byte, error) {
	return client.bootstrap, client.bootstrapErr
}

func (client *managerClientFake) PruneRunnerPools(context.Context, string, []string) error {
	client.prunes++
	return client.pruneErr
}

type managerSessionFake struct {
	key      transport.SessionKey
	messages []*upstream.RunnerScaleSetMessage
	events   []string
	closed   bool
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

func (*managerSessionFake) DeleteMessage(context.Context, int) error    { return nil }
func (*managerSessionFake) AcquireRequest(context.Context, int64) error { return nil }
func (session *managerSessionFake) Close(context.Context) error {
	session.closed = true
	return session.closeErr
}
