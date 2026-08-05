package github_test

import (
	"context"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
	statecontract "github.com/meza/RunnerFoundry/internal/state"
)

func TestNewManagerRejectsMissingDependencies(t *testing.T) {
	t.Parallel()
	discoverer := &managerDiscovererFake{}
	factory := newManagerFactoryFake(nil)
	repository := &registrationRepositoryFake{}
	if _, err := githubsource.NewManager(nil, factory.Client, repository); err == nil {
		t.Fatal("missing discoverer accepted")
	}
	if _, err := githubsource.NewManager(discoverer, nil, repository); err == nil {
		t.Fatal("missing factory accepted")
	}
	if _, err := githubsource.NewManager(discoverer, factory.Client, nil); err == nil {
		t.Fatal("missing repository accepted")
	}
}

func TestManagerReconcileRejectsInvalidConfigurationAndDiscoveryFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("discovery failed")
	tests := []struct {
		name       string
		config     githubsource.ManagerConfig
		discoverer *managerDiscovererFake
	}{
		{name: "incomplete pool", config: githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{}}}, discoverer: &managerDiscovererFake{}},
		{name: "duplicate pool", config: githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{
			{RunnerPool: core.RunnerPool("linux"), RunnerGroup: "one"},
			{RunnerPool: core.RunnerPool("linux"), RunnerGroup: "two"},
		}}, discoverer: &managerDiscovererFake{}},
		{name: "discovery", discoverer: &managerDiscovererFake{err: wantErr}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			manager := newManagerFixture(t, testCase.discoverer, newManagerFactoryFake(nil).Client, &registrationRepositoryFake{})
			if err := manager.Reconcile(context.Background(), testCase.config); err == nil {
				t.Fatal("Reconcile() error = nil, want failure")
			}
		})
	}
	manager := newManagerFixture(
		t, &installationFailureDiscoverer{err: wantErr}, newManagerFactoryFake(nil).Client, &registrationRepositoryFake{},
	)
	if err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{}); !errors.Is(err, wantErr) {
		t.Fatalf("installation failure = %v, want %v", err, wantErr)
	}
}

func TestManagerReconcileReportsClientFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("operation failed")
	repository := transport.Repository{InstallationID: 1, URL: "https://github.com/octo/repo"}
	config := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
		RunnerPool: core.RunnerPool("linux"), RunnerGroup: "default",
	}}}
	newFixture := func(factory *managerFactoryFake, store *registrationRepositoryFake) *githubsource.Manager {
		return newManagerFixture(t, &managerDiscovererFake{repositories: []transport.Repository{repository}}, factory.Client, store)
	}

	factoryFailure := newManagerFactoryFake(nil)
	factoryFailure.err = wantErr
	assertReconcileFails(t, newFixture(factoryFailure, &registrationRepositoryFake{}), config, wantErr)

	ensureFailure := newManagerFactoryFake([]transport.Repository{repository})
	ensureFailure.clients[repository.URL].ensureErr = wantErr
	assertReconcileFails(t, newFixture(ensureFailure, &registrationRepositoryFake{}), config, wantErr)

	pruneFailure := newManagerFactoryFake([]transport.Repository{repository})
	pruneFailure.clients[repository.URL].pruneErr = wantErr
	config.Prune = true
	assertReconcileFails(t, newFixture(pruneFailure, &registrationRepositoryFake{}), config, wantErr)
	config.Prune = false
}

func TestManagerReconcileReportsPersistenceFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("operation failed")
	repository := transport.Repository{InstallationID: 1, URL: "https://github.com/octo/repo"}
	config := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
		RunnerPool: core.RunnerPool("linux"), RunnerGroup: "default",
	}}}
	newFixture := func(factory *managerFactoryFake, store *registrationRepositoryFake) *githubsource.Manager {
		return newManagerFixture(t, &managerDiscovererFake{repositories: []transport.Repository{repository}}, factory.Client, store)
	}
	assertReconcileFails(
		t, newFixture(newManagerFactoryFake([]transport.Repository{repository}), &registrationRepositoryFake{listErr: wantErr}),
		config, wantErr,
	)
	assertReconcileFails(
		t, newFixture(newManagerFactoryFake([]transport.Repository{repository}), &registrationRepositoryFake{upsertErr: wantErr}),
		config, wantErr,
	)
}

func TestManagerReconcileRetainsDesiredAndRemovesStaleRegistrations(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("operation failed")
	repository := transport.Repository{InstallationID: 1, URL: "https://github.com/octo/repo"}
	config := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
		RunnerPool: core.RunnerPool("linux"), RunnerGroup: "default",
	}}}
	newFixture := func(factory *managerFactoryFake, store *registrationRepositoryFake) *githubsource.Manager {
		return newManagerFixture(t, &managerDiscovererFake{repositories: []transport.Repository{repository}}, factory.Client, store)
	}
	retained := managerRecord(repository.URL, 11)
	retainedManager := newFixture(
		newManagerFactoryFake([]transport.Repository{repository}),
		&registrationRepositoryFake{records: []statecontract.Registration{retained}},
	)
	if err := retainedManager.Reconcile(context.Background(), config); err != nil {
		t.Fatalf("retain desired registration: %v", err)
	}

	stale := managerRecord("https://github.com/octo/stale", 9)
	deleteStore := &registrationRepositoryFake{records: []statecontract.Registration{stale}, deleteErr: wantErr}
	emptyManager := newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client, deleteStore)
	assertReconcileFails(t, emptyManager, githubsource.ManagerConfig{}, wantErr)
	deleteStore.deleteErr = nil
	if err := emptyManager.Reconcile(context.Background(), githubsource.ManagerConfig{}); err != nil {
		t.Fatalf("delete stale registration: %v", err)
	}
}

func TestManagerRefreshMembersReportsFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("refresh failed")
	manager := newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client, &registrationRepositoryFake{listErr: wantErr})
	if err := manager.RefreshMembers(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("list error = %v", err)
	}

	record := managerRecord("https://github.com/octo/repo", 17)
	factory := newManagerFactoryFake(nil)
	factory.err = wantErr
	manager = newManagerFixture(t, &managerDiscovererFake{}, factory.Client, &registrationRepositoryFake{records: []statecontract.Registration{record}})
	if err := manager.RefreshMembers(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("factory error = %v", err)
	}

	factory = newManagerFactoryFake([]transport.Repository{managerTransportRepository(record)})
	manager = newManagerFixture(t, &managerDiscovererFake{}, factory.Client, &registrationRepositoryFake{records: []statecontract.Registration{record, record}})
	if err := manager.RefreshMembers(context.Background()); err == nil {
		t.Fatal("duplicate member routes accepted")
	}
}

func TestManagerOpenLeaderReportsFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("open failed")
	handler := &recordingEventHandler{events: &[]string{}}
	manager := newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client, &registrationRepositoryFake{})
	if _, err := manager.OpenLeader(context.Background(), "", handler); err == nil {
		t.Fatal("empty owner accepted")
	}
	if _, err := manager.OpenLeader(context.Background(), "owner", nil); err == nil {
		t.Fatal("nil handler accepted")
	}
	manager = newManagerFixture(t, &managerDiscovererFake{}, newManagerFactoryFake(nil).Client, &registrationRepositoryFake{listErr: wantErr})
	if _, err := manager.OpenLeader(context.Background(), "owner", handler); !errors.Is(err, wantErr) {
		t.Fatalf("list error = %v", err)
	}
}

func TestManagerOpenLeaderReportsFactoryAndSessionFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("open failed")
	handler := &recordingEventHandler{events: &[]string{}}
	record := managerRecord("https://github.com/octo/repo", 17)
	store := &registrationRepositoryFake{records: []statecontract.Registration{record}}
	factory := newManagerFactoryFake(nil)
	factory.err = wantErr
	manager := newManagerFixture(t, &managerDiscovererFake{}, factory.Client, store)
	if _, err := manager.OpenLeader(context.Background(), "owner", handler); !errors.Is(err, wantErr) {
		t.Fatalf("factory error = %v", err)
	}

	factory = newManagerFactoryFake([]transport.Repository{managerTransportRepository(record)})
	factory.clients[record.RepositoryURL].openErr = wantErr
	manager = newManagerFixture(t, &managerDiscovererFake{}, factory.Client, store)
	if _, err := manager.OpenLeader(context.Background(), "owner", handler); !errors.Is(err, wantErr) {
		t.Fatalf("open error = %v", err)
	}
	factory.clients[record.RepositoryURL].openErr = nil
	if _, err := manager.OpenLeader(context.Background(), "owner", handler); err == nil {
		t.Fatal("nil session accepted")
	}
}

func TestManagerOpenLeaderClosesSessionsAfterRouteAndReplacementFailures(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 17)
	store := &registrationRepositoryFake{records: []statecontract.Registration{record, record}}
	factory := newManagerFactoryFake([]transport.Repository{managerTransportRepository(record)})
	session := &managerSessionFake{
		key:      transport.SessionKey{Repository: record.RepositoryURL, RunnerPool: string(record.RunnerPool)},
		closeErr: errors.New("close failed"),
	}
	factory.clients[record.RepositoryURL].session = session
	manager := newManagerFixture(t, &managerDiscovererFake{}, factory.Client, store)
	handler := &recordingEventHandler{events: &[]string{}}
	if _, err := manager.OpenLeader(context.Background(), "owner", handler); err == nil {
		t.Fatal("duplicate leader routes accepted")
	}
	if !session.closed {
		t.Fatal("failed replacement did not close opened sessions")
	}
}

func assertReconcileFails(
	t *testing.T,
	manager *githubsource.Manager,
	config githubsource.ManagerConfig,
	wantErr error,
) {
	t.Helper()
	if err := manager.Reconcile(context.Background(), config); !errors.Is(err, wantErr) {
		t.Fatalf("Reconcile() error = %v, want %v", err, wantErr)
	}
}

type installationFailureDiscoverer struct{ err error }

func (discoverer *installationFailureDiscoverer) Discover(context.Context) (transport.DiscoveryResult, error) {
	return transport.DiscoveryResult{InstallationFailures: map[transport.InstallationID]error{1: discoverer.err}}, nil
}
