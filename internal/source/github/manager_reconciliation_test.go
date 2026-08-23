package github_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
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
	snapshot, err := manager.Reconcile(context.Background(), config)
	if err != nil {
		t.Fatalf("reconcile registrations: %v", err)
	}
	if len(repository.records) != 2 {
		t.Fatalf("persisted registrations = %#v, want two", repository.records)
	}
	if len(snapshot) != 2 || snapshot[0].RepositoryURL != discoverer.repositories[0].URL ||
		snapshot[1].RepositoryURL != discoverer.repositories[1].URL {
		t.Fatalf("returned registrations = %#v", snapshot)
	}
	for _, client := range factory.clients {
		if len(client.ensured) != 1 || client.ensured[0] != "linux" || client.prunes != 1 {
			t.Fatalf("client state: ensured=%#v prunes=%d", client.ensured, client.prunes)
		}
	}
}

func TestManagerPersistsSuccessfulRepositoryWhenSiblingReconciliationFails(t *testing.T) {
	t.Parallel()
	repositories := []transport.Repository{
		{InstallationID: 1, URL: "https://github.com/octo/first"},
		{InstallationID: 2, URL: "https://github.com/octo/second"},
	}
	factory := newManagerFactoryFake(repositories)
	wantErr := errors.New("ensure failed")
	factory.clients[repositories[1].URL].ensureErr = wantErr
	repository := &registrationRepositoryFake{}
	manager := newManagerFixture(t, &managerDiscovererFake{repositories: repositories}, factory.Client, repository)

	_, err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
		RunnerPool: "linux", RunnerGroup: "default",
	}}})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Reconcile() error = %v, want %v", err, wantErr)
	}
	if len(repository.records) != 1 || repository.records[0].RepositoryURL != repositories[0].URL {
		t.Fatalf("persisted registrations = %#v", repository.records)
	}
	if len(repository.owned) != 1 || repository.owned[0] != repository.records[0] {
		t.Fatalf("persisted ownership = %#v", repository.owned)
	}
}

func TestManagerRetainsOwnershipWhenConfigurationIsRemovedWithoutPrune(t *testing.T) {
	t.Parallel()
	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &snapshotRepositoryFake{snapshot: githubsource.RegistrationSnapshot{
		Active: []githubsource.Registration{record}, Owned: []githubsource.Registration{record},
	}}
	discoverer := &managerDiscovererFake{repositories: []transport.Repository{{InstallationID: 1, URL: record.RepositoryURL}}}
	factory := newManagerFactoryFake(discoverer.repositories)
	manager := newManagerFixture(t, discoverer, factory.Client, repository)
	if _, err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{}); err != nil {
		t.Fatalf("reconcile removed configuration: %v", err)
	}
	if len(repository.snapshot.Active) != 1 || repository.snapshot.Active[0] != record {
		t.Fatalf("active registrations = %#v, want retained %#v", repository.snapshot.Active, record)
	}
	if len(repository.snapshot.Owned) != 1 || repository.snapshot.Owned[0] != record {
		t.Fatalf("owned registrations = %#v, want retained %#v", repository.snapshot.Owned, record)
	}
}

func TestManagerPreservesConflictedPoolDuringPruning(t *testing.T) {
	t.Parallel()
	record := managerRecord("https://github.com/octo/repo", 17)
	repository := &snapshotRepositoryFake{snapshot: githubsource.RegistrationSnapshot{
		Active: []githubsource.Registration{record}, Owned: []githubsource.Registration{record},
	}}
	discoverer := &managerDiscovererFake{repositories: []transport.Repository{{InstallationID: 1, URL: record.RepositoryURL}}}
	factory := newManagerFactoryFake(discoverer.repositories)
	manager := newManagerFixture(t, discoverer, factory.Client, repository)
	config := githubsource.ManagerConfig{Prune: true, PreserveRunnerPools: []core.RunnerPool{record.RunnerPool}}
	active, err := manager.Reconcile(context.Background(), config)
	if err != nil {
		t.Fatalf("reconcile conflicted pool: %v", err)
	}
	if len(active) != 1 || active[0] != record || len(repository.snapshot.Active) != 1 {
		t.Fatalf("active registrations = %#v, persisted %#v", active, repository.snapshot.Active)
	}
}
