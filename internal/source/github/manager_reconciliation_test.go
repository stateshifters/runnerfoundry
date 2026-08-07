package github_test

import (
	"context"
	"reflect"
	"testing"

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
	snapshot, err := manager.Reconcile(context.Background(), config)
	if err != nil {
		t.Fatalf("reconcile registrations: %v", err)
	}
	if !reflect.DeepEqual(snapshot.Active, repository.records) {
		t.Fatalf("returned snapshot = %#v, committed %#v", snapshot.Active, repository.records)
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
		Active: []githubsource.Registration{record}, Owned: []githubsource.Registration{record},
	}}
	discoverer := &managerDiscovererFake{repositories: []transport.Repository{{InstallationID: 1, URL: record.RepositoryURL}}}
	factory := newManagerFactoryFake(discoverer.repositories)
	manager := newManagerFixture(t, discoverer, factory.Client, repository)
	if _, err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{}); err != nil {
		t.Fatalf("reconcile removed configuration: %v", err)
	}
	if len(repository.snapshot.Active) != 0 {
		t.Fatalf("active registrations = %#v, want none", repository.snapshot.Active)
	}
	if len(repository.snapshot.Owned) != 1 || repository.snapshot.Owned[0] != record {
		t.Fatalf("owned registrations = %#v, want retained %#v", repository.snapshot.Owned, record)
	}
}
