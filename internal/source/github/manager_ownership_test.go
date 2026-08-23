package github_test

import (
	"context"
	"testing"

	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestManagerPrunesStoredGroupWithoutCurrentConfiguration(t *testing.T) {
	t.Parallel()

	record := managerRecord("https://github.com/octo/repo", 17)
	record.RunnerGroupID = 71
	repository := &snapshotRepositoryFake{snapshot: githubsource.RegistrationSnapshot{Owned: []githubsource.Registration{record}}}
	discoverer := &managerDiscovererFake{repositories: []transport.Repository{{InstallationID: 1, URL: record.RepositoryURL}}}
	factory := newManagerFactoryFake(discoverer.repositories)
	factory.clients[record.RepositoryURL].pruneFunc = func(groupID transport.RunnerGroupID, _ []transport.OwnedRunnerPool, owned []transport.OwnedRunnerPool) (transport.PruneResult, error) {
		if groupID != 71 {
			t.Fatalf("pruned runner group %d, want stored group 71", groupID)
		}
		return transport.PruneResult{Removed: owned}, nil
	}
	manager := newManagerFixture(t, discoverer, factory.Client, repository)
	if _, err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{Prune: true}); err != nil {
		t.Fatalf("prune stored runner group: %v", err)
	}
	if len(repository.snapshot.Owned) != 0 {
		t.Fatalf("owned registrations = %#v, want none", repository.snapshot.Owned)
	}
}

func TestManagerReplacesChangedOwnershipAndPrunesOldFingerprint(t *testing.T) {
	t.Parallel()

	old := managerRecord("https://github.com/octo/repo", 11)
	old.RunnerGroupID = 17
	repository := &snapshotRepositoryFake{snapshot: githubsource.RegistrationSnapshot{Owned: []githubsource.Registration{old}}}
	discoverer := &managerDiscovererFake{repositories: []transport.Repository{{InstallationID: 1, URL: old.RepositoryURL}}}
	factory := newManagerFactoryFake(discoverer.repositories)
	client := factory.clients[old.RepositoryURL]
	client.nextID = 12
	client.runnerGroupID = 23
	client.pruneFunc = func(groupID transport.RunnerGroupID, _ []transport.OwnedRunnerPool, owned []transport.OwnedRunnerPool) (transport.PruneResult, error) {
		if groupID == 17 {
			return transport.PruneResult{Removed: owned}, nil
		}
		return transport.PruneResult{}, nil
	}
	manager := newManagerFixture(t, discoverer, factory.Client, repository)
	config := githubsource.ManagerConfig{Prune: true, RunnerPools: []githubsource.RunnerPoolConfig{{RunnerPool: "linux", RunnerGroup: "new"}}}
	if _, err := manager.Reconcile(context.Background(), config); err != nil {
		t.Fatalf("reconcile changed registration: %v", err)
	}
	if len(repository.snapshot.Owned) != 1 || repository.snapshot.Owned[0].ScaleSetID != 12 ||
		repository.snapshot.Owned[0].RunnerGroupID != 23 {
		t.Fatalf("owned registrations = %#v, want only current fingerprint", repository.snapshot.Owned)
	}
}
