package daemon

import (
	"context"
	"testing"

	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
)

func TestRepositoryRepairStopsWhenCallerIsCanceled(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, nil)
	addDueRepositoryRepair(leader, "https://github.com/acme/caller-canceled")
	entered := make(chan struct{})
	daemon.newGitHubClient = blockingRepositoryClientFactory(entered)
	repairCtx, cancelRepair := context.WithCancel(context.Background())
	cancelRepair()
	leader.repairPendingRepositories(repairCtx)
	leader.stop(context.Background())
}

func TestRepositoryRepairStopsWhenLeadershipContextIsCanceled(t *testing.T) {
	leader, _ := repositoryRepairTestLeader(t, nil)
	addDueRepositoryRepair(leader, "https://github.com/acme/leadership-canceled")
	termCtx, cancelTerm := context.WithCancel(context.Background())
	cancelTerm()
	leader.stopRepositoryWorkers()
	leader.repairPendingRepositories(termCtx)
	leader.stop(context.Background())
}

func TestObsoleteRepositoryRepairCompletesWithoutRetry(t *testing.T) {
	repositoryURL := "https://github.com/acme/obsolete"
	leader, daemon := repositoryRepairTestLeader(t, nil)
	addDueRepositoryRepair(leader, repositoryURL)
	daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errRepositoryWorkObsolete
	}
	leader.repairPendingRepositories(context.Background())
	leader.repairMutex.Lock()
	_, pending := leader.pendingRepositoryRepairs[repositoryURL]
	leader.repairMutex.Unlock()
	if pending {
		t.Fatal("obsolete repository repair remained pending")
	}
	leader.stop(context.Background())
}

func TestDueRepositoryRepairsAreReturnedInRepositoryOrder(t *testing.T) {
	leader, _ := repositoryRepairTestLeader(t, nil)
	addDueRepositoryRepair(leader, "https://github.com/acme/z-last")
	addDueRepositoryRepair(leader, "https://github.com/acme/a-first")
	repairs := leader.dueRepositoryRepairs()
	if len(repairs) != 2 || repairs[0].repository.URL != "https://github.com/acme/a-first" {
		t.Fatalf("due repairs = %#v, want repository order", repairs)
	}
	leader.stop(context.Background())
}

func addDueRepositoryRepair(leader *clusterLeader, repositoryURL string) {
	leader.repairMutex.Lock()
	leader.pendingRepositoryRepairs[repositoryURL] = repositoryRepair{
		repository: githubapp.Repository{InstallationID: 1, URL: repositoryURL},
		statuses: []cluster.ScaleSetStatus{{
			Name: "linux", RunnerGroup: "Default", Compatible: true, EffectiveCapacity: 1,
		}},
	}
	leader.repairMutex.Unlock()
}
