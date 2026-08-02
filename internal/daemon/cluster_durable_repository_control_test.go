package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func TestClusterLeaderReconnectsDurableRepositoryBeforeDiscoveryCompletes(t *testing.T) {
	repositoryURL := "https://github.com/acme/durable"
	discoveryEntered := make(chan struct{})
	releaseDiscovery := make(chan struct{})
	closeSignalOnCleanup(t, releaseDiscovery)
	discoverer := controlledRepositoryDiscoverer{
		entered: discoveryEntered, release: releaseDiscovery, err: errors.New("GitHub unavailable"),
	}
	repositoryStore := &fakeRepositoryStateStore{snapshot: repositorystate.Snapshot{
		Control: repositorystate.Control{AppID: 42, Initialized: true, DiscoveryGeneration: 3},
		Repositories: []repositorystate.Repository{{
			AppID: 42, URL: repositoryURL, InstallationID: 7, LastSeenGeneration: 3,
		}},
	}}
	leader, daemon := durableRepositoryTestLeader(t, repositoryStore, discoverer)
	reconnectStarted := make(chan struct{})
	releaseReconnect := make(chan struct{})
	closeSignalOnCleanup(t, releaseReconnect)
	daemon.newGitHubClient = blockedReconnectClient(reconnectStarted, releaseReconnect)
	reconciled := make(chan bool, 1)
	go func() { reconciled <- leader.reconcile(context.Background()) }()
	awaitCompletionSignal(t, discoveryEntered)
	awaitCompletionSignal(t, reconnectStarted)
	if !awaitBoolSignal(t, reconciled) {
		t.Fatal("cached discovery failure degraded the leader")
	}
	close(releaseReconnect)
	close(releaseDiscovery)
	awaitDurableRegistration(t, repositoryStore, repositoryURL)
	leader.stop(context.Background())
}

func awaitDurableRegistration(t *testing.T, repositoryStore repositorystate.Store, repositoryURL string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := repositoryStore.LoadSnapshot(context.Background(), 42)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Registrations) == 1 && snapshot.Registrations[0].RepositoryURL == repositoryURL {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("durable reconnect registration was not persisted")
}

func TestClusterLeaderDegradesWhenDiscoveryFailsWithoutDurableSnapshot(t *testing.T) {
	repositoryStore := &fakeRepositoryStateStore{}
	leader, _ := durableRepositoryTestLeader(t, repositoryStore, staticDiscoverer{err: errors.New("GitHub unavailable")})
	if leader.reconcile(context.Background()) {
		t.Fatal("leader without a durable snapshot reported operational")
	}
	leader.stop(context.Background())
}

func TestClusterLeaderDegradesOnAggregationFailureWithDurableSnapshot(t *testing.T) {
	repositoryStore := &fakeRepositoryStateStore{snapshot: repositorystate.Snapshot{
		Control: repositorystate.Control{AppID: 42, Initialized: true},
	}}
	leader, daemon := durableRepositoryTestLeader(t, repositoryStore, staticDiscoverer{})
	repositoryRepairState(t, daemon).statusesErr = errors.New("PostgreSQL unavailable")
	if leader.reconcile(context.Background()) {
		t.Fatal("leader with failed cluster aggregation reported operational")
	}
	leader.stop(context.Background())
}

func TestClusterRepositoryRepairProgressesWhileDiscoveryIsBlocked(t *testing.T) {
	repository := githubapp.Repository{InstallationID: 7, URL: "https://github.com/acme/repair"}
	statuses := []cluster.ScaleSetStatus{{
		Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: true,
	}}
	discoveryEntered := make(chan struct{})
	releaseDiscovery := make(chan struct{})
	closeSignalOnCleanup(t, releaseDiscovery)
	leader, daemon := repositoryRepairTestLeader(t, nil)
	daemon.discoverer = controlledRepositoryDiscoverer{entered: discoveryEntered, release: releaseDiscovery}
	repairStarted := make(chan struct{})
	daemon.newGitHubClient = signalingRepositoryClient(repairStarted)
	leader.pendingRepositoryRepairs[repository.URL] = repositoryRepair{
		repository: repository, statuses: statuses, attempts: 1,
	}
	reconciled := make(chan bool, 1)
	go func() { reconciled <- leader.reconcile(context.Background()) }()
	awaitCompletionSignal(t, discoveryEntered)
	repaired := make(chan struct{})
	go func() {
		leader.repairPendingRepositories(context.Background())
		close(repaired)
	}()
	awaitCompletionSignal(t, repairStarted)
	awaitCompletionSignal(t, repaired)
	close(releaseDiscovery)
	awaitBoolSignal(t, reconciled)
	leader.stop(context.Background())
}

func closeSignalOnCleanup(t *testing.T, signal chan struct{}) {
	t.Helper()
	t.Cleanup(func() {
		select {
		case <-signal:
		default:
			close(signal)
		}
	})
}

func blockedReconnectClient(
	started chan<- struct{},
	release <-chan struct{},
) func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
	return func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		close(started)
		client := successfulRepositoryRepairClient()
		client.getRunnerGroupByName = func(context.Context, string) (*upstream.RunnerGroup, error) {
			<-release
			return &upstream.RunnerGroup{ID: 1}, nil
		}
		return client, nil
	}
}

func signalingRepositoryClient(
	started chan<- struct{},
) func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
	return func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		close(started)
		return successfulRepositoryRepairClient(), nil
	}
}

func durableRepositoryTestLeader(
	t *testing.T,
	repositoryStore repositorystate.Store,
	discoverer repositoryDiscoverer,
) (*clusterLeader, *clusteredDaemon) {
	t.Helper()
	state := &fakeClusterState{statuses: []cluster.ScaleSetStatus{{
		Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: true,
	}}}
	daemon := activeClusteredDaemon(t, state)
	daemon.credentials.AppID = 42
	daemon.repositoryState = repositoryStore
	daemon.discoverer = discoverer
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{alive: true})
	daemon.leader = leader
	return leader, &daemon
}

type controlledRepositoryDiscoverer struct {
	entered chan<- struct{}
	release <-chan struct{}
	result  githubapp.DiscoveryResult
	err     error
}

func (discoverer controlledRepositoryDiscoverer) Discover(context.Context) (githubapp.DiscoveryResult, error) {
	close(discoverer.entered)
	<-discoverer.release
	return discoverer.result, discoverer.err
}
