package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
)

func TestRepositoryRepairTimeoutDoesNotPoisonLaterRepository(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, []githubapp.Repository{
		{InstallationID: 1, URL: "https://github.com/acme/a-timeout"},
		{InstallationID: 1, URL: "https://github.com/acme/b-working"},
	})
	daemon.newGitHubClient = timeoutIsolationClientFactory()
	leader.githubOperationTimeout = 10 * time.Millisecond

	if !leader.reconcile(context.Background()) {
		t.Fatal("repository timeout degraded global reconciliation")
	}
	assertTimeoutIsolation(t, leader)
	leader.stop(context.Background())
}

func TestRepositoryRepairRetriesOnlyPendingRepository(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, []githubapp.Repository{
		{InstallationID: 1, URL: "https://github.com/acme/a-failing"},
		{InstallationID: 1, URL: "https://github.com/acme/b-working"},
	})
	now := time.Date(2026, 8, 2, 18, 0, 0, 0, time.UTC)
	leader.now = func() time.Time { return now }
	clientFactory := &transientRepositoryClientFactory{calls: map[string]int{}, failingRepository: "https://github.com/acme/a-failing"}
	daemon.newGitHubClient = func(repository string, installationID githubapp.InstallationID, credentials config.GitHubAppCredentials) (scaleSetClient, error) {
		return clientFactory.client(repository, installationID, credentials)
	}

	if !leader.reconcile(context.Background()) {
		t.Fatal("repository failure degraded global reconciliation")
	}
	now = now.Add(5 * time.Second)
	leader.repairPendingRepositories(context.Background())
	if clientFactory.calls["https://github.com/acme/a-failing"] != 2 || clientFactory.calls["https://github.com/acme/b-working"] != 1 {
		t.Fatalf("repository calls = %#v, want only the failed repository retried", clientFactory.calls)
	}
	if len(leader.pendingRepositoryRepairs) != 0 {
		t.Fatalf("pending repairs = %d, want repaired repository removed", len(leader.pendingRepositoryRepairs))
	}
	leader.stop(context.Background())
}

func TestRepositoryRepairUsesBoundedBackoff(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, []githubapp.Repository{{
		InstallationID: 1, URL: "https://github.com/acme/failing",
	}})
	now := time.Date(2026, 8, 2, 18, 0, 0, 0, time.UTC)
	leader.now = func() time.Time { return now }
	attempts := 0
	daemon.newGitHubClient = func(
		string,
		githubapp.InstallationID,
		config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		attempts++
		return nil, errors.New("unavailable")
	}

	if !leader.reconcile(context.Background()) {
		t.Fatal("repository failure degraded global reconciliation")
	}
	for _, delay := range []time.Duration{
		5 * time.Second, 10 * time.Second, 20 * time.Second,
		40 * time.Second, 60 * time.Second, 60 * time.Second,
	} {
		assertRepositoryRepairDelay(t, leader, &now, &attempts, delay)
	}
	leader.stop(context.Background())
}

func TestRepositoryRepairIsDiscardedWhenRepositoryIsRemoved(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, []githubapp.Repository{{
		InstallationID: 1, URL: "https://github.com/acme/removed",
	}})
	attempts := 0
	daemon.newGitHubClient = func(
		string,
		githubapp.InstallationID,
		config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		attempts++
		return nil, errors.New("unavailable")
	}
	if !leader.reconcile(context.Background()) || len(leader.pendingRepositoryRepairs) != 1 {
		t.Fatal("failed repository was not retained for repair")
	}
	daemon.discoverer = staticDiscoverer{}
	if !leader.reconcile(context.Background()) {
		t.Fatal("repository removal degraded global reconciliation")
	}
	if len(leader.pendingRepositoryRepairs) != 0 {
		t.Fatal("removed repository retained obsolete repair work")
	}
	leader.repairPendingRepositories(context.Background())
	if attempts != 1 {
		t.Fatalf("repair attempts after removal = %d, want no retry", attempts)
	}
	leader.stop(context.Background())
}

func TestRepositoryRepairIsDiscardedWhenScaleSetIsWithdrawn(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, []githubapp.Repository{{
		InstallationID: 1, URL: "https://github.com/acme/retained",
	}})
	daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errors.New("unavailable")
	}
	if !leader.reconcile(context.Background()) || len(leader.pendingRepositoryRepairs) != 1 {
		t.Fatal("failed repository was not retained for repair")
	}
	repositoryRepairState(t, daemon).statuses = nil
	if !leader.reconcile(context.Background()) {
		t.Fatal("scale-set withdrawal degraded global reconciliation")
	}
	if len(leader.pendingRepositoryRepairs) != 0 {
		t.Fatal("withdrawn scale set retained obsolete repository repair")
	}
	leader.stop(context.Background())
}

func TestLeadershipLossCancelsRepositoryRepair(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, []githubapp.Repository{{
		InstallationID: 1, URL: "https://github.com/acme/cancelled",
	}})
	now := time.Date(2026, 8, 2, 18, 0, 0, 0, time.UTC)
	leader.now = func() time.Time { return now }
	clientFactory := &cancellableRepositoryClientFactory{entered: make(chan struct{})}
	daemon.newGitHubClient = func(repository string, installationID githubapp.InstallationID, credentials config.GitHubAppCredentials) (scaleSetClient, error) {
		return clientFactory.client(repository, installationID, credentials)
	}
	if !leader.reconcile(context.Background()) {
		t.Fatal("repository failure degraded global reconciliation")
	}
	now = now.Add(5 * time.Second)
	runRepairUntilLeadershipStops(leader, clientFactory.entered)
	if len(leader.pendingRepositoryRepairs) != 0 {
		t.Fatal("leadership loss retained repository repair work")
	}
}

func TestRepositoryRepairLogsLifecycle(t *testing.T) {
	logs := &bytes.Buffer{}
	leader, daemon := repositoryRepairTestLeader(t, []githubapp.Repository{{
		InstallationID: 1, URL: "https://github.com/acme/retry",
	}})
	daemon.logger = newClusterLogSnapshotLogger(logs)
	now := time.Date(2026, 8, 2, 18, 0, 0, 0, time.UTC)
	leader.now = func() time.Time { return now }
	attempts := 0
	daemon.newGitHubClient = func(
		string,
		githubapp.InstallationID,
		config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("unavailable")
		}
		return successfulRepositoryRepairClient(), nil
	}
	if !leader.reconcile(context.Background()) {
		t.Fatal("repository failure degraded global reconciliation")
	}
	now = now.Add(5 * time.Second)
	leader.repairPendingRepositories(context.Background())
	assertClusterLogSnapshot(t, logs,
		"{\"level\":\"ERROR\",\"msg\":\"leader repository session group failed\",\"repository\":\"https://github.com/acme/retry\",\"error\":\"unavailable\"}\n"+
			"{\"level\":\"WARN\",\"msg\":\"repository_repair\",\"action\":\"repository_repair\",\"phase\":\"pending\",\"repository\":\"https://github.com/acme/retry\",\"attempt\":1,\"retryAfter\":5000000000,\"error\":\"unavailable\"}\n"+
			"{\"level\":\"INFO\",\"msg\":\"repository_repair\",\"action\":\"repository_repair\",\"phase\":\"retrying\",\"repository\":\"https://github.com/acme/retry\",\"attempt\":2}\n"+
			"{\"level\":\"INFO\",\"msg\":\"repository_repair\",\"action\":\"repository_repair\",\"phase\":\"completed\",\"repository\":\"https://github.com/acme/retry\",\"attempt\":2}")
	leader.stop(context.Background())
}

func TestRepositoryCleanupFailureDoesNotDegradeLeader(t *testing.T) {
	logs := &bytes.Buffer{}
	leader, daemon := repositoryRepairTestLeader(t, nil)
	daemon.logger = newClusterLogSnapshotLogger(logs)
	key := repositoryScaleSetSessionKey{repository: "https://github.com/acme/removed", scaleSet: "linux"}
	leader.sessions[key] = stoppedSession(
		key,
		fakeScaleSetDeletionClient{deleteRunnerScaleSet: func(context.Context, int) error { return nil }},
		&fakeMessageClient{closeErr: errors.New("close failed")},
	)

	if !leader.reconcile(context.Background()) {
		t.Fatal("repository cleanup failure degraded global reconciliation")
	}
	assertClusterLogSnapshot(t, logs,
		"{\"level\":\"ERROR\",\"msg\":\"close leader message session failed\",\"hostname\":\"\",\"error\":\"close message session: close failed\"}\n"+
			"{\"level\":\"ERROR\",\"msg\":\"stop outdated leader sessions failed\",\"hostname\":\"\",\"error\":\"close message session: close failed\"}")
	leader.stop(context.Background())
}

func TestLeadershipLossCancelsFullRepositoryReconciliation(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, []githubapp.Repository{{
		InstallationID: 1, URL: "https://github.com/acme/cancelled-full-pass",
	}})
	entered := make(chan struct{})
	daemon.newGitHubClient = blockingRepositoryClientFactory(entered)
	reconcileResult := make(chan bool, 1)
	go func() { reconcileResult <- leader.reconcile(context.Background()) }()
	<-entered
	leader.stop(context.Background())
	if <-reconcileResult {
		t.Fatal("canceled full reconciliation reported success")
	}
	leader.repairPendingRepositories(context.Background())
}

func repositoryRepairTestLeader(
	t *testing.T,
	repositories []githubapp.Repository,
) (*clusterLeader, *clusteredDaemon) {
	t.Helper()
	state := &fakeClusterState{statuses: []cluster.ScaleSetStatus{{
		Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: true,
	}}}
	daemon := activeClusteredDaemon(t, state)
	daemon.logger = slog.Default()
	daemon.discoverer = staticDiscoverer{result: githubapp.DiscoveryResult{Repositories: repositories}}
	leader := newClusterLeader(&daemon, &fakeClusterLease{alive: true})
	daemon.leader = leader
	return leader, &daemon
}

func repositoryRepairState(t *testing.T, daemon *clusteredDaemon) *fakeClusterState {
	t.Helper()
	state, ok := daemon.nodes.(*fakeClusterState)
	if !ok {
		t.Fatal("repository repair test daemon does not use fake cluster state")
	}
	return state
}

func successfulRepositoryRepairClient() *fakeScaleSetClient {
	messageClient := &blockingMessageClient{entered: make(chan struct{}), exited: make(chan struct{})}
	return fakeExistingScaleSetClient(
		&upstream.RunnerGroup{ID: 1},
		&upstream.RunnerScaleSet{ID: 2, Name: "linux"},
		scaleSetMessageClient{Client: messageClient},
	)
}

func timeoutIsolationClientFactory() func(
	string,
	githubapp.InstallationID,
	config.GitHubAppCredentials,
) (scaleSetClient, error) {
	timedOutClient := successfulRepositoryRepairClient()
	timedOutClient.getRunnerGroupByName = func(ctx context.Context, _ string) (*upstream.RunnerGroup, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	workingClient := successfulRepositoryRepairClient()
	return func(repository string, _ githubapp.InstallationID, _ config.GitHubAppCredentials) (scaleSetClient, error) {
		if repository == "https://github.com/acme/a-timeout" {
			return timedOutClient, nil
		}
		return workingClient, nil
	}
}

func assertTimeoutIsolation(t *testing.T, leader *clusterLeader) {
	t.Helper()
	workingKey := repositoryScaleSetSessionKey{repository: "https://github.com/acme/b-working", scaleSet: "linux"}
	if leader.sessions[workingKey] == nil {
		t.Fatal("later repository did not activate after an earlier repository timeout")
	}
	if _, pending := leader.pendingRepositoryRepairs["https://github.com/acme/a-timeout"]; !pending {
		t.Fatal("timed-out repository was not retained for repair")
	}
}

type transientRepositoryClientFactory struct {
	calls             map[string]int
	failingRepository string
}

func (factory *transientRepositoryClientFactory) client(
	repository string,
	_ githubapp.InstallationID,
	_ config.GitHubAppCredentials,
) (*fakeScaleSetClient, error) {
	factory.calls[repository]++
	if repository == factory.failingRepository && factory.calls[repository] == 1 {
		return nil, errors.New("temporarily unavailable")
	}
	return successfulRepositoryRepairClient(), nil
}

func assertRepositoryRepairDelay(
	t *testing.T,
	leader *clusterLeader,
	now *time.Time,
	attempts *int,
	delay time.Duration,
) {
	t.Helper()
	attemptsBeforeDelay := *attempts
	*now = now.Add(delay - time.Nanosecond)
	leader.repairPendingRepositories(context.Background())
	if *attempts != attemptsBeforeDelay {
		t.Fatalf("attempts before delay %s = %d, want %d", delay, *attempts, attemptsBeforeDelay)
	}
	*now = now.Add(time.Nanosecond)
	leader.repairPendingRepositories(context.Background())
	if *attempts != attemptsBeforeDelay+1 {
		t.Fatalf("attempts after delay %s = %d, want %d", delay, *attempts, attemptsBeforeDelay+1)
	}
}

type cancellableRepositoryClientFactory struct {
	attempts int
	entered  chan struct{}
}

func (factory *cancellableRepositoryClientFactory) client(
	string,
	githubapp.InstallationID,
	config.GitHubAppCredentials,
) (*fakeScaleSetClient, error) {
	factory.attempts++
	if factory.attempts == 1 {
		return nil, errors.New("unavailable")
	}
	client := successfulRepositoryRepairClient()
	client.getRunnerGroupByName = func(ctx context.Context, _ string) (*upstream.RunnerGroup, error) {
		close(factory.entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return client, nil
}

func runRepairUntilLeadershipStops(leader *clusterLeader, entered <-chan struct{}) {
	repairDone := make(chan struct{})
	go func() {
		leader.repairPendingRepositories(context.Background())
		close(repairDone)
	}()
	<-entered
	leader.stop(context.Background())
	<-repairDone
}

func blockingRepositoryClientFactory(entered chan<- struct{}) func(
	string,
	githubapp.InstallationID,
	config.GitHubAppCredentials,
) (scaleSetClient, error) {
	client := successfulRepositoryRepairClient()
	client.getRunnerGroupByName = func(ctx context.Context, _ string) (*upstream.RunnerGroup, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return client, nil
	}
}
