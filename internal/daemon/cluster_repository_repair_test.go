package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
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

func TestBlockedRepositoryDoesNotDelayLaterRepositoryActivation(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, []githubapp.Repository{
		{InstallationID: 1, URL: "https://github.com/acme/a-blocked"},
		{InstallationID: 2, URL: "https://github.com/acme/b-working"},
	})
	blockedEntered := make(chan struct{})
	releaseBlocked := make(chan struct{})
	closeSignalOnCleanup(t, releaseBlocked)
	workingEntered := make(chan struct{})
	daemon.newGitHubClient = blockedAndWorkingClientFactory(blockedEntered, releaseBlocked, workingEntered)
	reconciled := make(chan bool, 1)
	go func() { reconciled <- leader.reconcile(context.Background()) }()
	awaitCompletionSignal(t, blockedEntered)
	awaitCompletionSignal(t, workingEntered)
	close(releaseBlocked)
	if !awaitBoolSignal(t, reconciled) {
		t.Fatal("repository reconciliation did not remain operational")
	}
}

func blockedAndWorkingClientFactory(
	blockedEntered chan<- struct{},
	releaseBlocked <-chan struct{},
	workingEntered chan<- struct{},
) func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
	blockedClient := successfulRepositoryRepairClient()
	blockedClient.getRunnerGroupByName = func(context.Context, string) (*upstream.RunnerGroup, error) {
		close(blockedEntered)
		<-releaseBlocked
		return &upstream.RunnerGroup{ID: 1}, nil
	}
	workingClient := successfulRepositoryRepairClient()
	return func(
		repository string,
		_ githubapp.InstallationID,
		_ config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		if repository == "https://github.com/acme/a-blocked" {
			return blockedClient, nil
		}
		close(workingEntered)
		return workingClient, nil
	}
}

func TestRepositoryRepairRetriesOnlyPendingRepository(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, []githubapp.Repository{
		{InstallationID: 1, URL: "https://github.com/acme/a-failing"},
		{InstallationID: 1, URL: "https://github.com/acme/b-working"},
	})
	var nowUnixNano atomic.Int64
	nowUnixNano.Store(time.Date(2026, 8, 2, 18, 0, 0, 0, time.UTC).UnixNano())
	leader.now = func() time.Time { return time.Unix(0, nowUnixNano.Load()).UTC() }
	clientFactory := &transientRepositoryClientFactory{calls: map[string]int{}, failingRepository: "https://github.com/acme/a-failing"}
	daemon.newGitHubClient = func(repository string, installationID githubapp.InstallationID, credentials config.GitHubAppCredentials) (scaleSetClient, error) {
		return clientFactory.client(repository, installationID, credentials)
	}

	if !leader.reconcile(context.Background()) {
		t.Fatal("repository failure degraded global reconciliation")
	}
	awaitInitialRepositoryFailure(t, leader, clientFactory)
	nowUnixNano.Add(int64(5 * time.Second))
	leader.repairPendingRepositories(context.Background())
	assertTargetedRepositoryRetry(t, leader, clientFactory)
	leader.stop(context.Background())
}

func TestOrdinaryReconciliationDoesNotRepeatPendingRepositoryBeforeRetry(t *testing.T) {
	leader, attempts := repeatedlyFailingRepositoryLeader(t)
	assertOrdinaryReconciliationCoalesced(t, leader, attempts)
	repair := pendingRepairSnapshot(leader, "https://github.com/acme/failing")
	assertDuplicateRepairFailureCoalesced(t, leader, repair)
	assertChangedDesiredStateReceivesFreshAttempt(t, leader, repair, attempts)
	leader.stop(context.Background())
}

func repeatedlyFailingRepositoryLeader(t *testing.T) (*clusterLeader, *atomic.Int32) {
	t.Helper()
	leader, daemon := repositoryRepairTestLeader(t, []githubapp.Repository{{
		InstallationID: 1, URL: "https://github.com/acme/failing",
	}})
	attempts := &atomic.Int32{}
	daemon.newGitHubClient = func(
		string,
		githubapp.InstallationID,
		config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		attempts.Add(1)
		return nil, errors.New("unavailable")
	}
	return leader, attempts
}

func assertOrdinaryReconciliationCoalesced(t *testing.T, leader *clusterLeader, attempts *atomic.Int32) {
	t.Helper()
	firstReconcile := leader.reconcile(context.Background())
	secondReconcile := leader.reconcile(context.Background())
	if !firstReconcile || !secondReconcile {
		t.Fatal("repository repair degraded global reconciliation")
	}
	if attempts.Load() != 1 {
		t.Fatalf("ordinary reconciliation attempts = %d, want 1 before retry deadline", attempts.Load())
	}
}

func pendingRepairSnapshot(leader *clusterLeader, repository string) repositoryRepair {
	leader.repairMutex.Lock()
	repair := leader.pendingRepositoryRepairs[repository]
	leader.repairMutex.Unlock()
	return repair
}

func assertDuplicateRepairFailureCoalesced(t *testing.T, leader *clusterLeader, repair repositoryRepair) {
	t.Helper()
	if repair.attempts != 1 {
		t.Fatalf("recorded repair attempts = %d, want 1", repair.attempts)
	}
	leader.recordRepositoryRepair(
		context.Background(), repair.repository, repair.statuses, errors.New("duplicate failure"),
	)
	repair = pendingRepairSnapshot(leader, "https://github.com/acme/failing")
	if repair.attempts != 1 {
		t.Fatalf("duplicate failure advanced repair attempts to %d", repair.attempts)
	}
}

func assertChangedDesiredStateReceivesFreshAttempt(
	t *testing.T,
	leader *clusterLeader,
	repair repositoryRepair,
	attempts *atomic.Int32,
) {
	t.Helper()
	changedDesired := map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{
		{repository: "https://github.com/acme/failing", scaleSet: "linux"}: {
			Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 2, Compatible: true,
		},
	}
	if err := leader.startMissingSessions(
		context.Background(), changedDesired,
		map[string]githubapp.Repository{repair.repository.URL: repair.repository},
	); err == nil {
		t.Fatal("expected changed desired state to receive a fresh repair attempt")
	}
	if attempts.Load() != 2 {
		t.Fatalf("changed desired-state attempts = %d, want 2", attempts.Load())
	}
}

func awaitInitialRepositoryFailure(
	t *testing.T,
	leader *clusterLeader,
	clientFactory *transientRepositoryClientFactory,
) {
	t.Helper()
	awaitRepositoryRepairCondition(t, func() bool {
		calls := clientFactory.callSnapshot()
		leader.repairMutex.Lock()
		_, pending := leader.pendingRepositoryRepairs["https://github.com/acme/a-failing"]
		leader.repairMutex.Unlock()
		return calls["https://github.com/acme/a-failing"] == 1 &&
			calls["https://github.com/acme/b-working"] == 1 && pending
	})
}

func assertTargetedRepositoryRetry(
	t *testing.T,
	leader *clusterLeader,
	clientFactory *transientRepositoryClientFactory,
) {
	t.Helper()
	calls := clientFactory.callSnapshot()
	if calls["https://github.com/acme/a-failing"] != 2 || calls["https://github.com/acme/b-working"] != 1 {
		t.Fatalf("repository calls = %#v, want only the failed repository retried", calls)
	}
	leader.repairMutex.Lock()
	pendingRepairs := len(leader.pendingRepositoryRepairs)
	leader.repairMutex.Unlock()
	if pendingRepairs != 0 {
		t.Fatalf("pending repairs = %d, want repaired repository removed", pendingRepairs)
	}
}

func TestRepositoryRepairUsesBoundedBackoff(t *testing.T) {
	leader, daemon := repositoryRepairTestLeader(t, []githubapp.Repository{{
		InstallationID: 1, URL: "https://github.com/acme/failing",
	}})
	now := time.Date(2026, 8, 2, 18, 0, 0, 0, time.UTC)
	leader.now = func() time.Time { return now }
	var attempts atomic.Int32
	daemon.newGitHubClient = func(
		string,
		githubapp.InstallationID,
		config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		attempts.Add(1)
		return nil, errors.New("unavailable")
	}

	if !leader.reconcile(context.Background()) {
		t.Fatal("repository failure degraded global reconciliation")
	}
	awaitRepositoryRepairCount(t, leader)
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
	var attempts atomic.Int32
	daemon.newGitHubClient = func(
		string,
		githubapp.InstallationID,
		config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		attempts.Add(1)
		return nil, errors.New("unavailable")
	}
	if !leader.reconcile(context.Background()) {
		t.Fatal("failed repository was not retained for repair")
	}
	awaitRepositoryRepairCount(t, leader)
	daemon.discoverer = staticDiscoverer{}
	if !leader.reconcile(context.Background()) {
		t.Fatal("repository removal degraded global reconciliation")
	}
	if repositoryRepairCount(leader) != 0 {
		t.Fatal("removed repository retained obsolete repair work")
	}
	leader.repairPendingRepositories(context.Background())
	if attempts.Load() != 1 {
		t.Fatalf("repair attempts after removal = %d, want no retry", attempts.Load())
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
	if !leader.reconcile(context.Background()) {
		t.Fatal("failed repository was not retained for repair")
	}
	awaitRepositoryRepairCount(t, leader)
	repositoryRepairState(t, daemon).statuses = nil
	if !leader.reconcile(context.Background()) {
		t.Fatal("scale-set withdrawal degraded global reconciliation")
	}
	if repositoryRepairCount(leader) != 0 {
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
	awaitRepositoryRepairCount(t, leader)
	now = now.Add(5 * time.Second)
	runRepairUntilLeadershipStops(leader, clientFactory.entered)
	if repositoryRepairCount(leader) != 0 {
		t.Fatal("leadership loss retained repository repair work")
	}
}

func TestRepositoryRepairLogsLifecycle(t *testing.T) {
	logs := &synchronizedLogBuffer{}
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
	assertClusterLogSnapshot(t, withoutClusterLogLinesContaining(logs.String(), `"action":"assigned_recovery"`),
		"{\"level\":\"ERROR\",\"msg\":\"leader repository session group failed\",\"repository\":\"https://github.com/acme/retry\",\"error\":\"unavailable\"}\n"+
			"{\"level\":\"WARN\",\"msg\":\"repository_repair\",\"action\":\"repository_repair\",\"phase\":\"pending\",\"repository\":\"https://github.com/acme/retry\",\"attempt\":1,\"retryAfter\":5000000000,\"error\":\"unavailable\"}\n"+
			"{\"level\":\"INFO\",\"msg\":\"repository_repair\",\"action\":\"repository_repair\",\"phase\":\"retrying\",\"repository\":\"https://github.com/acme/retry\",\"attempt\":2}\n"+
			"{\"level\":\"INFO\",\"msg\":\"session_statistics\",\"action\":\"session_statistics\",\"phase\":\"observed\",\"trigger\":\"session\",\"repository\":\"https://github.com/acme/retry\",\"scaleSet\":\"linux\",\"assignedJobs\":0,\"availableJobs\":0,\"acquiredJobs\":0,\"runningJobs\":0,\"registeredRunners\":0,\"busyRunners\":0,\"idleRunners\":0}\n"+
			"{\"level\":\"INFO\",\"msg\":\"assigned_recovery_trigger\",\"action\":\"assigned_recovery_trigger\",\"phase\":\"queued\",\"repository\":\"https://github.com/acme/retry\",\"scaleSet\":\"linux\",\"trigger\":\"session\",\"epoch\":0,\"queueDepth\":1,\"activeWorkers\":0}\n"+
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
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{alive: true})
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
	awaitRepositoryRepairCondition(t, func() bool {
		leader.sessionsMutex.Lock()
		working := leader.sessions[workingKey] != nil
		leader.sessionsMutex.Unlock()
		leader.repairMutex.Lock()
		_, pending := leader.pendingRepositoryRepairs["https://github.com/acme/a-timeout"]
		leader.repairMutex.Unlock()
		return working && pending
	})
}

type transientRepositoryClientFactory struct {
	mutex             sync.Mutex
	calls             map[string]int
	failingRepository string
}

func (factory *transientRepositoryClientFactory) client(
	repository string,
	_ githubapp.InstallationID,
	_ config.GitHubAppCredentials,
) (*fakeScaleSetClient, error) {
	factory.mutex.Lock()
	defer factory.mutex.Unlock()
	factory.calls[repository]++
	if repository == factory.failingRepository && factory.calls[repository] == 1 {
		return nil, errors.New("temporarily unavailable")
	}
	return successfulRepositoryRepairClient(), nil
}

func (factory *transientRepositoryClientFactory) callSnapshot() map[string]int {
	factory.mutex.Lock()
	defer factory.mutex.Unlock()
	return map[string]int{
		"https://github.com/acme/a-failing": factory.calls["https://github.com/acme/a-failing"],
		"https://github.com/acme/b-working": factory.calls["https://github.com/acme/b-working"],
	}
}

func awaitRepositoryRepairCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for repository repair state")
}

func repositoryRepairCount(leader *clusterLeader) int {
	leader.repairMutex.Lock()
	defer leader.repairMutex.Unlock()
	return len(leader.pendingRepositoryRepairs)
}

func awaitRepositoryRepairCount(t *testing.T, leader *clusterLeader) {
	t.Helper()
	awaitRepositoryRepairCondition(t, func() bool { return repositoryRepairCount(leader) == 1 })
}

func assertRepositoryRepairDelay(
	t *testing.T,
	leader *clusterLeader,
	now *time.Time,
	attempts *atomic.Int32,
	delay time.Duration,
) {
	t.Helper()
	attemptsBeforeDelay := attempts.Load()
	*now = now.Add(delay - time.Nanosecond)
	leader.repairPendingRepositories(context.Background())
	if attempts.Load() != attemptsBeforeDelay {
		t.Fatalf("attempts before delay %s = %d, want %d", delay, attempts.Load(), attemptsBeforeDelay)
	}
	*now = now.Add(time.Nanosecond)
	leader.repairPendingRepositories(context.Background())
	if attempts.Load() != attemptsBeforeDelay+1 {
		t.Fatalf("attempts after delay %s = %d, want %d", delay, attempts.Load(), attemptsBeforeDelay+1)
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
