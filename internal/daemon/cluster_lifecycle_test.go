package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
)

func TestRunClusteredDaemonPublishesStartupAndShutdown(t *testing.T) {
	configured := clusteredConfig(t)
	state := &fakeClusterState{}
	ctx, cancel := context.WithCancel(context.Background())
	daemonUnderTest := clusteredDaemonForTest(state)
	daemonUnderTest.notifier = cancellingNotifier{cancel: cancel}

	if err := runClusteredDaemon(ctx, Options{}, daemonUnderTest, configured); err != nil {
		t.Fatal(err)
	}
	if state.migrations != 1 || len(state.publications) != 2 {
		t.Fatalf("migrations=%d publications=%#v", state.migrations, state.publications)
	}
	if !state.publications[0].healthy || state.publications[1].healthy {
		t.Fatalf("startup and shutdown health was not published: %#v", state.publications)
	}
	if state.publications[0].nodeID != "runner-host-1" || state.publications[1].nodeID != "runner-host-1" {
		t.Fatalf("operating-system hostname was not used as node identity: %#v", state.publications)
	}
	contribution := state.publications[0].contributions[0]
	if contribution.ScaleSet != "linux" || contribution.MaxRunners != 10 || !contribution.Healthy {
		t.Fatalf("host-local contribution was not published: %#v", contribution)
	}
}

func TestClusteredLifecycleSmoke(t *testing.T) {
	ctx, fixture := clusteredLifecycleFixture(t)
	if err := runClusteredDaemon(ctx, Options{ReloadRequests: fixture.reloadRequests}, fixture.daemon, fixture.config); err != nil {
		t.Fatal(err)
	}
	if !fixture.notifier.ready || fixture.state.migrations != 1 || len(fixture.state.publications) != 3 {
		t.Fatalf("ready=%t migrations=%d publications=%d", fixture.notifier.ready, fixture.state.migrations, len(fixture.state.publications))
	}
	if !fixture.state.publications[0].contributions[0].Healthy || fixture.state.publications[1].contributions[0].Healthy {
		t.Fatalf("provider health transition was not published: %#v", fixture.state.publications)
	}
	if fixture.state.publications[2].healthy {
		t.Fatalf("shutdown did not withdraw the node: %#v", fixture.state.publications[2])
	}
}

type clusteredLifecycleTestFixture struct {
	config          config.Config
	state           *fakeClusterState
	daemon          *clusteredDaemon
	notifier        *clusterLifecycleNotifier
	reloadRequests  chan struct{}
	providerHealthy bool
}

func clusteredLifecycleFixture(t *testing.T) (context.Context, *clusteredLifecycleTestFixture) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	fixture := &clusteredLifecycleTestFixture{
		config: clusteredConfig(t), providerHealthy: true, reloadRequests: make(chan struct{}, 1),
		state: &fakeClusterState{
			leases:   []cluster.Lease{&fakeClusterLease{alive: true}},
			statuses: []cluster.ScaleSetStatus{{Name: "linux", Compatible: false}},
		},
	}
	fixture.notifier = &clusterLifecycleNotifier{onReady: fixture.onReady}
	fixture.state.onPublish = func() {
		if len(fixture.state.publications) == 2 {
			cancel()
		}
	}
	fixture.daemon = clusteredDaemonForTest(fixture.state)
	fixture.daemon.loadConfig = func(string) (config.Config, error) { return fixture.config, nil }
	fixture.daemon.notifier = fixture.notifier
	fixture.daemon.providerHealthy = fixture.providerHealth
	return ctx, fixture
}

func (fixture *clusteredLifecycleTestFixture) onReady() {
	fixture.providerHealthy = false
	fixture.reloadRequests <- struct{}{}
}

func (fixture *clusteredLifecycleTestFixture) providerHealth(context.Context, config.ScaleSetConfig) error {
	if fixture.providerHealthy {
		return nil
	}
	return errors.New("provider unavailable")
}

func TestClusteredLeaderRemainsOperationalWithRepositoryRepairPending(t *testing.T) {
	ctx, fixture := clusteredLeaderStatusFixture(t)
	if err := runClusteredDaemon(ctx, Options{}, fixture.daemon, fixture.config); err != nil {
		t.Fatal(err)
	}

	expected := "STATUS=leader reconciling\nSTATUS=leader operational"
	if actual := strings.Join(fixture.notifier.statuses, "\n"); actual != expected {
		t.Fatalf("cluster status snapshot mismatch\nexpected: %s\nactual:   %s", expected, actual)
	}
	if len(fixture.activeSessions) != 0 {
		t.Fatalf("sessions active before the independent repository workers completed = %#v", fixture.activeSessions)
	}
}

type clusteredLeaderStatusTestFixture struct {
	config                   config.Config
	daemon                   *clusteredDaemon
	notifier                 *clusterStatusSnapshotNotifier
	successfulClient         *fakeScaleSetClient
	repositoryWorkMayProceed chan struct{}
	activeSessions           []repositoryScaleSetSessionKey
}

func clusteredLeaderStatusFixture(t *testing.T) (context.Context, *clusteredLeaderStatusTestFixture) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	fixture := newClusteredLeaderStatusTestFixture(t)
	fixture.configureDaemon(cancel)
	return ctx, fixture
}

func newClusteredLeaderStatusTestFixture(t *testing.T) *clusteredLeaderStatusTestFixture {
	t.Helper()
	state := &fakeClusterState{
		leases:   []cluster.Lease{&fakeClusterLease{alive: true}},
		statuses: []cluster.ScaleSetStatus{{Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: true}},
	}
	fixture := &clusteredLeaderStatusTestFixture{
		config: clusteredConfig(t), daemon: clusteredDaemonForTest(state),
		repositoryWorkMayProceed: make(chan struct{}, 1),
	}
	fixture.successfulClient = fixture.newOperationallyBlockedClient()
	return fixture
}

func (fixture *clusteredLeaderStatusTestFixture) newOperationallyBlockedClient() *fakeScaleSetClient {
	runnerGroup := &upstream.RunnerGroup{ID: 1}
	client := fakeExistingScaleSetClient(
		runnerGroup, &upstream.RunnerScaleSet{ID: 2, Name: "linux"},
		scaleSetMessageClient{Client: &blockingMessageClient{entered: make(chan struct{}), exited: make(chan struct{})}},
	)
	client.getRunnerGroupByName = func(ctx context.Context, _ string) (*upstream.RunnerGroup, error) {
		select {
		case <-fixture.repositoryWorkMayProceed:
			return runnerGroup, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return client
}

func (fixture *clusteredLeaderStatusTestFixture) configureDaemon(cancel context.CancelFunc) {
	fixture.notifier = &clusterStatusSnapshotNotifier{cancel: cancel, onStatus: fixture.captureActiveSessions}
	fixture.daemon.newDiscoverer = func(config.GitHubAppCredentials) (repositoryDiscoverer, error) {
		return fixture.discovery(), nil
	}
	fixture.daemon.newGitHubClient = func(
		repositoryURL string,
		_ githubapp.InstallationID,
		_ config.GitHubAppCredentials,
	) (scaleSetClient, error) {
		return fixture.clientForRepository(repositoryURL)
	}
	fixture.daemon.notifier = fixture.notifier
}

func (*clusteredLeaderStatusTestFixture) discovery() staticDiscoverer {
	return staticDiscoverer{result: githubapp.DiscoveryResult{Repositories: []githubapp.Repository{
		{InstallationID: 1, URL: "https://github.com/acme/working"},
		{InstallationID: 2, URL: "https://github.com/acme/failing"},
	}}}
}

func (fixture *clusteredLeaderStatusTestFixture) clientForRepository(repositoryURL string) (*fakeScaleSetClient, error) {
	if repositoryURL == "https://github.com/acme/failing" {
		return nil, errors.New("session unavailable")
	}
	return fixture.successfulClient, nil
}

func (fixture *clusteredLeaderStatusTestFixture) captureActiveSessions(status clusterServiceStatus) {
	if status != clusterServiceStatusLeaderOperational {
		return
	}
	leader := fixture.daemon.currentLeader()
	leader.sessionsMutex.Lock()
	defer leader.sessionsMutex.Unlock()
	for key := range leader.sessions {
		fixture.activeSessions = append(fixture.activeSessions, key)
	}
	select {
	case fixture.repositoryWorkMayProceed <- struct{}{}:
	default:
	}
}

func TestRunClusteredDaemonReconcilesImmediatelyWhenElected(t *testing.T) {
	configured := clusteredConfig(t)
	scaleSetReads := 0
	ctx, cancel := context.WithCancel(context.Background())
	state := &fakeClusterState{
		leases:      []cluster.Lease{&fakeClusterLease{alive: true}},
		statuses:    []cluster.ScaleSetStatus{{Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: false}},
		onScaleSets: func() { scaleSetReads++; cancel() },
	}
	daemonUnderTest := clusteredDaemonForTest(state)
	ready := false
	daemonUnderTest.notifier = readyRecordingNotifier{ready: &ready}
	if err := runClusteredDaemon(ctx, Options{}, daemonUnderTest, configured); err != nil {
		t.Fatal(err)
	}
	if scaleSetReads != 1 {
		t.Fatalf("startup reconciliation read scale sets %d times", scaleSetReads)
	}
}

func TestLeadershipContenderRetriesThenAcquires(t *testing.T) {
	attempts := 0
	waits := 0
	state := &fakeClusterState{waitForLeader: func(context.Context) (cluster.Lease, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("temporary failure")
		}
		return &cluster.LeaderLease{}, nil
	}}
	results := make(chan leadershipResult, 1)
	daemon := clusteredDaemon{
		waitForLeader: fakeWaitForLeader(state), logger: slog.Default(),
		waitBeforeLeadershipRetry: func(context.Context, time.Duration) bool {
			waits++
			return true
		},
	}
	daemon.contendForLeadership(context.Background(), results, make(chan struct{}))
	if result := <-results; result.lease == nil || attempts != 2 || waits != 1 {
		t.Fatalf("result=%#v attempts=%d waits=%d", result, attempts, waits)
	}
}

func TestCanceledLeadershipContenderStopsWithoutAcquiring(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	state := &fakeClusterState{}
	daemon := clusteredDaemon{waitForLeader: fakeWaitForLeader(state), logger: slog.Default()}
	daemon.contendForLeadership(ctx, make(chan leadershipResult, 1), make(chan struct{}))
}

func TestCanceledLeadershipDeliveryClosesLease(t *testing.T) {
	logs := &bytes.Buffer{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lease := &fakeClusterLease{closeErr: errors.New("close failed")}
	daemon := clusteredDaemon{nodeID: "runner-host-1", logger: newClusterLogSnapshotLogger(logs)}
	daemon.deliverLeadership(ctx, make(chan leadershipResult), lease)
	if !lease.closed {
		t.Fatal("canceled delivery did not close lease")
	}
	assertClusterLogSnapshot(t, logs,
		`{"level":"WARN","msg":"close canceled leader lease failed","hostname":"runner-host-1","error":"close failed"}`)
}

func TestLeadershipContenderShutdownClosesUnclaimedLease(t *testing.T) {
	logs := &bytes.Buffer{}
	lease := &fakeClusterLease{closeErr: errors.New("close failed")}
	done := make(chan struct{})
	close(done)
	results := make(chan leadershipResult, 1)
	results <- leadershipResult{lease: lease}
	daemon := clusteredDaemon{
		nodeID: "runner-host-1", logger: newClusterLogSnapshotLogger(logs), leadershipResults: results,
		stopContender: func() {}, contenderDone: done,
	}
	daemon.stopLeadershipContender(context.Background())
	if !lease.closed {
		t.Fatal("unclaimed lease was not closed")
	}
	assertClusterLogSnapshot(t, logs,
		`{"level":"WARN","msg":"close unclaimed leader lease failed","hostname":"runner-host-1","error":"close failed"}`)
	daemon.stopLeadershipContender(context.Background())
}

func TestStartingLeadershipContenderTwiceOpensOneWaiter(t *testing.T) {
	waiterStarted := make(chan struct{}, 2)
	state := &fakeClusterState{waitForLeader: func(ctx context.Context) (cluster.Lease, error) {
		waiterStarted <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	daemon := clusteredDaemon{waitForLeader: fakeWaitForLeader(state), logger: slog.Default()}
	ctx, cancel := context.WithCancel(context.Background())
	daemon.startContender(ctx)
	<-waiterStarted
	daemon.startContender(ctx)
	select {
	case <-waiterStarted:
		t.Fatal("second contender opened another PostgreSQL waiter")
	default:
	}
	cancel()
	daemon.stopLeadershipContender(context.Background())
}

func TestLeadershipRetryStopsWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	daemon := clusteredDaemon{waitBeforeLeadershipRetry: func(context.Context, time.Duration) bool {
		t.Fatal("canceled retry invoked its wait boundary")
		return true
	}}
	if daemon.waitForLeadershipRetry(ctx) {
		t.Fatal("canceled retry waited")
	}
}

func TestLeadershipRetryUsesDefaultDelay(t *testing.T) {
	var observedDelay time.Duration
	daemon := clusteredDaemon{waitBeforeLeadershipRetry: func(_ context.Context, delay time.Duration) bool {
		observedDelay = delay
		return true
	}}
	if !daemon.waitForLeadershipRetry(context.Background()) {
		t.Fatal("elapsed retry did not continue leadership contention")
	}
	if observedDelay != clusterLeadershipRetryDelay {
		t.Fatalf("leadership retry delay = %s", observedDelay)
	}
}

func TestLeadershipRetryUsesConfiguredDelay(t *testing.T) {
	configuredDelay := 17 * time.Second
	var observedDelay time.Duration
	daemon := clusteredDaemon{
		leadershipRetryDelay: configuredDelay,
		waitBeforeLeadershipRetry: func(_ context.Context, delay time.Duration) bool {
			observedDelay = delay
			return true
		},
	}
	if !daemon.waitForLeadershipRetry(context.Background()) {
		t.Fatal("elapsed retry did not continue leadership contention")
	}
	if observedDelay != configuredDelay {
		t.Fatalf("leadership retry delay = %s", observedDelay)
	}
}

func TestLeadershipContenderStopsWhenCanceledDuringRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	state := &fakeClusterState{waitForLeader: func(context.Context) (cluster.Lease, error) {
		attempts++
		return nil, errors.New("temporary failure")
	}}
	daemon := clusteredDaemon{
		waitForLeader: fakeWaitForLeader(state), logger: slog.Default(),
		waitBeforeLeadershipRetry: func(retryCtx context.Context, _ time.Duration) bool {
			cancel()
			return retryCtx.Err() == nil
		},
	}
	daemon.contendForLeadership(ctx, make(chan leadershipResult, 1), make(chan struct{}))
	if attempts != 1 {
		t.Fatalf("leadership attempts = %d", attempts)
	}
}

func TestLeadershipContenderFailureLogsNodeIdentity(t *testing.T) {
	logs := &bytes.Buffer{}
	ctx, cancel := context.WithCancel(context.Background())
	state := &fakeClusterState{waitForLeader: func(context.Context) (cluster.Lease, error) {
		return nil, errors.New("temporary failure")
	}}
	daemon := clusteredDaemon{
		nodeID: "runner-host-1", waitForLeader: fakeWaitForLeader(state),
		logger: newClusterLogSnapshotLogger(cancelingWriter{cancel: cancel, writer: logs}),
	}

	daemon.contendForLeadership(ctx, make(chan leadershipResult, 1), make(chan struct{}))

	assertClusterLogSnapshot(t, logs,
		`{"level":"ERROR","msg":"leader contender failed","hostname":"runner-host-1","error":"temporary failure"}`)
}

type cancelingWriter struct {
	cancel context.CancelFunc
	writer io.Writer
}

type clusterStatusSnapshotNotifier struct {
	statuses []string
	cancel   context.CancelFunc
	onStatus func(clusterServiceStatus)
}

func (*clusterStatusSnapshotNotifier) Ready(context.Context) error     { return nil }
func (*clusterStatusSnapshotNotifier) Reloading(context.Context) error { return nil }

func (notifier *clusterStatusSnapshotNotifier) Status(_ context.Context, status clusterServiceStatus) error {
	notifier.statuses = append(notifier.statuses, "STATUS="+string(status))
	if notifier.onStatus != nil {
		notifier.onStatus(status)
	}
	if status != clusterServiceStatusLeaderReconciling && notifier.cancel != nil {
		notifier.cancel()
	}
	return nil
}

func (writer cancelingWriter) Write(contents []byte) (int, error) {
	writer.cancel()
	if writer.writer != nil {
		written, err := writer.writer.Write(contents)
		if err != nil {
			return written, fmt.Errorf("capture cluster log: %w", err)
		}
		return written, nil
	}
	return len(contents), nil
}
