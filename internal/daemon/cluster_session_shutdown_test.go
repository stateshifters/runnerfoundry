package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
)

type blockingSessionCloseClient struct {
	tracker *sessionCloseTracker
}

type sessionCloseTracker struct {
	mutex   sync.Mutex
	active  int
	maximum int
	entered chan struct{}
}

func TestClusterLeaderReportsDurableSchedulingFailure(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{assignErr: errors.New("database unavailable")}
	daemon := clusteredDaemonForTest(state)
	daemon.assignments = state
	daemon.logger = slog.New(slog.NewTextHandler(logs, nil))
	leader := newClusterLeader(context.Background(), daemon, &fakeClusterLease{alive: true})
	t.Cleanup(func() {
		leader.stopRepositoryWorkers()
		leader.repositoryWorkers.wait()
		leader.scheduler.wait()
	})

	leader.scheduler.scheduleAndReport(context.Background(), "linux")
	output := logs.String()
	if !strings.Contains(output, "capacity obligation scheduling failed") ||
		!strings.Contains(output, "scaleSet=linux") || !strings.Contains(output, "database unavailable") {
		t.Fatalf("scheduling failure log = %q", output)
	}
}

func (client blockingSessionCloseClient) GetMessage(context.Context, int, int) (*upstream.RunnerScaleSetMessage, error) {
	return nil, errors.New("no message")
}

func (blockingSessionCloseClient) DeleteMessage(context.Context, int) error { return nil }

func (blockingSessionCloseClient) AcquireJobs(context.Context, []int64) ([]int64, error) {
	return nil, nil
}

func (client blockingSessionCloseClient) Close(ctx context.Context) error {
	client.tracker.mutex.Lock()
	client.tracker.active++
	client.tracker.maximum = max(client.tracker.maximum, client.tracker.active)
	client.tracker.mutex.Unlock()
	client.tracker.entered <- struct{}{}
	<-ctx.Done()
	client.tracker.mutex.Lock()
	client.tracker.active--
	client.tracker.mutex.Unlock()
	return fmt.Errorf("close blocked message session: %w", ctx.Err())
}

func (blockingSessionCloseClient) Session() upstream.RunnerScaleSetSession {
	return upstream.RunnerScaleSetSession{Statistics: &upstream.RunnerScaleSetStatistic{}}
}

func TestRepositorySessionLivesUntilLeadershipEnds(t *testing.T) {
	leader, messageClient := activeRepositorySession(t)
	pollerCtx := <-messageClient.contexts
	if pollerCtx.Err() != nil {
		t.Fatalf("repository polling ended with reconciliation: %v", pollerCtx.Err())
	}
	leader.stop(context.Background())
	<-messageClient.exited
}

func activeRepositorySession(t *testing.T) (*clusterLeader, *blockingMessageClient) {
	t.Helper()
	messageClient := &blockingMessageClient{
		entered: make(chan struct{}), exited: make(chan struct{}), contexts: make(chan context.Context, 1),
	}
	daemon := activeClusteredDaemon(t, &fakeClusterState{})
	daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return fakeExistingScaleSetClient(
			&upstream.RunnerGroup{ID: 1},
			&upstream.RunnerScaleSet{ID: 2},
			scaleSetMessageClient{Client: messageClient},
		), nil
	}
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{alive: true})
	operationCtx, cancelOperation := context.WithCancel(context.Background())
	session, err := leader.startSession(operationCtx, githubapp.Repository{URL: "repo", InstallationID: 1}, cluster.ScaleSetStatus{
		Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = leader.activateRepositorySessionGeneration(operationCtx, "repo", []cluster.ScaleSetStatus{{
		Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: true,
	}}, []*repositoryScaleSetSession{session}); err != nil {
		t.Fatal(err)
	}
	<-messageClient.entered
	cancelOperation()
	return leader, messageClient
}

func TestLeadershipLossCancelsBlockedReconciliation(t *testing.T) {
	discoveryStarted := make(chan struct{})
	daemon := activeClusteredDaemon(t, &fakeClusterState{})
	daemon.discoverer = blockingDiscoverer{started: discoveryStarted}
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{alive: true})
	reconciliationDone := make(chan struct{})
	go func() {
		leader.reconcile(context.Background())
		close(reconciliationDone)
	}()
	<-discoveryStarted
	leader.stop(context.Background())
	<-reconciliationDone
}

func TestLeaderDoesNotRestartExistingRepositorySession(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.newGitHubClient = func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errors.New("must not be called")
	}
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{})
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	session := &repositoryScaleSetSession{key: key}
	leader.sessions[key] = session
	err := leader.startMissingSessions(context.Background(), map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{
		key: {Name: "linux", EffectiveCapacity: 1},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if leader.sessions[key] != session {
		t.Fatal("existing session was replaced")
	}
}

func TestLeaderRetainsCapacitySessionUntilRepositoryGroupCanBeReconciled(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{})
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	deletedScaleSetIDs := []int{}
	githubClient := fakeScaleSetDeletionClient{deleteRunnerScaleSet: func(_ context.Context, runnerScaleSetID int) error {
		deletedScaleSetIDs = append(deletedScaleSetIDs, runnerScaleSetID)
		return nil
	}}
	leader.sessions[key] = stoppedSession(key, githubClient, &fakeMessageClient{})
	if err := leader.stopOutdatedSessions(context.Background(), map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{
		key: {Name: "linux", EffectiveCapacity: 2},
	}, []cluster.ScaleSetStatus{{Name: "linux"}}); err != nil {
		t.Fatal(err)
	}
	if _, exists := leader.sessions[key]; !exists {
		t.Fatal("capacity session was removed before its repository group was audited")
	}
	if len(deletedScaleSetIDs) != 0 {
		t.Fatalf("declared scale set was deleted: %v", deletedScaleSetIDs)
	}
}

func TestLeaderSessionShutdownAttemptsMessageCloseAndScaleSetRemoval(t *testing.T) {
	logs := &bytes.Buffer{}
	closeErr := errors.New("close failed")
	deleteErr := errors.New("delete failed")
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.logger = newLoggerForWriter(logs, "json", "debug")
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{})
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	session := stoppedSession(
		key,
		fakeScaleSetDeletionClient{deleteRunnerScaleSet: func(context.Context, int) error { return deleteErr }},
		&fakeMessageClient{closeErr: closeErr},
	)
	err := leader.stopSession(context.Background(), session, removeGitHubScaleSet)
	if !errors.Is(err, closeErr) || !errors.Is(err, deleteErr) {
		t.Fatalf("session shutdown error = %v, want close and delete failures", err)
	}
	if !strings.Contains(logs.String(), "close leader message session failed") {
		t.Fatalf("expected message session close failure, got %q", logs.String())
	}
	assertStructuredAction(t, logs.Bytes(), "runner_scale_set_delete", "failed")
}

func TestLeaderSessionCloseFailureLogsNodeIdentity(t *testing.T) {
	logs := &bytes.Buffer{}
	daemon := activeClusteredDaemon(t, &fakeClusterState{})
	daemon.nodeID = "runner-host-1"
	daemon.logger = newClusterLogSnapshotLogger(logs)
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{})
	session := stoppedSession(
		repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"},
		fakeScaleSetDeletionClient{},
		&fakeMessageClient{closeErr: errors.New("close failed")},
	)

	if err := leader.stopSession(context.Background(), session, retainGitHubScaleSet); err == nil {
		t.Fatal("expected session close failure")
	}

	assertClusterLogSnapshot(t, logs,
		`{"level":"ERROR","msg":"close leader message session failed","hostname":"runner-host-1","error":"close message session: close failed"}`)
}

func TestLeaderStopClearsSessionsAfterLeaseCloseFailure(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.nodeID = "runner-host-1"
	daemon.logger = newClusterLogSnapshotLogger(logs)
	lease := &fakeClusterLease{closeErr: errors.New("failed")}
	leader := newClusterLeader(context.Background(), &daemon, lease)
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	leader.sessions[key] = stoppedSession(key, fakeScaleSetDeletionClient{}, &fakeMessageClient{})
	leader.scheduler.sessions[key] = leader.sessions[key]
	leader.stop(context.Background())
	if len(leader.sessions) != 0 || len(leader.scheduler.sessions) != 0 || !lease.closed {
		t.Fatalf("sessions=%v scheduled=%v leaseClosed=%t", leader.sessions, leader.scheduler.sessions, lease.closed)
	}
	assertClusterLogSnapshot(t, logs,
		`{"level":"WARN","msg":"close leader lease failed","hostname":"runner-host-1","error":"failed"}`)
}

func TestLeaderStopLogsSessionAndLeaseCloseFailuresSeparately(t *testing.T) {
	logs := &bytes.Buffer{}
	daemon := activeClusteredDaemon(t, &fakeClusterState{})
	daemon.nodeID = "runner-host-1"
	daemon.logger = newClusterLogSnapshotLogger(logs)
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{closeErr: errors.New("lease failed")})
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	leader.sessions[key] = stoppedSession(
		key,
		fakeScaleSetDeletionClient{},
		&fakeMessageClient{closeErr: errors.New("session failed")},
	)

	leader.stop(context.Background())

	assertClusterLogSnapshot(t, logs,
		"{\"level\":\"ERROR\",\"msg\":\"close leader message session failed\",\"hostname\":\"runner-host-1\",\"error\":\"close message session: session failed\"}\n"+
			"{\"level\":\"ERROR\",\"msg\":\"stop leader session failed\",\"hostname\":\"runner-host-1\",\"error\":\"close message session: session failed\"}\n"+
			"{\"level\":\"WARN\",\"msg\":\"close leader lease failed\",\"hostname\":\"runner-host-1\",\"error\":\"lease failed\"}")
}

func TestLeaderStopBoundsConcurrentSessionClosureAndUsesAggregateDeadline(t *testing.T) {
	daemon := activeClusteredDaemon(t, &fakeClusterState{})
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{})
	leader.githubOperationTimeout = 20 * time.Millisecond
	tracker := &sessionCloseTracker{entered: make(chan struct{}, 5)}
	for index := 0; index < 5; index++ {
		key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: fmt.Sprintf("linux-%d", index)}
		leader.sessions[key] = &repositoryScaleSetSession{
			key: key, messageClient: scaleSetMessageClient{Client: blockingSessionCloseClient{tracker: tracker}},
			stopPolling: func() {}, pollerDone: closedChannel(),
		}
	}
	completed := make(chan struct{})
	go func() {
		leader.stop(context.Background())
		close(completed)
	}()
	for index := 0; index < sessionCloseWorkerCount; index++ {
		awaitCompletionSignal(t, tracker.entered)
	}
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("leader shutdown exceeded its aggregate session-close deadline")
	}
	tracker.mutex.Lock()
	maximum := tracker.maximum
	tracker.mutex.Unlock()
	if maximum != sessionCloseWorkerCount {
		t.Fatalf("maximum session-close concurrency = %d", maximum)
	}
}

func TestSessionCloseWorkerStopsBeforeTakingWorkAfterCancellation(t *testing.T) {
	daemon := activeClusteredDaemon(t, &fakeClusterState{})
	leader := newClusterLeader(context.Background(), &daemon, &fakeClusterLease{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	jobs := make(chan *repositoryScaleSetSession)
	var remaining atomic.Int64
	var workers sync.WaitGroup
	workers.Add(1)
	leader.runSessionCloseWorker(ctx, jobs, &remaining, &workers)
	workers.Wait()
	leader.stopRepositoryWorkers()
	leader.repositoryWorkers.wait()
}

func TestClusterShutdownLogsContributionWithdrawalFailure(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{publishErr: errors.New("failed")}
	daemon := activeClusteredDaemon(t, state)
	daemon.logger = slog.New(slog.NewTextHandler(logs, nil))
	daemon.shutdown(context.Background())
	if !strings.Contains(logs.String(), "publish node shutdown failed") {
		t.Fatalf("withdrawal failure was not logged: %q", logs.String())
	}
}

func stoppedSession(
	key repositoryScaleSetSessionKey,
	client scaleSetDeletionClient,
	messageClient *fakeMessageClient,
) *repositoryScaleSetSession {
	return &repositoryScaleSetSession{
		key: key, capacity: 1, client: client,
		messageClient:  scaleSetMessageClient{Client: messageClient},
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 1},
		stopPolling:    func() {}, pollerDone: closedChannel(),
	}
}
