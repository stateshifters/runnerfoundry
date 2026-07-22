package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
)

func TestRepositorySessionLivesUntilLeadershipEnds(t *testing.T) {
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
	leader := newClusterLeader(&daemon, &fakeClusterLease{alive: true})
	operationCtx, cancelOperation := context.WithCancel(context.Background())
	session, err := leader.startSession(operationCtx, githubapp.Repository{URL: "repo", InstallationID: 1}, cluster.ScaleSetStatus{
		Name: "linux", RunnerGroup: "Default", EffectiveCapacity: 1, Compatible: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	<-messageClient.entered
	pollerCtx := <-messageClient.contexts
	cancelOperation()
	if pollerCtx.Err() != nil {
		t.Fatalf("repository polling ended with reconciliation: %v", pollerCtx.Err())
	}
	leader.sessions[session.key] = session
	leader.stop(context.Background())
	<-messageClient.exited
}

func TestLeadershipLossCancelsBlockedReconciliation(t *testing.T) {
	discoveryStarted := make(chan struct{})
	daemon := activeClusteredDaemon(t, &fakeClusterState{})
	daemon.discoverer = blockingDiscoverer{started: discoveryStarted}
	leader := newClusterLeader(&daemon, &fakeClusterLease{alive: true})
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
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
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

func TestLeaderReplacesCapacityWithoutDeletingDeclaredScaleSet(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
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
	if _, exists := leader.sessions[key]; exists {
		t.Fatal("outdated capacity session was retained")
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
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
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
	daemon.nodeID = "spock"
	daemon.logger = newClusterLogSnapshotLogger(logs)
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
	session := stoppedSession(
		repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"},
		fakeScaleSetDeletionClient{},
		&fakeMessageClient{closeErr: errors.New("close failed")},
	)

	if err := leader.stopSession(context.Background(), session, retainGitHubScaleSet); err == nil {
		t.Fatal("expected session close failure")
	}

	assertClusterLogSnapshot(t, logs,
		`{"level":"ERROR","msg":"close leader message session failed","hostname":"spock","error":"close message session: close failed"}`)
}

func TestLeaderStopClearsSessionsAfterLeaseCloseFailure(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.nodeID = "spock"
	daemon.logger = newClusterLogSnapshotLogger(logs)
	lease := &fakeClusterLease{closeErr: errors.New("failed")}
	leader := newClusterLeader(&daemon, lease)
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	leader.sessions[key] = stoppedSession(key, fakeScaleSetDeletionClient{}, &fakeMessageClient{})
	leader.scheduler.sessions[key] = leader.sessions[key]
	leader.stop(context.Background())
	if len(leader.sessions) != 0 || len(leader.scheduler.sessions) != 0 || !lease.closed {
		t.Fatalf("sessions=%v scheduled=%v leaseClosed=%t", leader.sessions, leader.scheduler.sessions, lease.closed)
	}
	assertClusterLogSnapshot(t, logs,
		`{"level":"WARN","msg":"close leader lease failed","hostname":"spock","error":"failed"}`)
}

func TestLeaderStopLogsSessionAndLeaseCloseFailuresSeparately(t *testing.T) {
	logs := &bytes.Buffer{}
	daemon := activeClusteredDaemon(t, &fakeClusterState{})
	daemon.nodeID = "spock"
	daemon.logger = newClusterLogSnapshotLogger(logs)
	leader := newClusterLeader(&daemon, &fakeClusterLease{closeErr: errors.New("lease failed")})
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	leader.sessions[key] = stoppedSession(
		key,
		fakeScaleSetDeletionClient{},
		&fakeMessageClient{closeErr: errors.New("session failed")},
	)

	leader.stop(context.Background())

	assertClusterLogSnapshot(t, logs,
		"{\"level\":\"ERROR\",\"msg\":\"close leader message session failed\",\"hostname\":\"spock\",\"error\":\"close message session: session failed\"}\n"+
			"{\"level\":\"ERROR\",\"msg\":\"stop leader session failed\",\"hostname\":\"spock\",\"error\":\"close message session: session failed\"}\n"+
			"{\"level\":\"WARN\",\"msg\":\"close leader lease failed\",\"hostname\":\"spock\",\"error\":\"lease failed\"}")
}

func TestSuccessfulReconciliationResultReturnsTrue(t *testing.T) {
	leader := newClusterLeader(&clusteredDaemon{logger: slog.Default()}, &fakeClusterLease{})
	if !leader.reportReconciliationResult(nil) {
		t.Fatal("successful reconciliation result returned false")
	}
}

func TestFailedReconciliationResultLogsAndReturnsFalse(t *testing.T) {
	logs := &bytes.Buffer{}
	daemon := &clusteredDaemon{nodeID: "spock", logger: newClusterLogSnapshotLogger(logs)}
	leader := newClusterLeader(daemon, &fakeClusterLease{})
	if leader.reportReconciliationResult(errors.New("shutdown failed")) {
		t.Fatal("failed reconciliation result returned true")
	}
	assertClusterLogSnapshot(t, logs,
		`{"level":"ERROR","msg":"stop outdated leader sessions failed","hostname":"spock","error":"shutdown failed"}`)
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
