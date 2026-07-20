package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
)

func TestRepositorySessionLivesUntilLeadershipEnds(t *testing.T) {
	messageClient := &blockingMessageClient{
		entered: make(chan struct{}), exited: make(chan struct{}), contexts: make(chan context.Context, 1),
	}
	daemon := activeClusteredDaemon(t, &fakeClusterState{})
	daemon.newGitHubClient = func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
		return &fakeScaleSetClient{
			runnerGroup: &upstream.RunnerGroup{ID: 1}, runnerScaleSet: &upstream.RunnerScaleSet{ID: 2},
			messageClient: scaleSetMessageClient{Client: messageClient},
		}, nil
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
	daemon.newGitHubClient = func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error) {
		return nil, errors.New("must not be called")
	}
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	session := &repositoryScaleSetSession{key: key}
	leader.sessions[key] = session
	leader.startMissingSessions(context.Background(), map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{
		key: {Name: "linux", EffectiveCapacity: 1},
	}, nil)
	if leader.sessions[key] != session {
		t.Fatal("existing session was replaced")
	}
}

func TestLeaderReplacesCapacityWithoutDeletingDeclaredScaleSet(t *testing.T) {
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	githubClient := &fakeScaleSetClient{}
	leader.sessions[key] = stoppedSession(key, githubClient, &fakeMessageClient{})
	leader.stopOutdatedSessions(context.Background(), map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{
		key: {Name: "linux", EffectiveCapacity: 2},
	}, []cluster.ScaleSetStatus{{Name: "linux"}})
	if _, exists := leader.sessions[key]; exists {
		t.Fatal("outdated capacity session was retained")
	}
	if len(githubClient.deletedScaleSetIDs) != 0 {
		t.Fatalf("declared scale set was deleted: %v", githubClient.deletedScaleSetIDs)
	}
}

func TestLeaderSessionShutdownAttemptsMessageCloseAndScaleSetRemoval(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.logger = slog.New(slog.NewTextHandler(logs, nil))
	leader := newClusterLeader(&daemon, &fakeClusterLease{})
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	session := stoppedSession(
		key,
		&fakeScaleSetClient{deleteScaleSetErr: errors.New("delete failed")},
		&fakeMessageClient{closeErr: errors.New("close failed")},
	)
	leader.stopSession(context.Background(), session, removeGitHubScaleSet)
	if !strings.Contains(logs.String(), "close leader message session failed") ||
		!strings.Contains(logs.String(), "remove withdrawn runner scale set failed") {
		t.Fatalf("expected both cleanup failures, got %q", logs.String())
	}
}

func TestLeaderStopClearsSessionsAfterLeaseCloseFailure(t *testing.T) {
	logs := &bytes.Buffer{}
	state := &fakeClusterState{}
	daemon := activeClusteredDaemon(t, state)
	daemon.logger = slog.New(slog.NewTextHandler(logs, nil))
	lease := &fakeClusterLease{closeErr: errors.New("failed")}
	leader := newClusterLeader(&daemon, lease)
	key := repositoryScaleSetSessionKey{repository: "repo", scaleSet: "linux"}
	leader.sessions[key] = stoppedSession(key, &fakeScaleSetClient{}, &fakeMessageClient{})
	leader.scheduler.sessions[key] = leader.sessions[key]
	leader.stop(context.Background())
	if len(leader.sessions) != 0 || len(leader.scheduler.sessions) != 0 || !lease.closed {
		t.Fatalf("sessions=%v scheduled=%v leaseClosed=%t", leader.sessions, leader.scheduler.sessions, lease.closed)
	}
	if !strings.Contains(logs.String(), "close leader lease failed") {
		t.Fatalf("lease close failure was not logged: %q", logs.String())
	}
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
	client *fakeScaleSetClient,
	messageClient *fakeMessageClient,
) *repositoryScaleSetSession {
	return &repositoryScaleSetSession{
		key: key, capacity: 1, client: client,
		messageClient:  scaleSetMessageClient{Client: messageClient},
		runnerScaleSet: &upstream.RunnerScaleSet{ID: 1},
		stopPolling:    func() {}, pollerDone: closedChannel(),
	}
}
