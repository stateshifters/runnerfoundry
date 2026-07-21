package daemon

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	scalesetpoller "github.com/meza/github-scaler/internal/scaleset"
)

type clusterLeader struct {
	daemon          *clusteredDaemon
	lease           cluster.Lease
	scheduler       *clusterScheduler
	sessions        map[repositoryScaleSetSessionKey]*repositoryScaleSetSession
	operationMutex  sync.Mutex
	cancelMutex     sync.Mutex
	cancelOperation context.CancelFunc
	stopped         bool
}

type repositoryScaleSetSessionKey struct {
	repository string
	scaleSet   string
}

type repositoryScaleSetSession struct {
	key            repositoryScaleSetSessionKey
	installationID githubapp.InstallationID
	capacity       int
	client         scaleSetClient
	messageClient  scaleSetMessageClient
	runnerScaleSet *upstream.RunnerScaleSet
	stopPolling    context.CancelFunc
	pollerDone     <-chan struct{}
}

func newClusterLeader(daemon *clusteredDaemon, lease cluster.Lease) *clusterLeader {
	scheduler := &clusterScheduler{store: daemon.assignments, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}}
	return &clusterLeader{daemon: daemon, lease: lease, scheduler: scheduler, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}}
}

func (leader *clusterLeader) reconcile(ctx context.Context) bool {
	leader.operationMutex.Lock()
	defer leader.operationMutex.Unlock()
	reconcileCtx, cancelReconcile := context.WithTimeout(ctx, clusterGitHubTimeout)
	if !leader.setOperationCancellation(cancelReconcile) {
		cancelReconcile()
		return false
	}
	defer leader.clearOperationCancellation()
	defer cancelReconcile()
	configuration := leader.daemon.configurationSnapshot()
	discovery, err := configuration.discoverer.Discover(reconcileCtx)
	if err != nil {
		leader.daemon.logger.Error("leader repository discovery failed",
			slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", err.Error()))
		return false
	}
	statusCtx, cancelStatus := context.WithTimeout(reconcileCtx, clusterCoordinationTimeout)
	statuses, err := leader.daemon.nodes.ScaleSets(statusCtx, time.Now(), clusterNodeLifetime)
	cancelStatus()
	if err != nil {
		leader.daemon.logger.Error("leader scale-set aggregation failed",
			slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", err.Error()))
		return false
	}
	desired, repositories := leader.desiredSessions(discovery.Repositories, statuses)
	stopErr := leader.stopOutdatedSessions(reconcileCtx, desired, statuses)
	leader.startMissingSessions(reconcileCtx, desired, repositories)
	return leader.reportReconciliationResult(stopErr)
}

func (leader *clusterLeader) reportReconciliationResult(err error) bool {
	if err == nil {
		return true
	}
	leader.daemon.logger.Error("stop outdated leader sessions failed",
		slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", err.Error()))
	return false
}

func (leader *clusterLeader) desiredSessions(
	discovered []githubapp.Repository,
	statuses []cluster.ScaleSetStatus,
) (map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus, map[string]githubapp.Repository) {
	desired := map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus{}
	repositories := make(map[string]githubapp.Repository, len(discovered))
	for _, repository := range discovered {
		repositories[repository.URL] = repository
		for _, status := range statuses {
			if !status.Compatible {
				leader.daemon.logger.Error("conflicting clustered scale-set declarations", slog.String("scaleSet", status.Name))
				continue
			}
			desired[repositoryScaleSetSessionKey{repository: repository.URL, scaleSet: status.Name}] = status
		}
	}
	return desired, repositories
}

func (leader *clusterLeader) stopOutdatedSessions(
	ctx context.Context,
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
	statuses []cluster.ScaleSetStatus,
) error {
	var resultErr error
	for key, session := range leader.sessions {
		status, wanted := desired[key]
		if wanted && session.capacity == status.EffectiveCapacity {
			continue
		}
		scaleSetStillDeclared := hasClusterStatus(statuses, key.scaleSet)
		removal := retainGitHubScaleSet
		if !scaleSetStillDeclared {
			removal = removeGitHubScaleSet
		}
		resultErr = errors.Join(resultErr, leader.stopSession(ctx, session, removal))
		delete(leader.sessions, key)
		leader.scheduler.remove(key)
	}
	return resultErr
}

func (leader *clusterLeader) startMissingSessions(
	ctx context.Context,
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
	repositories map[string]githubapp.Repository,
) {
	for key, status := range desired {
		if _, exists := leader.sessions[key]; exists {
			continue
		}
		session, startErr := leader.startSession(ctx, repositories[key.repository], status)
		if startErr != nil {
			leader.daemon.logger.Error("leader repository scale-set session failed",
				slog.String("repository", key.repository), slog.String("scaleSet", key.scaleSet), slog.String("error", startErr.Error()))
			continue
		}
		leader.sessions[key] = session
		leader.scheduler.add(session)
	}
}

func hasClusterStatus(statuses []cluster.ScaleSetStatus, name string) bool {
	for _, status := range statuses {
		if status.Name == name {
			return true
		}
	}
	return false
}

func (leader *clusterLeader) startSession(
	ctx context.Context,
	repository githubapp.Repository,
	status cluster.ScaleSetStatus,
) (*repositoryScaleSetSession, error) {
	configuration := leader.daemon.configurationSnapshot()
	client, err := leader.daemon.newGitHubClient(
		repository.URL, repository.InstallationID, configuration.credentials,
	)
	if err != nil {
		return nil, err
	}
	scaleSetConfig := config.ScaleSetConfig{
		Name: status.Name, RunnerGroup: status.RunnerGroup, MaxRunners: status.EffectiveCapacity,
	}
	runnerScaleSet, messageClient, err := prepareScaleSetSession(
		ctx, leader.daemon.logger, client, repository.URL, scaleSetConfig, runnerHostname(),
	)
	if err != nil {
		return nil, err
	}
	session := &repositoryScaleSetSession{
		key:            repositoryScaleSetSessionKey{repository: repository.URL, scaleSet: status.Name},
		installationID: repository.InstallationID, capacity: status.EffectiveCapacity,
		client: client, messageClient: messageClient, runnerScaleSet: runnerScaleSet,
	}
	pollerCtx, stopPolling := context.WithCancel(context.WithoutCancel(ctx))
	session.stopPolling = stopPolling
	session.pollerDone = leader.startSessionPoller(pollerCtx, session)
	return session, nil
}

func (leader *clusterLeader) startSessionPoller(ctx context.Context, session *repositoryScaleSetSession) <-chan struct{} {
	poller := scalesetpoller.Poller{
		Client:      session.messageClient,
		Handler:     clusterMessageHandler{store: leader.daemon.assignments, scheduler: leader.scheduler, session: session},
		MaxCapacity: session.capacity,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		pollUntilShutdown(ctx, leader.daemon.logger, session.key.repository, session.key.scaleSet, &poller, pollRetryDelay)
	}()
	return done
}

type scaleSetSessionRemoval int

const (
	retainGitHubScaleSet scaleSetSessionRemoval = iota
	removeGitHubScaleSet
)

func (leader *clusterLeader) stopSession(ctx context.Context, session *repositoryScaleSetSession, removal scaleSetSessionRemoval) error {
	session.stopPolling()
	<-session.pollerDone
	closeErr := session.messageClient.Close(ctx)
	if closeErr != nil {
		leader.daemon.logger.Error("close leader message session failed",
			slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", closeErr.Error()))
	}
	var deleteErr error
	if removal == removeGitHubScaleSet {
		deleteErr = deleteRunnerScaleSet(
			ctx, leader.daemon.logger, session.key.repository, session.key.scaleSet, session.client, session.runnerScaleSet,
		)
	}
	return errors.Join(closeErr, deleteErr)
}

func (leader *clusterLeader) stop(ctx context.Context) {
	leader.cancelActiveOperation()
	leader.operationMutex.Lock()
	defer leader.operationMutex.Unlock()
	for _, session := range leader.sessions {
		if err := leader.stopSession(ctx, session, retainGitHubScaleSet); err != nil {
			leader.daemon.logger.Error("stop leader session failed",
				slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", err.Error()))
		}
	}
	leader.sessions = map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}
	leader.scheduler.clear()
	if err := leader.lease.Close(ctx); err != nil {
		leader.daemon.logger.Warn("close leader lease failed",
			slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", err.Error()))
	}
}

func (leader *clusterLeader) setOperationCancellation(cancel context.CancelFunc) bool {
	leader.cancelMutex.Lock()
	defer leader.cancelMutex.Unlock()
	if leader.stopped {
		return false
	}
	leader.cancelOperation = cancel
	return true
}

func (leader *clusterLeader) clearOperationCancellation() {
	leader.cancelMutex.Lock()
	leader.cancelOperation = nil
	leader.cancelMutex.Unlock()
}

func (leader *clusterLeader) cancelActiveOperation() {
	leader.cancelMutex.Lock()
	defer leader.cancelMutex.Unlock()
	leader.stopped = true
	if leader.cancelOperation != nil {
		leader.cancelOperation()
	}
}
