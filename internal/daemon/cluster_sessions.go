package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	scalesetpoller "github.com/meza/RunnerFoundry/internal/scaleset"
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
	client         scaleSetDeletionClient
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
	startErr := leader.startMissingSessions(reconcileCtx, desired, repositories)
	return leader.reportReconciliationResult(stopErr) && startErr == nil
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
		_, wanted := desired[key]
		if wanted {
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
) error {
	var resultErr error
	for repositoryURL, repository := range repositories {
		statuses := desiredRepositoryStatuses(repositoryURL, desired)
		if len(statuses) == 0 {
			continue
		}
		if err := leader.reconcileRepositorySessions(ctx, repository, statuses); err != nil {
			leader.daemon.logger.Error("leader repository session group failed",
				slog.String("repository", repositoryURL), slog.String("error", err.Error()))
			resultErr = errors.Join(resultErr, err)
		}
	}
	return resultErr
}

func desiredRepositoryStatuses(
	repository string,
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
) []cluster.ScaleSetStatus {
	statuses := make([]cluster.ScaleSetStatus, 0)
	for key, status := range desired {
		if key.repository == repository {
			statuses = append(statuses, status)
		}
	}
	sort.Slice(statuses, func(left, right int) bool { return statuses[left].Name < statuses[right].Name })
	return statuses
}

func (leader *clusterLeader) reconcileRepositorySessions(
	ctx context.Context,
	repository githubapp.Repository,
	statuses []cluster.ScaleSetStatus,
) error {
	audit, err := leader.auditClusterRepositoryRegistrations(ctx, repository, statuses)
	if err != nil {
		return err
	}
	if leader.repositorySessionsMatch(repository.URL, statuses, audit.registrations) {
		return nil
	}
	if suspendErr := leader.suspendRepositorySessions(ctx, repository.URL); suspendErr != nil {
		return suspendErr
	}
	started, err := leader.startRepositorySessionGeneration(ctx, repository, statuses, audit.client, audit.registrations)
	if err != nil {
		return err
	}
	leader.activateRepositorySessionGeneration(started)
	return nil
}

type clusterRepositoryRegistrationAudit struct {
	client        scaleSetClient
	registrations map[string]*upstream.RunnerScaleSet
}

func (leader *clusterLeader) auditClusterRepositoryRegistrations(
	ctx context.Context,
	repository githubapp.Repository,
	statuses []cluster.ScaleSetStatus,
) (clusterRepositoryRegistrationAudit, error) {
	configuration := leader.daemon.configurationSnapshot()
	client, err := leader.daemon.newGitHubClient(repository.URL, repository.InstallationID, configuration.credentials)
	if err != nil {
		return clusterRepositoryRegistrationAudit{}, err
	}
	configured := make([]config.ScaleSetConfig, 0, len(statuses))
	for _, status := range statuses {
		configured = append(configured, config.ScaleSetConfig{
			Name: status.Name, RunnerGroup: status.RunnerGroup, MaxRunners: status.EffectiveCapacity,
		})
	}
	registrations, err := ensureRepositoryScaleSetRegistrations(
		ctx, leader.daemon.logger, repository.URL, client, configured,
	)
	return clusterRepositoryRegistrationAudit{client: client, registrations: registrations}, err
}

func (leader *clusterLeader) startRepositorySessionGeneration(
	ctx context.Context,
	repository githubapp.Repository,
	statuses []cluster.ScaleSetStatus,
	client scaleSetClient,
	registrations map[string]*upstream.RunnerScaleSet,
) ([]*repositoryScaleSetSession, error) {
	started := make([]*repositoryScaleSetSession, 0, len(statuses))
	for _, status := range statuses {
		registration := registrations[status.Name]
		if registration == nil {
			return nil, errors.New("confirmed registration is missing from the complete repository set")
		}
		session, startErr := leader.startConfirmedSession(ctx, repository, status, client, registration)
		if startErr != nil {
			return nil, errors.Join(startErr, leader.closePartialSessionGeneration(ctx, started))
		}
		started = append(started, session)
	}
	return started, nil
}

func (leader *clusterLeader) closePartialSessionGeneration(
	ctx context.Context,
	started []*repositoryScaleSetSession,
) error {
	var resultErr error
	for _, partial := range started {
		resultErr = errors.Join(
			resultErr, leader.stopSession(context.WithoutCancel(ctx), partial, retainGitHubScaleSet),
		)
	}
	return resultErr
}

func (leader *clusterLeader) activateRepositorySessionGeneration(started []*repositoryScaleSetSession) {
	for _, session := range started {
		leader.sessions[session.key] = session
		leader.scheduler.add(session)
	}
}

func (leader *clusterLeader) repositorySessionsMatch(
	repository string,
	statuses []cluster.ScaleSetStatus,
	registrations map[string]*upstream.RunnerScaleSet,
) bool {
	for _, status := range statuses {
		session := leader.sessions[repositoryScaleSetSessionKey{repository: repository, scaleSet: status.Name}]
		registration := registrations[status.Name]
		if session == nil || registration == nil || session.runnerScaleSet.ID != registration.ID ||
			session.capacity != status.EffectiveCapacity {
			return false
		}
	}
	return true
}

func (leader *clusterLeader) suspendRepositorySessions(ctx context.Context, repository string) error {
	var resultErr error
	for key, session := range leader.sessions {
		if key.repository != repository {
			continue
		}
		resultErr = errors.Join(resultErr, leader.stopSession(ctx, session, retainGitHubScaleSet))
		delete(leader.sessions, key)
		leader.scheduler.remove(key)
	}
	return resultErr
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

func (leader *clusterLeader) startConfirmedSession(
	ctx context.Context,
	repository githubapp.Repository,
	status cluster.ScaleSetStatus,
	client scaleSetClient,
	runnerScaleSet *upstream.RunnerScaleSet,
) (*repositoryScaleSetSession, error) {
	messageClient, err := client.MessageSessionClient(ctx, runnerScaleSet.ID, runnerHostname())
	if err != nil {
		return nil, fmt.Errorf("create confirmed scale-set message session: %w", err)
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
		Client: session.messageClient,
		Handler: clusterMessageHandler{
			store: leader.daemon.assignments, scheduler: leader.scheduler, session: session, logger: leader.daemon.logger,
		},
		MaxCapacity: session.capacity,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		pollUntilShutdown(
			ctx, leader.daemon.logger, session.key.repository, session.key.scaleSet, &poller,
			pollRetryAfter(pollRetryDelay),
		)
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
