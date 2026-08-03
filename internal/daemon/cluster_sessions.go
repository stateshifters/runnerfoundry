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
	"github.com/meza/RunnerFoundry/internal/repositorystate"
	scalesetpoller "github.com/meza/RunnerFoundry/internal/scaleset"
)

type clusterLeader struct {
	daemon                    *clusteredDaemon
	lease                     cluster.Lease
	scheduler                 *clusterScheduler
	sessions                  map[repositoryScaleSetSessionKey]*repositoryScaleSetSession
	sessionsMutex             sync.Mutex
	operationMutex            sync.Mutex
	repairMutex               sync.Mutex
	cancelMutex               sync.Mutex
	cancelOperation           context.CancelFunc
	stopped                   bool
	githubOperationTimeout    time.Duration
	repositoryRepairDelays    []time.Duration
	now                       func() time.Time
	pendingRepositoryRepairs  map[string]repositoryRepair
	repositoryWorkers         *repositoryWorkerPool
	assignedJobWorkers        *assignedJobRecoveryExecutor
	repositoryWorkerDone      <-chan struct{}
	stopRepositoryWorkers     context.CancelFunc
	repositorySnapshot        repositorystate.Snapshot
	repositoryControlStarted  bool
	repositoryAppID           int64
	repositoryControllerEpoch int64
	registrationMutex         sync.Mutex
	confirmedRegistrations    map[repositoryScaleSetSessionKey]repositorystate.Registration
	desiredMutex              sync.Mutex
	desiredRepositories       map[string][]cluster.ScaleSetStatus
	desiredStateKnown         bool
	background                sync.WaitGroup
	discoveryMutex            sync.Mutex
	discoveryRunning          bool
}

type repositoryScaleSetSessionKey struct {
	repository string
	scaleSet   string
}

var errRepositoryWorkObsolete = errors.New("repository reconciliation is obsolete")

type repositoryScaleSetSession struct {
	key            repositoryScaleSetSessionKey
	installationID githubapp.InstallationID
	capacity       int
	client         scaleSetDeletionClient
	messageClient  scaleSetMessageClient
	runnerScaleSet *upstream.RunnerScaleSet
	stopPolling    context.CancelFunc
	pollerDone     <-chan struct{}
	assignedJobs   *clusterAssignedJobRecovery
}

type repositorySessionRemoval struct {
	key     repositoryScaleSetSessionKey
	session *repositoryScaleSetSession
	removal scaleSetSessionRemoval
}

func newClusterLeader(ctx context.Context, daemon *clusteredDaemon, lease cluster.Lease) *clusterLeader {
	scheduler := &clusterScheduler{store: daemon.assignments, sessions: map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}}
	workerCtx, stopRepositoryWorkers := context.WithCancel(ctx)
	leader := &clusterLeader{
		daemon: daemon, lease: lease, scheduler: scheduler,
		sessions:               map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{},
		githubOperationTimeout: clusterGitHubTimeout,
		repositoryRepairDelays: repositoryRepairRetryDelays[:], now: time.Now,
		pendingRepositoryRepairs: make(map[string]repositoryRepair),
		stopRepositoryWorkers:    stopRepositoryWorkers,
		repositoryWorkerDone:     workerCtx.Done(),
		desiredRepositories:      map[string][]cluster.ScaleSetStatus{},
		confirmedRegistrations:   map[repositoryScaleSetSessionKey]repositorystate.Registration{},
	}
	leader.repositoryWorkers = newRepositoryWorkerPool(workerCtx, leader.githubOperationTimeout)
	leader.assignedJobWorkers = newAssignedJobRecoveryExecutor(workerCtx, clusterCoordinationTimeout)
	return leader
}

func (leader *clusterLeader) reconcile(ctx context.Context) bool {
	leader.operationMutex.Lock()
	defer leader.operationMutex.Unlock()
	reconcileCtx, cancelReconcile := context.WithCancel(ctx)
	if !leader.setOperationCancellation(cancelReconcile) {
		cancelReconcile()
		return false
	}
	defer leader.clearOperationCancellation()
	defer cancelReconcile()
	if leader.daemon.repositoryState != nil {
		return leader.reconcileDurableRepositories(reconcileCtx, ctx)
	}
	discovery, statuses, loaded := leader.loadReconciliationState(reconcileCtx)
	if !loaded {
		return false
	}
	desired, repositories := leader.desiredSessions(discovery.Repositories, statuses)
	leader.publishDesiredRepositories(repositories, desired)
	leader.stopUndesiredSessions(reconcileCtx, desired, statuses)
	leader.retainDesiredRepositoryRepairs(repositories, desired)
	repositoryErr := leader.startMissingSessions(reconcileCtx, desired, repositories)
	return repositoryErr == nil || reconcileCtx.Err() == nil
}

func (leader *clusterLeader) loadReconciliationState(
	ctx context.Context,
) (githubapp.DiscoveryResult, []cluster.ScaleSetStatus, bool) {
	configuration := leader.daemon.configurationSnapshot()
	discoveryCtx, cancelDiscovery := context.WithTimeout(ctx, leader.githubOperationTimeout)
	discovery, err := configuration.discoverer.Discover(discoveryCtx)
	cancelDiscovery()
	if err != nil {
		leader.daemon.logger.Error("leader repository discovery failed",
			slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", err.Error()))
		return githubapp.DiscoveryResult{}, nil, false
	}
	statusCtx, cancelStatus := context.WithTimeout(ctx, clusterCoordinationTimeout)
	statuses, err := leader.daemon.nodes.ScaleSets(statusCtx, time.Now(), clusterNodeLifetime)
	cancelStatus()
	if err != nil {
		leader.daemon.logger.Error("leader scale-set aggregation failed",
			slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", err.Error()))
		return githubapp.DiscoveryResult{}, nil, false
	}
	return discovery, statuses, true
}

func (leader *clusterLeader) stopUndesiredSessions(
	ctx context.Context,
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
	statuses []cluster.ScaleSetStatus,
) {
	if stopErr := leader.stopOutdatedSessions(ctx, desired, statuses); stopErr != nil {
		leader.daemon.logger.Error("stop outdated leader sessions failed",
			slog.String("hostname", string(leader.daemon.nodeID)), slog.String("error", stopErr.Error()))
	}
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
	removals := leader.detachOutdatedSessions(desired, statuses)
	return leader.cleanDetachedSessions(ctx, removals)
}

func (leader *clusterLeader) detachOutdatedSessions(
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
	statuses []cluster.ScaleSetStatus,
) []repositorySessionRemoval {
	leader.sessionsMutex.Lock()
	defer leader.sessionsMutex.Unlock()
	removals := make([]repositorySessionRemoval, 0)
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
		delete(leader.sessions, key)
		leader.scheduler.remove(key)
		removals = append(removals, repositorySessionRemoval{key: key, session: session, removal: removal})
	}
	return removals
}

func (leader *clusterLeader) cleanDetachedSessions(
	ctx context.Context,
	removals []repositorySessionRemoval,
) error {
	var resultErr error
	for _, removed := range removals {
		cleanupCtx, cancelCleanup := context.WithTimeout(ctx, leader.githubOperationTimeout)
		stopErr := leader.stopSession(cleanupCtx, removed.session, removed.removal)
		resultErr = errors.Join(resultErr, stopErr)
		if stopErr == nil && removed.removal == removeGitHubScaleSet {
			resultErr = errors.Join(resultErr, leader.deletePersistedRegistration(cleanupCtx, removed.key))
		}
		cancelCleanup()
	}
	return resultErr
}

func (leader *clusterLeader) startMissingSessions(
	ctx context.Context,
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
	repositories map[string]githubapp.Repository,
) error {
	results, submitted := leader.enqueueMissingSessions(desired, repositories)
	return leader.awaitMissingSessions(ctx, desired, results, submitted)
}

type repositorySessionResult struct {
	repository githubapp.Repository
	err        error
}

func (leader *clusterLeader) enqueueMissingSessions(
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
	repositories map[string]githubapp.Repository,
) (<-chan repositorySessionResult, int) {
	repositoryURLs := make([]string, 0, len(repositories))
	for repositoryURL := range repositories {
		repositoryURLs = append(repositoryURLs, repositoryURL)
	}
	sort.Strings(repositoryURLs)
	results := make(chan repositorySessionResult, len(repositoryURLs))
	submitted := 0
	for _, repositoryURL := range repositoryURLs {
		repository := repositories[repositoryURL]
		statuses := desiredRepositoryStatuses(repositoryURL, desired)
		if len(statuses) == 0 {
			continue
		}
		if leader.repositoryRepairAlreadyScheduled(repository, statuses) {
			continue
		}
		submitted++
		leader.repositoryWorkers.enqueue(repositoryURL, repositoryWorkRoutine, func(attemptCtx context.Context) error {
			return leader.reconcileDesiredRepository(attemptCtx, repository, statuses)
		}, func(err error) {
			results <- repositorySessionResult{repository: repository, err: err}
		})
	}
	return results, submitted
}

func (leader *clusterLeader) awaitMissingSessions(
	ctx context.Context,
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
	results <-chan repositorySessionResult,
	submitted int,
) error {
	var resultErr error
	for completed := 0; completed < submitted; completed++ {
		var result repositorySessionResult
		select {
		case <-ctx.Done():
			return errors.Join(resultErr, ctx.Err())
		case result = <-results:
		}
		resultErr = errors.Join(resultErr, leader.handleMissingSessionResult(ctx, desired, result))
	}
	return resultErr
}

func (leader *clusterLeader) handleMissingSessionResult(
	ctx context.Context,
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
	result repositorySessionResult,
) error {
	if result.err == nil {
		leader.completeRepositoryRepair(ctx, result.repository.URL)
		return nil
	}
	if ctx.Err() != nil {
		return result.err
	}
	statuses := desiredRepositoryStatuses(result.repository.URL, desired)
	leader.daemon.logger.Error("leader repository session group failed",
		slog.String("repository", result.repository.URL), slog.String("error", result.err.Error()))
	leader.recordRepositoryRepair(ctx, result.repository, statuses, result.err)
	return result.err
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
	if err = leader.activateRepositorySessionGeneration(ctx, repository.URL, statuses, started); err != nil {
		return errors.Join(err, leader.closePartialSessionGeneration(ctx, started))
	}
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
	persistenceErr := leader.persistConfirmedRegistrations(ctx, repository.URL, statuses, registrations)
	return clusterRepositoryRegistrationAudit{client: client, registrations: registrations}, errors.Join(err, persistenceErr)
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
			resultErr, leader.stopSession(ctx, partial, retainGitHubScaleSet),
		)
	}
	return resultErr
}

func (leader *clusterLeader) detachRepositorySessionGeneration(sessions []*repositoryScaleSetSession) {
	leader.sessionsMutex.Lock()
	defer leader.sessionsMutex.Unlock()
	for _, session := range sessions {
		delete(leader.sessions, session.key)
		leader.scheduler.remove(session.key)
	}
}

func sameScaleSetStatuses(left []cluster.ScaleSetStatus, right []cluster.ScaleSetStatus) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (leader *clusterLeader) repositorySessionsMatch(
	repository string,
	statuses []cluster.ScaleSetStatus,
	registrations map[string]*upstream.RunnerScaleSet,
) bool {
	leader.sessionsMutex.Lock()
	defer leader.sessionsMutex.Unlock()
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
	leader.sessionsMutex.Lock()
	removed := make([]*repositoryScaleSetSession, 0)
	for key, session := range leader.sessions {
		if key.repository == repository {
			removed = append(removed, session)
			delete(leader.sessions, key)
			leader.scheduler.remove(key)
		}
	}
	leader.sessionsMutex.Unlock()
	var resultErr error
	for _, session := range removed {
		resultErr = errors.Join(resultErr, leader.stopSession(ctx, session, retainGitHubScaleSet))
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
	leader.prepareSessionRuntime(session)
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
	leader.prepareSessionRuntime(session)
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
	if session.assignedJobs != nil {
		session.assignedJobs.stop()
	}
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
	leader.stopRepositoryWorkers()
	if leader.assignedJobWorkers != nil {
		leader.assignedJobWorkers.stop()
	}
	leader.background.Wait()
	leader.repositoryWorkers.wait()
	leader.operationMutex.Lock()
	defer leader.operationMutex.Unlock()
	leader.sessionsMutex.Lock()
	sessions := leader.sessions
	leader.sessions = map[repositoryScaleSetSessionKey]*repositoryScaleSetSession{}
	leader.sessionsMutex.Unlock()
	leader.stopLeaderSessions(ctx, sessions)
	leader.repairMutex.Lock()
	leader.pendingRepositoryRepairs = map[string]repositoryRepair{}
	leader.repairMutex.Unlock()
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
