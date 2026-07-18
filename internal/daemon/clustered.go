package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	scalesetpoller "github.com/meza/github-scaler/internal/scaleset"
)

const (
	clusterHeartbeatInterval    = 10 * time.Second
	clusterNodeLifetime         = cluster.NodeLifetime
	clusterLeadershipRetryDelay = 2 * time.Second
	clusterExecutorInterval     = time.Second
)

type clusteredDaemon struct {
	nodeID            string
	config            config.Config
	credentials       config.GitHubAppCredentials
	discoverer        repositoryDiscoverer
	logger            *slog.Logger
	runtime           runtime
	store             clusterState
	leader            *clusterLeader
	leadershipResults chan leadershipResult
	stopContender     context.CancelFunc
	contenderDone     <-chan struct{}
}

type leadershipResult struct {
	lease cluster.Lease
}

type clusterState interface { //nolint:interfacebloat // One state boundary keeps transactional coordination replaceable in orchestration tests.
	Migrate(ctx context.Context) error
	MigrationStatus(ctx context.Context) (cluster.MigrationStatus, error)
	Publish(ctx context.Context, nodeID string, healthy bool, contributions []cluster.Contribution, now time.Time) error
	ScaleSets(ctx context.Context, now time.Time, liveFor time.Duration) ([]cluster.ScaleSetStatus, error)
	Enqueue(ctx context.Context, demand cluster.Demand) error
	AssignOldest(ctx context.Context, scaleSet string, now time.Time, liveFor time.Duration) (*cluster.Assignment, error)
	Ready(ctx context.Context, assignmentID int64, installationID int64, githubScaleSetID int) error
	Retry(ctx context.Context, assignmentID int64) error
	Started(ctx context.Context, assignmentID int64) error
	ClaimReady(ctx context.Context, nodeID string) (*cluster.Assignment, error)
	RemoveDemand(ctx context.Context, scaleSet string, repository string, requestID int64) error
	Release(ctx context.Context, assignmentID int64) error
	TryLeader(ctx context.Context) (cluster.Lease, error)
	WaitForLeader(ctx context.Context) (*cluster.LeaderLease, error)
}

func runClusteredDaemon(
	ctx context.Context,
	options Options,
	daemonRuntime runtime,
	configured config.Config,
) error {
	daemonRuntime = withRuntimeDefaults(daemonRuntime)
	daemon, err := prepareClusteredDaemon(ctx, daemonRuntime, configured)
	if err != nil {
		return err
	}
	if err = daemon.publish(ctx, true); err != nil {
		return err
	}
	defer daemon.shutdown(context.WithoutCancel(ctx))
	daemon.tryLeadership(ctx)
	if daemon.leader == nil {
		daemon.startContender(ctx)
	}
	if err = daemonRuntime.Notifier.Ready(ctx); err != nil {
		return fmt.Errorf("notify ready: %w", err)
	}
	logAction(ctx, daemon.logger, slog.LevelInfo, "daemon", "ready",
		slog.String("mode", "clustered"), slog.String("hostname", daemon.nodeID))
	if daemon.leader != nil {
		daemon.leader.reconcile(ctx)
	}
	return daemon.run(ctx, options)
}

func prepareClusteredDaemon(ctx context.Context, daemonRuntime runtime, configured config.Config) (*clusteredDaemon, error) {
	postgresState := configured.State.Postgres
	if postgresState == nil {
		return nil, errors.New("PostgreSQL clustered state is required")
	}
	nodeID, err := clusteredNodeID(daemonRuntime)
	if err != nil {
		return nil, err
	}
	store, err := daemonRuntime.OpenClusterStore(*postgresState)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL clustered state: %w", err)
	}
	logger := daemonRuntime.NewLogger(configured.Log.Format, configured.Log.Level)
	if err = migrateClusteredStore(ctx, logger, store); err != nil {
		return nil, err
	}
	credentials, err := configured.GitHub.AppCredentials()
	if err != nil {
		return nil, fmt.Errorf("load GitHub App credentials: %w", err)
	}
	discoverer, err := daemonRuntime.NewDiscoverer(credentials)
	if err != nil {
		return nil, fmt.Errorf("create repository discoverer: %w", err)
	}
	return &clusteredDaemon{
		nodeID: nodeID,
		config: configured, credentials: credentials, discoverer: discoverer,
		logger:  logger,
		runtime: daemonRuntime, store: store, leadershipResults: make(chan leadershipResult, 1),
	}, nil
}

func clusteredNodeID(daemonRuntime runtime) (string, error) {
	if daemonRuntime.Hostname == nil {
		return "", errors.New("clustered host name reader is required")
	}
	nodeID, err := daemonRuntime.Hostname()
	if err != nil {
		return "", fmt.Errorf("read clustered host name: %w", err)
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return "", errors.New("clustered host name is empty")
	}
	return nodeID, nil
}

func migrateClusteredStore(ctx context.Context, logger *slog.Logger, store clusterState) error {
	migrationStatus, err := store.MigrationStatus(ctx)
	if err != nil {
		return fmt.Errorf("inspect PostgreSQL clustered state: %w", err)
	}
	logAction(ctx, logger, slog.LevelInfo, "migration", "waiting",
		slog.String("direction", "up"), slog.Int("currentVersion", int(migrationStatus.Current)),
		slog.Int("targetVersion", int(migrationStatus.Target)))
	if err = store.Migrate(ctx); err != nil {
		return fmt.Errorf("initialize PostgreSQL clustered state: %w", err)
	}
	completedVersion := max(migrationStatus.Target, migrationStatus.Current)
	logAction(ctx, logger, slog.LevelInfo, "migration", "completed",
		slog.String("direction", "up"), slog.Int("version", int(completedVersion)))
	return nil
}

func (daemon *clusteredDaemon) run(ctx context.Context, options Options) error {
	heartbeat := time.NewTicker(clusterHeartbeatInterval)
	executor := time.NewTicker(clusterExecutorInterval)
	defer heartbeat.Stop()
	defer executor.Stop()
	return daemon.runEvents(ctx, options, clusterEvents{
		heartbeat: heartbeat.C, leadership: daemon.leadershipResults, executor: executor.C,
	})
}

type clusterEvents struct {
	heartbeat  <-chan time.Time
	leadership <-chan leadershipResult
	executor   <-chan time.Time
}

func (daemon *clusteredDaemon) runEvents(ctx context.Context, options Options, events clusterEvents) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-events.heartbeat:
			daemon.heartbeat(ctx)
		case result := <-events.leadership:
			daemon.contenderDone = nil
			daemon.stopContender = nil
			daemon.acceptLeadership(ctx, result.lease)
			daemon.leader.reconcile(ctx)
		case <-events.executor:
			daemon.executeAssignments(ctx)
		case <-options.ReloadRequests:
			if err := daemon.reload(ctx, options.ConfigPath); err != nil {
				return err
			}
		}
	}
}

func (daemon *clusteredDaemon) heartbeat(ctx context.Context) {
	if err := daemon.publish(ctx, true); err != nil {
		daemon.logger.Error("cluster heartbeat failed", slog.String("error", err.Error()))
	}
	if daemon.leader != nil && !daemon.leader.lease.Alive(ctx) {
		daemon.leader.stop(context.WithoutCancel(ctx))
		daemon.leader = nil
	}
	if daemon.leader == nil {
		daemon.startContender(ctx)
		return
	}
	daemon.leader.reconcile(ctx)
}

func (daemon *clusteredDaemon) publish(ctx context.Context, nodeHealthy bool) error {
	contributions := make([]cluster.Contribution, 0, len(daemon.config.ScaleSets))
	for _, scaleSet := range daemon.config.ScaleSets {
		healthy := daemon.runtime.ProviderHealthy(ctx, scaleSet) == nil
		if !healthy {
			daemon.logger.Error("scale-set provider is unhealthy",
				slog.String("scaleSet", scaleSet.Name), slog.String("hostname", daemon.nodeID))
		}
		contributions = append(contributions, cluster.Contribution{
			ScaleSet: scaleSet.Name, RunnerGroup: scaleSet.RunnerGroup,
			MaxRunners: scaleSet.MaxRunners, Healthy: healthy,
		})
	}
	if err := daemon.store.Publish(ctx, daemon.nodeID, nodeHealthy, contributions, time.Now()); err != nil {
		return fmt.Errorf("publish clustered node contributions: %w", err)
	}
	return nil
}

func (daemon *clusteredDaemon) tryLeadership(ctx context.Context) {
	lease, err := daemon.store.TryLeader(ctx)
	if errors.Is(err, cluster.ErrNotLeader) {
		return
	}
	if err != nil {
		daemon.logger.Error("leader election failed", slog.String("error", err.Error()))
		return
	}
	if lease == nil {
		return
	}
	daemon.acceptLeadership(ctx, lease)
}

func (daemon *clusteredDaemon) acceptLeadership(ctx context.Context, lease cluster.Lease) {
	daemon.leader = newClusterLeader(daemon, lease)
	logAction(ctx, daemon.logger, slog.LevelInfo, "leader_election", "acquired",
		slog.String("hostname", daemon.nodeID))
}

func (daemon *clusteredDaemon) startContender(ctx context.Context) {
	if daemon.contenderDone != nil {
		return
	}
	contenderCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	daemon.stopContender = cancel
	daemon.contenderDone = done
	results := daemon.leadershipResults
	if results == nil {
		results = make(chan leadershipResult, 1)
		daemon.leadershipResults = results
	}
	go daemon.contendForLeadership(contenderCtx, results, done)
}

func (daemon *clusteredDaemon) contendForLeadership(
	ctx context.Context,
	results chan<- leadershipResult,
	done chan<- struct{},
) {
	defer close(done)
	for {
		lease, err := daemon.store.WaitForLeader(ctx)
		if err == nil {
			daemon.deliverLeadership(ctx, results, lease)
			return
		}
		if ctx.Err() != nil {
			return
		}
		daemon.logger.Error("leader contender failed", slog.String("error", err.Error()))
		if !waitForLeadershipRetry(ctx) {
			return
		}
	}
}

func (daemon *clusteredDaemon) deliverLeadership(
	ctx context.Context,
	results chan<- leadershipResult,
	lease cluster.Lease,
) {
	select {
	case results <- leadershipResult{lease: lease}:
	case <-ctx.Done():
		if err := lease.Close(context.WithoutCancel(ctx)); err != nil {
			daemon.logger.Warn("close canceled leader lease failed")
		}
	}
}

func waitForLeadershipRetry(ctx context.Context) bool {
	timer := time.NewTimer(clusterLeadershipRetryDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (daemon *clusteredDaemon) stopLeadershipContender(ctx context.Context) {
	if daemon.stopContender == nil {
		return
	}
	daemon.stopContender()
	<-daemon.contenderDone
	select {
	case result := <-daemon.leadershipResults:
		if err := result.lease.Close(context.WithoutCancel(ctx)); err != nil {
			daemon.logger.Warn("close unclaimed leader lease failed")
		}
	default:
	}
	daemon.stopContender = nil
	daemon.contenderDone = nil
}

func (daemon *clusteredDaemon) executeAssignments(ctx context.Context) {
	for {
		if !daemon.executeNextAssignment(ctx) {
			return
		}
	}
}

func (daemon *clusteredDaemon) executeNextAssignment(ctx context.Context) bool {
	assignment, err := daemon.store.ClaimReady(ctx, daemon.nodeID)
	if errors.Is(err, cluster.ErrNoReadyAssignment) {
		return false
	}
	if err != nil {
		daemon.logger.Error("claim runner assignment failed", slog.String("error", err.Error()))
		return false
	}
	if err = daemon.executeAssignment(ctx, assignment); err == nil {
		return true
	}
	daemon.logger.Error("runner assignment failed",
		slog.String("scaleSet", assignment.ScaleSet), slog.String("repository", assignment.Repository),
		slog.Int64("requestID", assignment.RequestID), slog.String("error", err.Error()))
	if retryErr := daemon.store.Retry(context.WithoutCancel(ctx), assignment.ID); retryErr != nil {
		daemon.logger.Error("return runner assignment for retry failed", slog.String("error", retryErr.Error()))
	}
	return false
}

func (daemon *clusteredDaemon) executeAssignment(ctx context.Context, assignment *cluster.Assignment) error {
	scaleSet, exists := localScaleSet(daemon.config.ScaleSets, assignment.ScaleSet)
	if !exists {
		if err := daemon.store.Release(ctx, assignment.ID); err != nil {
			return fmt.Errorf("release assignment for removed local scale set: %w", err)
		}
		return nil
	}
	client, err := daemon.runtime.NewGitHubClient(assignment.Repository, assignment.InstallationID, daemon.credentials)
	if err != nil {
		return fmt.Errorf("create assignment GitHub client: %w", err)
	}
	reconciler, err := daemon.runtime.BuildReconciler(ctx, assignment.Repository, scaleSet, client, assignment.GitHubScaleSetID)
	if err != nil {
		return err
	}
	if err = reconciler.StartNamed(ctx, assignment.RequestID); err != nil {
		return fmt.Errorf("start assigned runner: %w", err)
	}
	exists, err = reconciler.Exists(ctx, assignment.RequestID)
	if err != nil {
		return fmt.Errorf("verify assigned runner: %w", err)
	}
	if !exists {
		return errors.New("provider did not start the assigned runner")
	}
	if err = daemon.store.Started(ctx, assignment.ID); err != nil {
		return fmt.Errorf("record started runner assignment: %w", err)
	}
	return nil
}

func localScaleSet(scaleSets []config.ScaleSetConfig, name string) (config.ScaleSetConfig, bool) {
	for _, scaleSet := range scaleSets {
		if scaleSet.Name == name {
			return scaleSet, true
		}
	}
	return config.ScaleSetConfig{}, false
}

func (daemon *clusteredDaemon) reload(ctx context.Context, configPath string) error {
	loaded, err := daemon.runtime.LoadConfig(configPath)
	if err != nil {
		return err
	}
	if loaded.State.Postgres == nil || *loaded.State.Postgres != *daemon.config.State.Postgres {
		return errors.New("clustered reload cannot change PostgreSQL state configuration or switch state backends")
	}
	credentials, err := loaded.GitHub.AppCredentials()
	if err != nil {
		return fmt.Errorf("load reloaded GitHub App credentials: %w", err)
	}
	discoverer, err := daemon.runtime.NewDiscoverer(credentials)
	if err != nil {
		return fmt.Errorf("create reloaded repository discoverer: %w", err)
	}
	daemon.config, daemon.credentials, daemon.discoverer = loaded, credentials, discoverer
	if err = daemon.publish(ctx, true); err != nil {
		return err
	}
	logAction(ctx, daemon.logger, slog.LevelInfo, "reload", "completed")
	return nil
}

func (daemon *clusteredDaemon) shutdown(ctx context.Context) {
	daemon.stopLeadershipContender(ctx)
	if daemon.leader != nil {
		daemon.leader.stop(ctx)
	}
	if err := daemon.publish(ctx, false); err != nil {
		daemon.logger.Warn("publish node shutdown failed", slog.String("error", err.Error()))
	}
}

type clusterLeader struct {
	daemon    *clusteredDaemon
	lease     cluster.Lease
	scheduler *clusterScheduler
	managers  map[clusterManagerKey]*clusterManager
}

type clusterManagerKey struct {
	repository string
	scaleSet   string
}

type clusterManager struct {
	key            clusterManagerKey
	installationID int64
	capacity       int
	client         scaleSetClient
	messageClient  scaleSetMessageClient
	runnerScaleSet *upstream.RunnerScaleSet
	stopPolling    context.CancelFunc
	pollerDone     <-chan struct{}
}

func newClusterLeader(daemon *clusteredDaemon, lease cluster.Lease) *clusterLeader {
	scheduler := &clusterScheduler{store: daemon.store, managers: map[clusterManagerKey]*clusterManager{}}
	return &clusterLeader{daemon: daemon, lease: lease, scheduler: scheduler, managers: map[clusterManagerKey]*clusterManager{}}
}

func (leader *clusterLeader) reconcile(ctx context.Context) {
	discovery, err := leader.daemon.discoverer.Discover(ctx)
	if err != nil {
		leader.daemon.logger.Error("leader repository discovery failed", slog.String("error", err.Error()))
		return
	}
	statuses, err := leader.daemon.store.ScaleSets(ctx, time.Now(), clusterNodeLifetime)
	if err != nil {
		leader.daemon.logger.Error("leader scale-set aggregation failed", slog.String("error", err.Error()))
		return
	}
	desired, repositories := leader.desiredManagers(discovery.Repositories, statuses)
	leader.stopOutdatedManagers(ctx, desired, statuses)
	leader.startMissingManagers(ctx, desired, repositories)
}

func (leader *clusterLeader) desiredManagers(
	discovered []githubapp.Repository,
	statuses []cluster.ScaleSetStatus,
) (map[clusterManagerKey]cluster.ScaleSetStatus, map[string]githubapp.Repository) {
	desired := map[clusterManagerKey]cluster.ScaleSetStatus{}
	repositories := make(map[string]githubapp.Repository, len(discovered))
	for _, repository := range discovered {
		repositories[repository.URL] = repository
		for _, status := range statuses {
			if !status.Compatible {
				leader.daemon.logger.Error("conflicting clustered scale-set declarations", slog.String("scaleSet", status.Name))
				continue
			}
			desired[clusterManagerKey{repository: repository.URL, scaleSet: status.Name}] = status
		}
	}
	return desired, repositories
}

func (leader *clusterLeader) stopOutdatedManagers(
	ctx context.Context,
	desired map[clusterManagerKey]cluster.ScaleSetStatus,
	statuses []cluster.ScaleSetStatus,
) {
	for key, manager := range leader.managers {
		status, wanted := desired[key]
		if wanted && manager.capacity == status.EffectiveCapacity {
			continue
		}
		scaleSetStillDeclared := hasClusterStatus(statuses, key.scaleSet)
		removal := retainGitHubScaleSet
		if !scaleSetStillDeclared {
			removal = removeGitHubScaleSet
		}
		leader.stopManager(ctx, manager, removal)
		delete(leader.managers, key)
		leader.scheduler.remove(key)
	}
}

func (leader *clusterLeader) startMissingManagers(
	ctx context.Context,
	desired map[clusterManagerKey]cluster.ScaleSetStatus,
	repositories map[string]githubapp.Repository,
) {
	for key, status := range desired {
		if _, exists := leader.managers[key]; exists {
			continue
		}
		manager, startErr := leader.startManager(ctx, repositories[key.repository], status)
		if startErr != nil {
			leader.daemon.logger.Error("leader scale-set manager failed",
				slog.String("repository", key.repository), slog.String("scaleSet", key.scaleSet), slog.String("error", startErr.Error()))
			continue
		}
		leader.managers[key] = manager
		leader.scheduler.add(manager)
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

func (leader *clusterLeader) startManager(
	ctx context.Context,
	repository githubapp.Repository,
	status cluster.ScaleSetStatus,
) (*clusterManager, error) {
	client, err := leader.daemon.runtime.NewGitHubClient(repository.URL, repository.InstallationID, leader.daemon.credentials)
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
	manager := &clusterManager{
		key:            clusterManagerKey{repository: repository.URL, scaleSet: status.Name},
		installationID: repository.InstallationID, capacity: status.EffectiveCapacity,
		client: client, messageClient: messageClient, runnerScaleSet: runnerScaleSet,
	}
	pollerCtx, stopPolling := context.WithCancel(ctx)
	manager.stopPolling = stopPolling
	manager.pollerDone = leader.startManagerPoller(pollerCtx, manager)
	return manager, nil
}

func (leader *clusterLeader) startManagerPoller(ctx context.Context, manager *clusterManager) <-chan struct{} {
	poller := scalesetpoller.Poller{
		Client:      manager.messageClient,
		Handler:     clusterMessageHandler{store: leader.daemon.store, scheduler: leader.scheduler, manager: manager},
		MaxCapacity: manager.capacity,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		pollUntilShutdown(ctx, leader.daemon.logger, manager.key.repository, manager.key.scaleSet, &poller, pollRetryDelay)
	}()
	return done
}

type managerRemoval int

const (
	retainGitHubScaleSet managerRemoval = iota
	removeGitHubScaleSet
)

func (leader *clusterLeader) stopManager(ctx context.Context, manager *clusterManager, removal managerRemoval) {
	manager.stopPolling()
	<-manager.pollerDone
	if err := manager.messageClient.Close(ctx); err != nil {
		leader.daemon.logger.Error("close leader message session failed", slog.String("error", err.Error()))
	}
	if removal == removeGitHubScaleSet {
		if err := manager.client.DeleteRunnerScaleSet(ctx, manager.runnerScaleSet.ID); err != nil {
			leader.daemon.logger.Error("remove withdrawn runner scale set failed", slog.String("error", err.Error()))
		}
	}
}

func (leader *clusterLeader) stop(ctx context.Context) {
	for _, manager := range leader.managers {
		leader.stopManager(ctx, manager, retainGitHubScaleSet)
	}
	leader.managers = map[clusterManagerKey]*clusterManager{}
	leader.scheduler.clear()
	if err := leader.lease.Close(ctx); err != nil {
		leader.daemon.logger.Warn("close leader lease failed", slog.String("error", err.Error()))
	}
}

type clusterScheduler struct {
	store    clusterState
	mutex    sync.Mutex
	managers map[clusterManagerKey]*clusterManager
}

func (scheduler *clusterScheduler) add(manager *clusterManager) {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	scheduler.managers[manager.key] = manager
}

func (scheduler *clusterScheduler) remove(key clusterManagerKey) {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	delete(scheduler.managers, key)
}

func (scheduler *clusterScheduler) clear() {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	scheduler.managers = map[clusterManagerKey]*clusterManager{}
}

func (scheduler *clusterScheduler) schedule(ctx context.Context, scaleSet string) error {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	for {
		done, err := scheduler.scheduleOne(ctx, scaleSet)
		if done || err != nil {
			return err
		}
	}
}

func (scheduler *clusterScheduler) scheduleOne(ctx context.Context, scaleSet string) (bool, error) {
	assignment, err := scheduler.store.AssignOldest(ctx, scaleSet, time.Now(), clusterNodeLifetime)
	if errors.Is(err, cluster.ErrNoAssignment) {
		return true, nil
	}
	if err != nil {
		return true, fmt.Errorf("assign oldest clustered demand: %w", err)
	}
	manager := scheduler.managers[clusterManagerKey{repository: assignment.Repository, scaleSet: scaleSet}]
	if manager == nil {
		if releaseErr := scheduler.store.Release(ctx, assignment.ID); releaseErr != nil {
			return true, fmt.Errorf("release demand without a repository manager: %w", releaseErr)
		}
		return true, nil
	}
	if err = manager.messageClient.Acquire(ctx, assignment.RequestID); err != nil {
		if releaseErr := scheduler.store.Release(context.WithoutCancel(ctx), assignment.ID); releaseErr != nil {
			return true, errors.Join(err, fmt.Errorf("release unacquired clustered demand: %w", releaseErr))
		}
		return true, err
	}
	if err = scheduler.store.Ready(ctx, assignment.ID, manager.installationID, manager.runnerScaleSet.ID); err != nil {
		return true, fmt.Errorf("make clustered assignment ready: %w", err)
	}
	return false, nil
}

type clusterMessageHandler struct {
	store     clusterState
	scheduler *clusterScheduler
	manager   *clusterManager
}

func (handler clusterMessageHandler) HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	if message.Statistics == nil {
		return errors.New("scale set message statistics are required")
	}
	if err := handler.enqueueAvailable(ctx, message.JobAvailableMessages); err != nil {
		return err
	}
	if err := handler.removeStarted(ctx, message.JobStartedMessages); err != nil {
		return err
	}
	if err := handler.removeCompleted(ctx, message.JobCompletedMessages); err != nil {
		return err
	}
	return handler.scheduler.schedule(ctx, handler.manager.key.scaleSet)
}

func (handler clusterMessageHandler) enqueueAvailable(ctx context.Context, messages []*upstream.JobAvailable) error {
	for _, available := range messages {
		if available == nil || available.RunnerRequestID <= 0 || available.QueueTime.IsZero() {
			return errors.New("job available message requires a positive runner request ID and queue time")
		}
		if err := handler.store.Enqueue(ctx, cluster.Demand{
			ScaleSet: handler.manager.key.scaleSet, Repository: handler.manager.key.repository,
			RequestID: available.RunnerRequestID, QueueTime: available.QueueTime,
		}); err != nil {
			return fmt.Errorf("persist clustered job demand: %w", err)
		}
	}
	return nil
}

func (handler clusterMessageHandler) removeStarted(ctx context.Context, messages []*upstream.JobStarted) error {
	for _, started := range messages {
		if started != nil && started.RunnerRequestID > 0 {
			if err := handler.store.RemoveDemand(ctx, handler.manager.key.scaleSet, handler.manager.key.repository, started.RunnerRequestID); err != nil {
				return fmt.Errorf("remove started clustered demand: %w", err)
			}
		}
	}
	return nil
}

func (handler clusterMessageHandler) removeCompleted(ctx context.Context, messages []*upstream.JobCompleted) error {
	for _, completed := range messages {
		if completed != nil && completed.RunnerRequestID > 0 {
			if err := handler.store.RemoveDemand(ctx, handler.manager.key.scaleSet, handler.manager.key.repository, completed.RunnerRequestID); err != nil {
				return fmt.Errorf("remove completed clustered demand: %w", err)
			}
		}
	}
	return nil
}
