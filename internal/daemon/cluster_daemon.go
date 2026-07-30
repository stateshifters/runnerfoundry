package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

const (
	// A node expires after three heartbeat intervals. Coordination work is
	// bounded below one interval so a slow dependency cannot consume the
	// failover window. Provider starts run in their own worker because they may
	// legitimately take several minutes.
	clusterHeartbeatInterval      = 10 * time.Second
	clusterNodeLifetime           = cluster.NodeLifetime
	clusterLeadershipRetryDelay   = 2 * time.Second
	clusterExecutorInterval       = time.Second
	clusterCoordinationTimeout    = 5 * time.Second
	clusterGitHubTimeout          = 30 * time.Second
	clusterProviderInspectTimeout = 30 * time.Second
	clusterProviderStartTimeout   = 5 * time.Minute
	libvirtReapTimeout            = 30 * time.Second
)

type clusteredDaemon struct {
	nodeID                    cluster.NodeID
	config                    config.Config
	credentials               config.GitHubAppCredentials
	discoverer                repositoryDiscoverer
	logger                    *slog.Logger
	logging                   *loggingManager
	hostname                  func() (string, error)
	newLogger                 func(string, string) *slog.Logger
	newLogging                func(config.LogConfig) (*loggingManager, error)
	openStore                 clusterStateOpener
	loadConfig                func(string) (config.Config, error)
	loadCredentials           func(config.GitHubConfig) (config.GitHubAppCredentials, error)
	newDiscoverer             func(config.GitHubAppCredentials) (repositoryDiscoverer, error)
	newGitHubClient           func(string, githubapp.InstallationID, config.GitHubAppCredentials) (scaleSetClient, error)
	providerHealthy           func(context.Context, config.ScaleSetConfig) error
	buildReconciler           func(context.Context, string, config.ScaleSetConfig, scaleSetJITClient, int) (scaler.Reconciler, error)
	notifier                  serviceNotifier
	nodes                     clusterNodeState
	assignments               clusterAssignmentState
	tryLeader                 func(context.Context) (leadershipResult, error)
	waitForLeader             func(context.Context) (leadershipResult, error)
	leader                    *clusterLeader
	leadershipResults         chan leadershipResult
	stopContender             context.CancelFunc
	contenderDone             <-chan struct{}
	leadershipMutex           sync.Mutex
	workers                   sync.WaitGroup
	leadershipRetryDelay      time.Duration
	waitBeforeLeadershipRetry func(context.Context, time.Duration) bool
	configurationMutex        sync.RWMutex
}

type leadershipResult struct {
	lease cluster.Lease
}

type clusterMigrationState interface {
	Migrate(ctx context.Context) error
	MigrationStatus(ctx context.Context) (cluster.MigrationStatus, error)
}

type clusterNodeState interface {
	Publish(ctx context.Context, nodeID cluster.NodeID, health cluster.NodeHealth, contributions []cluster.Contribution, now time.Time) error
	ScaleSets(ctx context.Context, now time.Time, liveFor time.Duration) ([]cluster.ScaleSetStatus, error)
}

type clusterDemandState interface {
	Enqueue(ctx context.Context, demand cluster.Demand) error
	CompleteDemand(ctx context.Context, scaleSet string, repository string, requestID int64) error
	CompleteJob(ctx context.Context, completion cluster.JobCompletion) error
}

type clusterSchedulingState interface {
	AssignOldest(ctx context.Context, scaleSet string, now time.Time, liveFor time.Duration) (*cluster.Assignment, error)
	BeginAcquisition(ctx context.Context, assignmentID cluster.AssignmentID, installationID githubapp.InstallationID, githubScaleSetID int) error
	ConfirmAcquisition(ctx context.Context, assignmentID cluster.AssignmentID) error
	PendingAcquisitions(ctx context.Context) ([]cluster.Assignment, error)
	CompleteAssignment(ctx context.Context, assignmentID cluster.AssignmentID, nodeID cluster.NodeID, expectedState cluster.AssignmentState) error
}

type clusterAssignmentState interface {
	clusterDemandState
	clusterSchedulingState
	clusterAssignmentExecutionState
	clusterLibvirtLifecycleState
}

type clusterAssignmentExecutionState interface {
	ClaimForStart(ctx context.Context, nodeID cluster.NodeID) (*cluster.Assignment, error)
	Starting(ctx context.Context, nodeID cluster.NodeID) ([]cluster.Assignment, error)
	ConfirmRunning(ctx context.Context, assignmentID cluster.AssignmentID, nodeID cluster.NodeID) error
}

type clusterLibvirtLifecycleState interface {
	RegisterLibvirtRunner(ctx context.Context, assignmentID cluster.AssignmentID, runner cluster.LibvirtRunnerLifecycle) error
	PendingLibvirtReaps(ctx context.Context, nodeID cluster.NodeID) ([]cluster.LibvirtRunnerLifecycle, error)
	ConfirmLibvirtReaped(ctx context.Context, runner cluster.LibvirtRunnerLifecycle) error
}

type clusterState interface {
	clusterMigrationState
	clusterNodeState
	clusterAssignmentState
}

type clusterStoreConnection struct {
	state         clusterState
	tryLeader     func(context.Context) (leadershipResult, error)
	waitForLeader func(context.Context) (leadershipResult, error)
}

type clusterStateOpener func(postgresconfig.Settings) (clusterStoreConnection, error)

func newClusteredDaemon() *clusteredDaemon {
	return &clusteredDaemon{
		hostname: os.Hostname, newLogger: newLogger, loadConfig: loadConfig,
		loadCredentials: func(githubConfig config.GitHubConfig) (config.GitHubAppCredentials, error) {
			return githubConfig.AppCredentials(os.ReadFile)
		},
		newLogging: func(logConfig config.LogConfig) (*loggingManager, error) {
			return newLoggingManager(logConfig, newProductionLogDestinationFactory())
		},
		openStore: openClusterStore,
		newDiscoverer: func(credentials config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return githubapp.NewDiscoverer(credentials)
		},
		newGitHubClient: func(repositoryURL string, installationID githubapp.InstallationID, credentials config.GitHubAppCredentials) (scaleSetClient, error) {
			return newGitHubClient(repositoryURL, installationID, credentials)
		},
		providerHealthy:           validateProductionScaleSetProvider,
		buildReconciler:           buildReconciler,
		notifier:                  newSystemdNotifier(),
		leadershipRetryDelay:      clusterLeadershipRetryDelay,
		waitBeforeLeadershipRetry: waitForRetryDelay,
	}
}

func openClusterStore(state postgresconfig.Settings) (clusterStoreConnection, error) {
	store := cluster.NewStore(state)
	return clusterStoreConnection{
		state:         store,
		tryLeader:     func(ctx context.Context) (leadershipResult, error) { return tryClusterLeadership(ctx, store) },
		waitForLeader: func(ctx context.Context) (leadershipResult, error) { return waitForClusterLeadership(ctx, store) },
	}, nil
}

func tryClusterLeadership(ctx context.Context, store *cluster.Store) (leadershipResult, error) {
	lease, err := store.TryLeader(ctx)
	return clusterLeadershipResult("try cluster leadership", lease, err)
}

func waitForClusterLeadership(ctx context.Context, store *cluster.Store) (leadershipResult, error) {
	lease, err := store.WaitForLeader(ctx)
	return clusterLeadershipResult("wait for cluster leadership", lease, err)
}

func clusterLeadershipResult(operation string, lease cluster.Lease, err error) (leadershipResult, error) {
	if err != nil {
		return leadershipResult{}, fmt.Errorf("%s: %w", operation, err)
	}
	return leadershipResult{lease: lease}, nil
}

func runClusteredDaemon(
	ctx context.Context,
	options Options,
	daemon *clusteredDaemon,
	configured config.Config,
) (runErr error) {
	var err error
	daemon, err = prepareClusteredDaemon(ctx, daemon, configured)
	if err != nil {
		return err
	}
	if err = daemon.publish(ctx, cluster.NodeHealthy); err != nil {
		return err
	}
	defer func() {
		daemon.shutdown(context.WithoutCancel(ctx))
		if daemon.logging != nil {
			runErr = errors.Join(runErr, daemon.logging.Close())
		}
	}()
	daemon.tryLeadership(ctx)
	if daemon.currentLeader() == nil {
		daemon.startContender(ctx)
		daemon.notifyStatus(ctx, clusterServiceStatusStandby)
	}
	if err = daemon.notifier.Ready(ctx); err != nil {
		return fmt.Errorf("notify ready: %w", err)
	}
	logAction(ctx, daemon.logger, slog.LevelInfo, "daemon", "ready",
		slog.String("mode", "clustered"), slog.String("hostname", string(daemon.nodeID)))
	return daemon.run(ctx, options)
}

func prepareClusteredDaemon(ctx context.Context, daemon *clusteredDaemon, configured config.Config) (*clusteredDaemon, error) {
	nodeID, err := clusteredNodeID(daemon.hostname)
	if err != nil {
		return nil, err
	}
	logger, logging, err := daemon.prepareLogging(configured.Log)
	if err != nil {
		return nil, err
	}
	if err = daemon.connectState(ctx, configured, logger); err != nil {
		return nil, closePreparedDaemonLogging(err, logging)
	}
	if err = daemon.connectGitHub(configured); err != nil {
		return nil, closePreparedDaemonLogging(err, logging)
	}
	daemon.nodeID = nodeID
	daemon.config = configured
	daemon.logger = logger
	daemon.logging = logging
	daemon.leadershipResults = make(chan leadershipResult, 1)
	return daemon, nil
}

func (daemon *clusteredDaemon) prepareLogging(logConfig config.LogConfig) (*slog.Logger, *loggingManager, error) {
	if daemon.newLogging == nil {
		return daemon.newLogger(logConfig.Format, logConfig.Level), nil, nil
	}
	logging, err := daemon.newLogging(logConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("open log destination: %w", err)
	}
	return logging.Logger(), logging, nil
}

func closePreparedDaemonLogging(preparationError error, logging *loggingManager) error {
	if logging == nil {
		return preparationError
	}
	return errors.Join(preparationError, logging.Close())
}

func (daemon *clusteredDaemon) connectState(ctx context.Context, configured config.Config, logger *slog.Logger) error {
	if configured.State.Postgres == nil {
		return errors.New("PostgreSQL clustered state is required")
	}
	connection, err := daemon.openStore(*configured.State.Postgres)
	if err != nil {
		return fmt.Errorf("open PostgreSQL clustered state: %w", err)
	}
	if err = migrateClusteredStore(ctx, logger, connection.state); err != nil {
		return err
	}
	daemon.nodes = connection.state
	daemon.assignments = connection.state
	daemon.tryLeader = connection.tryLeader
	daemon.waitForLeader = connection.waitForLeader
	return nil
}

func (daemon *clusteredDaemon) connectGitHub(configured config.Config) error {
	credentials, err := daemon.credentialsFor(configured.GitHub)
	if err != nil {
		return fmt.Errorf("load GitHub App credentials: %w", err)
	}
	discoverer, err := daemon.newDiscoverer(credentials)
	if err != nil {
		return fmt.Errorf("create repository discoverer: %w", err)
	}
	daemon.credentials = credentials
	daemon.discoverer = discoverer
	return nil
}

func (daemon *clusteredDaemon) credentialsFor(githubConfig config.GitHubConfig) (config.GitHubAppCredentials, error) {
	if daemon.loadCredentials != nil {
		return daemon.loadCredentials(githubConfig)
	}
	credentials, err := githubConfig.AppCredentials(os.ReadFile)
	if err != nil {
		return config.GitHubAppCredentials{}, fmt.Errorf("read configured GitHub App credentials: %w", err)
	}
	return credentials, nil
}

func clusteredNodeID(hostname func() (string, error)) (cluster.NodeID, error) {
	if hostname == nil {
		return "", errors.New("clustered host name reader is required")
	}
	nodeID, err := hostname()
	if err != nil {
		return "", fmt.Errorf("read clustered host name: %w", err)
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return "", errors.New("clustered host name is empty")
	}
	return cluster.NodeID(nodeID), nil
}

func migrateClusteredStore(ctx context.Context, logger *slog.Logger, store clusterMigrationState) error {
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
	workerCtx, stopWorkers := context.WithCancel(ctx)
	reconciliationRequests := daemon.startClusterWorkers(workerCtx, events)
	if daemon.currentLeader() != nil {
		requestReconciliation(reconciliationRequests)
	}
	defer func() {
		stopWorkers()
		daemon.waitForWorkers()
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case result := <-events.leadership:
			daemon.clearContenderRegistration()
			daemon.acceptLeadership(ctx, result.lease)
			requestReconciliation(reconciliationRequests)
		case <-options.ReloadRequests:
			if err := daemon.reload(ctx, options.ConfigPath); err != nil {
				return err
			}
			if daemon.currentLeader() != nil {
				requestReconciliation(reconciliationRequests)
			}
		}
	}
}
