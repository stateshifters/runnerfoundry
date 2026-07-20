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

	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	"github.com/meza/github-scaler/internal/scaler"
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
)

type clusteredDaemon struct {
	nodeID               string
	config               config.Config
	credentials          config.GitHubAppCredentials
	discoverer           repositoryDiscoverer
	logger               *slog.Logger
	hostname             func() (string, error)
	newLogger            func(string, string) *slog.Logger
	openStore            clusterStateOpener
	loadConfig           func(string) (config.Config, error)
	newDiscoverer        func(config.GitHubAppCredentials) (repositoryDiscoverer, error)
	newGitHubClient      func(string, int64, config.GitHubAppCredentials) (scaleSetClient, error)
	providerHealthy      func(context.Context, config.ScaleSetConfig) error
	buildReconciler      func(context.Context, string, config.ScaleSetConfig, scaleSetClient, int) (scaler.Reconciler, error)
	notifier             serviceNotifier
	nodes                clusterNodeState
	assignments          clusterAssignmentState
	tryLeader            func(context.Context) (cluster.Lease, error)
	waitForLeader        func(context.Context) (cluster.Lease, error)
	leader               *clusterLeader
	leadershipResults    chan leadershipResult
	stopContender        context.CancelFunc
	contenderDone        <-chan struct{}
	leadershipMutex      sync.Mutex
	workers              sync.WaitGroup
	leadershipRetryDelay time.Duration
	configurationMutex   sync.RWMutex
}

type leadershipResult struct {
	lease cluster.Lease
}

type clusterMigrationState interface {
	Migrate(ctx context.Context) error
	MigrationStatus(ctx context.Context) (cluster.MigrationStatus, error)
}

type clusterNodeState interface {
	Publish(ctx context.Context, nodeID string, health cluster.NodeHealth, contributions []cluster.Contribution, now time.Time) error
	ScaleSets(ctx context.Context, now time.Time, liveFor time.Duration) ([]cluster.ScaleSetStatus, error)
}

type clusterDemandState interface {
	Enqueue(ctx context.Context, demand cluster.Demand) error
	CompleteDemand(ctx context.Context, scaleSet string, repository string, requestID int64) error
}

type clusterSchedulingState interface {
	AssignOldest(ctx context.Context, scaleSet string, now time.Time, liveFor time.Duration) (*cluster.Assignment, error)
	BeginAcquisition(ctx context.Context, assignmentID int64, installationID int64, githubScaleSetID int) error
	ConfirmAcquisition(ctx context.Context, assignmentID int64) error
	PendingAcquisitions(ctx context.Context) ([]cluster.Assignment, error)
	CompleteAssignment(ctx context.Context, assignmentID int64, nodeID string, expectedState cluster.AssignmentState) error
}

type clusterAssignmentState interface {
	clusterDemandState
	clusterSchedulingState
	ClaimForStart(ctx context.Context, nodeID string) (*cluster.Assignment, error)
	Starting(ctx context.Context, nodeID string) ([]cluster.Assignment, error)
	ConfirmRunning(ctx context.Context, assignmentID int64, nodeID string) error
}

type clusterState interface {
	clusterMigrationState
	clusterNodeState
	clusterAssignmentState
}

type clusterStateOpener func(config.PostgreSQLStateConfig) (
	state clusterState,
	tryLeader func(context.Context) (cluster.Lease, error),
	waitForLeader func(context.Context) (cluster.Lease, error),
	err error,
)

func newClusteredDaemon() *clusteredDaemon {
	return &clusteredDaemon{
		hostname: os.Hostname, newLogger: newLogger, loadConfig: loadConfig,
		openStore: func(state config.PostgreSQLStateConfig) (
			clusterState,
			func(context.Context) (cluster.Lease, error),
			func(context.Context) (cluster.Lease, error),
			error,
		) {
			store := cluster.NewStore(state)
			return store,
				func(ctx context.Context) (cluster.Lease, error) { return store.TryLeader(ctx) },
				func(ctx context.Context) (cluster.Lease, error) { return store.WaitForLeader(ctx) },
				nil
		},
		newDiscoverer: func(credentials config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return githubapp.NewDiscoverer(credentials)
		},
		newGitHubClient: func(repositoryURL string, installationID int64, credentials config.GitHubAppCredentials) (scaleSetClient, error) {
			return newGitHubClient(repositoryURL, installationID, credentials)
		},
		providerHealthy:      validateProductionScaleSetProvider,
		buildReconciler:      buildReconciler,
		notifier:             systemdNotifier{},
		leadershipRetryDelay: clusterLeadershipRetryDelay,
	}
}

func runClusteredDaemon(
	ctx context.Context,
	options Options,
	daemon *clusteredDaemon,
	configured config.Config,
) error {
	var err error
	daemon, err = prepareClusteredDaemon(ctx, daemon, configured)
	if err != nil {
		return err
	}
	if err = daemon.publish(ctx, cluster.NodeHealthy); err != nil {
		return err
	}
	defer daemon.shutdown(context.WithoutCancel(ctx))
	daemon.tryLeadership(ctx)
	if daemon.currentLeader() == nil {
		daemon.startContender(ctx)
		daemon.notifyStatus(ctx, clusterServiceStatusStandby)
	}
	if err = daemon.notifier.Ready(ctx); err != nil {
		return fmt.Errorf("notify ready: %w", err)
	}
	logAction(ctx, daemon.logger, slog.LevelInfo, "daemon", "ready",
		slog.String("mode", "clustered"), slog.String("hostname", daemon.nodeID))
	return daemon.run(ctx, options)
}

func prepareClusteredDaemon(ctx context.Context, daemon *clusteredDaemon, configured config.Config) (*clusteredDaemon, error) {
	nodeID, err := clusteredNodeID(daemon.hostname)
	if err != nil {
		return nil, err
	}
	logger := daemon.newLogger(configured.Log.Format, configured.Log.Level)
	if err = daemon.connectState(ctx, configured, logger); err != nil {
		return nil, err
	}
	if err = daemon.connectGitHub(configured); err != nil {
		return nil, err
	}
	daemon.nodeID = nodeID
	daemon.config = configured
	daemon.logger = logger
	daemon.leadershipResults = make(chan leadershipResult, 1)
	return daemon, nil
}

func (daemon *clusteredDaemon) connectState(ctx context.Context, configured config.Config, logger *slog.Logger) error {
	if configured.State.Postgres == nil {
		return errors.New("PostgreSQL clustered state is required")
	}
	store, tryLeader, waitForLeader, err := daemon.openStore(*configured.State.Postgres)
	if err != nil {
		return fmt.Errorf("open PostgreSQL clustered state: %w", err)
	}
	if err = migrateClusteredStore(ctx, logger, store); err != nil {
		return err
	}
	daemon.nodes = store
	daemon.assignments = store
	daemon.tryLeader = tryLeader
	daemon.waitForLeader = waitForLeader
	return nil
}

func (daemon *clusteredDaemon) connectGitHub(configured config.Config) error {
	credentials, err := configured.GitHub.AppCredentials()
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

func clusteredNodeID(hostname func() (string, error)) (string, error) {
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
	return nodeID, nil
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
