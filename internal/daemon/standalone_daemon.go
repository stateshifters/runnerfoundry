package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type standaloneDaemon struct {
	loadConfig      func(configPath string) (config.Config, error)
	loadCredentials func(config.GitHubConfig) (config.GitHubAppCredentials, error)
	newDiscoverer   func(credentials config.GitHubAppCredentials) (repositoryDiscoverer, error)
	newGitHubClient func(repositoryURL string, installationID githubapp.InstallationID, credentials config.GitHubAppCredentials) (scaleSetClient, error)
	newLogger       func(format string, level string) *slog.Logger
	newLogging      func(config.LogConfig) (*loggingManager, error)
	openDemandStore func(path string) (scaler.DemandStore, io.Closer, error)
	notifier        serviceNotifier
	startScaleSet   func(
		ctx context.Context,
		logger *slog.Logger,
		client scaleSetClient,
		repositoryURL string,
		scaleSetConfig config.ScaleSetConfig,
		coordinator *scaler.Coordinator,
	) (scaleSetLifecycle, error)
	startConfirmedScaleSet func(
		ctx context.Context,
		logger *slog.Logger,
		client scaleSetClient,
		repositoryURL string,
		scaleSetConfig config.ScaleSetConfig,
		coordinator *scaler.Coordinator,
		registration *upstream.RunnerScaleSet,
	) (scaleSetLifecycle, error)
	drainInterval            time.Duration
	reconciliationRetryDelay time.Duration
}

type noopNotifier struct{}

func (noopNotifier) Ready(context.Context) error     { return nil }
func (noopNotifier) Reloading(context.Context) error { return nil }

type managedRepository struct {
	InstallationID     githubapp.InstallationID
	Client             scaleSetClient
	Lifecycles         map[string]scaleSetLifecycle
	RegistrationStatus repositoryRegistrationStatus
}

type repositoryRegistrationStatus string

const (
	repositoryRegistrationDegraded    repositoryRegistrationStatus = "degraded"
	repositoryRegistrationOperational repositoryRegistrationStatus = "operational"
)

type activeConfiguration struct {
	Config                config.Config
	Credentials           config.GitHubAppCredentials
	Discoverer            repositoryDiscoverer
	Coordinators          map[string]*scaler.Coordinator
	Repositories          map[string]managedRepository
	Logger                *slog.Logger
	Logging               *loggingManager
	PreparedLogging       *preparedLogging
	DemandStore           scaler.DemandStore
	StateCloser           io.Closer
	ReconciliationPending bool
}

func newStandaloneDaemon() standaloneDaemon {
	return standaloneDaemon{
		loadConfig: loadConfig,
		loadCredentials: func(githubConfig config.GitHubConfig) (config.GitHubAppCredentials, error) {
			return githubConfig.AppCredentials(os.ReadFile)
		},
		newDiscoverer: func(credentials config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return githubapp.NewDiscoverer(credentials)
		},
		newGitHubClient: func(repositoryURL string, installationID githubapp.InstallationID, credentials config.GitHubAppCredentials) (scaleSetClient, error) {
			return newGitHubClient(repositoryURL, installationID, credentials)
		},
		newLogger: newLogger,
		newLogging: func(logConfig config.LogConfig) (*loggingManager, error) {
			return newLoggingManager(logConfig, newProductionLogDestinationFactory())
		},
		openDemandStore: func(path string) (scaler.DemandStore, io.Closer, error) {
			store, err := openSQLiteDemandStore(path)
			if err != nil {
				return nil, nil, fmt.Errorf("open SQLite demand store: %w", err)
			}
			return store, store, nil
		},
		notifier:                 newSystemdNotifier(),
		startScaleSet:            startCoordinatedScaleSet,
		startConfirmedScaleSet:   startConfirmedCoordinatedScaleSet,
		drainInterval:            time.Second,
		reconciliationRetryDelay: pollRetryDelay,
	}
}

func runDaemon(ctx context.Context, options Options, daemon standaloneDaemon) (runErr error) {
	daemon = withStandaloneDefaults(daemon)
	activeConfig, err := loadConfiguration(options.ConfigPath, daemon)
	if err != nil {
		return err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "configuration", "loaded",
		slog.String("configPath", options.ConfigPath),
		slog.Int("scaleSetCount", len(activeConfig.Config.ScaleSets)),
		slog.String("logFormat", activeConfig.Config.Log.Format),
		slog.String("logLevel", activeConfig.Config.Log.Level),
	)
	defer func() {
		runErr = errors.Join(runErr, activeConfig.StateCloser.Close())
		if activeConfig.Logging != nil {
			runErr = errors.Join(runErr, activeConfig.Logging.Close())
		}
	}()
	discoveryInterval, err := activeConfig.Config.GitHub.DiscoveryDuration()
	if err != nil {
		return fmt.Errorf("load discovery interval: %w", err)
	}
	if notifyErr := daemon.notifier.Ready(ctx); notifyErr != nil {
		return fmt.Errorf("notify ready: %w", notifyErr)
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "daemon", "ready")
	reconcileDiscovery(ctx, daemon, &activeConfig)
	return runActiveConfiguration(ctx, options, daemon, activeConfig, discoveryInterval)
}

func runActiveConfiguration(
	ctx context.Context,
	options Options,
	daemon standaloneDaemon,
	activeConfig activeConfiguration,
	discoveryInterval time.Duration,
) error {
	discoveryTimer := time.NewTimer(nextReconciliationDelay(activeConfig, discoveryInterval, daemon.reconciliationRetryDelay))
	defer discoveryTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdownActiveConfiguration(ctx, activeConfig)
			return nil
		case <-discoveryTimer.C:
			reconcileDiscovery(ctx, daemon, &activeConfig)
			discoveryTimer.Reset(nextReconciliationDelay(activeConfig, discoveryInterval, daemon.reconciliationRetryDelay))
		case <-options.ReloadRequests:
			reloadedConfig, reloadedInterval, reloadErr := reloadAndReadInterval(
				ctx, options.ConfigPath, daemon, activeConfig,
			)
			if reloadErr != nil {
				return reloadErr
			}
			activeConfig = reloadedConfig
			discoveryInterval = reloadedInterval
			discoveryTimer.Reset(nextReconciliationDelay(activeConfig, discoveryInterval, daemon.reconciliationRetryDelay))
		}
	}
}

func shutdownActiveConfiguration(ctx context.Context, activeConfig activeConfiguration) {
	shutdownCtx := context.WithoutCancel(ctx)
	logAction(shutdownCtx, activeConfig.Logger, slog.LevelInfo, "shutdown", "started")
	if err := shutdownWithTimeout(ctx, activeConfig, drainShutdown); err != nil {
		activeConfig.Logger.Warn("shutdown cleanup incomplete",
			slog.String("action", "shutdown"),
			slog.String("phase", "failed"),
			slog.String("error", err.Error()),
		)
	}
	logAction(shutdownCtx, activeConfig.Logger, slog.LevelInfo, "shutdown", "completed")
}

func reloadAndReadInterval(
	ctx context.Context,
	configPath string,
	daemon standaloneDaemon,
	activeConfig activeConfiguration,
) (activeConfiguration, time.Duration, error) {
	reloadedConfig, err := reloadConfiguration(ctx, configPath, daemon, activeConfig)
	if err != nil {
		return activeConfiguration{}, 0, err
	}
	discoveryInterval, err := reloadedConfig.Config.GitHub.DiscoveryDuration()
	if err != nil {
		return activeConfiguration{}, 0, fmt.Errorf("load reloaded discovery interval: %w", err)
	}
	logAction(ctx, reloadedConfig.Logger, slog.LevelInfo, "reload", "replacement_reconciliation_started")
	reconcileDiscovery(ctx, daemon, &reloadedConfig)
	logAction(ctx, reloadedConfig.Logger, slog.LevelInfo, "reload", "completed")
	return reloadedConfig, discoveryInterval, nil
}

func withStandaloneDefaults(daemon standaloneDaemon) standaloneDaemon {
	if daemon.notifier == nil {
		daemon.notifier = noopNotifier{}
	}
	if daemon.drainInterval == 0 {
		daemon.drainInterval = time.Second
	}
	if daemon.reconciliationRetryDelay == 0 {
		daemon.reconciliationRetryDelay = pollRetryDelay
	}
	if daemon.openDemandStore == nil {
		daemon.openDemandStore = func(string) (scaler.DemandStore, io.Closer, error) {
			return scaler.NewMemoryDemandStore(), noOpCloser{}, nil
		}
	}
	if daemon.loadCredentials == nil {
		daemon.loadCredentials = func(githubConfig config.GitHubConfig) (config.GitHubAppCredentials, error) {
			return githubConfig.AppCredentials(os.ReadFile)
		}
	}
	return daemon
}

type noOpCloser struct{}

func (noOpCloser) Close() error { return nil }

func loadConfiguration(configPath string, daemon standaloneDaemon) (activeConfiguration, error) {
	daemon = withStandaloneDefaults(daemon)
	loadedConfig, err := daemon.loadConfig(configPath)
	if err != nil {
		return activeConfiguration{}, err
	}
	var logging *loggingManager
	var logger *slog.Logger
	if daemon.newLogging != nil {
		logging, err = daemon.newLogging(loadedConfig.Log)
		if err != nil {
			return activeConfiguration{}, fmt.Errorf("open log destination: %w", err)
		}
		logger = logging.Logger()
	} else {
		logger = daemon.newLogger(loadedConfig.Log.Format, loadedConfig.Log.Level)
	}
	activeConfig, err := buildActiveConfiguration(loadedConfig, daemon, logger, logging)
	if err != nil && logging != nil {
		err = errors.Join(err, logging.Close())
	}
	return activeConfig, err
}

func buildActiveConfiguration(
	loadedConfig config.Config,
	daemon standaloneDaemon,
	logger *slog.Logger,
	logging *loggingManager,
) (activeConfiguration, error) {
	credentials, err := daemon.loadCredentials(loadedConfig.GitHub)
	if err != nil {
		return activeConfiguration{}, fmt.Errorf("load GitHub App credentials: %w", err)
	}
	discoverer, err := daemon.newDiscoverer(credentials)
	if err != nil {
		return activeConfiguration{}, fmt.Errorf("create repository discoverer: %w", err)
	}
	demandStore, stateCloser, err := daemon.openDemandStore(loadedConfig.State.Path)
	if err != nil {
		return activeConfiguration{}, fmt.Errorf("open durable state: %w", err)
	}
	activeConfig := activeConfiguration{
		Config:       loadedConfig,
		Credentials:  credentials,
		Discoverer:   discoverer,
		Coordinators: coordinators(loadedConfig.ScaleSets, demandStore),
		Repositories: map[string]managedRepository{},
		Logger:       logger,
		Logging:      logging,
		DemandStore:  demandStore,
		StateCloser:  stateCloser,
	}
	return activeConfig, nil
}
