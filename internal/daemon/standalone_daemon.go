package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

type standaloneState interface {
	scaler.DemandStore
	repositorystate.Store
	io.Closer
}

type standaloneDaemon struct {
	loadConfig      func(configPath string) (config.Config, error)
	loadCredentials func(config.GitHubConfig) (config.GitHubAppCredentials, error)
	newDiscoverer   func(credentials config.GitHubAppCredentials) (repositoryDiscoverer, error)
	newGitHubClient func(repositoryURL string, installationID githubapp.InstallationID, credentials config.GitHubAppCredentials) (scaleSetClient, error)
	newLogger       func(format string, level string) *slog.Logger
	newLogging      func(config.LogConfig) (*loggingManager, error)
	openState       func(path string) (standaloneState, error)
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
	Config                 config.Config
	Credentials            config.GitHubAppCredentials
	Discoverer             repositoryDiscoverer
	Coordinators           map[string]*scaler.Coordinator
	Repositories           map[string]managedRepository
	Logger                 *slog.Logger
	Logging                *loggingManager
	PreparedLogging        *preparedLogging
	DemandStore            scaler.DemandStore
	RepositoryState        repositorystate.Store
	RepositorySnapshot     repositorystate.Snapshot
	ConfirmedDiscovery     *githubapp.DiscoveryResult
	ConfirmedRegistrations []repositorystate.Registration
	RepositoryMutex        *sync.Mutex
	StateCloser            io.Closer
	DiscoveryInterval      time.Duration
	ReconciliationPending  bool
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
		openState: func(path string) (standaloneState, error) {
			store, err := openSQLiteDemandStore(path)
			if err != nil {
				return nil, fmt.Errorf("open SQLite demand store: %w", err)
			}
			return store, nil
		},
		notifier:                 newSystemdNotifier(),
		startScaleSet:            startCoordinatedScaleSet,
		startConfirmedScaleSet:   startConfirmedCoordinatedScaleSet,
		reconciliationRetryDelay: pollRetryDelay,
	}
}

func runDaemon(ctx context.Context, options Options, daemon standaloneDaemon) (runErr error) {
	daemon = withStandaloneDefaults(daemon)
	activeConfig, discoveryInterval, err := prepareStandaloneRuntime(ctx, options.ConfigPath, daemon)
	if err != nil {
		return err
	}
	defer func() {
		runErr = errors.Join(runErr, activeConfig.StateCloser.Close())
		if activeConfig.Logging != nil {
			runErr = errors.Join(runErr, activeConfig.Logging.Close())
		}
	}()
	control, err := startStandaloneControl(ctx, daemon, &activeConfig, standaloneControlOptions{})
	if err != nil {
		return fmt.Errorf("start standalone repository control: %w", err)
	}
	if notifyErr := daemon.notifier.Ready(ctx); notifyErr != nil {
		control.Stop()
		return fmt.Errorf("notify ready: %w", notifyErr)
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "daemon", "ready")
	return runActiveConfigurationWithControl(ctx, options, daemon, &activeConfig, discoveryInterval, control)
}

func prepareStandaloneRuntime(
	ctx context.Context,
	configPath string,
	daemon standaloneDaemon,
) (activeConfiguration, time.Duration, error) {
	activeConfig, err := loadConfiguration(configPath, daemon)
	if err != nil {
		return activeConfiguration{}, 0, err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "configuration", "loaded",
		slog.String("configPath", configPath),
		slog.Int("scaleSetCount", len(activeConfig.Config.ScaleSets)),
		slog.String("logFormat", activeConfig.Config.Log.Format),
		slog.String("logLevel", activeConfig.Config.Log.Level),
	)
	discoveryInterval, err := activeConfig.Config.GitHub.DiscoveryDuration()
	if err != nil {
		return activeConfiguration{}, 0, fmt.Errorf("load discovery interval: %w", err)
	}
	return activeConfig, discoveryInterval, nil
}

func runActiveConfiguration(
	ctx context.Context,
	options Options,
	daemon standaloneDaemon,
	activeConfig activeConfiguration,
) error {
	if activeConfig.RepositoryState == nil {
		state := newMemoryStandaloneState()
		activeConfig.RepositoryState = state
	}
	control, err := startStandaloneControl(ctx, daemon, &activeConfig, standaloneControlOptions{})
	if err != nil {
		return err
	}
	return runActiveConfigurationWithControl(ctx, options, daemon, &activeConfig, time.Hour, control)
}

func runActiveConfigurationWithControl(
	ctx context.Context,
	options Options,
	daemon standaloneDaemon,
	activeConfig *activeConfiguration,
	discoveryInterval time.Duration,
	control *standaloneControl,
) error {
	control.RequestDiscovery(ctx)
	discoveryTimer := time.NewTimer(discoveryInterval)
	defer discoveryTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			control.Stop()
			shutdownActiveConfiguration(ctx, *activeConfig)
			return nil
		case <-discoveryTimer.C:
			control.RequestDiscovery(ctx)
			discoveryTimer.Reset(discoveryInterval)
		case <-options.ReloadRequests:
			outcome, err := reloadStandaloneControl(
				ctx, options, daemon, activeConfig, discoveryTimer, discoveryInterval, control,
			)
			if err != nil {
				return err
			}
			if outcome.stop {
				shutdownActiveConfiguration(ctx, *activeConfig)
				return nil
			}
			control = outcome.control
			discoveryInterval = outcome.discoveryInterval
		}
	}
}

type standaloneReloadOutcome struct {
	control           *standaloneControl
	discoveryInterval time.Duration
	stop              bool
}

func reloadStandaloneControl(
	ctx context.Context,
	options Options,
	daemon standaloneDaemon,
	activeConfig *activeConfiguration,
	discoveryTimer *time.Timer,
	discoveryInterval time.Duration,
	control *standaloneControl,
) (standaloneReloadOutcome, error) {
	control.Stop()
	reloadedConfig, reloadedInterval, reloadErr := reloadAndReadInterval(
		ctx, options.ConfigPath, daemon, *activeConfig,
	)
	if reloadErr != nil {
		return resumeAfterRejectedReload(
			ctx, daemon, activeConfig, discoveryTimer, discoveryInterval, reloadErr,
		)
	}
	return activateReloadedStandaloneControl(
		ctx, daemon, activeConfig, discoveryTimer, reloadedConfig, reloadedInterval,
	)
}

func resumeAfterRejectedReload(
	ctx context.Context,
	daemon standaloneDaemon,
	activeConfig *activeConfiguration,
	discoveryTimer *time.Timer,
	discoveryInterval time.Duration,
	reloadFailure error,
) (standaloneReloadOutcome, error) {
	reloadFailure = errors.Join(reloadFailure, scheduleRetainedDemandAfterRejectedReload(ctx, activeConfig))
	if standaloneShutdownRequested(ctx) {
		continueAfterRejectedReload(ctx, daemon, activeConfig, discoveryTimer, discoveryInterval, reloadFailure)
		return standaloneReloadOutcome{discoveryInterval: discoveryInterval, stop: true}, nil
	}
	control, err := startStandaloneControl(ctx, daemon, activeConfig, standaloneControlOptions{})
	if err != nil {
		return standaloneReloadOutcome{}, fmt.Errorf(
			"resume standalone repository control after rejected reload: %w", err,
		)
	}
	continueAfterRejectedReload(ctx, daemon, activeConfig, discoveryTimer, discoveryInterval, reloadFailure)
	return standaloneReloadOutcome{control: control, discoveryInterval: discoveryInterval}, nil
}

func standaloneShutdownRequested(ctx context.Context) bool {
	return ctx.Err() != nil
}

func scheduleRetainedDemandAfterRejectedReload(ctx context.Context, activeConfig *activeConfiguration) error {
	recoveryCtx, cancelRecovery := context.WithTimeout(context.WithoutCancel(ctx), standaloneGitHubAttemptTimeout)
	defer cancelRecovery()
	if err := schedulePersistedDemand(recoveryCtx, activeConfig); err != nil {
		return fmt.Errorf("schedule retained demand after rejected reload: %w", err)
	}
	return nil
}

func activateReloadedStandaloneControl(
	ctx context.Context,
	daemon standaloneDaemon,
	activeConfig *activeConfiguration,
	discoveryTimer *time.Timer,
	reloadedConfig activeConfiguration,
	reloadedInterval time.Duration,
) (standaloneReloadOutcome, error) {
	*activeConfig = reloadedConfig
	control, err := startStandaloneControl(ctx, daemon, activeConfig, standaloneControlOptions{})
	if err != nil {
		return standaloneReloadOutcome{}, fmt.Errorf("start reloaded standalone repository control: %w", err)
	}
	control.RequestDiscovery(ctx)
	discoveryTimer.Reset(reloadedInterval)
	notifyReloadCompleted(ctx, daemon, activeConfig.Logger)
	return standaloneReloadOutcome{control: control, discoveryInterval: reloadedInterval}, nil
}

func continueAfterRejectedReload(
	ctx context.Context,
	daemon standaloneDaemon,
	activeConfig *activeConfiguration,
	discoveryTimer *time.Timer,
	discoveryInterval time.Duration,
	reloadErr error,
) {
	logAction(ctx, activeConfig.Logger, slog.LevelWarn, "reload", "preflight_rejected",
		slog.String("error", reloadErr.Error()),
	)
	activeConfig.ReconciliationPending = true
	discoveryTimer.Reset(nextReconciliationDelay(
		*activeConfig, discoveryInterval, daemon.reconciliationRetryDelay,
	))
	notifyReloadCompleted(ctx, daemon, activeConfig.Logger)
}

func notifyReloadCompleted(ctx context.Context, daemon standaloneDaemon, logger *slog.Logger) {
	if err := daemon.notifier.Ready(ctx); err != nil {
		logAction(ctx, logger, slog.LevelWarn, "reload", "notification_failed",
			slog.String("error", err.Error()),
		)
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
	logAction(ctx, reloadedConfig.Logger, slog.LevelInfo, "reload", "replacement_reconciliation_started")
	logAction(ctx, reloadedConfig.Logger, slog.LevelInfo, "reload", "completed")
	return reloadedConfig, reloadedConfig.DiscoveryInterval, nil
}

func withStandaloneDefaults(daemon standaloneDaemon) standaloneDaemon {
	if daemon.notifier == nil {
		daemon.notifier = noopNotifier{}
	}
	if daemon.reconciliationRetryDelay == 0 {
		daemon.reconciliationRetryDelay = pollRetryDelay
	}
	if daemon.openState == nil {
		daemon.openState = func(string) (standaloneState, error) {
			return newMemoryStandaloneState(), nil
		}
	}
	if daemon.loadCredentials == nil {
		daemon.loadCredentials = func(githubConfig config.GitHubConfig) (config.GitHubAppCredentials, error) {
			return githubConfig.AppCredentials(os.ReadFile)
		}
	}
	return daemon
}

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
	state, err := daemon.openState(loadedConfig.State.Path)
	if err != nil {
		return activeConfiguration{}, fmt.Errorf("open durable state: %w", err)
	}
	activeConfig := activeConfiguration{
		Config:          loadedConfig,
		Credentials:     credentials,
		Discoverer:      discoverer,
		Coordinators:    coordinators(loadedConfig.ScaleSets, state),
		Repositories:    map[string]managedRepository{},
		Logger:          logger,
		Logging:         logging,
		DemandStore:     state,
		RepositoryState: state,
		RepositoryMutex: &sync.Mutex{},
		StateCloser:     state,
	}
	return activeConfig, nil
}
