package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/meza/github-scaler/internal/cluster"
	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	"github.com/meza/github-scaler/internal/scaler"
)

type Options struct {
	ConfigPath     string
	ReloadRequests <-chan struct{}
}

type repositoryDiscoverer interface {
	Discover(ctx context.Context) (githubapp.DiscoveryResult, error)
}

type runtime struct {
	LoadConfig       func(configPath string) (config.Config, error)
	Hostname         func() (string, error)
	NewDiscoverer    func(credentials config.GitHubAppCredentials) (repositoryDiscoverer, error)
	NewGitHubClient  func(repositoryURL string, installationID int64, credentials config.GitHubAppCredentials) (scaleSetClient, error)
	NewLogger        func(format string, level string) *slog.Logger
	OpenDemandStore  func(path string) (scaler.DemandStore, io.Closer, error)
	OpenClusterStore func(state config.PostgreSQLStateConfig) (clusterState, error)
	ProviderHealthy  func(ctx context.Context, scaleSet config.ScaleSetConfig) error
	BuildReconciler  func(ctx context.Context, repositoryURL string, scaleSet config.ScaleSetConfig, client scaleSetClient, scaleSetID int) (scaler.Reconciler, error)
	Notifier         serviceNotifier
	StartScaleSet    func(
		ctx context.Context,
		logger *slog.Logger,
		client scaleSetClient,
		repositoryURL string,
		scaleSetConfig config.ScaleSetConfig,
		coordinator *scaler.Coordinator,
	) (scaleSetLifecycle, error)
	DrainInterval            time.Duration
	ReconciliationRetryDelay time.Duration
}

type serviceNotifier interface {
	Ready(ctx context.Context) error
	Reloading(ctx context.Context) error
}

type noopNotifier struct{}

func (noopNotifier) Ready(context.Context) error     { return nil }
func (noopNotifier) Reloading(context.Context) error { return nil }

type managedRepository struct {
	InstallationID int64
	Lifecycles     map[string]scaleSetLifecycle
}

type activeConfiguration struct {
	Config                config.Config
	Credentials           config.GitHubAppCredentials
	Discoverer            repositoryDiscoverer
	Coordinators          map[string]*scaler.Coordinator
	Repositories          map[string]managedRepository
	Logger                *slog.Logger
	DemandStore           scaler.DemandStore
	StateCloser           io.Closer
	ReconciliationPending bool
}

func Run(ctx context.Context, options Options) error {
	return runWithRuntime(ctx, options, productionRuntime())
}

func runWithRuntime(ctx context.Context, options Options, daemonRuntime runtime) error {
	configured, err := daemonRuntime.LoadConfig(options.ConfigPath)
	if err != nil {
		return err
	}
	if configured.State.Postgres != nil {
		return runClusteredDaemon(ctx, options, daemonRuntime, configured)
	}
	return runDaemon(ctx, options, daemonRuntime)
}

func productionRuntime() runtime { //nolint:maintidx // Production dependency wiring is intentionally centralized.
	return runtime{
		LoadConfig: loadConfig,
		Hostname:   os.Hostname,
		NewDiscoverer: func(credentials config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return githubapp.NewDiscoverer(credentials)
		},
		NewGitHubClient: func(repositoryURL string, installationID int64, credentials config.GitHubAppCredentials) (scaleSetClient, error) {
			return newGitHubClient(repositoryURL, installationID, credentials)
		},
		NewLogger: newLogger,
		OpenDemandStore: func(path string) (scaler.DemandStore, io.Closer, error) {
			store, err := scaler.OpenSQLiteDemandStore(path)
			if err != nil {
				return nil, nil, fmt.Errorf("open SQLite demand store: %w", err)
			}
			return store, store, nil
		},
		OpenClusterStore: func(state config.PostgreSQLStateConfig) (clusterState, error) {
			return cluster.NewStoreFromConfig(state), nil
		},
		ProviderHealthy: func(ctx context.Context, scaleSet config.ScaleSetConfig) error {
			return validateScaleSetProvider(ctx, scaleSet, productionProviderRuntime())
		},
		BuildReconciler: buildReconciler,
		Notifier:        systemdNotifier{},
		StartScaleSet: func(
			ctx context.Context,
			logger *slog.Logger,
			client scaleSetClient,
			repositoryURL string,
			scaleSetConfig config.ScaleSetConfig,
			coordinator *scaler.Coordinator,
		) (scaleSetLifecycle, error) {
			return runCoordinatedScaleSet(ctx, logger, client, repositoryURL, scaleSetConfig, coordinator, productionScaleSetRuntime())
		},
		DrainInterval:            time.Second,
		ReconciliationRetryDelay: pollRetryDelay,
	}
}

func runDaemon(ctx context.Context, options Options, daemonRuntime runtime) (runErr error) {
	daemonRuntime = withRuntimeDefaults(daemonRuntime)
	activeConfig, err := loadConfiguration(options.ConfigPath, daemonRuntime)
	if err != nil {
		return err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "configuration", "loaded",
		slog.String("configPath", options.ConfigPath),
		slog.Int("scaleSetCount", len(activeConfig.Config.ScaleSets)),
		slog.String("logFormat", activeConfig.Config.Log.Format),
		slog.String("logLevel", activeConfig.Config.Log.Level),
	)
	defer func() { runErr = errors.Join(runErr, activeConfig.StateCloser.Close()) }()
	discoveryInterval, err := activeConfig.Config.GitHub.DiscoveryDuration()
	if err != nil {
		return fmt.Errorf("load discovery interval: %w", err)
	}
	if notifyErr := daemonRuntime.Notifier.Ready(ctx); notifyErr != nil {
		return fmt.Errorf("notify ready: %w", notifyErr)
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "daemon", "ready")
	reconcileDiscovery(ctx, daemonRuntime, &activeConfig)
	return runActiveConfiguration(ctx, options, daemonRuntime, activeConfig, discoveryInterval)
}

func runActiveConfiguration(
	ctx context.Context,
	options Options,
	daemonRuntime runtime,
	activeConfig activeConfiguration,
	discoveryInterval time.Duration,
) error {
	discoveryTimer := time.NewTimer(nextReconciliationDelay(activeConfig, discoveryInterval, daemonRuntime.ReconciliationRetryDelay))
	defer discoveryTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdownActiveConfiguration(ctx, activeConfig)
			return nil
		case <-discoveryTimer.C:
			reconcileDiscovery(ctx, daemonRuntime, &activeConfig)
			discoveryTimer.Reset(nextReconciliationDelay(activeConfig, discoveryInterval, daemonRuntime.ReconciliationRetryDelay))
		case <-options.ReloadRequests:
			reloadedConfig, reloadedInterval, reloadErr := reloadAndReadInterval(
				ctx, options.ConfigPath, daemonRuntime, activeConfig,
			)
			if reloadErr != nil {
				return reloadErr
			}
			activeConfig = reloadedConfig
			discoveryInterval = reloadedInterval
			discoveryTimer.Reset(nextReconciliationDelay(activeConfig, discoveryInterval, daemonRuntime.ReconciliationRetryDelay))
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
	daemonRuntime runtime,
	activeConfig activeConfiguration,
) (activeConfiguration, time.Duration, error) {
	reloadedConfig, err := reloadConfiguration(ctx, configPath, daemonRuntime, activeConfig)
	if err != nil {
		return activeConfiguration{}, 0, err
	}
	discoveryInterval, err := reloadedConfig.Config.GitHub.DiscoveryDuration()
	if err != nil {
		return activeConfiguration{}, 0, fmt.Errorf("load reloaded discovery interval: %w", err)
	}
	logAction(ctx, reloadedConfig.Logger, slog.LevelInfo, "reload", "replacement_reconciliation_started")
	reconcileDiscovery(ctx, daemonRuntime, &reloadedConfig)
	logAction(ctx, reloadedConfig.Logger, slog.LevelInfo, "reload", "completed")
	return reloadedConfig, discoveryInterval, nil
}

func withRuntimeDefaults(daemonRuntime runtime) runtime {
	if daemonRuntime.Notifier == nil {
		daemonRuntime.Notifier = noopNotifier{}
	}
	if daemonRuntime.DrainInterval == 0 {
		daemonRuntime.DrainInterval = time.Second
	}
	if daemonRuntime.ReconciliationRetryDelay == 0 {
		daemonRuntime.ReconciliationRetryDelay = pollRetryDelay
	}
	if daemonRuntime.OpenDemandStore == nil {
		daemonRuntime.OpenDemandStore = func(string) (scaler.DemandStore, io.Closer, error) {
			return scaler.NewMemoryDemandStore(), noOpCloser{}, nil
		}
	}
	if daemonRuntime.OpenClusterStore == nil {
		daemonRuntime.OpenClusterStore = func(state config.PostgreSQLStateConfig) (clusterState, error) {
			return cluster.NewStoreFromConfig(state), nil
		}
	}
	if daemonRuntime.ProviderHealthy == nil {
		daemonRuntime.ProviderHealthy = func(context.Context, config.ScaleSetConfig) error { return nil }
	}
	if daemonRuntime.BuildReconciler == nil {
		daemonRuntime.BuildReconciler = buildReconciler
	}
	return daemonRuntime
}

type noOpCloser struct{}

func (noOpCloser) Close() error { return nil }

func loadConfiguration(configPath string, daemonRuntime runtime) (activeConfiguration, error) {
	daemonRuntime = withRuntimeDefaults(daemonRuntime)
	loadedConfig, err := daemonRuntime.LoadConfig(configPath)
	if err != nil {
		return activeConfiguration{}, err
	}
	credentials, err := loadedConfig.GitHub.AppCredentials()
	if err != nil {
		return activeConfiguration{}, fmt.Errorf("load GitHub App credentials: %w", err)
	}
	discoverer, err := daemonRuntime.NewDiscoverer(credentials)
	if err != nil {
		return activeConfiguration{}, fmt.Errorf("create repository discoverer: %w", err)
	}
	demandStore, stateCloser, err := daemonRuntime.OpenDemandStore(loadedConfig.State.Path)
	if err != nil {
		return activeConfiguration{}, fmt.Errorf("open durable state: %w", err)
	}
	activeConfig := activeConfiguration{
		Config:       loadedConfig,
		Credentials:  credentials,
		Discoverer:   discoverer,
		Coordinators: coordinators(loadedConfig.ScaleSets, demandStore),
		Repositories: map[string]managedRepository{},
		Logger:       daemonRuntime.NewLogger(loadedConfig.Log.Format, loadedConfig.Log.Level),
		DemandStore:  demandStore,
		StateCloser:  stateCloser,
	}
	return activeConfig, nil
}

func coordinators(scaleSets []config.ScaleSetConfig, store scaler.DemandStore) map[string]*scaler.Coordinator {
	result := make(map[string]*scaler.Coordinator, len(scaleSets))
	for _, scaleSetConfig := range scaleSets {
		result[scaleSetConfig.Name] = scaler.NewDurableCoordinator(scaleSetConfig.Name, scaleSetConfig.MaxRunners, store)
	}
	return result
}

func reconcileDiscovery(ctx context.Context, daemonRuntime runtime, activeConfig *activeConfiguration) {
	startedAt := time.Now()
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "discovery", "started")
	discovery, err := activeConfig.Discoverer.Discover(ctx)
	if err != nil {
		activeConfig.ReconciliationPending = true
		logActionFailure(ctx, activeConfig.Logger, "discovery", err, slog.Duration("duration", time.Since(startedAt)))
		return
	}
	if err := applyDiscovery(ctx, daemonRuntime, activeConfig, discovery); err != nil {
		activeConfig.ReconciliationPending = true
		logActionFailure(ctx, activeConfig.Logger, "discovery", err,
			slog.Int("repositoryCount", len(discovery.Repositories)),
			slog.Int("installationFailureCount", len(discovery.InstallationFailures)),
			slog.Duration("duration", time.Since(startedAt)),
		)
		return
	}
	activeConfig.ReconciliationPending = false
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "discovery", "completed",
		slog.Int("repositoryCount", len(discovery.Repositories)),
		slog.Int("installationFailureCount", len(discovery.InstallationFailures)),
		slog.Duration("duration", time.Since(startedAt)),
	)
}

func nextReconciliationDelay(activeConfig activeConfiguration, discoveryInterval time.Duration, retryDelay time.Duration) time.Duration {
	if activeConfig.ReconciliationPending {
		return retryDelay
	}
	return discoveryInterval
}

func applyDiscovery(
	ctx context.Context,
	daemonRuntime runtime,
	activeConfig *activeConfiguration,
	discovery githubapp.DiscoveryResult,
) error {
	desired := desiredRepositories(activeConfig, discovery)
	if err := validateDiscoveredNames(desired, activeConfig.Config.ScaleSets); err != nil {
		for repositoryURL, repository := range desired {
			logRepositoryReconciliationFailure(activeConfig.Logger, repositoryURL, repository.InstallationID, err)
		}
		return err
	}
	if err := removeUndiscoveredRepositories(ctx, activeConfig, desired); err != nil {
		return err
	}
	startupErr := startDiscoveredRepositories(ctx, daemonRuntime, activeConfig, desired)
	recoveryErr := schedulePersistedDemand(ctx, activeConfig)
	return errors.Join(startupErr, recoveryErr)
}

func schedulePersistedDemand(ctx context.Context, activeConfig *activeConfiguration) error {
	startedAt := time.Now()
	logAction(ctx, activeConfig.Logger, slog.LevelDebug, "demand_recovery", "started")
	var recoveryErr error
	for _, scaleSetConfig := range activeConfig.Config.ScaleSets {
		logAction(ctx, activeConfig.Logger, slog.LevelDebug, "demand_schedule", "started",
			slog.String("scaleSet", scaleSetConfig.Name),
		)
		if err := activeConfig.Coordinators[scaleSetConfig.Name].Schedule(ctx); err != nil {
			recoveryErr = errors.Join(recoveryErr, fmt.Errorf("recover scale set %q demand: %w", scaleSetConfig.Name, err))
			logActionFailure(ctx, activeConfig.Logger, "demand_schedule", err,
				slog.String("scaleSet", scaleSetConfig.Name),
			)
			continue
		}
		logAction(ctx, activeConfig.Logger, slog.LevelDebug, "demand_schedule", "completed",
			slog.String("scaleSet", scaleSetConfig.Name),
		)
	}
	if recoveryErr != nil {
		logActionFailure(ctx, activeConfig.Logger, "demand_recovery", recoveryErr,
			slog.Duration("duration", time.Since(startedAt)),
		)
		return recoveryErr
	}
	logAction(ctx, activeConfig.Logger, slog.LevelDebug, "demand_recovery", "completed",
		slog.Duration("duration", time.Since(startedAt)),
	)
	return nil
}

func desiredRepositories(
	activeConfig *activeConfiguration,
	discovery githubapp.DiscoveryResult,
) map[string]githubapp.Repository {
	desired := map[string]githubapp.Repository{}
	for _, repository := range discovery.Repositories {
		desired[repository.URL] = repository
	}
	for repositoryURL, repository := range activeConfig.Repositories {
		if discovery.InstallationFailures[repository.InstallationID] != nil {
			desired[repositoryURL] = githubapp.Repository{InstallationID: repository.InstallationID, URL: repositoryURL}
		}
	}
	return desired
}

func validateDiscoveredNames(desired map[string]githubapp.Repository, scaleSets []config.ScaleSetConfig) error {
	repositoryURLs := make([]string, 0, len(desired))
	for repositoryURL := range desired {
		repositoryURLs = append(repositoryURLs, repositoryURL)
	}
	return validateLocalScaleSetNames(repositoryURLs, scaleSets)
}

func removeUndiscoveredRepositories(
	ctx context.Context,
	activeConfig *activeConfiguration,
	desired map[string]githubapp.Repository,
) error {
	for repositoryURL, repository := range activeConfig.Repositories {
		if _, exists := desired[repositoryURL]; !exists {
			if err := removeRepositoryImmediately(ctx, activeConfig, repositoryURL, repository); err != nil {
				logRepositoryReconciliationFailure(activeConfig.Logger, repositoryURL, repository.InstallationID, err)
				return err
			}
		}
	}
	return nil
}

func startDiscoveredRepositories(
	ctx context.Context,
	daemonRuntime runtime,
	activeConfig *activeConfiguration,
	desired map[string]githubapp.Repository,
) error {
	var startupErr error
	for repositoryURL, repository := range desired {
		if _, exists := activeConfig.Repositories[repositoryURL]; exists {
			continue
		}
		managed, err := startRepository(ctx, daemonRuntime, activeConfig, repository)
		if err != nil {
			activeConfig.Logger.Error(
				"repository startup failed",
				slog.String("action", "repository_start"),
				slog.String("phase", "failed"),
				slog.String("repository", repositoryURL),
				slog.Int64("installationID", repository.InstallationID),
				slog.String("error", err.Error()),
			)
			startupErr = errors.Join(startupErr, fmt.Errorf("start repository %q: %w", repositoryURL, err))
			continue
		}
		activeConfig.Repositories[repositoryURL] = managed
	}
	if startupErr == nil {
		return nil
	}
	return startupErr
}

func logRepositoryReconciliationFailure(logger *slog.Logger, repositoryURL string, installationID int64, err error) {
	logger.Error(
		"repository reconciliation failed",
		slog.String("action", "repository_reconcile"),
		slog.String("phase", "failed"),
		slog.String("repository", repositoryURL),
		slog.Int64("installationID", installationID),
		slog.String("error", err.Error()),
	)
}

func startRepository(
	ctx context.Context,
	daemonRuntime runtime,
	activeConfig *activeConfiguration,
	repository githubapp.Repository,
) (managed managedRepository, resultErr error) {
	startedAt := time.Now()
	logAction(ctx, activeConfig.Logger, slog.LevelDebug, "repository_start", "started",
		slog.String("repository", repository.URL),
		slog.Int64("installationID", repository.InstallationID),
	)
	defer func() { logRepositoryStartupResult(ctx, activeConfig.Logger, repository, startedAt, resultErr) }()
	client, err := daemonRuntime.NewGitHubClient(repository.URL, repository.InstallationID, activeConfig.Credentials)
	if err != nil {
		return managedRepository{}, fmt.Errorf("create GitHub client for %q: %w", repository.URL, err)
	}
	if err := pruneRepositoryWhenEnabled(ctx, activeConfig, client, repository.URL); err != nil {
		return managedRepository{}, err
	}
	managed = managedRepository{InstallationID: repository.InstallationID, Lifecycles: map[string]scaleSetLifecycle{}}
	resultErr = startRepositoryScaleSets(ctx, daemonRuntime, activeConfig, repository.URL, &managed, client)
	if resultErr != nil {
		return managedRepository{}, resultErr
	}
	return managed, nil
}

func logRepositoryStartupResult(
	ctx context.Context,
	logger *slog.Logger,
	repository githubapp.Repository,
	startedAt time.Time,
	resultErr error,
) {
	attributes := []slog.Attr{
		slog.String("repository", repository.URL),
		slog.Int64("installationID", repository.InstallationID),
		slog.Duration("duration", time.Since(startedAt)),
	}
	if resultErr != nil {
		logActionFailure(ctx, logger, "repository_start", resultErr, attributes...)
		return
	}
	logAction(ctx, logger, slog.LevelDebug, "repository_start", "completed", attributes...)
}

func startRepositoryScaleSets(
	ctx context.Context,
	daemonRuntime runtime,
	activeConfig *activeConfiguration,
	repositoryURL string,
	managed *managedRepository,
	client scaleSetClient,
) error {
	for _, scaleSetConfig := range activeConfig.Config.ScaleSets {
		lifecycle, startErr := startRepositoryScaleSet(ctx, daemonRuntime, activeConfig, client, repositoryURL, scaleSetConfig)
		if startErr != nil {
			rollbackErr := rollbackRepositoryStartup(ctx, activeConfig, repositoryURL, *managed)
			return errors.Join(
				fmt.Errorf("start scale set %q for %q: %w", scaleSetConfig.Name, repositoryURL, startErr),
				rollbackErr,
			)
		}
		managed.Lifecycles[scaleSetConfig.Name] = lifecycle
	}
	return nil
}

func startRepositoryScaleSet(
	ctx context.Context,
	daemonRuntime runtime,
	activeConfig *activeConfiguration,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
) (scaleSetLifecycle, error) {
	startedAt := time.Now()
	attributes := make([]slog.Attr, 0, 3)
	attributes = append(attributes, slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetConfig.Name))
	logAction(ctx, activeConfig.Logger, slog.LevelDebug, "scale_set_start", "started", attributes...)
	lifecycle, err := daemonRuntime.StartScaleSet(
		ctx, activeConfig.Logger, client, repositoryURL, scaleSetConfig, activeConfig.Coordinators[scaleSetConfig.Name],
	)
	attributes = append(attributes, slog.Duration("duration", time.Since(startedAt)))
	if err != nil {
		logActionFailure(ctx, activeConfig.Logger, "scale_set_start", err, attributes...)
		return scaleSetLifecycle{}, err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelDebug, "scale_set_start", "completed", attributes...)
	return lifecycle, nil
}

func pruneRepositoryWhenEnabled(
	ctx context.Context,
	activeConfig *activeConfiguration,
	client scaleSetClient,
	repositoryURL string,
) error {
	if !activeConfig.Config.Prune {
		return nil
	}
	return pruneRepository(ctx, activeConfig.Logger, client, repositoryURL, activeConfig.Config.ScaleSets)
}

func rollbackRepositoryStartup(
	ctx context.Context,
	activeConfig *activeConfiguration,
	repositoryURL string,
	repository managedRepository,
) error {
	var rollbackErr error
	for scaleSetName, lifecycle := range repository.Lifecycles {
		rollbackErr = errors.Join(rollbackErr, activeConfig.Coordinators[scaleSetName].RemoveRepository(context.WithoutCancel(ctx), repositoryURL))
		lifecycle.StopPolling()
		rollbackErr = errors.Join(rollbackErr, lifecycle.DestroyActive(context.WithoutCancel(ctx)))
		rollbackErr = errors.Join(rollbackErr, lifecycle.Shutdown(context.WithoutCancel(ctx)))
	}
	if rollbackErr != nil {
		return fmt.Errorf("rollback repository %q startup: %w", repositoryURL, rollbackErr)
	}
	return nil
}

func removeRepositoryImmediately(
	ctx context.Context,
	activeConfig *activeConfiguration,
	repositoryURL string,
	repository managedRepository,
) (resultErr error) {
	logAction(ctx, activeConfig.Logger, slog.LevelDebug, "repository_remove", "started",
		slog.String("repository", repositoryURL),
		slog.Int64("installationID", repository.InstallationID),
	)
	defer func() {
		attributes := []slog.Attr{
			slog.String("repository", repositoryURL),
			slog.Int64("installationID", repository.InstallationID),
		}
		if resultErr != nil {
			logActionFailure(ctx, activeConfig.Logger, "repository_remove", resultErr, attributes...)
			return
		}
		logAction(ctx, activeConfig.Logger, slog.LevelDebug, "repository_remove", "completed", attributes...)
	}()
	for scaleSetName, lifecycle := range repository.Lifecycles {
		if err := removeRepositoryScaleSet(ctx, activeConfig, repositoryURL, scaleSetName, lifecycle); err != nil {
			return err
		}
	}
	delete(activeConfig.Repositories, repositoryURL)
	return nil
}

func removeRepositoryScaleSet(
	ctx context.Context,
	activeConfig *activeConfiguration,
	repositoryURL string,
	scaleSetName string,
	lifecycle scaleSetLifecycle,
) error {
	actionAttributes := []slog.Attr{slog.String("repository", repositoryURL), slog.String("scaleSet", scaleSetName)}
	logAction(ctx, activeConfig.Logger, slog.LevelDebug, "demand_remove", "started", actionAttributes...)
	if err := activeConfig.Coordinators[scaleSetName].RemoveRepository(context.WithoutCancel(ctx), repositoryURL); err != nil {
		logActionFailure(ctx, activeConfig.Logger, "demand_remove", err, actionAttributes...)
		return fmt.Errorf("remove repository %q queued demand: %w", repositoryURL, err)
	}
	logAction(ctx, activeConfig.Logger, slog.LevelDebug, "demand_remove", "completed", actionAttributes...)
	stopLifecyclePolling(ctx, activeConfig.Logger, repositoryURL, scaleSetName, lifecycle)
	cleanupErr := destroyRepositoryRunners(ctx, activeConfig.Logger, lifecycle, actionAttributes)
	shutdownErr := shutdownRepositoryScaleSet(ctx, activeConfig.Logger, lifecycle, actionAttributes)
	if err := errors.Join(cleanupErr, shutdownErr); err != nil {
		return fmt.Errorf("remove repository %q scale set %q: %w", repositoryURL, scaleSetName, err)
	}
	return nil
}

func destroyRepositoryRunners(ctx context.Context, logger *slog.Logger, lifecycle scaleSetLifecycle, attributes []slog.Attr) error {
	logAction(ctx, logger, slog.LevelDebug, "runner_destroy", "started", attributes...)
	cleanupErr := lifecycle.DestroyActive(context.WithoutCancel(ctx))
	if cleanupErr != nil {
		logActionFailure(ctx, logger, "runner_destroy", cleanupErr, attributes...)
		return cleanupErr
	}
	logAction(ctx, logger, slog.LevelDebug, "runner_destroy", "completed", attributes...)
	return nil
}

func shutdownRepositoryScaleSet(ctx context.Context, logger *slog.Logger, lifecycle scaleSetLifecycle, attributes []slog.Attr) error {
	logAction(ctx, logger, slog.LevelDebug, "scale_set_shutdown", "started", attributes...)
	shutdownErr := lifecycle.Shutdown(context.WithoutCancel(ctx))
	if shutdownErr != nil {
		logActionFailure(ctx, logger, "scale_set_shutdown", shutdownErr, attributes...)
		return shutdownErr
	}
	logAction(ctx, logger, slog.LevelDebug, "scale_set_shutdown", "completed", attributes...)
	return nil
}

func reloadConfiguration(
	ctx context.Context,
	configPath string,
	daemonRuntime runtime,
	activeConfig activeConfiguration,
) (activeConfiguration, error) {
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "requested")
	reloaded, err := loadReloadedConfiguration(ctx, configPath, daemonRuntime, activeConfig)
	if err != nil {
		return activeConfiguration{}, err
	}
	if err := stopActiveConfigurationForReload(ctx, daemonRuntime, activeConfig, reloaded); err != nil {
		return activeConfiguration{}, err
	}
	return reloaded, nil
}

func loadReloadedConfiguration(
	ctx context.Context,
	configPath string,
	daemonRuntime runtime,
	activeConfig activeConfiguration,
) (activeConfiguration, error) {
	if err := daemonRuntime.Notifier.Reloading(ctx); err != nil {
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return activeConfiguration{}, fmt.Errorf("notify reloading: %w", err)
	}
	reloadRuntime := runtimeForReload(daemonRuntime, activeConfig)
	reloaded, err := loadConfiguration(configPath, reloadRuntime)
	if err != nil {
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return activeConfiguration{}, err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "replacement_validated")
	if err := daemonRuntime.Notifier.Ready(ctx); err != nil {
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return activeConfiguration{}, fmt.Errorf("notify ready after reload: %w", err)
	}
	return reloaded, nil
}

func stopActiveConfigurationForReload(
	ctx context.Context,
	daemonRuntime runtime,
	activeConfig activeConfiguration,
	reloaded activeConfiguration,
) error {
	stopPolling(ctx, activeConfig)
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "polling_stopped")
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "drain_started")
	if err := drainConfiguration(ctx, activeConfig, daemonRuntime.DrainInterval); err != nil {
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "drain_completed")
	if err := shutdownWithTimeout(ctx, activeConfig, drainShutdown); err != nil {
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "old_configuration_stopped")
	if err := removeUnconfiguredScaleSets(ctx, activeConfig, reloaded); err != nil {
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "state_reconciled")
	return nil
}

func runtimeForReload(daemonRuntime runtime, activeConfig activeConfiguration) runtime {
	reloadRuntime := daemonRuntime
	reloadRuntime.OpenDemandStore = func(path string) (scaler.DemandStore, io.Closer, error) {
		if path != activeConfig.Config.State.Path {
			return nil, nil, errors.New("state.path cannot change during reload; restart the daemon")
		}
		return activeConfig.DemandStore, noOpCloser{}, nil
	}
	return reloadRuntime
}

func removeUnconfiguredScaleSets(ctx context.Context, activeConfig activeConfiguration, reloaded activeConfiguration) error {
	logAction(ctx, activeConfig.Logger, slog.LevelDebug, "state_cleanup", "started")
	configuredScaleSets := map[string]bool{}
	for _, scaleSetConfig := range reloaded.Config.ScaleSets {
		configuredScaleSets[scaleSetConfig.Name] = true
	}
	for _, scaleSetConfig := range activeConfig.Config.ScaleSets {
		if !configuredScaleSets[scaleSetConfig.Name] {
			if err := activeConfig.DemandStore.RemoveScaleSet(ctx, scaleSetConfig.Name); err != nil {
				logActionFailure(ctx, activeConfig.Logger, "state_cleanup", err,
					slog.String("scaleSet", scaleSetConfig.Name),
				)
				return fmt.Errorf("remove unconfigured scale set %q demand: %w", scaleSetConfig.Name, err)
			}
			logAction(ctx, activeConfig.Logger, slog.LevelDebug, "state_remove", "completed",
				slog.String("scaleSet", scaleSetConfig.Name),
			)
		}
	}
	logAction(ctx, activeConfig.Logger, slog.LevelDebug, "state_cleanup", "completed")
	return nil
}

func drainConfiguration(ctx context.Context, activeConfig activeConfiguration, interval time.Duration) error {
	for {
		runningCount, err := drainOnce(ctx, activeConfig)
		if err != nil {
			return err
		}
		if runningCount == 0 {
			return nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("reload drain interrupted: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func drainOnce(ctx context.Context, activeConfig activeConfiguration) (int, error) {
	runningCount := 0
	for repositoryURL, repository := range activeConfig.Repositories {
		for scaleSetName, lifecycle := range repository.Lifecycles {
			logAction(ctx, activeConfig.Logger, slog.LevelDebug, "runner_drain", "started",
				slog.String("repository", repositoryURL),
				slog.String("scaleSet", scaleSetName),
			)
			count, err := lifecycle.Drain(ctx)
			if err != nil {
				logActionFailure(ctx, activeConfig.Logger, "runner_drain", err,
					slog.String("repository", repositoryURL),
					slog.String("scaleSet", scaleSetName),
				)
				return 0, fmt.Errorf("drain scale set: %w", err)
			}
			runningCount += count
			logAction(ctx, activeConfig.Logger, slog.LevelDebug, "runner_drain", "completed",
				slog.String("repository", repositoryURL),
				slog.String("scaleSet", scaleSetName),
				slog.Int("runningRunnerCount", count),
			)
		}
	}
	return runningCount, nil
}

type shutdownMode int

const (
	drainShutdown shutdownMode = iota
	immediateShutdown
)

type lifecycleCleanup struct {
	repositoryURL string
	scaleSetName  string
	lifecycle     scaleSetLifecycle
	logger        *slog.Logger
}

func shutdownWithTimeout(ctx context.Context, activeConfig activeConfiguration, mode shutdownMode) error {
	targets := lifecycleCleanupTargets(ctx, activeConfig)
	return cleanupLifecycles(ctx, targets, mode)
}

func lifecycleCleanupTargets(ctx context.Context, activeConfig activeConfiguration) []lifecycleCleanup {
	targets := make([]lifecycleCleanup, 0)
	for repositoryURL, repository := range activeConfig.Repositories {
		for scaleSetName, lifecycle := range repository.Lifecycles {
			stopLifecyclePolling(ctx, activeConfig.Logger, repositoryURL, scaleSetName, lifecycle)
			targets = append(targets, lifecycleCleanup{
				repositoryURL: repositoryURL,
				scaleSetName:  scaleSetName,
				lifecycle:     lifecycle,
				logger:        activeConfig.Logger,
			})
		}
	}
	return targets
}

func cleanupLifecycles(ctx context.Context, targets []lifecycleCleanup, mode shutdownMode) error {
	results := make(chan error, len(targets))
	concurrency := make(chan struct{}, 8)
	var cleanups sync.WaitGroup
	for _, target := range targets {
		cleanups.Add(1)
		go func(target lifecycleCleanup) {
			defer cleanups.Done()
			concurrency <- struct{}{}
			defer func() { <-concurrency }()
			if err := cleanupLifecycle(ctx, target, mode); err != nil {
				results <- err
			}
		}(target)
	}
	cleanups.Wait()
	close(results)
	var shutdownErr error
	for err := range results {
		shutdownErr = errors.Join(shutdownErr, err)
	}
	return shutdownErr
}

func cleanupLifecycle(ctx context.Context, target lifecycleCleanup, mode shutdownMode) error {
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancelShutdown()
	logAction(shutdownCtx, target.logger, slog.LevelDebug, "scale_set_shutdown", "started",
		slog.String("repository", target.repositoryURL),
		slog.String("scaleSet", target.scaleSetName),
		slog.Bool("destroyActive", mode == immediateShutdown),
	)
	var err error
	if mode == immediateShutdown {
		err = errors.Join(err, target.lifecycle.DestroyActive(shutdownCtx))
	}
	err = errors.Join(err, target.lifecycle.Shutdown(shutdownCtx))
	if err == nil {
		logAction(shutdownCtx, target.logger, slog.LevelDebug, "scale_set_shutdown", "completed",
			slog.String("repository", target.repositoryURL),
			slog.String("scaleSet", target.scaleSetName),
		)
		return nil
	}
	logActionFailure(shutdownCtx, target.logger, "scale_set_shutdown", err,
		slog.String("repository", target.repositoryURL),
		slog.String("scaleSet", target.scaleSetName),
	)
	return fmt.Errorf("shutdown %q %q: %w", target.repositoryURL, target.scaleSetName, err)
}

func stopPolling(ctx context.Context, activeConfig activeConfiguration) {
	for repositoryURL, repository := range activeConfig.Repositories {
		for scaleSetName, lifecycle := range repository.Lifecycles {
			stopLifecyclePolling(ctx, activeConfig.Logger, repositoryURL, scaleSetName, lifecycle)
		}
	}
}

func stopLifecyclePolling(ctx context.Context, logger *slog.Logger, repositoryURL string, scaleSetName string, lifecycle scaleSetLifecycle) {
	logAction(ctx, logger, slog.LevelDebug, "polling_stop", "started",
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetName),
	)
	lifecycle.StopPolling()
	logAction(ctx, logger, slog.LevelDebug, "polling_stop", "completed",
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetName),
	)
}

func pruneRepository(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSets []config.ScaleSetConfig,
) error {
	pruneClient, ok := client.(scaleSetPruneClient)
	if !ok {
		return fmt.Errorf("GitHub client for %q does not support pruning", repositoryURL)
	}
	byGroup := map[string]map[string]bool{}
	for _, scaleSetConfig := range scaleSets {
		if byGroup[scaleSetConfig.RunnerGroup] == nil {
			byGroup[scaleSetConfig.RunnerGroup] = map[string]bool{}
		}
		byGroup[scaleSetConfig.RunnerGroup][scaleSetConfig.Name] = true
	}
	for runnerGroup, names := range byGroup {
		if err := pruneScaleSetTarget(ctx, logger, pruneClient, pruneTarget{RepositoryURL: repositoryURL, RunnerGroup: runnerGroup}, names); err != nil {
			return err
		}
	}
	return nil
}

func loadConfig(configPath string) (config.Config, error) {
	configBytes, err := readConfigFile(configPath)
	if err != nil {
		return config.Config{}, err
	}
	loadedConfig, err := config.Load(bytes.NewReader(configBytes))
	if err != nil {
		return config.Config{}, fmt.Errorf("load config: %w", err)
	}
	return loadedConfig, nil
}

func readConfigFile(configPath string) ([]byte, error) {
	configRoot, err := os.OpenRoot(filepath.Dir(configPath))
	if err != nil {
		return nil, fmt.Errorf("open config directory: %w", err)
	}
	configFile, err := configRoot.Open(filepath.Base(configPath))
	if err != nil {
		rootCloseErr := configRoot.Close()
		return nil, errors.Join(fmt.Errorf("open config: %w", err), rootCloseErr)
	}
	configBytes, err := io.ReadAll(configFile)
	fileCloseErr := configFile.Close()
	rootCloseErr := configRoot.Close()
	if combinedErr := errors.Join(err, fileCloseErr, rootCloseErr); combinedErr != nil {
		return nil, fmt.Errorf("read config: %w", combinedErr)
	}
	return configBytes, nil
}

func newLogger(format string, level string) *slog.Logger {
	return newLoggerForWriter(os.Stdout, format, level)
}

func newLoggerForWriter(writer io.Writer, format string, level string) *slog.Logger {
	handlerOptions := &slog.HandlerOptions{Level: configuredLogLevel(level)}
	if format == "json" {
		return slog.New(slog.NewJSONHandler(writer, handlerOptions))
	}
	return slog.New(slog.NewTextHandler(writer, handlerOptions))
}

func configuredLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func startScaleSet(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
) (scaleSetLifecycle, error) {
	return runScaleSet(ctx, logger, client, repositoryURL, scaleSetConfig, productionScaleSetRuntime())
}

type pruneTarget struct {
	RepositoryURL string
	RunnerGroup   string
}

func pruneScaleSetTarget(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetPruneClient,
	target pruneTarget,
	desiredNames map[string]bool,
) error {
	runnerGroup, err := client.GetRunnerGroupByName(ctx, target.RunnerGroup)
	if err != nil {
		return fmt.Errorf("get runner group %q for prune: %w", target.RunnerGroup, err)
	}
	runnerScaleSets, err := client.ListRunnerScaleSets(ctx, runnerGroup.ID)
	if err != nil {
		return fmt.Errorf("list runner scale sets for prune: %w", err)
	}
	for _, runnerScaleSet := range runnerScaleSets {
		if desiredNames[runnerScaleSet.Name] {
			continue
		}
		logger.Info("pruning unmanaged runner scale set",
			slog.String("action", "runner_scale_set_prune"),
			slog.String("phase", "started"),
			slog.String("repository", target.RepositoryURL),
			slog.String("scaleSet", runnerScaleSet.Name),
		)
		if err := client.DeleteRunnerScaleSet(ctx, runnerScaleSet.ID); err != nil {
			return fmt.Errorf("delete runner scale set %q: %w", runnerScaleSet.Name, err)
		}
		logAction(ctx, logger, slog.LevelInfo, "runner_scale_set_prune", "completed",
			slog.String("repository", target.RepositoryURL),
			slog.String("scaleSet", runnerScaleSet.Name),
		)
	}
	return nil
}
