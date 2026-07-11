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
	LoadConfig      func(configPath string) (config.Config, error)
	NewDiscoverer   func(credentials config.GitHubAppCredentials) (repositoryDiscoverer, error)
	NewGitHubClient func(repositoryURL string, installationID int64, credentials config.GitHubAppCredentials) (scaleSetClient, error)
	NewLogger       func(format string) *slog.Logger
	Notifier        serviceNotifier
	StartScaleSet   func(
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
	ReconciliationPending bool
}

func Run(ctx context.Context, options Options) error {
	return runDaemon(ctx, options, productionRuntime())
}

func productionRuntime() runtime {
	return runtime{
		LoadConfig: loadConfig,
		NewDiscoverer: func(credentials config.GitHubAppCredentials) (repositoryDiscoverer, error) {
			return githubapp.NewDiscoverer(credentials)
		},
		NewGitHubClient: func(repositoryURL string, installationID int64, credentials config.GitHubAppCredentials) (scaleSetClient, error) {
			return newGitHubClient(repositoryURL, installationID, credentials)
		},
		NewLogger: newLogger,
		Notifier:  systemdNotifier{},
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

func runDaemon(ctx context.Context, options Options, daemonRuntime runtime) error {
	daemonRuntime = withRuntimeDefaults(daemonRuntime)
	activeConfig, err := loadConfiguration(options.ConfigPath, daemonRuntime)
	if err != nil {
		return err
	}
	discoveryInterval, err := activeConfig.Config.GitHub.DiscoveryDuration()
	if err != nil {
		return fmt.Errorf("load discovery interval: %w", err)
	}
	if notifyErr := daemonRuntime.Notifier.Ready(ctx); notifyErr != nil {
		return fmt.Errorf("notify ready: %w", notifyErr)
	}
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
			if err := shutdownWithTimeout(ctx, activeConfig, drainShutdown); err != nil {
				activeConfig.Logger.Warn("shutdown cleanup incomplete", slog.String("error", err.Error()))
			}
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
	reconcileDiscovery(ctx, daemonRuntime, &reloadedConfig)
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
	return daemonRuntime
}

func loadConfiguration(configPath string, daemonRuntime runtime) (activeConfiguration, error) {
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
	activeConfig := activeConfiguration{
		Config:       loadedConfig,
		Credentials:  credentials,
		Discoverer:   discoverer,
		Coordinators: coordinators(loadedConfig.ScaleSets),
		Repositories: map[string]managedRepository{},
		Logger:       daemonRuntime.NewLogger(loadedConfig.Log.Format),
	}
	return activeConfig, nil
}

func coordinators(scaleSets []config.ScaleSetConfig) map[string]*scaler.Coordinator {
	result := make(map[string]*scaler.Coordinator, len(scaleSets))
	for _, scaleSetConfig := range scaleSets {
		result[scaleSetConfig.Name] = scaler.NewCoordinator(scaleSetConfig.MaxRunners)
	}
	return result
}

func reconcileDiscovery(ctx context.Context, daemonRuntime runtime, activeConfig *activeConfiguration) {
	discovery, err := activeConfig.Discoverer.Discover(ctx)
	if err != nil {
		activeConfig.ReconciliationPending = true
		activeConfig.Logger.Error("repository discovery failed", slog.String("error", err.Error()))
		return
	}
	if err := applyDiscovery(ctx, daemonRuntime, activeConfig, discovery); err != nil {
		activeConfig.ReconciliationPending = true
		activeConfig.Logger.Error("repository reconciliation failed", slog.String("error", err.Error()))
		return
	}
	activeConfig.ReconciliationPending = false
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
		return err
	}
	if err := removeUndiscoveredRepositories(ctx, activeConfig, desired); err != nil {
		return err
	}
	return startDiscoveredRepositories(ctx, daemonRuntime, activeConfig, desired)
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
	for repositoryURL, repository := range desired {
		if _, exists := activeConfig.Repositories[repositoryURL]; exists {
			continue
		}
		managed, err := startRepository(ctx, daemonRuntime, activeConfig, repository)
		if err != nil {
			return err
		}
		activeConfig.Repositories[repositoryURL] = managed
	}
	return nil
}

func startRepository(
	ctx context.Context,
	daemonRuntime runtime,
	activeConfig *activeConfiguration,
	repository githubapp.Repository,
) (managedRepository, error) {
	client, err := daemonRuntime.NewGitHubClient(repository.URL, repository.InstallationID, activeConfig.Credentials)
	if err != nil {
		return managedRepository{}, fmt.Errorf("create GitHub client for %q: %w", repository.URL, err)
	}
	if err := pruneRepositoryWhenEnabled(ctx, activeConfig, client, repository.URL); err != nil {
		return managedRepository{}, err
	}
	managed := managedRepository{InstallationID: repository.InstallationID, Lifecycles: map[string]scaleSetLifecycle{}}
	for _, scaleSetConfig := range activeConfig.Config.ScaleSets {
		lifecycle, startErr := daemonRuntime.StartScaleSet(ctx, activeConfig.Logger, client, repository.URL, scaleSetConfig, activeConfig.Coordinators[scaleSetConfig.Name])
		if startErr != nil {
			rollbackErr := rollbackRepositoryStartup(ctx, activeConfig, repository.URL, managed)
			return managedRepository{}, errors.Join(
				fmt.Errorf("start scale set %q for %q: %w", scaleSetConfig.Name, repository.URL, startErr),
				rollbackErr,
			)
		}
		managed.Lifecycles[scaleSetConfig.Name] = lifecycle
	}
	return managed, nil
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
		activeConfig.Coordinators[scaleSetName].RemoveRepository(repositoryURL)
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
) error {
	for scaleSetName, lifecycle := range repository.Lifecycles {
		activeConfig.Coordinators[scaleSetName].RemoveRepository(repositoryURL)
		lifecycle.StopPolling()
		cleanupErr := lifecycle.DestroyActive(context.WithoutCancel(ctx))
		shutdownErr := lifecycle.Shutdown(context.WithoutCancel(ctx))
		if err := errors.Join(cleanupErr, shutdownErr); err != nil {
			return fmt.Errorf("remove repository %q scale set %q: %w", repositoryURL, scaleSetName, err)
		}
	}
	delete(activeConfig.Repositories, repositoryURL)
	return nil
}

func reloadConfiguration(
	ctx context.Context,
	configPath string,
	daemonRuntime runtime,
	activeConfig activeConfiguration,
) (activeConfiguration, error) {
	if err := daemonRuntime.Notifier.Reloading(ctx); err != nil {
		return activeConfiguration{}, fmt.Errorf("notify reloading: %w", err)
	}
	reloaded, err := loadConfiguration(configPath, daemonRuntime)
	if err != nil {
		return activeConfiguration{}, err
	}
	if err := daemonRuntime.Notifier.Ready(ctx); err != nil {
		return activeConfiguration{}, fmt.Errorf("notify ready after reload: %w", err)
	}
	stopPolling(activeConfig)
	if err := drainConfiguration(ctx, activeConfig, daemonRuntime.DrainInterval); err != nil {
		return activeConfiguration{}, err
	}
	if err := shutdownWithTimeout(ctx, activeConfig, drainShutdown); err != nil {
		return activeConfiguration{}, err
	}
	return reloaded, nil
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
	for _, repository := range activeConfig.Repositories {
		for _, lifecycle := range repository.Lifecycles {
			count, err := lifecycle.Drain(ctx)
			if err != nil {
				return 0, fmt.Errorf("drain scale set: %w", err)
			}
			runningCount += count
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
}

func shutdownWithTimeout(ctx context.Context, activeConfig activeConfiguration, mode shutdownMode) error {
	targets := lifecycleCleanupTargets(activeConfig)
	return cleanupLifecycles(ctx, targets, mode)
}

func lifecycleCleanupTargets(activeConfig activeConfiguration) []lifecycleCleanup {
	targets := make([]lifecycleCleanup, 0)
	for repositoryURL, repository := range activeConfig.Repositories {
		for scaleSetName, lifecycle := range repository.Lifecycles {
			lifecycle.StopPolling()
			targets = append(targets, lifecycleCleanup{repositoryURL: repositoryURL, scaleSetName: scaleSetName, lifecycle: lifecycle})
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
	var err error
	if mode == immediateShutdown {
		err = errors.Join(err, target.lifecycle.DestroyActive(shutdownCtx))
	}
	err = errors.Join(err, target.lifecycle.Shutdown(shutdownCtx))
	if err == nil {
		return nil
	}
	return fmt.Errorf("shutdown %q %q: %w", target.repositoryURL, target.scaleSetName, err)
}

func stopPolling(activeConfig activeConfiguration) {
	for _, repository := range activeConfig.Repositories {
		for _, lifecycle := range repository.Lifecycles {
			lifecycle.StopPolling()
		}
	}
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

func newLogger(format string) *slog.Logger {
	if format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, nil))
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
		logger.Info("pruning unmanaged runner scale set", slog.String("repository", target.RepositoryURL), slog.String("scaleSet", runnerScaleSet.Name))
		if err := client.DeleteRunnerScaleSet(ctx, runnerScaleSet.ID); err != nil {
			return fmt.Errorf("delete runner scale set %q: %w", runnerScaleSet.Name, err)
		}
	}
	return nil
}
