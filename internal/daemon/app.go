package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/meza/github-scaler/internal/config"
)

type Options struct {
	ConfigPath     string
	ReloadRequests <-chan struct{}
}

func Run(ctx context.Context, options Options) error {
	return runDaemon(ctx, options, productionRuntime())
}

type runtime struct {
	LoadConfig      func(configPath string) (config.Config, error)
	NewGitHubClient func(repositoryURL string, token string) (scaleSetClient, error)
	NewLogger       func(format string) *slog.Logger
	Notifier        serviceNotifier
	StartScaleSet   func(ctx context.Context, logger *slog.Logger, client scaleSetClient, repositoryURL string, scaleSetConfig config.ScaleSetConfig) (scaleSetLifecycle, error)
	DrainInterval   time.Duration
}

type serviceNotifier interface {
	Ready(ctx context.Context) error
	Reloading(ctx context.Context) error
}

type noopNotifier struct{}

func (notifier noopNotifier) Ready(ctx context.Context) error {
	return nil
}

func (notifier noopNotifier) Reloading(ctx context.Context) error {
	return nil
}

type activeConfiguration struct {
	LogFormat  string
	ScaleSets  []scaleSetLifecycle
	CancelWork context.CancelFunc
}

func productionRuntime() runtime {
	return runtime{
		LoadConfig: loadConfig,
		NewGitHubClient: func(repositoryURL string, token string) (scaleSetClient, error) {
			return newGitHubClient(repositoryURL, token)
		},
		NewLogger:     newLogger,
		Notifier:      systemdNotifier{},
		StartScaleSet: startScaleSet,
		DrainInterval: time.Second,
	}
}

func runDaemon(ctx context.Context, options Options, daemonRuntime runtime) error {
	daemonRuntime = withRuntimeDefaults(daemonRuntime)
	activeConfig, err := loadAndStartConfiguration(ctx, options.ConfigPath, daemonRuntime)
	if err != nil {
		return err
	}
	if err := daemonRuntime.Notifier.Ready(ctx); err != nil {
		return fmt.Errorf("notify ready: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancelShutdown()
			return shutdownConfiguredScaleSets(shutdownCtx, activeConfig)
		case <-options.ReloadRequests:
			reloadedConfig, err := reloadConfiguration(ctx, options.ConfigPath, daemonRuntime, activeConfig)
			if err != nil {
				return err
			}
			activeConfig = reloadedConfig
		}
	}
}

func withRuntimeDefaults(daemonRuntime runtime) runtime {
	if daemonRuntime.Notifier == nil {
		daemonRuntime.Notifier = noopNotifier{}
	}
	if daemonRuntime.DrainInterval == 0 {
		daemonRuntime.DrainInterval = time.Second
	}
	return daemonRuntime
}

func loadAndStartConfiguration(ctx context.Context, configPath string, daemonRuntime runtime) (activeConfiguration, error) {
	loadedConfig, err := daemonRuntime.LoadConfig(configPath)
	if err != nil {
		return activeConfiguration{}, err
	}
	token, err := loadedConfig.Token()
	if err != nil {
		return activeConfiguration{}, fmt.Errorf("load github token: %w", err)
	}
	return startConfiguredScaleSets(ctx, daemonRuntime, loadedConfig, token)
}

func startConfiguredScaleSets(ctx context.Context, daemonRuntime runtime, loadedConfig config.Config, token string) (activeConfiguration, error) {
	logger := daemonRuntime.NewLogger(loadedConfig.Log.Format)
	if loadedConfig.Prune {
		if err := pruneConfiguredScaleSets(ctx, daemonRuntime, logger, loadedConfig.ScaleSets, token); err != nil {
			return activeConfiguration{}, err
		}
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	activeConfig := activeConfiguration{
		LogFormat:  loadedConfig.Log.Format,
		CancelWork: cancelWork,
	}
	for _, scaleSetConfig := range loadedConfig.ScaleSets {
		repositoryScaleSets, err := startScaleSetRepositories(workCtx, daemonRuntime, logger, scaleSetConfig, token)
		if err != nil {
			shutdownErr := shutdownConfiguredScaleSets(context.WithoutCancel(ctx), activeConfig)
			if shutdownErr != nil {
				return activeConfiguration{}, errors.Join(err, fmt.Errorf("shutdown started scale sets: %w", shutdownErr))
			}
			return activeConfiguration{}, err
		}
		activeConfig.ScaleSets = append(activeConfig.ScaleSets, repositoryScaleSets...)
	}
	return activeConfig, nil
}

func startScaleSetRepositories(
	ctx context.Context,
	daemonRuntime runtime,
	logger *slog.Logger,
	scaleSetConfig config.ScaleSetConfig,
	token string,
) ([]scaleSetLifecycle, error) {
	var repositoryScaleSets []scaleSetLifecycle
	for _, repositoryURL := range scaleSetConfig.Repositories {
		client, err := daemonRuntime.NewGitHubClient(repositoryURL, token)
		if err != nil {
			return nil, fmt.Errorf("create github client for %q: %w", repositoryURL, err)
		}
		scaleSet, err := daemonRuntime.StartScaleSet(ctx, logger, client, repositoryURL, scaleSetConfig)
		if err != nil {
			return nil, fmt.Errorf("run scale set %q for repository %q: %w", scaleSetConfig.Name, repositoryURL, err)
		}
		repositoryScaleSets = append(repositoryScaleSets, scaleSet)
	}
	return repositoryScaleSets, nil
}

func shutdownConfiguredScaleSets(ctx context.Context, activeConfig activeConfiguration) error {
	cancelActiveConfigurationWork(activeConfig)
	for shutdownIndex := len(activeConfig.ScaleSets) - 1; shutdownIndex >= 0; shutdownIndex-- {
		activeConfig.ScaleSets[shutdownIndex].StopPolling()
		if err := activeConfig.ScaleSets[shutdownIndex].Shutdown(ctx); err != nil {
			return fmt.Errorf("shutdown scale set: %w", err)
		}
	}
	return nil
}

func reloadConfiguration(
	ctx context.Context,
	configPath string,
	daemonRuntime runtime,
	activeConfig activeConfiguration,
) (activeConfiguration, error) {
	logger := daemonRuntime.NewLogger(activeConfig.LogFormat)
	logger.Info("reload requested")
	stopPolling(activeConfig)
	if err := daemonRuntime.Notifier.Reloading(ctx); err != nil {
		return activeConfiguration{}, fmt.Errorf("notify reloading: %w", err)
	}
	reloadedConfig, err := loadAndStartConfigurationAfterDrain(ctx, configPath, daemonRuntime, activeConfig, logger)
	if err != nil {
		return activeConfiguration{}, err
	}
	if err := daemonRuntime.Notifier.Ready(ctx); err != nil {
		return activeConfiguration{}, fmt.Errorf("notify ready after reload: %w", err)
	}
	logger.Info("reload applied")
	return reloadedConfig, nil
}

func loadAndStartConfigurationAfterDrain(
	ctx context.Context,
	configPath string,
	daemonRuntime runtime,
	activeConfig activeConfiguration,
	logger *slog.Logger,
) (activeConfiguration, error) {
	reloadedConfig, err := daemonRuntime.LoadConfig(configPath)
	if err != nil {
		return stopAfterRejectedReload(ctx, activeConfig, fmt.Errorf("load reload config: %w", err))
	}
	token, err := reloadedConfig.Token()
	if err != nil {
		return stopAfterRejectedReload(ctx, activeConfig, fmt.Errorf("load reload github token: %w", err))
	}
	if err := drainConfiguration(ctx, activeConfig, daemonRuntime.DrainInterval, logger); err != nil {
		return activeConfiguration{}, err
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancelShutdown()
	if err := shutdownConfiguredScaleSets(shutdownCtx, activeConfig); err != nil {
		return activeConfiguration{}, err
	}
	return startConfiguredScaleSets(ctx, daemonRuntime, reloadedConfig, token)
}

func stopAfterRejectedReload(ctx context.Context, activeConfig activeConfiguration, reloadErr error) (activeConfiguration, error) {
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancelShutdown()
	if err := shutdownConfiguredScaleSets(shutdownCtx, activeConfig); err != nil {
		return activeConfiguration{}, errors.Join(reloadErr, fmt.Errorf("shutdown old scale sets before resume: %w", err))
	}
	return activeConfiguration{}, reloadErr
}

func stopPolling(activeConfig activeConfiguration) {
	cancelActiveConfigurationWork(activeConfig)
	for _, scaleSet := range activeConfig.ScaleSets {
		scaleSet.StopPolling()
	}
}

func cancelActiveConfigurationWork(activeConfig activeConfiguration) {
	if activeConfig.CancelWork != nil {
		activeConfig.CancelWork()
	}
}

func drainConfiguration(ctx context.Context, activeConfig activeConfiguration, drainInterval time.Duration, logger *slog.Logger) error {
	logger.Info("reload draining active runners")
	for {
		runningCount, err := drainOnce(ctx, activeConfig)
		if err != nil {
			return err
		}
		if runningCount == 0 {
			return nil
		}
		logger.Info("reload waiting for active runners", slog.Int("activeRunners", runningCount))
		timer := time.NewTimer(drainInterval)
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
	for _, scaleSet := range activeConfig.ScaleSets {
		scaleSetRunningCount, err := scaleSet.Drain(ctx)
		if err != nil {
			return 0, fmt.Errorf("drain scale set: %w", err)
		}
		runningCount += scaleSetRunningCount
	}
	return runningCount, nil
}

func loadConfig(configPath string) (config.Config, error) {
	configBytes, err := os.ReadFile(configPath) //nolint:gosec // configPath is the daemon's required operator-provided config file.
	if err != nil {
		return config.Config{}, fmt.Errorf("read config: %w", err)
	}
	loadedConfig, err := config.Load(bytes.NewReader(configBytes))
	if err != nil {
		return config.Config{}, fmt.Errorf("load config: %w", err)
	}
	return loadedConfig, nil
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

func pruneConfiguredScaleSets(
	ctx context.Context,
	daemonRuntime runtime,
	logger *slog.Logger,
	scaleSetConfigs []config.ScaleSetConfig,
	token string,
) error {
	desiredNamesByTarget := desiredScaleSetNamesByTarget(scaleSetConfigs)
	for target, desiredNames := range desiredNamesByTarget {
		client, err := daemonRuntime.NewGitHubClient(target.RepositoryURL, token)
		if err != nil {
			return fmt.Errorf("create github client for prune %q: %w", target.RepositoryURL, err)
		}
		pruneClient, ok := client.(scaleSetPruneClient)
		if !ok {
			return fmt.Errorf("github client for %q does not support pruning", target.RepositoryURL)
		}
		if err := pruneScaleSetTarget(ctx, logger, pruneClient, target, desiredNames); err != nil {
			return err
		}
	}
	return nil
}

func desiredScaleSetNamesByTarget(scaleSetConfigs []config.ScaleSetConfig) map[pruneTarget]map[string]bool {
	desiredNamesByTarget := map[pruneTarget]map[string]bool{}
	for _, scaleSetConfig := range scaleSetConfigs {
		for _, repositoryURL := range scaleSetConfig.Repositories {
			target := pruneTarget{
				RepositoryURL: repositoryURL,
				RunnerGroup:   scaleSetConfig.RunnerGroup,
			}
			if desiredNamesByTarget[target] == nil {
				desiredNamesByTarget[target] = map[string]bool{}
			}
			desiredNamesByTarget[target][scaleSetConfig.Name] = true
		}
	}
	return desiredNamesByTarget
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
		logger.Info(
			"pruning unmanaged runner scale set",
			slog.String("repository", target.RepositoryURL),
			slog.String("runnerGroup", target.RunnerGroup),
			slog.String("scaleSet", runnerScaleSet.Name),
		)
		if err := client.DeleteRunnerScaleSet(ctx, runnerScaleSet.ID); err != nil {
			return fmt.Errorf("delete runner scale set %q: %w", runnerScaleSet.Name, err)
		}
	}
	return nil
}
