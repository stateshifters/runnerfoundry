package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

func reloadConfiguration(
	ctx context.Context,
	configPath string,
	daemon standaloneDaemon,
	activeConfig activeConfiguration,
) (activeConfiguration, error) {
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "requested")
	reloaded, err := loadReloadedConfiguration(ctx, configPath, daemon, activeConfig)
	if err != nil {
		return activeConfiguration{}, err
	}
	if err := confirmReloadRegistrations(ctx, daemon, &reloaded); err != nil {
		err = errors.Join(err, closePreparedLogging(&reloaded))
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return activeConfiguration{}, err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "registrations_confirmed")
	if reloaded.PreparedLogging != nil {
		if err := reloaded.PreparedLogging.Activate(); err != nil {
			return activeConfiguration{}, fmt.Errorf("activate reloaded logging: %w", err)
		}
		reloaded.PreparedLogging = nil
	}
	if err := stopActiveConfigurationForReload(ctx, daemon, activeConfig, reloaded); err != nil {
		return activeConfiguration{}, err
	}
	return reloaded, nil
}

func confirmReloadRegistrations(
	ctx context.Context,
	daemon standaloneDaemon,
	reloaded *activeConfiguration,
) error {
	discovery, err := reloaded.Discoverer.Discover(ctx)
	if err != nil {
		return fmt.Errorf("discover repositories before reload activation: %w", err)
	}
	if err := validateDiscoveredNames(desiredRepositories(reloaded, discovery), reloaded.Config.ScaleSets); err != nil {
		return err
	}
	for _, repository := range discovery.Repositories {
		client, clientErr := daemon.newGitHubClient(repository.URL, repository.InstallationID, reloaded.Credentials)
		if clientErr != nil {
			return fmt.Errorf("create GitHub client for %q before reload activation: %w", repository.URL, clientErr)
		}
		if _, auditErr := ensureRepositoryScaleSetRegistrations(
			ctx, reloaded.Logger, repository.URL, client, reloaded.Config.ScaleSets,
		); auditErr != nil {
			return fmt.Errorf("confirm reload registrations for %q: %w", repository.URL, auditErr)
		}
		reloaded.Repositories[repository.URL] = managedRepository{
			InstallationID: repository.InstallationID,
			Client:         client,
			Lifecycles:     map[string]scaleSetLifecycle{},
		}
	}
	return nil
}

func loadReloadedConfiguration(
	ctx context.Context,
	configPath string,
	daemon standaloneDaemon,
	activeConfig activeConfiguration,
) (activeConfiguration, error) {
	if err := daemon.notifier.Reloading(ctx); err != nil {
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return activeConfiguration{}, fmt.Errorf("notify reloading: %w", err)
	}
	reloadedDaemon := standaloneForReload(daemon, activeConfig)
	reloaded, err := prepareStandaloneReloadConfiguration(configPath, reloadedDaemon, activeConfig)
	if err != nil {
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return activeConfiguration{}, err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "replacement_validated")
	if err := daemon.notifier.Ready(ctx); err != nil {
		err = errors.Join(err, closePreparedLogging(&reloaded))
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return activeConfiguration{}, fmt.Errorf("notify ready after reload: %w", err)
	}
	return reloaded, nil
}

func prepareStandaloneReloadConfiguration(
	configPath string,
	daemon standaloneDaemon,
	activeConfig activeConfiguration,
) (activeConfiguration, error) {
	loadedConfig, err := daemon.loadConfig(configPath)
	if err != nil {
		return activeConfiguration{}, err
	}
	var prepared *preparedLogging
	if activeConfig.Logging != nil {
		prepared, err = activeConfig.Logging.Prepare(loadedConfig.Log)
		if err != nil {
			return activeConfiguration{}, fmt.Errorf("prepare reloaded logging: %w", err)
		}
	}
	reloaded, err := buildActiveConfiguration(loadedConfig, daemon, activeConfig.Logger, activeConfig.Logging)
	if err != nil {
		if prepared != nil {
			err = errors.Join(err, prepared.Close())
		}
		return activeConfiguration{}, err
	}
	reloaded.PreparedLogging = prepared
	return reloaded, nil
}

func closePreparedLogging(configuration *activeConfiguration) error {
	if configuration.PreparedLogging == nil {
		return nil
	}
	err := configuration.PreparedLogging.Close()
	configuration.PreparedLogging = nil
	return err
}

func stopActiveConfigurationForReload(
	ctx context.Context,
	daemon standaloneDaemon,
	activeConfig activeConfiguration,
	reloaded activeConfiguration,
) error {
	stopPolling(ctx, activeConfig)
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "polling_stopped")
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "drain_started")
	if err := drainConfiguration(ctx, activeConfig, daemon.drainInterval); err != nil {
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "drain_completed")
	if err := shutdownWithTimeout(ctx, activeConfig, sessionOnlyShutdown); err != nil {
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

func standaloneForReload(daemon standaloneDaemon, activeConfig activeConfiguration) standaloneDaemon {
	reloadedDaemon := daemon
	reloadedDaemon.openDemandStore = func(path string) (scaler.DemandStore, io.Closer, error) {
		if path != activeConfig.Config.State.Path {
			return nil, nil, errors.New("state.path cannot change during reload; restart the daemon")
		}
		return activeConfig.DemandStore, noOpCloser{}, nil
	}
	return reloadedDaemon
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
