package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/meza/RunnerFoundry/internal/config"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

const reloadProviderInspectionTimeout = 30 * time.Second

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
	if err = confirmReloadRegistrations(ctx, daemon, &reloaded); err != nil {
		err = errors.Join(err, closePreparedLogging(&reloaded))
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return activeConfiguration{}, err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "registrations_confirmed")
	suspendedCoordinators, err := validateReloadProviderChanges(ctx, activeConfig, reloaded.Config)
	if err != nil {
		err = errors.Join(err, closePreparedLogging(&reloaded))
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return activeConfiguration{}, err
	}
	if reloaded.PreparedLogging != nil {
		if err := reloaded.PreparedLogging.Activate(); err != nil {
			resumeProvisioning(suspendedCoordinators)
			err = fmt.Errorf("activate reloaded logging: %w", err)
			return activeConfiguration{}, err
		}
		reloaded.PreparedLogging = nil
	}
	handoverActiveConfiguration(ctx, activeConfig, reloaded)
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
	reloaded.DiscoveryInterval, err = reloaded.Config.GitHub.DiscoveryDuration()
	if err != nil {
		err = errors.Join(fmt.Errorf("load reloaded discovery interval: %w", err), closePreparedLogging(&reloaded))
		logActionFailure(ctx, activeConfig.Logger, "reload", err)
		return activeConfiguration{}, err
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "replacement_validated")
	return reloaded, nil
}

func validateReloadProviderChanges(
	ctx context.Context,
	activeConfig activeConfiguration,
	reloadedConfig config.Config,
) ([]*scaler.Coordinator, error) {
	inspectionCtx, cancelInspection := context.WithTimeout(ctx, reloadProviderInspectionTimeout)
	defer cancelInspection()
	changedScaleSets := changedProviderScaleSets(activeConfig.Config, reloadedConfig)
	suspendedCoordinators, err := suspendProviderChangeProvisioning(inspectionCtx, activeConfig, changedScaleSets)
	if err != nil {
		return nil, err
	}
	if err = inspectChangedProviders(inspectionCtx, activeConfig, changedScaleSets); err != nil {
		resumeProvisioning(suspendedCoordinators)
		return nil, err
	}
	return suspendedCoordinators, nil
}

func changedProviderScaleSets(activeConfig config.Config, reloadedConfig config.Config) []string {
	reloadedScaleSets := make(map[string]config.ScaleSetConfig, len(reloadedConfig.ScaleSets))
	for _, scaleSetConfig := range reloadedConfig.ScaleSets {
		reloadedScaleSets[scaleSetConfig.Name] = scaleSetConfig
	}
	changedScaleSets := make([]string, 0, len(activeConfig.ScaleSets))
	for _, activeScaleSet := range activeConfig.ScaleSets {
		reloadedScaleSet, retained := reloadedScaleSets[activeScaleSet.Name]
		if !retained || sameProviderConfiguration(activeScaleSet, reloadedScaleSet) {
			continue
		}
		changedScaleSets = append(changedScaleSets, activeScaleSet.Name)
	}
	return changedScaleSets
}

func suspendProviderChangeProvisioning(
	ctx context.Context,
	activeConfig activeConfiguration,
	changedScaleSets []string,
) ([]*scaler.Coordinator, error) {
	suspendedCoordinators := make([]*scaler.Coordinator, 0, len(changedScaleSets))
	for _, scaleSetName := range changedScaleSets {
		coordinator := activeConfig.Coordinators[scaleSetName]
		if coordinator == nil {
			resumeProvisioning(suspendedCoordinators)
			return nil, fmt.Errorf("suspend scale set %q provisioning before reload: coordinator is unavailable", scaleSetName)
		}
		if err := coordinator.SuspendProvisioning(ctx); err != nil {
			resumeProvisioning(suspendedCoordinators)
			return nil, fmt.Errorf("suspend scale set %q provisioning before reload: %w", scaleSetName, err)
		}
		suspendedCoordinators = append(suspendedCoordinators, coordinator)
	}
	return suspendedCoordinators, nil
}

func inspectChangedProviders(ctx context.Context, activeConfig activeConfiguration, changedScaleSets []string) error {
	for _, scaleSetName := range changedScaleSets {
		if err := rejectActiveProviderChange(ctx, activeConfig, scaleSetName); err != nil {
			return err
		}
	}
	return nil
}

func resumeProvisioning(coordinators []*scaler.Coordinator) {
	for _, coordinator := range coordinators {
		coordinator.ResumeProvisioning()
	}
}

func sameProviderConfiguration(activeScaleSet config.ScaleSetConfig, reloadedScaleSet config.ScaleSetConfig) bool {
	if activeScaleSet.Docker != nil && reloadedScaleSet.Docker != nil {
		return *activeScaleSet.Docker == *reloadedScaleSet.Docker
	}
	if activeScaleSet.Libvirt != nil && reloadedScaleSet.Libvirt != nil {
		return *activeScaleSet.Libvirt == *reloadedScaleSet.Libvirt
	}
	return activeScaleSet.Docker == nil && activeScaleSet.Libvirt == nil &&
		reloadedScaleSet.Docker == nil && reloadedScaleSet.Libvirt == nil
}

func rejectActiveProviderChange(ctx context.Context, activeConfig activeConfiguration, scaleSetName string) error {
	runningRunners := 0
	for repositoryURL, repository := range activeConfig.Repositories {
		lifecycle, configured := repository.Lifecycles[scaleSetName]
		if !configured {
			continue
		}
		runningCount, err := lifecycle.Running(ctx)
		if err != nil {
			return fmt.Errorf(
				"inspect scale set %q provider runners for %q before reload: %w",
				scaleSetName, repositoryURL, err,
			)
		}
		runningRunners += runningCount
	}
	if runningRunners > 0 {
		return fmt.Errorf(
			"scale set %q provider configuration cannot change while %d runners are active",
			scaleSetName, runningRunners,
		)
	}
	return nil
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

func handoverActiveConfiguration(
	ctx context.Context,
	activeConfig activeConfiguration,
	reloaded activeConfiguration,
) {
	var cleanupErr error
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "handover_started")
	cleanupTargets := lifecycleCleanupTargets(ctx, activeConfig)
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "polling_stopped")
	if err := cleanupLifecycles(ctx, cleanupTargets, sessionOnlyShutdown); err != nil {
		cleanupErr = errors.Join(cleanupErr, err)
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "old_configuration_stopped")
	if err := removeUnconfiguredScaleSets(ctx, activeConfig, reloaded); err != nil {
		cleanupErr = errors.Join(cleanupErr, err)
	}
	if cleanupErr != nil {
		logAction(ctx, activeConfig.Logger, slog.LevelWarn, "reload", "cleanup_incomplete",
			slog.String("error", cleanupErr.Error()),
		)
	}
	logAction(ctx, activeConfig.Logger, slog.LevelInfo, "reload", "state_reconciled")
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
