package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/meza/github-scaler/internal/config"
	"github.com/meza/github-scaler/internal/githubapp"
	"github.com/meza/github-scaler/internal/scaler"
)

func coordinators(scaleSets []config.ScaleSetConfig, store scaler.DemandStore) map[string]*scaler.Coordinator {
	result := make(map[string]*scaler.Coordinator, len(scaleSets))
	for _, scaleSetConfig := range scaleSets {
		result[scaleSetConfig.Name] = scaler.NewDurableCoordinator(scaleSetConfig.Name, scaleSetConfig.MaxRunners, store)
	}
	return result
}

func reconcileDiscovery(ctx context.Context, daemon standaloneDaemon, activeConfig *activeConfiguration) {
	err := runTracedOperation(ctx, activeConfig.Logger, slog.LevelInfo, "discovery", nil, func() tracedOperationResult {
		discovery, discoveryErr := activeConfig.Discoverer.Discover(ctx)
		if discoveryErr != nil {
			return tracedOperationResult{terminalAttributes: nil, err: discoveryErr}
		}
		resultAttributes := []slog.Attr{
			slog.Int("repositoryCount", len(discovery.Repositories)),
			slog.Int("installationFailureCount", len(discovery.InstallationFailures)),
		}
		return tracedOperationResult{
			terminalAttributes: resultAttributes,
			err:                applyDiscovery(ctx, daemon, activeConfig, discovery),
		}
	})
	activeConfig.ReconciliationPending = err != nil
}

func nextReconciliationDelay(activeConfig activeConfiguration, discoveryInterval time.Duration, retryDelay time.Duration) time.Duration {
	if activeConfig.ReconciliationPending {
		return retryDelay
	}
	return discoveryInterval
}

func applyDiscovery(
	ctx context.Context,
	daemon standaloneDaemon,
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
	startupErr := startDiscoveredRepositories(ctx, daemon, activeConfig, desired)
	recoveryErr := schedulePersistedDemand(ctx, activeConfig)
	return errors.Join(startupErr, recoveryErr)
}

func schedulePersistedDemand(ctx context.Context, activeConfig *activeConfiguration) error {
	return runTracedOperation(ctx, activeConfig.Logger, slog.LevelDebug, "demand_recovery", nil, func() tracedOperationResult {
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
		return tracedOperationResult{terminalAttributes: nil, err: recoveryErr}
	})
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
	daemon standaloneDaemon,
	activeConfig *activeConfiguration,
	desired map[string]githubapp.Repository,
) error {
	var startupErr error
	for repositoryURL, repository := range desired {
		if _, exists := activeConfig.Repositories[repositoryURL]; exists {
			continue
		}
		managed, err := startRepository(ctx, daemon, activeConfig, repository)
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
	daemon standaloneDaemon,
	activeConfig *activeConfiguration,
	repository githubapp.Repository,
) (managedRepository, error) {
	initialAttributes := []slog.Attr{
		slog.String("repository", repository.URL),
		slog.Int64("installationID", repository.InstallationID),
	}
	var managed managedRepository
	err := runTracedOperation(ctx, activeConfig.Logger, slog.LevelDebug, "repository_start", initialAttributes, func() tracedOperationResult {
		client, clientErr := daemon.newGitHubClient(repository.URL, repository.InstallationID, activeConfig.Credentials)
		if clientErr != nil {
			return tracedOperationResult{
				terminalAttributes: nil,
				err:                fmt.Errorf("create GitHub client for %q: %w", repository.URL, clientErr),
			}
		}
		if pruneErr := pruneRepositoryWhenEnabled(ctx, activeConfig, client, repository.URL); pruneErr != nil {
			return tracedOperationResult{terminalAttributes: nil, err: pruneErr}
		}
		managed = managedRepository{InstallationID: repository.InstallationID, Lifecycles: map[string]scaleSetLifecycle{}}
		return tracedOperationResult{
			terminalAttributes: nil,
			err:                startRepositoryScaleSets(ctx, daemon, activeConfig, repository.URL, &managed, client),
		}
	})
	if err != nil {
		return managedRepository{}, err
	}
	return managed, nil
}

func startRepositoryScaleSets(
	ctx context.Context,
	daemon standaloneDaemon,
	activeConfig *activeConfiguration,
	repositoryURL string,
	managed *managedRepository,
	client scaleSetClient,
) error {
	for _, scaleSetConfig := range activeConfig.Config.ScaleSets {
		lifecycle, startErr := startRepositoryScaleSet(ctx, daemon, activeConfig, client, repositoryURL, scaleSetConfig)
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
	daemon standaloneDaemon,
	activeConfig *activeConfiguration,
	client scaleSetClient,
	repositoryURL string,
	scaleSetConfig config.ScaleSetConfig,
) (scaleSetLifecycle, error) {
	initialAttributes := []slog.Attr{
		slog.String("repository", repositoryURL),
		slog.String("scaleSet", scaleSetConfig.Name),
	}
	var lifecycle scaleSetLifecycle
	err := runTracedOperation(ctx, activeConfig.Logger, slog.LevelDebug, "scale_set_start", initialAttributes, func() tracedOperationResult {
		startedLifecycle, startErr := daemon.startScaleSet(
			ctx, activeConfig.Logger, client, repositoryURL, scaleSetConfig, activeConfig.Coordinators[scaleSetConfig.Name],
		)
		if startErr == nil {
			lifecycle = startedLifecycle
		}
		return tracedOperationResult{terminalAttributes: nil, err: startErr}
	})
	if err != nil {
		return scaleSetLifecycle{}, err
	}
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
