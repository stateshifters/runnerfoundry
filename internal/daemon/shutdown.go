package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

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
