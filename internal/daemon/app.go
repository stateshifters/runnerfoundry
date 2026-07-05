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
	ConfigPath string
}

func Run(ctx context.Context, options Options) error {
	return runDaemon(ctx, options, productionRuntime())
}

type runtime struct {
	LoadConfig      func(configPath string) (config.Config, error)
	NewGitHubClient func(repositoryURL string, token string) (scaleSetClient, error)
	NewLogger       func(format string) *slog.Logger
	StartScaleSet   func(ctx context.Context, logger *slog.Logger, client scaleSetClient, repositoryURL string, scaleSetConfig config.ScaleSetConfig) (shutdownFunc, error)
}

type shutdownFunc func(ctx context.Context) error

func productionRuntime() runtime {
	return runtime{
		LoadConfig: loadConfig,
		NewGitHubClient: func(repositoryURL string, token string) (scaleSetClient, error) {
			return newGitHubClient(repositoryURL, token)
		},
		NewLogger:     newLogger,
		StartScaleSet: startScaleSet,
	}
}

func runDaemon(ctx context.Context, options Options, daemonRuntime runtime) error {
	loadedConfig, err := daemonRuntime.LoadConfig(options.ConfigPath)
	if err != nil {
		return err
	}
	token, err := loadedConfig.Token()
	if err != nil {
		return fmt.Errorf("load github token: %w", err)
	}
	shutdownScaleSets, err := startConfiguredScaleSets(ctx, daemonRuntime, loadedConfig, token)
	if err != nil {
		return err
	}
	<-ctx.Done()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancelShutdown()
	return shutdownConfiguredScaleSets(shutdownCtx, shutdownScaleSets)
}

func startConfiguredScaleSets(ctx context.Context, daemonRuntime runtime, loadedConfig config.Config, token string) ([]shutdownFunc, error) {
	logger := daemonRuntime.NewLogger(loadedConfig.Log.Format)
	if loadedConfig.Prune {
		if err := pruneConfiguredScaleSets(ctx, daemonRuntime, logger, loadedConfig.ScaleSets, token); err != nil {
			return nil, err
		}
	}
	var shutdownScaleSets []shutdownFunc
	for _, scaleSetConfig := range loadedConfig.ScaleSets {
		repositoryShutdowns, err := startScaleSetRepositories(ctx, daemonRuntime, logger, scaleSetConfig, token)
		if err != nil {
			shutdownErr := shutdownConfiguredScaleSets(context.WithoutCancel(ctx), shutdownScaleSets)
			if shutdownErr != nil {
				return nil, errors.Join(err, fmt.Errorf("shutdown started scale sets: %w", shutdownErr))
			}
			return nil, err
		}
		shutdownScaleSets = append(shutdownScaleSets, repositoryShutdowns...)
	}
	return shutdownScaleSets, nil
}

func startScaleSetRepositories(
	ctx context.Context,
	daemonRuntime runtime,
	logger *slog.Logger,
	scaleSetConfig config.ScaleSetConfig,
	token string,
) ([]shutdownFunc, error) {
	var shutdownRepositories []shutdownFunc
	for _, repositoryURL := range scaleSetConfig.Repositories {
		client, err := daemonRuntime.NewGitHubClient(repositoryURL, token)
		if err != nil {
			return nil, fmt.Errorf("create github client for %q: %w", repositoryURL, err)
		}
		shutdownRepository, err := daemonRuntime.StartScaleSet(ctx, logger, client, repositoryURL, scaleSetConfig)
		if err != nil {
			return nil, fmt.Errorf("run scale set %q for repository %q: %w", scaleSetConfig.Name, repositoryURL, err)
		}
		shutdownRepositories = append(shutdownRepositories, shutdownRepository)
	}
	return shutdownRepositories, nil
}

func shutdownConfiguredScaleSets(ctx context.Context, shutdownScaleSets []shutdownFunc) error {
	for shutdownIndex := len(shutdownScaleSets) - 1; shutdownIndex >= 0; shutdownIndex-- {
		if err := shutdownScaleSets[shutdownIndex](ctx); err != nil {
			return fmt.Errorf("shutdown scale set: %w", err)
		}
	}
	return nil
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
) (shutdownFunc, error) {
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
