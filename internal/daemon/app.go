package daemon

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"

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
	StartScaleSet   func(ctx context.Context, logger *slog.Logger, client scaleSetClient, repositoryURL string, scaleSetConfig config.ScaleSetConfig) error
}

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
	if err := startConfiguredScaleSets(ctx, daemonRuntime, loadedConfig, token); err != nil {
		return err
	}
	<-ctx.Done()
	return fmt.Errorf("shutdown: %w", ctx.Err())
}

func startConfiguredScaleSets(ctx context.Context, daemonRuntime runtime, loadedConfig config.Config, token string) error {
	logger := daemonRuntime.NewLogger(loadedConfig.Log.Format)
	for _, scaleSetConfig := range loadedConfig.ScaleSets {
		if err := startScaleSetRepositories(ctx, daemonRuntime, logger, scaleSetConfig, token); err != nil {
			return err
		}
	}
	return nil
}

func startScaleSetRepositories(
	ctx context.Context,
	daemonRuntime runtime,
	logger *slog.Logger,
	scaleSetConfig config.ScaleSetConfig,
	token string,
) error {
	for _, repositoryURL := range scaleSetConfig.Repositories {
		client, err := daemonRuntime.NewGitHubClient(repositoryURL, token)
		if err != nil {
			return fmt.Errorf("create github client for %q: %w", repositoryURL, err)
		}
		if err := daemonRuntime.StartScaleSet(ctx, logger, client, repositoryURL, scaleSetConfig); err != nil {
			return fmt.Errorf("run scale set %q for repository %q: %w", scaleSetConfig.Name, repositoryURL, err)
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
) error {
	return runScaleSet(ctx, logger, client, repositoryURL, scaleSetConfig, productionScaleSetRuntime())
}
