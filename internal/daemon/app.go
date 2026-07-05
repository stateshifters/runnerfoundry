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
	NewGitHubClient func(loadedConfig config.Config, token string) (scaleSetClient, error)
	NewLogger       func(format string) *slog.Logger
	StartScaleSets  func(ctx context.Context, logger *slog.Logger, client scaleSetClient, scaleSetConfigs []config.ScaleSetConfig) error
}

func productionRuntime() runtime {
	return runtime{
		LoadConfig: loadConfig,
		NewGitHubClient: func(loadedConfig config.Config, token string) (scaleSetClient, error) {
			return newGitHubClient(loadedConfig, token)
		},
		NewLogger:      newLogger,
		StartScaleSets: startScaleSets,
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
	client, err := daemonRuntime.NewGitHubClient(loadedConfig, token)
	if err != nil {
		return err
	}
	if err := daemonRuntime.StartScaleSets(ctx, daemonRuntime.NewLogger(loadedConfig.Log.Format), client, loadedConfig.ScaleSets); err != nil {
		return err
	}
	<-ctx.Done()
	return fmt.Errorf("shutdown: %w", ctx.Err())
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

func startScaleSets(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	scaleSetConfigs []config.ScaleSetConfig,
) error {
	return startScaleSetsWithRuntime(ctx, logger, client, scaleSetConfigs, productionScaleSetRuntime())
}

func startScaleSetsWithRuntime(
	ctx context.Context,
	logger *slog.Logger,
	client scaleSetClient,
	scaleSetConfigs []config.ScaleSetConfig,
	scaleSets scaleSetRuntime,
) error {
	for _, scaleSetConfig := range scaleSetConfigs {
		if err := runScaleSet(ctx, logger, client, scaleSetConfig, scaleSets); err != nil {
			return fmt.Errorf("run scale set %q: %w", scaleSetConfig.Name, err)
		}
	}
	return nil
}
