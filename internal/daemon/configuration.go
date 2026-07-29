package daemon

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/meza/RunnerFoundry/internal/config"
)

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
		handlerOptions.ReplaceAttr = renameJSONTimestamp
		return slog.New(slog.NewJSONHandler(writer, handlerOptions))
	}
	return slog.New(slog.NewTextHandler(writer, handlerOptions))
}

func renameJSONTimestamp(groups []string, attribute slog.Attr) slog.Attr {
	if len(groups) == 0 && attribute.Key == slog.TimeKey {
		attribute.Key = "@timestamp"
	}
	return attribute
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
