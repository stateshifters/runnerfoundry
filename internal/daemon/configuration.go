package daemon

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/meza/RunnerFoundry/internal/config"
)

var readConfigFile = os.ReadFile

func loadConfig(configPath string) (config.Config, error) {
	configBytes, err := readConfigFile(configPath)
	if err != nil {
		return config.Config{}, err
	}
	return loadConfigBytes(configBytes)
}

func loadConfigBytes(configBytes []byte) (config.Config, error) {
	loadedConfig, err := config.Load(bytes.NewReader(configBytes))
	if err != nil {
		return config.Config{}, fmt.Errorf("load config: %w", err)
	}
	return loadedConfig, nil
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
