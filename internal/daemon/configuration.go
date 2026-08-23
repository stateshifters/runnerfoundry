package daemon

import (
	"bytes"
	"fmt"
	"log/slog"

	"github.com/stateshifters/runnerfoundry/internal/config"
)

func loadConfigWithReader(configPath string, readFile config.ReadFile) (config.Config, error) {
	configBytes, err := readFile(configPath)
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
