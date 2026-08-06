package daemon

import (
	"errors"
	"log/slog"
	"testing"
)

func TestConfigurationLoadingUsesConfiguredReader(t *testing.T) {
	contents := []byte("github:\n  discovery_interval: 1m\n  app:\n    app_id: 1\n    private_key_file: key.pem\nstate:\n  path: state.db\nscale_sets:\n  - name: linux\n    max_runners: 1\n    docker:\n      compose_file: compose.yml\n      service: runner\n")
	readConfigFile := func(path string) ([]byte, error) {
		if path != "config.yml" {
			t.Fatalf("path = %q", path)
		}
		return contents, nil
	}
	if configured, err := loadConfigWithReader("config.yml", readConfigFile); err != nil || len(configured.RunnerPools) != 1 {
		t.Fatalf("load config = %#v, %v", configured, err)
	}
	readConfigFile = func(string) ([]byte, error) { return nil, errors.New("unavailable") }
	if _, err := loadConfigWithReader("config.yml", readConfigFile); err == nil {
		t.Fatal("file read failure was ignored")
	}
	if _, err := loadConfigBytes([]byte("unknown: true\n")); err == nil {
		t.Fatal("invalid config was accepted")
	}
}

func TestLoggingConfigurationTranslation(t *testing.T) {
	t.Parallel()

	attribute := renameJSONTimestamp(nil, slog.Time(slog.TimeKey, builderTime()))
	if attribute.Key != "@timestamp" {
		t.Fatalf("timestamp key = %q", attribute.Key)
	}
	unchanged := renameJSONTimestamp([]string{"group"}, slog.String(slog.TimeKey, "value"))
	if unchanged.Key != slog.TimeKey {
		t.Fatalf("grouped key = %q", unchanged.Key)
	}
	levels := map[string]slog.Level{"debug": slog.LevelDebug, "warn": slog.LevelWarn, "error": slog.LevelError, "": slog.LevelInfo}
	for configured, want := range levels {
		if got := configuredLogLevel(configured); got != want {
			t.Fatalf("level %q = %v, want %v", configured, got, want)
		}
	}
}
