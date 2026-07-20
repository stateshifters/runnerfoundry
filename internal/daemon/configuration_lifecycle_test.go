package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
)

func TestDrainConfigurationWaitsForActiveRunnerAndSurfacesFailures(t *testing.T) {
	drainCalls := 0
	activeConfig := testActiveConfiguration()
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		Drain: func(context.Context) (int, error) {
			drainCalls++
			if drainCalls == 1 {
				return 1, nil
			}
			return 0, nil
		},
	}}}
	if err := drainConfiguration(context.Background(), activeConfig, time.Millisecond); err != nil || drainCalls != 2 {
		t.Fatalf("expected drain retry, calls=%d error=%v", drainCalls, err)
	}
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		Drain: func(context.Context) (int, error) { return 0, errors.New("drain failed") },
	}}}
	if err := drainConfiguration(context.Background(), activeConfig, time.Millisecond); err == nil {
		t.Fatal("expected drain failure")
	}
}

func TestDrainConfigurationReturnsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	activeConfig := testActiveConfiguration()
	activeConfig.Repositories["repo"] = managedRepository{Lifecycles: map[string]scaleSetLifecycle{"linux": {
		Drain: func(context.Context) (int, error) { return 1, nil },
	}}}
	if err := drainConfiguration(ctx, activeConfig, time.Hour); err == nil {
		t.Fatal("expected cancelled drain")
	}
}

func TestLoadConfigRejectsInvalidYAML(t *testing.T) {
	invalidConfigPath := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(invalidConfigPath, []byte("github: ["), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(invalidConfigPath); err == nil {
		t.Fatal("expected invalid config error")
	}
}

func TestLoadConfigReadsValidatedFileAndLoggerFormats(t *testing.T) {
	loadedConfig := runnableConfig(t)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	configText := "github:\n  app:\n    app_id: 1\n    private_key_file: " + loadedConfig.GitHub.App.PrivateKeyFile + "\nstate:\n  path: state.db\nscale_sets:\n  - name: linux\n    max_runners: 1\n    docker:\n      compose_file: compose.yaml\n      service: runner\n"
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(configPath); err != nil {
		t.Fatalf("expected config file to load, got %v", err)
	}
	if _, err := loadConfig(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("expected missing config error")
	}
	if _, err := readConfigFile(filepath.Join(t.TempDir(), "missing", "config.yaml")); err == nil {
		t.Fatal("expected missing config directory error")
	}
	if _, err := readConfigFile(t.TempDir()); err == nil {
		t.Fatal("expected directory read error")
	}
	if newLogger("json", "info") == nil || newLogger("text", "info") == nil {
		t.Fatal("expected both logger formats")
	}
}

func TestLoggerHonorsConfiguredLevel(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := newLoggerForWriter(logs, "json", "info")
	logger.Debug("hidden debug action", slog.String("action", "hidden"))
	logger.Info("visible info action", slog.String("action", "visible"))
	if strings.Contains(logs.String(), "hidden") || !strings.Contains(logs.String(), `"action":"visible"`) {
		t.Fatalf("expected info level to filter debug actions, got %q", logs.String())
	}

	logs.Reset()
	debugLogger := newLoggerForWriter(logs, "text", "debug")
	debugLogger.Debug("visible debug action", slog.String("action", "debug"))
	if !strings.Contains(logs.String(), "level=DEBUG") || !strings.Contains(logs.String(), "action=debug") {
		t.Fatalf("expected debug level action, got %q", logs.String())
	}
	if configuredLogLevel("warn") != slog.LevelWarn {
		t.Fatal("expected warn log level")
	}
	if configuredLogLevel("error") != slog.LevelError {
		t.Fatal("expected error log level")
	}
}

func TestLogActionIgnoresNilLogger(t *testing.T) {
	logAction(context.Background(), nil, slog.LevelInfo, "test", "started")
}

func TestPruneRepositoryDeletesOnlyUnconfiguredScaleSets(t *testing.T) {
	client := &fakeScaleSetClient{
		runnerGroup:     &upstream.RunnerGroup{ID: 5},
		runnerScaleSets: []upstream.RunnerScaleSet{{ID: 1, Name: "linux"}, {ID: 2, Name: "stale"}},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := pruneRepository(context.Background(), logger, client, "https://github.com/acme/repo", []config.ScaleSetConfig{{
		Name: "linux", RunnerGroup: "default",
	}}); err != nil {
		t.Fatalf("expected prune success, got %v", err)
	}
	if len(client.deletedScaleSetIDs) != 1 || client.deletedScaleSetIDs[0] != 2 {
		t.Fatalf("expected stale scale set deletion, got %#v", client.deletedScaleSetIDs)
	}
}

func TestPruneRepositoryRejectsUnsupportedClient(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := pruneRepository(context.Background(), logger, nonPruningScaleSetClient{}, "repo", []config.ScaleSetConfig{{Name: "linux"}}); err == nil {
		t.Fatal("expected unsupported prune client error")
	}
}

func TestPruneRepositorySurfacesGitHubFailures(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := map[string]*fakeScaleSetClient{
		"runner group": {runnerGroupErr: errors.New("group failed")},
		"list":         {runnerGroup: &upstream.RunnerGroup{ID: 1}, listScaleSetsErr: errors.New("list failed")},
		"delete": {
			runnerGroup:       &upstream.RunnerGroup{ID: 1},
			runnerScaleSets:   []upstream.RunnerScaleSet{{ID: 2, Name: "stale"}},
			deleteScaleSetErr: errors.New("delete failed"),
		},
	}
	for testName, client := range tests {
		t.Run(testName, func(t *testing.T) {
			if err := pruneRepository(context.Background(), logger, client, "repo", []config.ScaleSetConfig{{Name: "linux", RunnerGroup: "default"}}); err == nil {
				t.Fatal("expected prune failure")
			}
		})
	}
}
