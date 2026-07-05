package daemon

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
)

func TestRunUsesConfigTokenClientLoggerAndScaleSets(t *testing.T) {
	t.Setenv("GITHUB_SCALER_PAT", "secret")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var capturedConfigPath string
	var capturedToken string
	var capturedRepositories []string
	var capturedScaleSetNames []string
	var shutdowns int
	daemonRuntime := runtime{
		LoadConfig: func(configPath string) (config.Config, error) {
			capturedConfigPath = configPath
			return config.Config{
				GitHub: config.GitHubConfig{TokenEnv: "GITHUB_SCALER_PAT"},
				Log:    config.LogConfig{Format: "json"},
				ScaleSets: []config.ScaleSetConfig{{
					Name: "linux",
					Repositories: []string{
						"https://github.com/meza/repo-a",
						"https://github.com/meza/repo-b",
					},
					MaxRunners: 1,
					Docker:     &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
				}},
			}, nil
		},
		NewGitHubClient: func(repositoryURL string, token string) (scaleSetClient, error) {
			capturedToken = token
			capturedRepositories = append(capturedRepositories, repositoryURL)
			return &fakeScaleSetClient{}, nil
		},
		NewLogger: newLogger,
		StartScaleSet: func(
			ctx context.Context,
			logger *slog.Logger,
			client scaleSetClient,
			repositoryURL string,
			scaleSetConfig config.ScaleSetConfig,
		) (shutdownFunc, error) {
			if logger == nil {
				t.Fatal("expected logger")
			}
			capturedScaleSetNames = append(capturedScaleSetNames, scaleSetConfig.Name)
			if len(capturedScaleSetNames) == 2 {
				cancel()
			}
			return func(ctx context.Context) error {
				shutdowns++
				return nil
			}, nil
		},
	}

	err := runDaemon(ctx, Options{ConfigPath: "config.yaml"}, daemonRuntime)
	if err != nil {
		t.Fatalf("expected graceful shutdown, got %v", err)
	}
	if capturedConfigPath != "config.yaml" {
		t.Fatalf("expected config path, got %q", capturedConfigPath)
	}
	if capturedToken != "secret" {
		t.Fatalf("expected token, got %q", capturedToken)
	}
	if strings.Join(capturedRepositories, ",") != "https://github.com/meza/repo-a,https://github.com/meza/repo-b" {
		t.Fatalf("expected repositories, got %#v", capturedRepositories)
	}
	if strings.Join(capturedScaleSetNames, ",") != "linux,linux" {
		t.Fatalf("expected scale set per repository, got %#v", capturedScaleSetNames)
	}
	if shutdowns != 2 {
		t.Fatalf("expected two shutdowns, got %d", shutdowns)
	}
}

func TestRunSurfacesStartupFailures(t *testing.T) {
	tests := map[string]struct {
		daemonRuntime runtime
		expected      string
	}{
		"load config": {
			daemonRuntime: runtime{
				LoadConfig: func(configPath string) (config.Config, error) {
					return config.Config{}, errors.New("load failed")
				},
			},
			expected: "load failed",
		},
		"token": {
			daemonRuntime: runtime{
				LoadConfig: func(configPath string) (config.Config, error) {
					return config.Config{GitHub: config.GitHubConfig{TokenEnv: "MISSING"}}, nil
				},
			},
			expected: "load github token",
		},
		"client": {
			daemonRuntime: runtime{
				LoadConfig: func(configPath string) (config.Config, error) {
					return config.Config{
						GitHub: config.GitHubConfig{TokenFile: writeFile(t, "token", "secret")},
						ScaleSets: []config.ScaleSetConfig{{
							Name:         "linux",
							Repositories: []string{"https://github.com/meza/repo-a"},
							Docker:       &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
						}},
					}, nil
				},
				NewGitHubClient: func(repositoryURL string, token string) (scaleSetClient, error) {
					return nil, errors.New("client failed")
				},
				NewLogger: newLogger,
			},
			expected: "client failed",
		},
		"scale sets": {
			daemonRuntime: runtime{
				LoadConfig: func(configPath string) (config.Config, error) {
					return config.Config{
						GitHub: config.GitHubConfig{TokenFile: writeFile(t, "token", "secret")},
						ScaleSets: []config.ScaleSetConfig{{
							Name:         "linux",
							Repositories: []string{"https://github.com/meza/repo-a"},
							Docker:       &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
						}},
					}, nil
				},
				NewGitHubClient: func(repositoryURL string, token string) (scaleSetClient, error) {
					return &fakeScaleSetClient{}, nil
				},
				NewLogger: newLogger,
				StartScaleSet: func(ctx context.Context, logger *slog.Logger, client scaleSetClient, repositoryURL string, scaleSetConfig config.ScaleSetConfig) (shutdownFunc, error) {
					return nil, errors.New("scale failed")
				},
			},
			expected: "scale failed",
		},
	}
	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			err := runDaemon(context.Background(), Options{}, testCase.daemonRuntime)
			if err == nil || !strings.Contains(err.Error(), testCase.expected) {
				t.Fatalf("expected %q error, got %v", testCase.expected, err)
			}
		})
	}
}

func TestRunPrunesUnconfiguredScaleSetsBeforeStartup(t *testing.T) {
	t.Setenv("GITHUB_SCALER_PAT", "secret")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pruneClient := &fakeScaleSetClient{
		runnerGroup: &upstream.RunnerGroup{ID: 7},
		runnerScaleSets: []upstream.RunnerScaleSet{
			{ID: 1, Name: "linux"},
			{ID: 2, Name: "windows"},
			{ID: 3, Name: "stale"},
		},
	}
	var started []string
	daemonRuntime := runtime{
		LoadConfig: func(configPath string) (config.Config, error) {
			return config.Config{
				GitHub: config.GitHubConfig{TokenEnv: "GITHUB_SCALER_PAT"},
				Prune:  true,
				ScaleSets: []config.ScaleSetConfig{
					{
						Name:         "linux",
						Repositories: []string{"https://github.com/meza/repo-a"},
						RunnerGroup:  "default",
						MaxRunners:   1,
						Docker:       &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
					},
					{
						Name:         "windows",
						Repositories: []string{"https://github.com/meza/repo-a"},
						RunnerGroup:  "default",
						MaxRunners:   1,
						Docker:       &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
					},
				},
			}, nil
		},
		NewGitHubClient: func(repositoryURL string, token string) (scaleSetClient, error) {
			return pruneClient, nil
		},
		NewLogger: newLogger,
		StartScaleSet: func(
			ctx context.Context,
			logger *slog.Logger,
			client scaleSetClient,
			repositoryURL string,
			scaleSetConfig config.ScaleSetConfig,
		) (shutdownFunc, error) {
			if len(pruneClient.deletedScaleSetIDs) != 1 || pruneClient.deletedScaleSetIDs[0] != 3 {
				t.Fatalf("expected stale scale set pruned before startup, got %#v", pruneClient.deletedScaleSetIDs)
			}
			started = append(started, scaleSetConfig.Name)
			if len(started) == 2 {
				cancel()
			}
			return func(ctx context.Context) error {
				return nil
			}, nil
		},
	}

	if err := runDaemon(ctx, Options{ConfigPath: "config.yaml"}, daemonRuntime); err != nil {
		t.Fatalf("expected run success, got %v", err)
	}
	if strings.Join(started, ",") != "linux,windows" {
		t.Fatalf("expected configured scale sets to start, got %#v", started)
	}
}

func TestRunShutsDownStartedScaleSetsWhenLaterStartupFails(t *testing.T) {
	t.Setenv("GITHUB_SCALER_PAT", "secret")
	var shutdowns int
	starts := 0
	daemonRuntime := runtime{
		LoadConfig: func(configPath string) (config.Config, error) {
			return config.Config{
				GitHub: config.GitHubConfig{TokenEnv: "GITHUB_SCALER_PAT"},
				ScaleSets: []config.ScaleSetConfig{
					{
						Name:         "linux",
						Repositories: []string{"https://github.com/meza/repo-a"},
						RunnerGroup:  "default",
						MaxRunners:   1,
						Docker:       &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
					},
					{
						Name:         "windows",
						Repositories: []string{"https://github.com/meza/repo-a"},
						RunnerGroup:  "default",
						MaxRunners:   1,
						Docker:       &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
					},
				},
			}, nil
		},
		NewGitHubClient: func(repositoryURL string, token string) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		NewLogger: newLogger,
		StartScaleSet: func(
			ctx context.Context,
			logger *slog.Logger,
			client scaleSetClient,
			repositoryURL string,
			scaleSetConfig config.ScaleSetConfig,
		) (shutdownFunc, error) {
			starts++
			if starts == 2 {
				return nil, errors.New("start failed")
			}
			return func(ctx context.Context) error {
				shutdowns++
				return nil
			}, nil
		},
	}

	err := runDaemon(context.Background(), Options{ConfigPath: "config.yaml"}, daemonRuntime)
	if err == nil || !strings.Contains(err.Error(), "start failed") {
		t.Fatalf("expected startup error, got %v", err)
	}
	if shutdowns != 1 {
		t.Fatalf("expected started scale set to shut down, got %d", shutdowns)
	}
}

func TestRunReportsShutdownFailureAfterLaterStartupFails(t *testing.T) {
	t.Setenv("GITHUB_SCALER_PAT", "secret")
	starts := 0
	daemonRuntime := runtime{
		LoadConfig: func(configPath string) (config.Config, error) {
			return config.Config{
				GitHub: config.GitHubConfig{TokenEnv: "GITHUB_SCALER_PAT"},
				ScaleSets: []config.ScaleSetConfig{
					{
						Name:         "linux",
						Repositories: []string{"https://github.com/meza/repo-a"},
						RunnerGroup:  "default",
						MaxRunners:   1,
						Docker:       &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
					},
					{
						Name:         "windows",
						Repositories: []string{"https://github.com/meza/repo-a"},
						RunnerGroup:  "default",
						MaxRunners:   1,
						Docker:       &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
					},
				},
			}, nil
		},
		NewGitHubClient: func(repositoryURL string, token string) (scaleSetClient, error) {
			return &fakeScaleSetClient{}, nil
		},
		NewLogger: newLogger,
		StartScaleSet: func(
			ctx context.Context,
			logger *slog.Logger,
			client scaleSetClient,
			repositoryURL string,
			scaleSetConfig config.ScaleSetConfig,
		) (shutdownFunc, error) {
			starts++
			if starts == 2 {
				return nil, errors.New("start failed")
			}
			return func(ctx context.Context) error {
				return errors.New("shutdown failed")
			}, nil
		},
	}

	err := runDaemon(context.Background(), Options{ConfigPath: "config.yaml"}, daemonRuntime)
	if err == nil || !strings.Contains(err.Error(), "shutdown started scale sets") {
		t.Fatalf("expected startup shutdown error, got %v", err)
	}
}

func TestRunSurfacesPruneFailures(t *testing.T) {
	t.Setenv("GITHUB_SCALER_PAT", "secret")
	tests := map[string]struct {
		newClient func(repositoryURL string, token string) (scaleSetClient, error)
		expected  string
	}{
		"client": {
			newClient: func(repositoryURL string, token string) (scaleSetClient, error) {
				return nil, errors.New("client failed")
			},
			expected: "client failed",
		},
		"target": {
			newClient: func(repositoryURL string, token string) (scaleSetClient, error) {
				return &fakeScaleSetClient{runnerGroupErr: errors.New("group failed")}, nil
			},
			expected: "group failed",
		},
		"unsupported": {
			newClient: func(repositoryURL string, token string) (scaleSetClient, error) {
				return fakeNonPruningScaleSetClient{}, nil
			},
			expected: "does not support pruning",
		},
	}
	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			daemonRuntime := runtime{
				LoadConfig: func(configPath string) (config.Config, error) {
					return config.Config{
						GitHub: config.GitHubConfig{TokenEnv: "GITHUB_SCALER_PAT"},
						Prune:  true,
						ScaleSets: []config.ScaleSetConfig{{
							Name:         "linux",
							Repositories: []string{"https://github.com/meza/repo-a"},
							RunnerGroup:  "default",
							MaxRunners:   1,
							Docker:       &config.DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
						}},
					}, nil
				},
				NewGitHubClient: testCase.newClient,
				NewLogger:       newLogger,
				StartScaleSet: func(ctx context.Context, logger *slog.Logger, client scaleSetClient, repositoryURL string, scaleSetConfig config.ScaleSetConfig) (shutdownFunc, error) {
					return func(ctx context.Context) error {
						return nil
					}, nil
				},
			}
			err := runDaemon(context.Background(), Options{ConfigPath: "config.yaml"}, daemonRuntime)
			if err == nil || !strings.Contains(err.Error(), testCase.expected) {
				t.Fatalf("expected %q error, got %v", testCase.expected, err)
			}
		})
	}
}

func TestShutdownConfiguredScaleSetsSurfacesFailure(t *testing.T) {
	err := shutdownConfiguredScaleSets(context.Background(), []shutdownFunc{
		func(ctx context.Context) error {
			return errors.New("shutdown failed")
		},
	})
	if err == nil || !strings.Contains(err.Error(), "shutdown failed") {
		t.Fatalf("expected shutdown error, got %v", err)
	}
}

func TestPruneScaleSetTargetSurfacesListAndDeleteFailures(t *testing.T) {
	tests := map[string]struct {
		client   *fakeScaleSetClient
		expected string
	}{
		"list": {
			client:   &fakeScaleSetClient{runnerGroup: &upstream.RunnerGroup{ID: 1}, listScaleSetsErr: errors.New("list failed")},
			expected: "list failed",
		},
		"delete": {
			client: &fakeScaleSetClient{
				runnerGroup:       &upstream.RunnerGroup{ID: 1},
				runnerScaleSets:   []upstream.RunnerScaleSet{{ID: 2, Name: "stale"}},
				deleteScaleSetErr: errors.New("delete failed"),
			},
			expected: "delete failed",
		},
	}
	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			err := pruneScaleSetTarget(context.Background(), slog.Default(), testCase.client, pruneTarget{
				RepositoryURL: "https://github.com/meza/repo-a",
				RunnerGroup:   "default",
			}, map[string]bool{"linux": true})
			if err == nil || !strings.Contains(err.Error(), testCase.expected) {
				t.Fatalf("expected %q error, got %v", testCase.expected, err)
			}
		})
	}
}

type fakeNonPruningScaleSetClient struct{}

func (client fakeNonPruningScaleSetClient) GetRunnerGroupByName(ctx context.Context, name string) (*upstream.RunnerGroup, error) {
	return &upstream.RunnerGroup{ID: 1}, nil
}

func (client fakeNonPruningScaleSetClient) GetRunnerScaleSet(
	ctx context.Context,
	runnerGroupID int,
	scaleSetName string,
) (*upstream.RunnerScaleSet, error) {
	return &upstream.RunnerScaleSet{}, nil
}

func (client fakeNonPruningScaleSetClient) CreateRunnerScaleSet(
	ctx context.Context,
	runnerScaleSet *upstream.RunnerScaleSet,
) (*upstream.RunnerScaleSet, error) {
	return runnerScaleSet, nil
}

func (client fakeNonPruningScaleSetClient) MessageSessionClient(
	ctx context.Context,
	runnerScaleSetID int,
	runnerName string,
) (scaleSetMessageClient, error) {
	return scaleSetMessageClient{Client: &fakeMessageClient{}}, nil
}

func (client fakeNonPruningScaleSetClient) GenerateJitRunnerConfig(
	ctx context.Context,
	setting *upstream.RunnerScaleSetJitRunnerSetting,
	runnerScaleSetID int,
) (*upstream.RunnerScaleSetJitRunnerConfig, error) {
	return &upstream.RunnerScaleSetJitRunnerConfig{}, nil
}

func TestRunUsesProductionRuntime(t *testing.T) {
	if err := Run(context.Background(), Options{ConfigPath: filepath.Join(t.TempDir(), "missing.yaml")}); err == nil {
		t.Fatal("expected missing config error")
	}
}

func TestProductionRuntimeProvidesGitHubClientFactory(t *testing.T) {
	daemonRuntime := productionRuntime()
	if _, err := daemonRuntime.NewGitHubClient("https://github.com/meza/agent-playground", "secret"); err != nil {
		t.Fatalf("expected client factory success, got %v", err)
	}
}

func TestLoadConfigReadsOperatorConfig(t *testing.T) {
	t.Setenv("GITHUB_SCALER_PAT", "secret")
	configPath := writeFile(t, "config.yaml", `
github:
  token_env: GITHUB_SCALER_PAT
scale_sets:
  - name: linux
    repositories:
      - https://github.com/meza/agent-playground
    max_runners: 1
    docker:
      compose_file: compose.yaml
      service: runner
`)
	loadedConfig, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("expected config success, got %v", err)
	}
	if loadedConfig.ScaleSets[0].Name != "linux" {
		t.Fatalf("expected scale set, got %#v", loadedConfig.ScaleSets)
	}
}

func TestLoadConfigSurfacesFileAndYAMLErrors(t *testing.T) {
	if _, err := loadConfig(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("expected missing file error")
	}
	if _, err := loadConfig(writeFile(t, "broken.yaml", "github: [")); err == nil {
		t.Fatal("expected yaml error")
	}
}

func TestNewLoggerSupportsTextAndJSON(t *testing.T) {
	if newLogger("text") == nil {
		t.Fatal("expected text logger")
	}
	if newLogger("json") == nil {
		t.Fatal("expected json logger")
	}
}

func TestNewGitHubClientBuildsScaleSetClient(t *testing.T) {
	_, err := newGitHubClient("https://github.com/meza/agent-playground", "secret")
	if err != nil {
		t.Fatalf("expected client success, got %v", err)
	}
}

func writeFile(t *testing.T, name string, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
