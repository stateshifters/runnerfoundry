package daemon

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
		StartScaleSet: func(ctx context.Context, logger *slog.Logger, client scaleSetClient, repositoryURL string, scaleSetConfig config.ScaleSetConfig) error {
			if logger == nil {
				t.Fatal("expected logger")
			}
			capturedScaleSetNames = append(capturedScaleSetNames, scaleSetConfig.Name)
			if len(capturedScaleSetNames) == 2 {
				cancel()
			}
			return nil
		},
	}

	err := runDaemon(ctx, Options{ConfigPath: "config.yaml"}, daemonRuntime)
	if err == nil || !strings.Contains(err.Error(), "shutdown") {
		t.Fatalf("expected shutdown error, got %v", err)
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
				StartScaleSet: func(ctx context.Context, logger *slog.Logger, client scaleSetClient, repositoryURL string, scaleSetConfig config.ScaleSetConfig) error {
					return errors.New("scale failed")
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
