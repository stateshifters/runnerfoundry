package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAcceptsScaleSetsWithRepositoryLists(t *testing.T) {
	t.Setenv("GITHUB_SCALER_TOKEN", "token")
	tempDir := t.TempDir()
	composePath := filepath.Join(tempDir, "compose.yaml")
	xmlPath := filepath.Join(tempDir, "domain.xml")
	if err := os.WriteFile(composePath, []byte("services:\n  runner:\n    image: runner\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(xmlPath, []byte(`<domain type="kvm"><name>template</name></domain>`), 0o600); err != nil {
		t.Fatal(err)
	}

	validConfig := `
github:
  token_env: GITHUB_SCALER_TOKEN
log:
  format: json
scale_sets:
  - name: docker-set
    repositories:
      - https://github.com/meza/repo-a
      - https://github.com/meza/repo-b
    max_runners: 2
    docker:
      compose_file: ` + composePath + `
      service: runner
  - name: vm-set
    repositories:
      - https://github.com/meza/repo-c
    runner_group: default
    max_runners: 1
    libvirt:
      connection_uri: qemu:///system
      domain_xml_template: ` + xmlPath + `
`
	loadedConfig, err := Load(strings.NewReader(validConfig))
	if err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
	if loadedConfig.ScaleSets[0].Repositories[1] != "https://github.com/meza/repo-b" {
		t.Fatalf("expected repositories to load, got %#v", loadedConfig.ScaleSets[0].Repositories)
	}
	if loadedConfig.ScaleSets[0].RunnerGroup != "default" {
		t.Fatalf("expected default runner group, got %q", loadedConfig.ScaleSets[0].RunnerGroup)
	}
	if loadedConfig.ScaleSets[0].Docker.Service != "runner" {
		t.Fatalf("expected docker service to load, got %q", loadedConfig.ScaleSets[0].Docker.Service)
	}

	_, err = Load(strings.NewReader(strings.Replace(validConfig, "token_env: GITHUB_SCALER_TOKEN", "token: ghp_secret", 1)))
	if err == nil {
		t.Fatal("expected plaintext token to be rejected")
	}
}

func TestLoadRejectsDuplicateScaleSetNames(t *testing.T) {
	t.Setenv("GITHUB_SCALER_TOKEN", "token")
	configText := baseConfig(`
  - name: repeated
    repositories:
      - https://github.com/meza/repo-a
    max_runners: 1
    docker:
      compose_file: compose.yaml
      service: runner
  - name: repeated
    repositories:
      - https://github.com/meza/repo-b
    max_runners: 1
    docker:
      compose_file: compose.yaml
      service: runner
`)
	_, err := Load(strings.NewReader(configText))
	if err == nil {
		t.Fatal("expected duplicate name error")
	}
}

func TestScaleSetValidationPreservesExplicitRunnerGroup(t *testing.T) {
	scaleSet := ScaleSetConfig{
		Name:         "linux",
		Repositories: []string{"https://github.com/meza/repo-a"},
		RunnerGroup:  "custom",
		MaxRunners:   1,
		Docker:       &DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
	}
	if err := scaleSet.validate(map[string]bool{}); err != nil {
		t.Fatalf("expected scale set to validate, got %v", err)
	}
	if scaleSet.RunnerGroup != "custom" {
		t.Fatalf("expected explicit runner group, got %q", scaleSet.RunnerGroup)
	}
}

func TestLoadRejectsInvalidConfigurationShapes(t *testing.T) {
	t.Setenv("GITHUB_SCALER_TOKEN", "token")
	tests := map[string]string{
		"bad yaml":               "github: [",
		"global github url":      "github:\n  url: https://github.com/meza/agent-playground\n  token_env: GITHUB_SCALER_TOKEN\nscale_sets: []\n",
		"two token sources":      "github:\n  token_env: GITHUB_SCALER_TOKEN\n  token_file: token\nscale_sets: []\n",
		"missing token env":      "github:\n  token_env: MISSING_TOKEN\nscale_sets: []\n",
		"bad log format":         "github:\n  token_env: GITHUB_SCALER_TOKEN\nlog:\n  format: xml\nscale_sets: []\n",
		"missing scale sets":     "github:\n  token_env: GITHUB_SCALER_TOKEN\n",
		"missing scale set name": baseConfig("  - repositories:\n      - https://github.com/meza/repo-a\n    max_runners: 1\n    docker:\n      compose_file: compose.yaml\n      service: runner\n"),
		"missing repositories":   baseConfig(validScaleSet("bad", "")),
		"empty repository":       baseConfig(validScaleSet("bad", "    repositories:\n      - ''\n")),
		"negative max runners": baseConfig(`
  - name: bad
    repositories:
      - https://github.com/meza/repo-a
    max_runners: -1
    docker:
      compose_file: compose.yaml
      service: runner
`),
		"missing provider config":   baseConfig("  - name: bad\n    repositories:\n      - https://github.com/meza/repo-a\n    max_runners: 1\n"),
		"multiple provider configs": baseConfig(validScaleSet("bad", "    repositories:\n      - https://github.com/meza/repo-a\n    libvirt:\n      connection_uri: qemu:///system\n      domain_xml_template: domain.xml\n")),
		"missing docker compose file": baseConfig(`
  - name: bad
    repositories:
      - https://github.com/meza/repo-a
    max_runners: 1
    docker:
      service: runner
`),
		"missing docker service": baseConfig(`
  - name: bad
    repositories:
      - https://github.com/meza/repo-a
    max_runners: 1
    docker:
      compose_file: compose.yaml
`),
		"missing libvirt connection": baseConfig(`
  - name: bad
    repositories:
      - https://github.com/meza/repo-a
    max_runners: 1
    libvirt:
      domain_xml_template: domain.xml
`),
		"missing libvirt domain template": baseConfig(`
  - name: bad
    repositories:
      - https://github.com/meza/repo-a
    max_runners: 1
    libvirt:
      connection_uri: qemu:///system
`),
		"unknown field": "unknown: true\n",
	}
	for testName, configText := range tests {
		t.Run(testName, func(t *testing.T) {
			if _, err := Load(strings.NewReader(configText)); err == nil {
				t.Fatal("expected config error")
			}
		})
	}
}

func TestTokenReadsEnvironmentAndTokenFile(t *testing.T) {
	t.Setenv("GITHUB_SCALER_TOKEN", "env-token")
	envConfig := Config{GitHub: GitHubConfig{TokenEnv: "GITHUB_SCALER_TOKEN"}}
	token, err := envConfig.Token()
	if err != nil {
		t.Fatalf("expected env token, got %v", err)
	}
	if token != "env-token" {
		t.Fatalf("unexpected token %q", token)
	}

	tokenPath := filepath.Join(t.TempDir(), "token")
	if writeErr := os.WriteFile(tokenPath, []byte("file-token\n"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	fileConfig := Config{GitHub: GitHubConfig{TokenFile: tokenPath}}
	token, err = fileConfig.Token()
	if err != nil {
		t.Fatalf("expected file token, got %v", err)
	}
	if token != "file-token" {
		t.Fatalf("unexpected file token %q", token)
	}
}

func TestTokenRejectsEmptyAndUnreadableSources(t *testing.T) {
	t.Setenv("EMPTY_TOKEN", "")
	if _, err := (Config{GitHub: GitHubConfig{TokenEnv: "EMPTY_TOKEN"}}).Token(); err == nil {
		t.Fatal("expected empty env token error")
	}
	emptyTokenPath := filepath.Join(t.TempDir(), "empty-token")
	if err := os.WriteFile(emptyTokenPath, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (Config{GitHub: GitHubConfig{TokenFile: emptyTokenPath}}).Token(); err == nil {
		t.Fatal("expected empty file token error")
	}
	if _, err := (Config{GitHub: GitHubConfig{TokenFile: filepath.Join(t.TempDir(), "missing")}}).Token(); err == nil {
		t.Fatal("expected unreadable file token error")
	}
}

func baseConfig(scaleSets string) string {
	return "github:\n  token_env: GITHUB_SCALER_TOKEN\nscale_sets:\n" + scaleSets
}

func validScaleSet(name string, repositoryBlock string) string {
	return "  - name: " + name + "\n" +
		repositoryBlock +
		"    max_runners: 1\n" +
		"    docker:\n" +
		"      compose_file: compose.yaml\n" +
		"      service: runner\n"
}
