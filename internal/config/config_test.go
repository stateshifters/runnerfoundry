package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRejectsPlaintextTokenAndValidatesProviders(t *testing.T) {
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
  url: https://github.com/meza/agent-playground
  token_env: GITHUB_SCALER_TOKEN
log:
  format: json
scale_sets:
  - name: docker-set
    runner_group: default
    max_runners: 2
    docker:
      compose_file: ` + composePath + `
      service: runner
  - name: vm-set
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
	configText := `
github:
  url: https://github.com/meza/agent-playground
  token_env: GITHUB_SCALER_TOKEN
scale_sets:
  - name: repeated
    max_runners: 1
    docker:
      compose_file: compose.yaml
      service: runner
  - name: repeated
    max_runners: 1
    docker:
      compose_file: compose.yaml
      service: runner
`
	_, err := Load(strings.NewReader(configText))
	if err == nil {
		t.Fatal("expected duplicate name error")
	}
}

func TestLoadRejectsInvalidConfigurationShapes(t *testing.T) {
	t.Setenv("GITHUB_SCALER_TOKEN", "token")
	tests := map[string]string{
		"bad yaml":                        "github: [",
		"missing github url":              "github:\n  token_env: GITHUB_SCALER_TOKEN\nscale_sets: []\n",
		"two token sources":               "github:\n  url: https://github.com/meza/agent-playground\n  token_env: GITHUB_SCALER_TOKEN\n  token_file: token\nscale_sets: []\n",
		"missing token env":               "github:\n  url: https://github.com/meza/agent-playground\n  token_env: MISSING_TOKEN\nscale_sets: []\n",
		"bad log format":                  "github:\n  url: https://github.com/meza/agent-playground\n  token_env: GITHUB_SCALER_TOKEN\nlog:\n  format: xml\nscale_sets: []\n",
		"missing scale sets":              "github:\n  url: https://github.com/meza/agent-playground\n  token_env: GITHUB_SCALER_TOKEN\n",
		"missing scale set name":          "github:\n  url: https://github.com/meza/agent-playground\n  token_env: GITHUB_SCALER_TOKEN\nscale_sets:\n  - max_runners: 1\n    docker:\n      compose_file: compose.yaml\n      service: runner\n",
		"negative max runners":            "github:\n  url: https://github.com/meza/agent-playground\n  token_env: GITHUB_SCALER_TOKEN\nscale_sets:\n  - name: bad\n    max_runners: -1\n    docker:\n      compose_file: compose.yaml\n      service: runner\n",
		"missing provider config":         "github:\n  url: https://github.com/meza/agent-playground\n  token_env: GITHUB_SCALER_TOKEN\nscale_sets:\n  - name: bad\n    max_runners: 1\n",
		"multiple provider configs":       "github:\n  url: https://github.com/meza/agent-playground\n  token_env: GITHUB_SCALER_TOKEN\nscale_sets:\n  - name: bad\n    max_runners: 1\n    docker:\n      compose_file: compose.yaml\n      service: runner\n    libvirt:\n      connection_uri: qemu:///system\n      domain_xml_template: domain.xml\n",
		"missing docker compose file":     "github:\n  url: https://github.com/meza/agent-playground\n  token_env: GITHUB_SCALER_TOKEN\nscale_sets:\n  - name: bad\n    max_runners: 1\n    docker:\n      service: runner\n",
		"missing docker service":          "github:\n  url: https://github.com/meza/agent-playground\n  token_env: GITHUB_SCALER_TOKEN\nscale_sets:\n  - name: bad\n    max_runners: 1\n    docker:\n      compose_file: compose.yaml\n",
		"missing libvirt connection":      "github:\n  url: https://github.com/meza/agent-playground\n  token_env: GITHUB_SCALER_TOKEN\nscale_sets:\n  - name: bad\n    max_runners: 1\n    libvirt:\n      domain_xml_template: domain.xml\n",
		"missing libvirt domain template": "github:\n  url: https://github.com/meza/agent-playground\n  token_env: GITHUB_SCALER_TOKEN\nscale_sets:\n  - name: bad\n    max_runners: 1\n    libvirt:\n      connection_uri: qemu:///system\n",
		"unknown field":                   "unknown: true\n",
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
