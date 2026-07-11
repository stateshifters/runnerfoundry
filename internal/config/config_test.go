package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadAcceptsGitHubAppConfiguration(t *testing.T) {
	privateKeyPath := writePrivateKey(t, "private-key")
	loadedConfig, err := Load(strings.NewReader(validConfig(privateKeyPath)))
	if err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
	if loadedConfig.GitHub.App.AppID != 12345 {
		t.Fatalf("expected app ID, got %d", loadedConfig.GitHub.App.AppID)
	}
	if loadedConfig.GitHub.App.ClientID != "Iv1.example" {
		t.Fatalf("expected client ID, got %q", loadedConfig.GitHub.App.ClientID)
	}
	if loadedConfig.GitHub.DiscoveryInterval != "5m" {
		t.Fatalf("expected default discovery interval, got %q", loadedConfig.GitHub.DiscoveryInterval)
	}
	if loadedConfig.ScaleSets[0].RunnerGroup != "default" {
		t.Fatalf("expected default runner group, got %q", loadedConfig.ScaleSets[0].RunnerGroup)
	}
	credentials, err := loadedConfig.GitHub.AppCredentials()
	if err != nil {
		t.Fatalf("expected app credentials, got %v", err)
	}
	if credentials.AppID != 12345 || credentials.ClientID != "Iv1.example" || credentials.PrivateKey != "private-key" {
		t.Fatalf("unexpected credentials %#v", credentials)
	}
	discoveryDuration, err := loadedConfig.GitHub.DiscoveryDuration()
	if err != nil || discoveryDuration != 5*time.Minute {
		t.Fatalf("expected five minute discovery duration, got %s, %v", discoveryDuration, err)
	}
	if _, err := (GitHubConfig{DiscoveryInterval: "invalid"}).DiscoveryDuration(); err == nil {
		t.Fatal("expected invalid unvalidated discovery duration error")
	}
}

func TestLoadRejectsInvalidGitHubAppConfiguration(t *testing.T) {
	privateKeyPath := writePrivateKey(t, "private-key")
	tests := map[string]string{
		"bad yaml":                   "github: [",
		"legacy token env":           strings.Replace(validConfig(privateKeyPath), "  app:\n", "  token_env: TOKEN\n  app:\n", 1),
		"legacy token file":          strings.Replace(validConfig(privateKeyPath), "  app:\n", "  token_file: token\n  app:\n", 1),
		"legacy plaintext token":     strings.Replace(validConfig(privateKeyPath), "  app:\n", "  token: secret\n  app:\n", 1),
		"plaintext private key":      strings.Replace(validConfig(privateKeyPath), "    private_key_file: "+privateKeyPath, "    private_key: secret", 1),
		"missing app ID":             strings.Replace(validConfig(privateKeyPath), "    app_id: 12345\n", "", 1),
		"invalid app ID":             strings.Replace(validConfig(privateKeyPath), "    app_id: 12345", "    app_id: -1", 1),
		"missing client ID":          strings.Replace(validConfig(privateKeyPath), "    client_id: Iv1.example\n", "", 1),
		"empty client ID":            strings.Replace(validConfig(privateKeyPath), "    client_id: Iv1.example", "    client_id: '   '", 1),
		"missing private key file":   strings.Replace(validConfig(privateKeyPath), "    private_key_file: "+privateKeyPath+"\n", "", 1),
		"invalid discovery interval": strings.Replace(validConfig(privateKeyPath), "github:\n", "github:\n  discovery_interval: soon\n", 1),
		"short discovery interval":   strings.Replace(validConfig(privateKeyPath), "github:\n", "github:\n  discovery_interval: 59s\n", 1),
		"bad log format":             strings.Replace(validConfig(privateKeyPath), "log:\n  format: json", "log:\n  format: xml", 1),
		"missing scale sets":         strings.Split(validConfig(privateKeyPath), "scale_sets:")[0],
		"missing scale set name":     baseConfig(privateKeyPath, "  - max_runners: 1\n    docker:\n      compose_file: compose.yaml\n      service: runner\n"),
		"duplicate scale set name":   baseConfig(privateKeyPath, validScaleSet("repeated")+validScaleSet("repeated")),
		"zero max runners":           baseConfig(privateKeyPath, strings.Replace(validScaleSet("bad"), "max_runners: 1", "max_runners: 0", 1)),
		"negative max runners":       baseConfig(privateKeyPath, strings.Replace(validScaleSet("bad"), "max_runners: 1", "max_runners: -1", 1)),
		"missing provider config":    baseConfig(privateKeyPath, "  - name: bad\n    max_runners: 1\n"),
		"multiple providers": baseConfig(privateKeyPath, validScaleSet("bad")+
			"    libvirt:\n      connection_uri: qemu:///system\n      domain_xml_template: domain.xml\n"),
		"missing docker compose file": baseConfig(privateKeyPath, "  - name: bad\n    max_runners: 1\n    docker:\n      service: runner\n"),
		"missing docker service":      baseConfig(privateKeyPath, "  - name: bad\n    max_runners: 1\n    docker:\n      compose_file: compose.yaml\n"),
		"missing libvirt connection":  baseConfig(privateKeyPath, "  - name: bad\n    max_runners: 1\n    libvirt:\n      domain_xml_template: domain.xml\n"),
		"missing libvirt template":    baseConfig(privateKeyPath, "  - name: bad\n    max_runners: 1\n    libvirt:\n      connection_uri: qemu:///system\n"),
		"repositories are obsolete":   baseConfig(privateKeyPath, strings.Replace(validScaleSet("bad"), "    max_runners", "    repositories:\n      - https://github.com/meza/repo\n    max_runners", 1)),
		"unknown field":               "unknown: true\n",
	}
	for testName, configText := range tests {
		t.Run(testName, func(t *testing.T) {
			if _, err := Load(strings.NewReader(configText)); err == nil {
				t.Fatal("expected config error")
			}
		})
	}
}

func TestAppCredentialsRejectUnreadableOrEmptyPrivateKey(t *testing.T) {
	emptyKeyPath := writePrivateKey(t, "\n")
	if _, err := (GitHubConfig{App: GitHubAppConfig{AppID: 12345, PrivateKeyFile: emptyKeyPath}}).AppCredentials(); err == nil {
		t.Fatal("expected empty private key error")
	}
	missingPath := filepath.Join(t.TempDir(), "missing.pem")
	if _, err := (GitHubConfig{App: GitHubAppConfig{AppID: 12345, PrivateKeyFile: missingPath}}).AppCredentials(); err == nil {
		t.Fatal("expected unreadable private key error")
	}
}

func TestScaleSetValidationPreservesExplicitRunnerGroup(t *testing.T) {
	scaleSet := ScaleSetConfig{
		Name:        "linux",
		RunnerGroup: "custom",
		MaxRunners:  1,
		Docker:      &DockerConfig{ComposeFile: "compose.yaml", Service: "runner"},
	}
	if err := scaleSet.validate(map[string]bool{}); err != nil {
		t.Fatalf("expected scale set to validate, got %v", err)
	}
	if scaleSet.RunnerGroup != "custom" {
		t.Fatalf("expected explicit runner group, got %q", scaleSet.RunnerGroup)
	}
}

func TestScaleSetValidationAcceptsCompleteLibvirtProvider(t *testing.T) {
	scaleSet := ScaleSetConfig{
		Name: "vm", MaxRunners: 1,
		Libvirt: &LibvirtConfig{ConnectionURI: "qemu:///system", DomainXMLTemplate: "domain.xml"},
	}
	if err := scaleSet.validate(map[string]bool{}); err != nil {
		t.Fatalf("expected libvirt scale set, got %v", err)
	}
}

func writePrivateKey(t *testing.T, contents string) string {
	t.Helper()
	privateKeyPath := filepath.Join(t.TempDir(), "app.pem")
	if err := os.WriteFile(privateKeyPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return privateKeyPath
}

func validConfig(privateKeyPath string) string {
	return `
github:
  app:
    app_id: 12345
    client_id: Iv1.example
    private_key_file: ` + privateKeyPath + `
log:
  format: json
prune: true
scale_sets:
` + validScaleSet("docker-set")
}

func baseConfig(privateKeyPath string, scaleSets string) string {
	return "github:\n  app:\n    app_id: 12345\n    client_id: Iv1.example\n    private_key_file: " + privateKeyPath + "\nscale_sets:\n" + scaleSets
}

func validScaleSet(name string) string {
	return "  - name: " + name + "\n" +
		"    max_runners: 1\n" +
		"    docker:\n" +
		"      compose_file: compose.yaml\n" +
		"      service: runner\n"
}
