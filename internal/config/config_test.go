package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestLoadAcceptsGitHubAppSettings(t *testing.T) {
	loadedConfig := loadValidConfig(t)
	if loadedConfig.GitHub.App.AppID != 12345 || loadedConfig.Log.Level != "debug" {
		t.Fatalf("loaded config = %#v", loadedConfig)
	}
}

func TestLoadAppliesGitHubDefaults(t *testing.T) {
	loadedConfig := loadValidConfig(t)
	if loadedConfig.GitHub.DiscoveryInterval != "5m" || loadedConfig.ScaleSets[0].RunnerGroup != "default" {
		t.Fatalf("loaded defaults = %#v", loadedConfig)
	}
}

func TestLoadedGitHubConfigurationResolvesAppCredentials(t *testing.T) {
	loadedConfig := loadValidConfig(t)
	credentials, err := loadedConfig.GitHub.AppCredentials()
	if err != nil {
		t.Fatalf("resolve app credentials: %v", err)
	}
	if credentials.AppID != 12345 || credentials.PrivateKey != "private-key" {
		t.Fatalf("app credentials = %#v", credentials)
	}
}

func TestLoadedGitHubConfigurationResolvesDiscoveryDuration(t *testing.T) {
	discoveryDuration, err := loadValidConfig(t).GitHub.DiscoveryDuration()
	if err != nil || discoveryDuration != 5*time.Minute {
		t.Fatalf("discovery duration = %s, %v", discoveryDuration, err)
	}
}

func TestDiscoveryDurationRejectsInvalidUnvalidatedValue(t *testing.T) {
	if _, err := (GitHubConfig{DiscoveryInterval: "invalid"}).DiscoveryDuration(); err == nil {
		t.Fatal("expected invalid discovery duration error")
	}
}

func loadValidConfig(t *testing.T) Config {
	t.Helper()
	privateKeyPath := writePrivateKey(t, "private-key")
	loadedConfig, err := Load(strings.NewReader(validConfig(privateKeyPath)))
	if err != nil {
		t.Fatalf("load valid config: %v", err)
	}
	return loadedConfig
}

func TestLoadPostgreSQLStateIgnoresUnrelatedDaemonConfiguration(t *testing.T) {
	postgresConfig, err := LoadPostgreSQLState(strings.NewReader(`github: invalid
state:
  postgres:
    role: github_scaler
    host: database.internal
scale_sets: invalid
`))
	if err != nil {
		t.Fatalf("load PostgreSQL state: %v", err)
	}
	if postgresConfig.Role != "github_scaler" || postgresConfig.Host != "database.internal" {
		t.Fatalf("PostgreSQL state = %#v", postgresConfig)
	}
}

func TestLoadPostgreSQLStateRejectsInvalidPostgreSQLConfiguration(t *testing.T) {
	tests := map[string]string{
		"empty document":      "",
		"malformed document":  "state: [",
		"missing state":       "github: invalid\n",
		"missing postgres":    "state: {}\n",
		"unknown primary key": "state:\n  postgres:\n    role: github_scaler\n    unknown: value\n",
		"unknown direct key":  "state:\n  postgres:\n    role: github_scaler\n    direct:\n      unknown: value\n",
		"missing role":        "state:\n  postgres: {}\n",
		"conflicting settings": "state:\n  postgres:\n    role: github_scaler\n    dsn: postgres://database\n" +
			"    host: database.internal\n",
	}
	for name, configText := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadPostgreSQLState(strings.NewReader(configText)); err == nil {
				t.Fatal("expected invalid PostgreSQL configuration to fail")
			}
		})
	}
}

func TestLoadDefaultsLogLevelToInfo(t *testing.T) {
	privateKeyPath := writePrivateKey(t, "private-key")
	configWithoutLevel := strings.Replace(validConfig(privateKeyPath), "  level: debug\n", "", 1)
	loadedConfig, err := Load(strings.NewReader(configWithoutLevel))
	if err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
	if loadedConfig.Log.Level != "info" {
		t.Fatalf("expected default info log level, got %q", loadedConfig.Log.Level)
	}
}

func TestLoadAcceptsPostgreSQLClusterStateSources(t *testing.T) {
	privateKeyPath := writePrivateKey(t, "private-key")
	tests := map[string]string{
		"standard environment": "\n    role: github_scaler\n",
		"direct DSN":           "\n    role: github_scaler\n    dsn: postgres://scaler@database/scaler\n",
		"referenced DSN":       "\n    role: env://POSTGRES_ROLE\n    dsn: env://DATABASE_URL\n",
		"structured settings":  "\n    role: file:///run/secrets/postgres-role\n    host: postgresql.internal\n    port: file:///run/secrets/postgres-port\n    name: github_scaler\n    user: env://POSTGRES_USER\n    password: file:///run/secrets/postgres-password\n    direct:\n      host: postgresql-direct.internal\n      port: 5432\n",
	}
	for testName, connectionSource := range tests {
		t.Run(testName, func(t *testing.T) {
			configText := strings.Replace(validConfig(privateKeyPath), "state:\n  path: state.db\n",
				"state:\n  postgres:"+connectionSource, 1)
			loadedConfig, err := Load(strings.NewReader(configText))
			if err != nil {
				t.Fatalf("expected PostgreSQL cluster config, got %v", err)
			}
			if loadedConfig.State.Postgres == nil {
				t.Fatalf("unexpected PostgreSQL state config %#v", loadedConfig.State.Postgres)
			}
		})
	}
}

func TestLoadRejectsConfiguredPostgreSQLNodeID(t *testing.T) {
	privateKeyPath := writePrivateKey(t, "private-key")
	configText := strings.Replace(validConfig(privateKeyPath), "state:\n  path: state.db\n",
		"state:\n  postgres:\n    node_id: spock\n", 1)

	_, err := Load(strings.NewReader(configText))
	if err == nil || !strings.Contains(err.Error(), "field node_id not found") {
		t.Fatalf("configured PostgreSQL node ID error = %v", err)
	}
}

func TestLoadRejectsAmbiguousPostgreSQLClusterState(t *testing.T) {
	privateKeyPath := writePrivateKey(t, "private-key")
	tests := map[string]struct {
		state         string
		expectedError string
	}{
		"SQLite and PostgreSQL": {
			state:         "  path: state.db\n  postgres: {}\n",
			expectedError: "state.path and state.postgres are mutually exclusive",
		},
		"DSN and structured settings": {
			state:         "  postgres:\n    role: github_scaler\n    dsn: postgres://database/scaler\n    host: database\n",
			expectedError: "state.postgres.dsn cannot be combined with structured PostgreSQL settings",
		},
		"missing role": {
			state:         "  postgres: {}\n",
			expectedError: "state.postgres.role is required",
		},
		"direct DSN and structured settings": {
			state:         "  postgres:\n    role: github_scaler\n    direct:\n      dsn: postgres://database/scaler\n      host: database\n",
			expectedError: "state.postgres.direct.dsn cannot be combined with structured PostgreSQL settings",
		},
		"opaque primary DSN with partial direct override": {
			state:         "  postgres:\n    role: github_scaler\n    dsn: postgres://pool/scaler\n    direct:\n      host: database\n",
			expectedError: "state.postgres.direct.dsn is required when overriding a primary DSN",
		},
	}
	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			configText := strings.Replace(validConfig(privateKeyPath), "state:\n  path: state.db\n", "state:\n"+testCase.state, 1)
			_, err := Load(strings.NewReader(configText))
			if err == nil || !strings.Contains(err.Error(), testCase.expectedError) {
				t.Fatalf("Load() error = %v, want an error containing %q", err, testCase.expectedError)
			}
		})
	}
}

func TestLoadRejectsLegacyPostgreSQLCredentialKeys(t *testing.T) {
	privateKeyPath := writePrivateKey(t, "private-key")
	for _, legacyKey := range []string{"dsn_env", "dsn_file"} {
		t.Run(legacyKey, func(t *testing.T) {
			configText := strings.Replace(validConfig(privateKeyPath), "state:\n  path: state.db\n",
				"state:\n  postgres:\n    "+legacyKey+": legacy\n", 1)
			_, err := Load(strings.NewReader(configText))
			if err == nil || !strings.Contains(err.Error(), "field "+legacyKey+" not found") {
				t.Fatalf("legacy PostgreSQL key error = %v", err)
			}
		})
	}
}

func TestLoadPostgreSQLStateRejectsEmptyDocument(t *testing.T) {
	if _, err := LoadPostgreSQLState(strings.NewReader("")); err == nil {
		t.Fatal("empty migration configuration was accepted")
	}
}

func TestDecodePostgreSQLStateNodeRejectsInvalidYAMLNode(t *testing.T) {
	if _, err := decodePostgreSQLStateNode(&yaml.Node{Kind: yaml.Kind(99)}); err == nil {
		t.Fatal("invalid PostgreSQL YAML node was accepted")
	}
}

func TestLoadRejectsInvalidGitHubAppConfiguration(t *testing.T) {
	privateKeyPath := writePrivateKey(t, "private-key")
	tests := map[string]string{
		"bad yaml":               "github: [",
		"legacy token env":       strings.Replace(validConfig(privateKeyPath), "  app:\n", "  token_env: TOKEN\n  app:\n", 1),
		"legacy token file":      strings.Replace(validConfig(privateKeyPath), "  app:\n", "  token_file: token\n  app:\n", 1),
		"legacy plaintext token": strings.Replace(validConfig(privateKeyPath), "  app:\n", "  token: secret\n  app:\n", 1),
		"plaintext private key":  strings.Replace(validConfig(privateKeyPath), "    private_key_file: "+privateKeyPath, "    private_key: secret", 1),
		"unknown field":          "unknown: true\n",
	}
	assertLoadRejectsConfigurations(t, tests)
}

func TestLoadRejectsInvalidGitHubAppAndLoggingValues(t *testing.T) {
	privateKeyPath := writePrivateKey(t, "private-key")
	tests := map[string]string{
		"missing app ID":             strings.Replace(validConfig(privateKeyPath), "    app_id: 12345\n", "", 1),
		"invalid app ID":             strings.Replace(validConfig(privateKeyPath), "    app_id: 12345", "    app_id: -1", 1),
		"missing private key file":   strings.Replace(validConfig(privateKeyPath), "    private_key_file: "+privateKeyPath+"\n", "", 1),
		"invalid discovery interval": strings.Replace(validConfig(privateKeyPath), "github:\n", "github:\n  discovery_interval: soon\n", 1),
		"short discovery interval":   strings.Replace(validConfig(privateKeyPath), "github:\n", "github:\n  discovery_interval: 59s\n", 1),
		"bad log format":             strings.Replace(validConfig(privateKeyPath), "log:\n  format: json", "log:\n  format: xml", 1),
		"bad log level":              strings.Replace(validConfig(privateKeyPath), "  level: debug", "  level: verbose", 1),
		"missing state path":         strings.Replace(validConfig(privateKeyPath), "state:\n  path: state.db\n", "", 1),
	}
	assertLoadRejectsConfigurations(t, tests)
}

func TestLoadRejectsInvalidScaleSetIdentityAndCapacity(t *testing.T) {
	privateKeyPath := writePrivateKey(t, "private-key")
	tests := map[string]string{
		"missing scale sets":       strings.Split(validConfig(privateKeyPath), "scale_sets:")[0],
		"missing scale set name":   baseConfig(privateKeyPath, "  - max_runners: 1\n    docker:\n      compose_file: compose.yaml\n      service: runner\n"),
		"duplicate scale set name": baseConfig(privateKeyPath, validScaleSet("repeated")+validScaleSet("repeated")),
		"zero max runners":         baseConfig(privateKeyPath, strings.Replace(validScaleSet("bad"), "max_runners: 1", "max_runners: 0", 1)),
		"negative max runners":     baseConfig(privateKeyPath, strings.Replace(validScaleSet("bad"), "max_runners: 1", "max_runners: -1", 1)),
	}
	assertLoadRejectsConfigurations(t, tests)
}

func TestLoadRejectsInvalidScaleSetProviderConfiguration(t *testing.T) {
	privateKeyPath := writePrivateKey(t, "private-key")
	tests := map[string]string{
		"missing provider config": baseConfig(privateKeyPath, "  - name: bad\n    max_runners: 1\n"),
		"multiple providers": baseConfig(privateKeyPath, validScaleSet("bad")+
			"    libvirt:\n      connection_uri: qemu:///system\n      domain_xml_template: domain.xml\n"),
		"missing docker compose file": baseConfig(privateKeyPath, "  - name: bad\n    max_runners: 1\n    docker:\n      service: runner\n"),
		"missing docker service":      baseConfig(privateKeyPath, "  - name: bad\n    max_runners: 1\n    docker:\n      compose_file: compose.yaml\n"),
		"missing libvirt connection":  baseConfig(privateKeyPath, "  - name: bad\n    max_runners: 1\n    libvirt:\n      domain_xml_template: domain.xml\n"),
		"missing libvirt template":    baseConfig(privateKeyPath, "  - name: bad\n    max_runners: 1\n    libvirt:\n      connection_uri: qemu:///system\n"),
		"repositories are obsolete":   baseConfig(privateKeyPath, strings.Replace(validScaleSet("bad"), "    max_runners", "    repositories:\n      - https://github.com/meza/repo\n    max_runners", 1)),
	}
	assertLoadRejectsConfigurations(t, tests)
}

func assertLoadRejectsConfigurations(t *testing.T, configurations map[string]string) {
	t.Helper()
	for testName, configText := range configurations {
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
    private_key_file: ` + privateKeyPath + `
log:
  format: json
  level: debug
prune: true
state:
  path: state.db
scale_sets:
` + validScaleSet("docker-set")
}

func baseConfig(privateKeyPath string, scaleSets string) string {
	return "github:\n  app:\n    app_id: 12345\n    private_key_file: " + privateKeyPath + "\nstate:\n  path: state.db\nscale_sets:\n" + scaleSets
}

func validScaleSet(name string) string {
	return "  - name: " + name + "\n" +
		"    max_runners: 1\n" +
		"    docker:\n" +
		"      compose_file: compose.yaml\n" +
		"      service: runner\n"
}
