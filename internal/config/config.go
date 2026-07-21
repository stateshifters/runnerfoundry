package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/meza/github-scaler/internal/postgresconfig"
	"gopkg.in/yaml.v3"
)

const defaultDiscoveryInterval = "5m"

type Config struct {
	GitHub    GitHubConfig     `yaml:"github"`
	Log       LogConfig        `yaml:"log"`
	State     StateConfig      `yaml:"state"`
	Prune     bool             `yaml:"prune"`
	ScaleSets []ScaleSetConfig `yaml:"scale_sets"`
}

type StateConfig struct {
	Path     string                   `yaml:"path"`
	Postgres *postgresconfig.Settings `yaml:"postgres"`
}

type GitHubConfig struct {
	App               GitHubAppConfig `yaml:"app"`
	DiscoveryInterval string          `yaml:"discovery_interval"`
	TokenEnv          string          `yaml:"token_env"`
	TokenFile         string          `yaml:"token_file"`
	Token             string          `yaml:"token"`
}

type GitHubAppConfig struct {
	AppID          int64  `yaml:"app_id"`
	PrivateKeyFile string `yaml:"private_key_file"`
	PrivateKey     string `yaml:"private_key"`
}

type GitHubAppCredentials struct {
	AppID      int64
	PrivateKey string
}

type LogConfig struct {
	Format string `yaml:"format"`
	Level  string `yaml:"level"`
}

type ScaleSetConfig struct {
	Name        string         `yaml:"name"`
	RunnerGroup string         `yaml:"runner_group"`
	MaxRunners  int            `yaml:"max_runners"`
	Docker      *DockerConfig  `yaml:"docker"`
	Libvirt     *LibvirtConfig `yaml:"libvirt"`
}

type DockerConfig struct {
	ComposeFile string `yaml:"compose_file"`
	Service     string `yaml:"service"`
}

type LibvirtConfig struct {
	ConnectionURI     string `yaml:"connection_uri"`
	DomainXMLTemplate string `yaml:"domain_xml_template"`
}

// Load decodes and validates one daemon configuration document.
//
// The returned Config has defaults applied for logging, discovery, and runner
// groups. GitHub App secrets are checked when AppCredentials reads the key so
// reload can validate a replacement before stopping active scale sets.
func Load(reader io.Reader) (Config, error) {
	var loadedConfig Config
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)
	if err := decoder.Decode(&loadedConfig); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if err := loadedConfig.Validate(); err != nil {
		return Config{}, err
	}
	return loadedConfig, nil
}

// LoadPostgreSQLState decodes only the clustered state settings needed by the
// migration commands. Other daemon settings are deliberately ignored, while
// the PostgreSQL subtree retains the same strict keys and validation as Load.
func LoadPostgreSQLState(reader io.Reader) (postgresconfig.Settings, error) {
	var document yaml.Node
	if err := yaml.NewDecoder(reader).Decode(&document); err != nil {
		return postgresconfig.Settings{}, fmt.Errorf("decode migration config: %w", err)
	}
	stateNode := yamlMappingValue(document.Content[0], "state")
	postgresNode := yamlMappingValue(stateNode, "postgres")
	if postgresNode == nil {
		return postgresconfig.Settings{}, errors.New("PostgreSQL clustered state is required")
	}
	return decodePostgreSQLStateNode(postgresNode)
}

func decodePostgreSQLStateNode(postgresNode *yaml.Node) (postgresconfig.Settings, error) {
	postgresContents, err := yaml.Marshal(postgresNode)
	if err != nil {
		return postgresconfig.Settings{}, fmt.Errorf("read PostgreSQL migration config: %w", err)
	}
	var postgresConfig postgresconfig.Settings
	decoder := yaml.NewDecoder(bytes.NewReader(postgresContents))
	decoder.KnownFields(true)
	if err := decoder.Decode(&postgresConfig); err != nil {
		return postgresconfig.Settings{}, fmt.Errorf("decode PostgreSQL migration config: %w", err)
	}
	if err := postgresconfig.Validate(postgresConfig); err != nil {
		return postgresconfig.Settings{}, fmt.Errorf("validate PostgreSQL migration config: %w", err)
	}
	return postgresConfig, nil
}

func yamlMappingValue(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index+1]
		}
	}
	return nil
}

// Validate applies defaults and rejects unsafe startup state.
func (loadedConfig *Config) Validate() error {
	if err := loadedConfig.GitHub.validate(); err != nil {
		return err
	}
	if loadedConfig.Log.Format == "" {
		loadedConfig.Log.Format = "text"
	}
	if loadedConfig.Log.Format != "text" && loadedConfig.Log.Format != "json" {
		return fmt.Errorf("unsupported log.format %q", loadedConfig.Log.Format)
	}
	if loadedConfig.Log.Level == "" {
		loadedConfig.Log.Level = "info"
	}
	switch loadedConfig.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("unsupported log.level %q", loadedConfig.Log.Level)
	}
	if err := loadedConfig.State.validate(); err != nil {
		return err
	}
	return loadedConfig.validateScaleSets()
}

func (stateConfig StateConfig) validate() error {
	hasSQLite := strings.TrimSpace(stateConfig.Path) != ""
	hasPostgreSQL := stateConfig.Postgres != nil
	if hasSQLite && hasPostgreSQL {
		return errors.New("state.path and state.postgres are mutually exclusive")
	}
	if !hasSQLite && !hasPostgreSQL {
		return errors.New("state.path is required when state.postgres is not configured")
	}
	if !hasPostgreSQL {
		return nil
	}
	if err := postgresconfig.Validate(*stateConfig.Postgres); err != nil {
		return fmt.Errorf("validate PostgreSQL state: %w", err)
	}
	return nil
}

func (githubConfig *GitHubConfig) validate() error {
	if githubConfig.TokenEnv != "" || githubConfig.TokenFile != "" || githubConfig.Token != "" {
		return errors.New("PAT authentication is not supported; configure github.app")
	}
	if githubConfig.App.AppID <= 0 {
		return errors.New("github.app.app_id must be positive")
	}
	if githubConfig.App.PrivateKey != "" {
		return errors.New("github.app.private_key is not allowed; use private_key_file")
	}
	if githubConfig.App.PrivateKeyFile == "" {
		return errors.New("github.app.private_key_file is required")
	}
	if githubConfig.DiscoveryInterval == "" {
		githubConfig.DiscoveryInterval = defaultDiscoveryInterval
	}
	discoveryInterval, err := time.ParseDuration(githubConfig.DiscoveryInterval)
	if err != nil {
		return fmt.Errorf("github.discovery_interval: %w", err)
	}
	if discoveryInterval < time.Minute {
		return errors.New("github.discovery_interval must be at least 1m")
	}
	return nil
}

func (loadedConfig *Config) validateScaleSets() error {
	if len(loadedConfig.ScaleSets) == 0 {
		return errors.New("at least one scale set is required")
	}
	seenNames := map[string]bool{}
	for scaleSetIndex := range loadedConfig.ScaleSets {
		if err := loadedConfig.ScaleSets[scaleSetIndex].validate(seenNames); err != nil {
			return err
		}
	}
	return nil
}

func (scaleSet *ScaleSetConfig) validate(seenNames map[string]bool) error {
	if scaleSet.Name == "" {
		return errors.New("scale_sets[].name is required")
	}
	if seenNames[scaleSet.Name] {
		return fmt.Errorf("duplicate scale set name %q", scaleSet.Name)
	}
	seenNames[scaleSet.Name] = true
	if scaleSet.RunnerGroup == "" {
		scaleSet.RunnerGroup = "default"
	}
	if scaleSet.MaxRunners <= 0 {
		return fmt.Errorf("scale set %q max_runners must be positive", scaleSet.Name)
	}
	if err := scaleSet.validateProvider(); err != nil {
		return fmt.Errorf("scale set %q: %w", scaleSet.Name, err)
	}
	return nil
}

func (scaleSet *ScaleSetConfig) validateProvider() error {
	if (scaleSet.Docker == nil) == (scaleSet.Libvirt == nil) {
		return errors.New("exactly one of docker or libvirt is required")
	}
	if scaleSet.Docker != nil {
		if scaleSet.Docker.ComposeFile == "" {
			return errors.New("docker.compose_file is required")
		}
		if scaleSet.Docker.Service == "" {
			return errors.New("docker.service is required")
		}
		return nil
	}
	if scaleSet.Libvirt.ConnectionURI == "" {
		return errors.New("libvirt.connection_uri is required")
	}
	if scaleSet.Libvirt.DomainXMLTemplate == "" {
		return errors.New("libvirt.domain_xml_template is required")
	}
	return nil
}

// AppCredentials reads the configured private key file.
func (githubConfig GitHubConfig) AppCredentials() (GitHubAppCredentials, error) {
	privateKeyBytes, err := os.ReadFile(githubConfig.App.PrivateKeyFile)
	if err != nil {
		return GitHubAppCredentials{}, fmt.Errorf("read github.app.private_key_file: %w", err)
	}
	privateKey := strings.TrimSpace(string(privateKeyBytes))
	if privateKey == "" {
		return GitHubAppCredentials{}, errors.New("github.app.private_key_file is empty")
	}
	return GitHubAppCredentials{AppID: githubConfig.App.AppID, PrivateKey: privateKey}, nil
}

// DiscoveryDuration returns the validated repository discovery interval.
func (githubConfig GitHubConfig) DiscoveryDuration() (time.Duration, error) {
	discoveryInterval, err := time.ParseDuration(githubConfig.DiscoveryInterval)
	if err != nil {
		return 0, fmt.Errorf("parse validated discovery interval: %w", err)
	}
	return discoveryInterval, nil
}
