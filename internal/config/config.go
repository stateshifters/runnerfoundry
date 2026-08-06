package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/meza/RunnerFoundry/internal/postgresconfig"
	"gopkg.in/yaml.v3"
)

const (
	defaultDiscoveryInterval = "5m"
	defaultLogFileMaxSizeMB  = 100
	defaultLogFileMaxBackups = 5
)

type Config struct {
	GitHub      GitHubConfig       `yaml:"github"`
	Log         LogConfig          `yaml:"log"`
	State       StateConfig        `yaml:"state"`
	Prune       bool               `yaml:"prune"`
	RunnerPools []RunnerPoolConfig `yaml:"scale_sets"`
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

type ReadFile func(string) ([]byte, error)

type LogConfig struct {
	Format string         `yaml:"format"`
	Level  string         `yaml:"level"`
	File   *LogFileConfig `yaml:"file"`
}

type LogFileConfig struct {
	Path       string `yaml:"path"`
	MaxSizeMB  *int   `yaml:"max_size_mb"`
	MaxBackups *int   `yaml:"max_backups"`
}

type RunnerPoolConfig struct {
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
// reload can validate a replacement before stopping active Runner Pools.
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
// AppCredentials reads the configured private key file.
func (githubConfig GitHubConfig) AppCredentials(readFile ReadFile) (GitHubAppCredentials, error) {
	privateKey := strings.TrimSpace(githubConfig.App.PrivateKey)
	if privateKey == "" {
		privateKeyBytes, err := readFile(githubConfig.App.PrivateKeyFile)
		if err != nil {
			return GitHubAppCredentials{}, fmt.Errorf("read github.app.private_key_file: %w", err)
		}
		privateKey = strings.TrimSpace(string(privateKeyBytes))
	}
	if privateKey == "" {
		return GitHubAppCredentials{}, errors.New("github.app.private_key_file is empty")
	}
	return GitHubAppCredentials{AppID: githubConfig.App.AppID, PrivateKey: privateKey}, nil
}
