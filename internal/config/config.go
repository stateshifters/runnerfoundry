package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const defaultDiscoveryInterval = "5m"

type Config struct {
	GitHub    GitHubConfig     `yaml:"github"`
	Log       LogConfig        `yaml:"log"`
	Prune     bool             `yaml:"prune"`
	ScaleSets []ScaleSetConfig `yaml:"scale_sets"`
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
	return loadedConfig.validateScaleSets()
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
