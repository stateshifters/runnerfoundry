package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	GitHub    GitHubConfig     `yaml:"github"`
	Log       LogConfig        `yaml:"log"`
	ScaleSets []ScaleSetConfig `yaml:"scale_sets"`
}

type GitHubConfig struct {
	URL       string `yaml:"url"`
	TokenEnv  string `yaml:"token_env"`
	TokenFile string `yaml:"token_file"`
	Token     string `yaml:"token"`
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

func (loadedConfig *Config) Validate() error {
	if loadedConfig.GitHub.URL == "" {
		return errors.New("github.url is required")
	}
	if loadedConfig.GitHub.Token != "" {
		return errors.New("github.token is not allowed; use token_env or token_file")
	}
	if (loadedConfig.GitHub.TokenEnv == "") == (loadedConfig.GitHub.TokenFile == "") {
		return errors.New("exactly one of github.token_env or github.token_file is required")
	}
	if loadedConfig.GitHub.TokenEnv != "" {
		if _, ok := os.LookupEnv(loadedConfig.GitHub.TokenEnv); !ok {
			return fmt.Errorf("github.token_env %q is not set", loadedConfig.GitHub.TokenEnv)
		}
	}
	if loadedConfig.Log.Format == "" {
		loadedConfig.Log.Format = "text"
	}
	if loadedConfig.Log.Format != "text" && loadedConfig.Log.Format != "json" {
		return fmt.Errorf("unsupported log.format %q", loadedConfig.Log.Format)
	}
	return loadedConfig.validateScaleSets()
}

func (loadedConfig *Config) validateScaleSets() error {
	if len(loadedConfig.ScaleSets) == 0 {
		return errors.New("at least one scale set is required")
	}
	seenNames := map[string]bool{}
	for scaleSetIndex := range loadedConfig.ScaleSets {
		scaleSet := &loadedConfig.ScaleSets[scaleSetIndex]
		if err := scaleSet.validate(seenNames); err != nil {
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
	if scaleSet.MaxRunners < 0 {
		return fmt.Errorf("scale set %q max_runners must be non-negative", scaleSet.Name)
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

func (loadedConfig Config) Token() (string, error) {
	if loadedConfig.GitHub.TokenEnv != "" {
		token := os.Getenv(loadedConfig.GitHub.TokenEnv)
		if token == "" {
			return "", fmt.Errorf("github.token_env %q is empty", loadedConfig.GitHub.TokenEnv)
		}
		return token, nil
	}
	tokenBytes, err := os.ReadFile(loadedConfig.GitHub.TokenFile)
	if err != nil {
		return "", fmt.Errorf("read github.token_file: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return "", errors.New("github.token_file is empty")
	}
	return token, nil
}
