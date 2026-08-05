package config

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/meza/RunnerFoundry/internal/postgresconfig"
)

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
	if err := loadedConfig.Log.validateFile(); err != nil {
		return err
	}
	if err := loadedConfig.State.validate(); err != nil {
		return err
	}
	return loadedConfig.validateScaleSets()
}

func (logConfig *LogConfig) validateFile() error {
	if logConfig.File == nil {
		return nil
	}
	if !filepath.IsAbs(logConfig.File.Path) {
		return errors.New("log.file.path must be absolute")
	}
	if logConfig.File.MaxSizeMB == nil {
		logConfig.File.MaxSizeMB = intPointer(defaultLogFileMaxSizeMB)
	}
	if *logConfig.File.MaxSizeMB <= 0 {
		return errors.New("log.file.max_size_mb must be positive")
	}
	if int64(*logConfig.File.MaxSizeMB) > math.MaxInt64/(1024*1024) {
		return errors.New("log.file.max_size_mb is too large")
	}
	if logConfig.File.MaxBackups == nil {
		logConfig.File.MaxBackups = intPointer(defaultLogFileMaxBackups)
	}
	if *logConfig.File.MaxBackups <= 0 {
		return errors.New("log.file.max_backups must be positive")
	}
	return nil
}

func intPointer(value int) *int {
	return &value
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

// DiscoveryDuration returns the validated repository discovery interval.
func (githubConfig GitHubConfig) DiscoveryDuration() (time.Duration, error) {
	discoveryInterval, err := time.ParseDuration(githubConfig.DiscoveryInterval)
	if err != nil {
		return 0, fmt.Errorf("parse validated discovery interval: %w", err)
	}
	return discoveryInterval, nil
}
