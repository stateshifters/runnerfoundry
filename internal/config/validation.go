package config

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/postgresconfig"
)

// ProviderKind identifies the single execution backend configured for a
// Runner Pool.
type ProviderKind string

const (
	ProviderKindDocker  ProviderKind = "docker"
	ProviderKindLibvirt ProviderKind = "libvirt"
	ProviderKindOrchard ProviderKind = "orchard"
)

func (runnerPool RunnerPoolConfig) ProviderKind() (ProviderKind, error) {
	kinds := make([]ProviderKind, 0, 3)
	if runnerPool.Docker != nil {
		kinds = append(kinds, ProviderKindDocker)
	}
	if runnerPool.Libvirt != nil {
		kinds = append(kinds, ProviderKindLibvirt)
	}
	if runnerPool.Orchard != nil {
		kinds = append(kinds, ProviderKindOrchard)
	}
	if len(kinds) != 1 {
		return "", errors.New("exactly one of docker, libvirt or orchard is required")
	}
	return kinds[0], nil
}

// Validate mutates Config only to apply documented defaults, then rejects
// unsupported authentication, invalid backend combinations, unsafe log paths,
// and incomplete Runner Pools. It validates credential paths but does not read
// credential contents; AppCredentials owns that boundary.
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
	return loadedConfig.validateRunnerPools()
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
		return errors.New("PAT authentication is not supported; configure github.apps")
	}
	if err := validateGitHubApps(githubConfig.Apps); err != nil {
		return err
	}
	return githubConfig.validateDiscoveryInterval()
}

func validateGitHubApps(apps []GitHubAppConfig) error {
	if len(apps) == 0 {
		return errors.New("github.apps must contain at least one App")
	}
	seenAppIDs := make(map[int64]struct{}, len(apps))
	for index, app := range apps {
		if app.AppID <= 0 {
			return fmt.Errorf("github.apps[%d].app_id must be positive", index)
		}
		if _, duplicate := seenAppIDs[app.AppID]; duplicate {
			return fmt.Errorf("github.apps[%d].app_id %d is duplicated", index, app.AppID)
		}
		seenAppIDs[app.AppID] = struct{}{}
		if app.PrivateKey != "" {
			return fmt.Errorf("github.apps[%d].private_key is not allowed; use private_key_file", index)
		}
		if app.PrivateKeyFile == "" {
			return fmt.Errorf("github.apps[%d].private_key_file is required", index)
		}
	}
	return nil
}

func (githubConfig *GitHubConfig) validateDiscoveryInterval() error {
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

func (loadedConfig *Config) validateRunnerPools() error {
	if len(loadedConfig.RunnerPools) == 0 {
		return errors.New("at least one Runner Pool is required")
	}
	seenNames := map[string]bool{}
	for runnerPoolIndex := range loadedConfig.RunnerPools {
		if err := loadedConfig.RunnerPools[runnerPoolIndex].validate(seenNames); err != nil {
			return err
		}
	}
	return nil
}

func (runnerPool *RunnerPoolConfig) validate(seenNames map[string]bool) error {
	if runnerPool.Name == "" {
		return errors.New("scale_sets[].name is required")
	}
	if seenNames[runnerPool.Name] {
		return fmt.Errorf("duplicate Runner Pool name %q", runnerPool.Name)
	}
	seenNames[runnerPool.Name] = true
	if runnerPool.RunnerGroup == "" {
		runnerPool.RunnerGroup = "default"
	}
	if runnerPool.MaxRunners <= 0 {
		return fmt.Errorf("runner pool %q max_runners must be positive", runnerPool.Name)
	}
	if _, err := runnerPool.StopIfNotStartedDuration(); err != nil {
		return err
	}
	if err := runnerPool.validateProvider(); err != nil {
		return fmt.Errorf("runner pool %q: %w", runnerPool.Name, err)
	}
	return nil
}

func (runnerPool *RunnerPoolConfig) validateProvider() error {
	kind, err := runnerPool.ProviderKind()
	if err != nil {
		return err
	}
	var validationError error
	switch kind {
	case ProviderKindDocker:
		validationError = runnerPool.Docker.validate()
	case ProviderKindLibvirt:
		validationError = runnerPool.Libvirt.validate()
	case ProviderKindOrchard:
		validationError = runnerPool.Orchard.validate()
	}
	return validationError
}

func (configured DockerConfig) validate() error {
	if configured.ComposeFile == "" {
		return errors.New("docker.compose_file is required")
	}
	if configured.Service == "" {
		return errors.New("docker.service is required")
	}
	return nil
}

func (configured LibvirtConfig) validate() error {
	if configured.ConnectionURI == "" {
		return errors.New("libvirt.connection_uri is required")
	}
	if configured.DomainXMLTemplate == "" {
		return errors.New("libvirt.domain_xml_template is required")
	}
	return nil
}

// DiscoveryDuration parses the configured discovery interval. Callers normally
// invoke it only after Validate; malformed unvalidated values return an error.
func (githubConfig GitHubConfig) DiscoveryDuration() (time.Duration, error) {
	discoveryInterval, err := time.ParseDuration(githubConfig.DiscoveryInterval)
	if err != nil {
		return 0, fmt.Errorf("parse validated discovery interval: %w", err)
	}
	return discoveryInterval, nil
}

func (runnerPool RunnerPoolConfig) StopIfNotStartedDuration() (time.Duration, error) {
	configured := runnerPool.StopIfNotStartedWithin
	if configured == "" {
		configured = "5m"
	}
	duration, err := time.ParseDuration(configured)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("runner pool %q stop_if_not_started_within must be a positive duration", runnerPool.Name)
	}
	return duration, nil
}
