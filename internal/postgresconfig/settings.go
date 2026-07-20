// Package postgresconfig owns PostgreSQL connection configuration and point-of-use resolution.
package postgresconfig

import (
	"errors"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Settings describes primary and optional session-affine PostgreSQL endpoints.
// Each non-empty value may be literal, env://NAME, or file:///path. Primary and
// Direct resolve references on every call so callers never cache credential values.
type Settings struct {
	DSN      string    `yaml:"dsn"`
	Host     string    `yaml:"host"`
	Port     string    `yaml:"port"`
	Name     string    `yaml:"name"`
	User     string    `yaml:"user"`
	Password string    `yaml:"password"`
	Role     string    `yaml:"role"`
	Direct   *Endpoint `yaml:"direct"`
}

// Endpoint contains optional overrides for a session-affine PostgreSQL endpoint.
type Endpoint struct {
	DSN      string `yaml:"dsn"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Name     string `yaml:"name"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// Validate rejects ambiguous endpoint shapes without resolving credential references.
func Validate(settings Settings) error {
	if strings.TrimSpace(settings.Role) == "" {
		return errors.New("state.postgres.role is required")
	}
	if strings.TrimSpace(settings.DSN) != "" && settings.hasStructuredValues() {
		return errors.New("state.postgres.dsn cannot be combined with structured PostgreSQL settings")
	}
	direct := Endpoint{}
	if settings.Direct != nil {
		direct = *settings.Direct
	}
	directHasDSN := strings.TrimSpace(direct.DSN) != ""
	directHasStructuredValues := direct.hasStructuredValues()
	if directHasDSN && directHasStructuredValues {
		return errors.New("state.postgres.direct.dsn cannot be combined with structured PostgreSQL settings")
	}
	if strings.TrimSpace(settings.DSN) != "" && directHasStructuredValues {
		return errors.New("state.postgres.direct.dsn is required when overriding a primary DSN")
	}
	return nil
}

// Primary resolves the configured primary endpoint and forces the stable role.
func (settings Settings) Primary() (*pgx.ConnConfig, error) {
	if settings.Role == "" {
		return nil, errors.New("PostgreSQL role is required")
	}
	resolvedRole, err := resolveSetting(settings.Role)
	if err != nil {
		return nil, err
	}
	config, err := settings.configWithoutRole()
	if err != nil {
		return nil, err
	}
	config.RuntimeParams["role"] = resolvedRole
	return config, nil
}

// Direct resolves the session-affine endpoint, inheriting structured primary values.
func (settings Settings) DirectConfig() (*pgx.ConnConfig, error) {
	if settings.Direct == nil {
		return settings.Primary()
	}
	direct := Settings{
		DSN: settings.Direct.DSN, Host: settings.Direct.Host, Port: settings.Direct.Port,
		Name: settings.Direct.Name, User: settings.Direct.User, Password: settings.Direct.Password,
		Role: settings.Role,
	}
	if settings.DSN != "" {
		if direct.DSN == "" && direct.hasStructuredValues() {
			return nil, errors.New("direct PostgreSQL DSN is required when overriding a primary DSN")
		}
		if direct.DSN == "" {
			return settings.Primary()
		}
		return direct.Primary()
	}
	if direct.DSN != "" {
		return direct.Primary()
	}
	direct.Host = inheritSetting(direct.Host, settings.Host)
	direct.Port = inheritSetting(direct.Port, settings.Port)
	direct.Name = inheritSetting(direct.Name, settings.Name)
	direct.User = inheritSetting(direct.User, settings.User)
	direct.Password = inheritSetting(direct.Password, settings.Password)
	return direct.Primary()
}

func (settings Settings) configWithoutRole() (*pgx.ConnConfig, error) {
	if settings.DSN != "" && settings.hasStructuredValues() {
		return nil, errors.New("PostgreSQL DSN cannot be combined with structured settings")
	}
	if settings.DSN != "" {
		return settings.dsnConfig()
	}
	return settings.structuredConfig()
}

func (settings Settings) dsnConfig() (*pgx.ConnConfig, error) {
	resolvedDSN, err := resolveSetting(settings.DSN)
	if err != nil {
		return nil, err
	}
	return parseConnectionConfig(resolvedDSN)
}

func (settings Settings) structuredConfig() (*pgx.ConnConfig, error) {
	values := url.Values{}
	for key, configuredValue := range map[string]string{
		"host": settings.Host, "port": settings.Port, "dbname": settings.Name,
		"user": settings.User, "password": settings.Password,
	} {
		resolvedValue, err := resolveStructuredSetting(key, configuredValue)
		if err != nil {
			return nil, err
		}
		if configuredValue != "" {
			values.Set(key, resolvedValue)
		}
	}
	connectionString := ""
	if len(values) != 0 {
		connectionString = "postgresql://?" + values.Encode()
	}
	return parseConnectionConfig(connectionString)
}

func (settings Settings) hasStructuredValues() bool {
	return settings.Host != "" || settings.Port != "" || settings.Name != "" || settings.User != "" || settings.Password != ""
}

func (endpoint Endpoint) hasStructuredValues() bool {
	return Settings{Host: endpoint.Host, Port: endpoint.Port, Name: endpoint.Name, User: endpoint.User, Password: endpoint.Password}.hasStructuredValues()
}

func inheritSetting(value string, inherited string) string {
	if value != "" {
		return value
	}
	return inherited
}

func resolveStructuredSetting(key string, configuredValue string) (string, error) {
	if configuredValue == "" {
		return "", nil
	}
	resolvedValue, err := resolveSetting(configuredValue)
	if err != nil {
		return "", err
	}
	if key == "port" {
		port, portErr := strconv.ParseUint(resolvedValue, 10, 16)
		if portErr != nil || port == 0 {
			return "", errors.New("invalid PostgreSQL port")
		}
	}
	return resolvedValue, nil
}

func parseConnectionConfig(connectionString string) (*pgx.ConnConfig, error) {
	config, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, errors.New("parse PostgreSQL connection settings")
	}
	return config, nil
}
