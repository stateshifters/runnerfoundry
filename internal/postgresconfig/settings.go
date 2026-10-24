// Package postgresconfig defines unresolved PostgreSQL settings and their structural invariants.
package postgresconfig

import (
	"errors"
	"strings"
)

// Settings describes primary and optional session-affine PostgreSQL endpoints.
// Each non-empty value may be literal, env://NAME, or file:///path.
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
	if settings.DSN != "" && settings.hasStructuredValues() {
		return errors.New("state.postgres.dsn cannot be combined with structured PostgreSQL settings")
	}
	direct := Endpoint{}
	if settings.Direct != nil {
		direct = *settings.Direct
	}
	directHasDSN := direct.DSN != ""
	directHasStructuredValues := direct.hasStructuredValues()
	if directHasDSN && directHasStructuredValues {
		return errors.New("state.postgres.direct.dsn cannot be combined with structured PostgreSQL settings")
	}
	if settings.DSN != "" && directHasStructuredValues {
		return errors.New("state.postgres.direct.dsn is required when overriding a primary DSN")
	}
	return nil
}

func (settings Settings) hasStructuredValues() bool {
	return settings.Host != "" || settings.Port != "" || settings.Name != "" || settings.User != "" || settings.Password != ""
}

func (endpoint Endpoint) hasStructuredValues() bool {
	return Settings{Host: endpoint.Host, Port: endpoint.Port, Name: endpoint.Name, User: endpoint.User, Password: endpoint.Password}.hasStructuredValues()
}
