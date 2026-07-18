package cluster

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ConnectionSettings describes PostgreSQL connection values that are resolved
// immediately before each new connection. Each non-empty value may be literal,
// env://NAME, or file:///path; resolved secrets are never retained in the store.
type ConnectionSettings struct {
	DSN      string
	Host     string
	Port     string
	Name     string
	User     string
	Password string
	Role     string
	Direct   *ConnectionEndpoint
}

// ConnectionEndpoint contains optional overrides for a session-affine PostgreSQL endpoint.
type ConnectionEndpoint struct {
	DSN      string
	Host     string
	Port     string
	Name     string
	User     string
	Password string
}

func (settings ConnectionSettings) config() (*pgx.ConnConfig, error) {
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

func (settings ConnectionSettings) configWithoutRole() (*pgx.ConnConfig, error) {
	if settings.DSN != "" && settings.hasStructuredValues() {
		return nil, errors.New("PostgreSQL DSN cannot be combined with structured settings")
	}
	if settings.DSN != "" {
		return settings.dsnConfig()
	}
	return settings.structuredConfig()
}

func (settings ConnectionSettings) directConfig() (*pgx.ConnConfig, error) {
	if settings.Direct == nil {
		return settings.config()
	}
	direct := ConnectionSettings{
		DSN: settings.Direct.DSN, Host: settings.Direct.Host, Port: settings.Direct.Port,
		Name: settings.Direct.Name, User: settings.Direct.User, Password: settings.Direct.Password,
		Role: settings.Role,
	}
	if settings.DSN != "" {
		if direct.DSN == "" && direct.hasStructuredValues() {
			return nil, errors.New("direct PostgreSQL DSN is required when overriding a primary DSN")
		}
		if direct.DSN == "" {
			return settings.config()
		}
		return direct.config()
	}
	if direct.DSN != "" {
		return direct.config()
	}
	direct.Host = inheritSetting(direct.Host, settings.Host)
	direct.Port = inheritSetting(direct.Port, settings.Port)
	direct.Name = inheritSetting(direct.Name, settings.Name)
	direct.User = inheritSetting(direct.User, settings.User)
	direct.Password = inheritSetting(direct.Password, settings.Password)
	return direct.config()
}

func inheritSetting(value string, inherited string) string {
	if value != "" {
		return value
	}
	return inherited
}

func (settings ConnectionSettings) dsnConfig() (*pgx.ConnConfig, error) {
	resolvedDSN, err := resolveSetting(settings.DSN)
	if err != nil {
		return nil, err
	}
	return parseConnectionConfig(resolvedDSN)
}

func (settings ConnectionSettings) structuredConfig() (*pgx.ConnConfig, error) {
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

func (settings ConnectionSettings) hasStructuredValues() bool {
	return settings.Host != "" || settings.Port != "" || settings.Name != "" || settings.User != "" || settings.Password != ""
}

func resolveSetting(configuredValue string) (string, error) {
	switch {
	case strings.HasPrefix(configuredValue, "env:"):
		return resolveEnvironmentSetting(configuredValue)
	case strings.HasPrefix(configuredValue, "file:"):
		return resolveFileSetting(configuredValue)
	default:
		return configuredValue, nil
	}
}

func resolveEnvironmentSetting(reference string) (string, error) {
	if !strings.HasPrefix(reference, "env://") {
		return "", errors.New("malformed PostgreSQL environment reference")
	}
	name := strings.TrimPrefix(reference, "env://")
	if name == "" || strings.ContainsAny(name, "/?#") {
		return "", errors.New("malformed PostgreSQL environment reference")
	}
	value := os.Getenv(name)
	if strings.TrimSpace(value) == "" {
		return "", errors.New("PostgreSQL environment reference is missing or empty")
	}
	return value, nil
}

func resolveFileSetting(reference string) (string, error) {
	if !strings.HasPrefix(reference, "file://") {
		return "", errors.New("malformed PostgreSQL file reference")
	}
	parsed, err := url.Parse(reference)
	if err != nil || malformedFileURL(parsed) {
		return "", errors.New("malformed PostgreSQL file reference")
	}
	path := nativeFilePath(parsed.Path)
	if path == "" || (!filepath.IsAbs(path) && filepath.VolumeName(path) == "") {
		return "", errors.New("malformed PostgreSQL file reference")
	}
	contents, readErr := os.ReadFile(path) //nolint:gosec // Reading an operator-configured credential path is the intended boundary.
	if readErr != nil {
		return "", errors.New("read PostgreSQL file reference")
	}
	value := strings.TrimSpace(string(contents))
	if value == "" {
		return "", errors.New("PostgreSQL file reference is empty")
	}
	return value, nil
}

func malformedFileURL(parsed *url.URL) bool {
	return parsed.Scheme != "file" || parsed.Host != "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || parsed.Opaque != ""
}

func nativeFilePath(urlPath string) string {
	path := filepath.FromSlash(urlPath)
	if len(path) >= 3 && os.IsPathSeparator(path[0]) && path[2] == ':' {
		return path[1:]
	}
	return path
}
