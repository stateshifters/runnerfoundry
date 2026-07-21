package cluster

import (
	"errors"
	"net/url"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/meza/github-scaler/internal/postgresconfig"
)

func primaryConnectionConfig(settings postgresconfig.Settings) (*pgx.ConnConfig, error) {
	if settings.Role == "" {
		return nil, errors.New("PostgreSQL role is required")
	}
	resolvedRole, err := resolveSetting(settings.Role)
	if err != nil {
		return nil, err
	}
	config, err := connectionConfigWithoutRole(settings)
	if err != nil {
		return nil, err
	}
	config.RuntimeParams["role"] = resolvedRole
	return config, nil
}

func directConnectionConfig(settings postgresconfig.Settings) (*pgx.ConnConfig, error) {
	if settings.Direct == nil {
		return primaryConnectionConfig(settings)
	}
	direct := postgresconfig.Settings{
		DSN: settings.Direct.DSN, Host: settings.Direct.Host, Port: settings.Direct.Port,
		Name: settings.Direct.Name, User: settings.Direct.User, Password: settings.Direct.Password,
		Role: settings.Role,
	}
	if settings.DSN != "" {
		if direct.DSN == "" && hasStructuredConnectionValues(direct) {
			return nil, errors.New("direct PostgreSQL DSN is required when overriding a primary DSN")
		}
		if direct.DSN == "" {
			return primaryConnectionConfig(settings)
		}
		return primaryConnectionConfig(direct)
	}
	if direct.DSN != "" {
		return primaryConnectionConfig(direct)
	}
	direct.Host = inheritConnectionSetting(direct.Host, settings.Host)
	direct.Port = inheritConnectionSetting(direct.Port, settings.Port)
	direct.Name = inheritConnectionSetting(direct.Name, settings.Name)
	direct.User = inheritConnectionSetting(direct.User, settings.User)
	direct.Password = inheritConnectionSetting(direct.Password, settings.Password)
	return primaryConnectionConfig(direct)
}

func connectionConfigWithoutRole(settings postgresconfig.Settings) (*pgx.ConnConfig, error) {
	if settings.DSN != "" && hasStructuredConnectionValues(settings) {
		return nil, errors.New("PostgreSQL DSN cannot be combined with structured settings")
	}
	if settings.DSN != "" {
		return dsnConnectionConfig(settings.DSN)
	}
	return structuredConnectionConfig(settings)
}

func dsnConnectionConfig(configuredDSN string) (*pgx.ConnConfig, error) {
	resolvedDSN, err := resolveSetting(configuredDSN)
	if err != nil {
		return nil, err
	}
	return parseConnectionConfig(resolvedDSN)
}

func structuredConnectionConfig(settings postgresconfig.Settings) (*pgx.ConnConfig, error) {
	values := url.Values{}
	for key, configuredValue := range map[string]string{
		"host": settings.Host, "port": settings.Port, "dbname": settings.Name,
		"user": settings.User, "password": settings.Password,
	} {
		resolvedValue, err := resolveStructuredConnectionSetting(key, configuredValue)
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

func hasStructuredConnectionValues(settings postgresconfig.Settings) bool {
	return settings.Host != "" || settings.Port != "" || settings.Name != "" || settings.User != "" || settings.Password != ""
}

func inheritConnectionSetting(value string, inherited string) string {
	if value != "" {
		return value
	}
	return inherited
}

func resolveStructuredConnectionSetting(key string, configuredValue string) (string, error) {
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
