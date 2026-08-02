package cluster

import (
	"errors"
	"net/url"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
)

var productionConnectionSettingResolver = operatingSystemConnectionSettingResolver()

func primaryConnectionConfig(settings postgresconfig.Settings) (*pgx.ConnConfig, error) {
	return primaryConnectionConfigWithResolver(settings, productionConnectionSettingResolver)
}

func primaryConnectionConfigWithResolver(
	settings postgresconfig.Settings,
	resolver connectionSettingResolver,
) (*pgx.ConnConfig, error) {
	if settings.Role == "" {
		return nil, errors.New("PostgreSQL role is required")
	}
	if _, err := resolver.resolve(settings.Role); err != nil {
		return nil, err
	}
	config, err := connectionConfigWithoutRole(settings, resolver)
	if err != nil {
		return nil, err
	}
	delete(config.RuntimeParams, "role")
	return config, nil
}

func directConnectionConfig(settings postgresconfig.Settings) (*pgx.ConnConfig, error) {
	return directConnectionConfigWithResolver(settings, productionConnectionSettingResolver)
}

func directConnectionConfigWithResolver(
	settings postgresconfig.Settings,
	resolver connectionSettingResolver,
) (*pgx.ConnConfig, error) {
	if settings.Direct == nil {
		return primaryConnectionConfigWithResolver(settings, resolver)
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
			return primaryConnectionConfigWithResolver(settings, resolver)
		}
		return primaryConnectionConfigWithResolver(direct, resolver)
	}
	if direct.DSN != "" {
		return primaryConnectionConfigWithResolver(direct, resolver)
	}
	direct.Host = inheritConnectionSetting(direct.Host, settings.Host)
	direct.Port = inheritConnectionSetting(direct.Port, settings.Port)
	direct.Name = inheritConnectionSetting(direct.Name, settings.Name)
	direct.User = inheritConnectionSetting(direct.User, settings.User)
	direct.Password = inheritConnectionSetting(direct.Password, settings.Password)
	return primaryConnectionConfigWithResolver(direct, resolver)
}

func connectionConfigWithoutRole(
	settings postgresconfig.Settings,
	resolver connectionSettingResolver,
) (*pgx.ConnConfig, error) {
	if settings.DSN != "" && hasStructuredConnectionValues(settings) {
		return nil, errors.New("PostgreSQL DSN cannot be combined with structured settings")
	}
	if settings.DSN != "" {
		return dsnConnectionConfig(settings.DSN, resolver)
	}
	return structuredConnectionConfig(settings, resolver)
}

func dsnConnectionConfig(configuredDSN string, resolver connectionSettingResolver) (*pgx.ConnConfig, error) {
	resolvedDSN, err := resolver.resolve(configuredDSN)
	if err != nil {
		return nil, err
	}
	return parseConnectionConfig(resolvedDSN, resolver)
}

func structuredConnectionConfig(
	settings postgresconfig.Settings,
	resolver connectionSettingResolver,
) (*pgx.ConnConfig, error) {
	values := url.Values{}
	for key, configuredValue := range map[string]string{
		"host": settings.Host, "port": settings.Port, "dbname": settings.Name,
		"user": settings.User, "password": settings.Password,
	} {
		resolvedValue, err := resolveStructuredConnectionSetting(key, configuredValue, resolver)
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
	return parseConnectionConfig(connectionString, resolver)
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

func resolveStructuredConnectionSetting(
	key string,
	configuredValue string,
	resolver connectionSettingResolver,
) (string, error) {
	if configuredValue == "" {
		return "", nil
	}
	resolvedValue, err := resolver.resolve(configuredValue)
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

func parseConnectionConfig(connectionString string, resolver connectionSettingResolver) (*pgx.ConnConfig, error) {
	config, err := resolver.parseConfig(connectionString)
	if err != nil {
		return nil, errors.New("parse PostgreSQL connection settings")
	}
	delete(config.RuntimeParams, "role")
	return config, nil
}
