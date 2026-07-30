package cluster

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
)

type connectionSettingResolver struct {
	readCredentialFile     func(string) ([]byte, error)
	localizeCredentialPath func(string) (string, error)
	lookupEnvironment      func(string) (string, bool)
	parseConfig            func(string) (*pgx.ConnConfig, error)
}

var errInvalidCredentialFilePath = errors.New("invalid credential file path")

func operatingSystemConnectionSettingResolver() connectionSettingResolver {
	return connectionSettingResolver{
		readCredentialFile:     os.ReadFile,
		localizeCredentialPath: operatingSystemCredentialPath,
		lookupEnvironment:      os.LookupEnv,
		parseConfig:            pgx.ParseConfig,
	}
}

func operatingSystemCredentialPath(path string) (string, error) {
	localizedPath := filepath.FromSlash(path)
	if !filepath.IsAbs(localizedPath) {
		return "", errInvalidCredentialFilePath
	}
	return localizedPath, nil
}

func (resolver connectionSettingResolver) resolve(configuredValue string) (string, error) {
	switch {
	case strings.HasPrefix(configuredValue, "env:"):
		return resolver.resolveEnvironment(configuredValue)
	case strings.HasPrefix(configuredValue, "file:"):
		return resolver.resolveFile(configuredValue)
	default:
		return configuredValue, nil
	}
}

func (resolver connectionSettingResolver) resolveEnvironment(reference string) (string, error) {
	if !strings.HasPrefix(reference, "env://") {
		return "", errors.New("malformed PostgreSQL environment reference")
	}
	name := strings.TrimPrefix(reference, "env://")
	if name == "" || strings.ContainsAny(name, "/?#") {
		return "", errors.New("malformed PostgreSQL environment reference")
	}
	value, exists := resolver.lookupEnvironment(name)
	if !exists || strings.TrimSpace(value) == "" {
		return "", errors.New("PostgreSQL environment reference is missing or empty")
	}
	return value, nil
}

func (resolver connectionSettingResolver) resolveFile(reference string) (string, error) {
	if !strings.HasPrefix(reference, "file://") {
		return "", errors.New("malformed PostgreSQL file reference")
	}
	path := strings.TrimPrefix(reference, "file://")
	if path == "" || strings.ContainsAny(path, "?#") {
		return "", errors.New("malformed PostgreSQL file reference")
	}
	localizedPath, pathErr := resolver.localizeCredentialPath(path)
	if pathErr != nil {
		return "", errors.New("malformed PostgreSQL file reference")
	}
	contents, readErr := resolver.readCredentialFile(localizedPath)
	if readErr != nil {
		return "", errors.New("read PostgreSQL file reference")
	}
	value := strings.TrimSpace(string(contents))
	if value == "" {
		return "", errors.New("PostgreSQL file reference is empty")
	}
	return value, nil
}
