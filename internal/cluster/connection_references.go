package cluster

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type connectionSettingResolver struct {
	readCredentialFile func(string) ([]byte, error)
}

var errInvalidCredentialFilePath = errors.New("invalid credential file path")

func operatingSystemConnectionSettingResolver() connectionSettingResolver {
	return connectionSettingResolver{readCredentialFile: readOperatingSystemCredentialFile}
}

func readOperatingSystemCredentialFile(path string) ([]byte, error) {
	localizedPath := filepath.FromSlash(path)
	if !filepath.IsAbs(localizedPath) {
		return nil, errInvalidCredentialFilePath
	}
	contents, err := fs.ReadFile(os.DirFS(filepath.Dir(localizedPath)), filepath.Base(localizedPath))
	if err != nil {
		return nil, fmt.Errorf("read credential file: %w", err)
	}
	return contents, nil
}

func (resolver connectionSettingResolver) resolve(configuredValue string) (string, error) {
	switch {
	case strings.HasPrefix(configuredValue, "env:"):
		return resolveEnvironmentSetting(configuredValue)
	case strings.HasPrefix(configuredValue, "file:"):
		return resolver.resolveFile(configuredValue)
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

func (resolver connectionSettingResolver) resolveFile(reference string) (string, error) {
	if !strings.HasPrefix(reference, "file://") {
		return "", errors.New("malformed PostgreSQL file reference")
	}
	path := strings.TrimPrefix(reference, "file://")
	if path == "" || strings.ContainsAny(path, "?#") {
		return "", errors.New("malformed PostgreSQL file reference")
	}
	contents, readErr := resolver.readCredentialFile(path)
	if errors.Is(readErr, errInvalidCredentialFilePath) {
		return "", errors.New("malformed PostgreSQL file reference")
	}
	if readErr != nil {
		return "", errors.New("read PostgreSQL file reference")
	}
	value := strings.TrimSpace(string(contents))
	if value == "" {
		return "", errors.New("PostgreSQL file reference is empty")
	}
	return value, nil
}
