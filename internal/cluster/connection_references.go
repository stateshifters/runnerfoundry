package cluster

import (
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

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
	contents, readErr := fs.ReadFile(os.DirFS(filepath.Dir(path)), filepath.Base(path))
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
