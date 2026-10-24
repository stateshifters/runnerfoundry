package main

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	postgres17Version  = "17.5.0"
	postgres18Version  = "18.3.0"
	postgresRepository = "https://repo1.maven.org/maven2"
)

func prepare(cfg options) error {
	versions, err := postgresVersions(cfg.postgresVersion)
	if err != nil {
		return err
	}
	for _, version := range versions {
		if err = preparePostgres(cfg.postgresRoot, version, runtime.GOOS, runtime.GOARCH); err != nil {
			return err
		}
	}
	return nil
}

func preparePostgres(postgresRoot string, version string, operatingSystem string, architecture string) error {
	artifactArchitecture, err := postgresArtifactArchitecture(version, operatingSystem, architecture, alpineLinux())
	if err != nil {
		return err
	}
	destination := postgresDestination(postgresRoot, version, operatingSystem, architecture)
	if postgresPrepared(destination) {
		fmt.Printf("PostgreSQL %s binaries already prepared at %s\n", version, destination)
		return nil
	}

	temporaryDirectory, err := os.MkdirTemp("", "runnerfoundry-e2e-")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer os.RemoveAll(temporaryDirectory)

	artifact := fmt.Sprintf("embedded-postgres-binaries-%s-%s-%s", operatingSystem, artifactArchitecture, version)
	downloadURL := fmt.Sprintf(
		"%s/io/zonky/test/postgres/embedded-postgres-binaries-%s-%s/%s/%s.jar",
		postgresRepository, operatingSystem, artifactArchitecture, version, artifact,
	)
	jarPath := filepath.Join(temporaryDirectory, "postgres.jar")
	archivePath := filepath.Join(temporaryDirectory, "postgres.txz")

	fmt.Printf("Preparing PostgreSQL %s binaries for %s/%s\n", version, operatingSystem, architecture)
	if err = runCommand("", "curl", "--fail", "--location", "--silent", "--show-error", "--output", jarPath, downloadURL); err != nil {
		return fmt.Errorf("download PostgreSQL %s: %w", version, err)
	}
	if err = extractPostgresArchive(jarPath, archivePath); err != nil {
		return err
	}
	if err = os.MkdirAll(destination, 0o755); err != nil {
		return fmt.Errorf("create PostgreSQL destination: %w", err)
	}
	if err = runCommand("", "tar", "-xJf", archivePath, "-C", destination); err != nil {
		return fmt.Errorf("extract PostgreSQL %s: %w", version, err)
	}
	if operatingSystem == "windows" {
		if err = copyFile(filepath.Join(destination, "bin", "pg_ctl.exe"), filepath.Join(destination, "bin", "pg_ctl")); err != nil {
			return fmt.Errorf("create extensionless pg_ctl: %w", err)
		}
	}
	if !postgresPrepared(destination) {
		return fmt.Errorf("prepared PostgreSQL %s archive does not contain bin/pg_ctl", version)
	}
	fmt.Printf("Prepared PostgreSQL %s binaries at %s\n", version, destination)
	return nil
}

func extractPostgresArchive(jarPath string, archivePath string) error {
	jar, err := zip.OpenReader(jarPath)
	if err != nil {
		return fmt.Errorf("open PostgreSQL binary jar: %w", err)
	}
	defer jar.Close()

	for _, entry := range jar.File {
		if entry.FileInfo().IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name), ".txz") {
			continue
		}
		return extractZipEntry(entry, archivePath)
	}
	return errors.New("PostgreSQL binary jar does not contain a .txz archive")
}

func extractZipEntry(entry *zip.File, destination string) error {
	source, err := entry.Open()
	if err != nil {
		return fmt.Errorf("open PostgreSQL binary archive: %w", err)
	}
	defer source.Close()

	target, err := os.Create(destination)
	if err != nil {
		return fmt.Errorf("create PostgreSQL binary archive: %w", err)
	}
	_, copyErr := io.Copy(target, source)
	closeErr := target.Close()
	if copyErr != nil {
		return fmt.Errorf("extract PostgreSQL binary archive: %w", copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close PostgreSQL binary archive: %w", closeErr)
	}
	return nil
}

func copyFile(sourcePath string, destinationPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()

	target, err := os.Create(destinationPath)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(target, source)
	closeErr := target.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func postgresVersions(configured string) ([]string, error) {
	requested := strings.FieldsFunc(configured, func(character rune) bool {
		return character == ',' || character == ' ' || character == '\t' || character == '\r' || character == '\n'
	})
	if len(requested) == 0 {
		return []string{postgres17Version, postgres18Version}, nil
	}

	versions := make([]string, 0, len(requested))
	for _, value := range requested {
		version, err := postgresVersion(value)
		if err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}
	return versions, nil
}

func postgresVersion(configured string) (string, error) {
	switch configured {
	case "17", "17.5", postgres17Version:
		return postgres17Version, nil
	case "18", "18.3", postgres18Version:
		return postgres18Version, nil
	default:
		return "", fmt.Errorf("unsupported E2E PostgreSQL version %q: expected 17 or 18", configured)
	}
}

func postgresArtifactArchitecture(version string, operatingSystem string, architecture string, alpine bool) (string, error) {
	var artifactArchitecture string
	switch operatingSystem + "/" + architecture {
	case "linux/amd64", "darwin/amd64", "windows/amd64":
		artifactArchitecture = "amd64"
	case "linux/arm64":
		artifactArchitecture = "arm64v8"
	case "darwin/arm64":
		if version == postgres17Version {
			artifactArchitecture = "amd64"
		} else {
			artifactArchitecture = "arm64v8"
		}
	default:
		return "", fmt.Errorf("unsupported E2E PostgreSQL platform: %s/%s", operatingSystem, architecture)
	}
	if operatingSystem == "linux" && alpine {
		artifactArchitecture += "-alpine"
	}
	return artifactArchitecture, nil
}

func postgresDestination(postgresRoot string, version string, operatingSystem string, architecture string) string {
	return filepath.Join(postgresRoot, version, operatingSystem+"-"+architecture)
}

func postgresPrepared(destination string) bool {
	info, err := os.Stat(filepath.Join(destination, "bin", "pg_ctl"))
	return err == nil && !info.IsDir()
}

func alpineLinux() bool {
	if runtime.GOOS != "linux" {
		return false
	}
	_, err := os.Stat("/etc/alpine-release")
	return err == nil
}
