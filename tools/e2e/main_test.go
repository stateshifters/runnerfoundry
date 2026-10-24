package main

import (
	"reflect"
	"testing"
)

func TestPostgresVersions(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		expected   []string
	}{
		{name: "defaults", expected: []string{postgres17Version, postgres18Version}},
		{name: "aliases", configured: "17, 17.5 17.5.0 18,18.3 18.3.0", expected: []string{
			postgres17Version, postgres17Version, postgres17Version,
			postgres18Version, postgres18Version, postgres18Version,
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := postgresVersions(test.configured)
			if err != nil {
				t.Fatalf("parse PostgreSQL versions: %v", err)
			}
			if !reflect.DeepEqual(actual, test.expected) {
				t.Fatalf("versions = %v, want %v", actual, test.expected)
			}
		})
	}
}

func TestPostgresVersionsRejectsUnsupportedVersion(t *testing.T) {
	if _, err := postgresVersions("16"); err == nil {
		t.Fatal("expected unsupported version error")
	}
}

func TestPostgresArtifactArchitecture(t *testing.T) {
	tests := []struct {
		name             string
		version          string
		operatingSystem  string
		architecture     string
		alpine           bool
		expectedArtifact string
	}{
		{name: "Linux AMD64", version: postgres17Version, operatingSystem: "linux", architecture: "amd64", expectedArtifact: "amd64"},
		{name: "Linux ARM64", version: postgres18Version, operatingSystem: "linux", architecture: "arm64", expectedArtifact: "arm64v8"},
		{name: "Alpine AMD64", version: postgres17Version, operatingSystem: "linux", architecture: "amd64", alpine: true, expectedArtifact: "amd64-alpine"},
		{name: "Alpine ARM64", version: postgres18Version, operatingSystem: "linux", architecture: "arm64", alpine: true, expectedArtifact: "arm64v8-alpine"},
		{name: "Darwin AMD64", version: postgres18Version, operatingSystem: "darwin", architecture: "amd64", expectedArtifact: "amd64"},
		{name: "Darwin ARM64 PostgreSQL 17", version: postgres17Version, operatingSystem: "darwin", architecture: "arm64", expectedArtifact: "amd64"},
		{name: "Darwin ARM64 PostgreSQL 18", version: postgres18Version, operatingSystem: "darwin", architecture: "arm64", expectedArtifact: "arm64v8"},
		{name: "Windows AMD64", version: postgres18Version, operatingSystem: "windows", architecture: "amd64", expectedArtifact: "amd64"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := postgresArtifactArchitecture(
				test.version, test.operatingSystem, test.architecture, test.alpine,
			)
			if err != nil {
				t.Fatalf("resolve artifact architecture: %v", err)
			}
			if actual != test.expectedArtifact {
				t.Fatalf("artifact architecture = %q, want %q", actual, test.expectedArtifact)
			}
		})
	}
}

func TestPostgresArtifactArchitectureRejectsUnsupportedPlatform(t *testing.T) {
	if _, err := postgresArtifactArchitecture(postgres18Version, "windows", "arm64", false); err == nil {
		t.Fatal("expected unsupported platform error")
	}
}
