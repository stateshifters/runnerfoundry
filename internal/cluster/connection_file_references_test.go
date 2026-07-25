package cluster

import (
	"errors"
	"path"
	"strings"
	"testing"
)

type credentialFileFixture struct {
	contents map[string]string
	readErr  error
	paths    []string
}

func (fixture *credentialFileFixture) read(filePath string) ([]byte, error) {
	fixture.paths = append(fixture.paths, filePath)
	if !path.IsAbs(filePath) {
		return nil, errInvalidCredentialFilePath
	}
	if fixture.readErr != nil {
		return nil, fixture.readErr
	}
	contents, exists := fixture.contents[filePath]
	if !exists {
		return nil, errors.New("credential file is missing")
	}
	return []byte(contents), nil
}

func TestSettingsResolveEveryStructuredFileReferenceInMemory(t *testing.T) {
	for _, field := range structuredConnectionFields() {
		t.Run(field.name, func(t *testing.T) { assertStructuredFileReference(t, field) })
	}
}

func assertStructuredFileReference(t *testing.T, field structuredConnectionField) {
	t.Helper()
	filePath := "/run/secrets/postgres-" + field.name
	fixture := &credentialFileFixture{contents: map[string]string{filePath: field.want + "\n"}}
	settings := Settings{Role: "runnerfoundry"}
	field.set(&settings, "file://"+filePath)
	resolved, err := primaryConnectionConfigWithResolver(
		settings, connectionSettingResolver{readCredentialFile: fixture.read},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := field.get(resolved); got != field.want {
		t.Fatalf("resolved %s = %q, want %q", field.name, got, field.want)
	}
	if len(fixture.paths) != 1 || fixture.paths[0] != filePath {
		t.Fatalf("credential file paths = %q", fixture.paths)
	}
}

func TestSettingsResolveRoleFileReferenceInMemory(t *testing.T) {
	const expectedRole = "database-runtime-role"
	fixture := &credentialFileFixture{contents: map[string]string{
		"/run/secrets/postgres-role": expectedRole + "\n",
	}}
	resolved, err := primaryConnectionConfigWithResolver(Settings{
		DSN: "postgres://login@primary/scaler?role=wrong", Role: "file:///run/secrets/postgres-role",
	}, connectionSettingResolver{readCredentialFile: fixture.read})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.RuntimeParams["role"] != expectedRole {
		t.Fatalf("resolved role = %q", resolved.RuntimeParams["role"])
	}
}

func TestSettingsReadFilesAtPointOfUse(t *testing.T) {
	const filePath = "/run/secrets/postgres-password"
	fixture := &credentialFileFixture{contents: map[string]string{filePath: "first"}}
	resolver := connectionSettingResolver{readCredentialFile: fixture.read}
	settings := Settings{Password: "file://" + filePath, Role: "runnerfoundry"}
	first, firstErr := primaryConnectionConfigWithResolver(settings, resolver)
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	fixture.contents[filePath] = "second"
	second, secondErr := primaryConnectionConfigWithResolver(settings, resolver)
	if secondErr != nil {
		t.Fatal(secondErr)
	}
	if first.Password != "first" || second.Password != "second" {
		t.Fatalf("file-backed connection configurations = %#v, %#v", first, second)
	}
}

func TestSettingsResolveDSNFileReferenceInMemory(t *testing.T) {
	values := newConnectionFixtureValues()
	dsn := values.dsn("database.internal", "6432", "runnerfoundry")
	fixture := &credentialFileFixture{contents: map[string]string{
		"/run/secrets/postgres-dsn": dsn + "\n",
	}}
	resolved, err := primaryConnectionConfigWithResolver(
		Settings{DSN: "file:///run/secrets/postgres-dsn", Role: "runnerfoundry"},
		connectionSettingResolver{readCredentialFile: fixture.read},
	)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Host != "database.internal" || resolved.Port != 6432 || resolved.Database != "runnerfoundry" || resolved.User != values.identity || resolved.Password != values.proof {
		t.Fatalf("resolved DSN configuration = %#v", resolved)
	}
}

func TestSettingsRejectInvalidFileReferenceSyntaxWithoutDisclosure(t *testing.T) {
	tests := map[string]Settings{
		"malformed role file": {Host: "database", Role: "file://remote/role"},
		"malformed file":      {Password: "file://", Role: "runnerfoundry"},
		"file host":           {Password: "file://remote/secret", Role: "runnerfoundry"},
		"file fragment":       {Password: "file:///secret#value", Role: "runnerfoundry"},
		"fallback query":      {Password: "file:///secret?fallback=value", Role: "runnerfoundry"},
		"short file scheme":   {Password: "file:secret", Role: "runnerfoundry"},
	}
	for name, settings := range tests {
		t.Run(name, func(t *testing.T) {
			assertConnectionSettingsRejectedWithoutDisclosure(t, settings)
		})
	}
}

func TestSettingsRejectMissingCredentialFileWithoutDisclosureInMemory(t *testing.T) {
	assertCredentialFileFailureRedacted(t, &credentialFileFixture{contents: map[string]string{}})
}

func TestSettingsRejectCredentialFileReadFailureWithoutDisclosureInMemory(t *testing.T) {
	assertCredentialFileFailureRedacted(t, &credentialFileFixture{
		readErr: errors.New("credential storage unavailable"),
	})
}

func TestSettingsRejectEmptyCredentialFileWithoutDisclosureInMemory(t *testing.T) {
	assertCredentialFileFailureRedacted(t, &credentialFileFixture{contents: map[string]string{
		"/run/secrets/postgres-password": " \n",
	}})
}

func assertCredentialFileFailureRedacted(t *testing.T, fixture *credentialFileFixture) {
	t.Helper()
	_, err := primaryConnectionConfigWithResolver(
		settingsWithProofReference("file:///run/secrets/postgres-password"),
		connectionSettingResolver{readCredentialFile: fixture.read},
	)
	if err == nil {
		t.Fatal("expected file resolution failure")
	}
	for _, forbidden := range []string{"postgres-password", "credential storage unavailable"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("error exposed credential file details: %q", err)
		}
	}
}
