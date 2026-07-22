//go:build integration

package cluster

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSettingsRejectFileResolutionFailuresWithoutDisclosure(t *testing.T) {
	unreadablePath := filepath.Join(t.TempDir(), "secret-location")
	if err := os.Mkdir(unreadablePath, 0o700); err != nil {
		t.Fatal(err)
	}
	tests := map[string]Settings{
		"missing file":    settingsWithProofReference("file:///definitely/missing/scaler-secret"),
		"unreadable file": settingsWithProofReference(connectionCredentialFileURL(unreadablePath)),
		"empty file":      settingsWithProofReference(writeCredentialReference(t, "empty", " \n")),
	}
	for name, settings := range tests {
		t.Run(name, func(t *testing.T) {
			assertConnectionSettingsRejectedWithoutDisclosure(t, settings)
		})
	}
}

func TestSettingsResolveStructuredFileReferences(t *testing.T) {
	for _, field := range structuredConnectionFields() {
		t.Run(field.name, func(t *testing.T) {
			valuePath := filepath.Join(t.TempDir(), field.name)
			if err := os.WriteFile(valuePath, []byte(field.want+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			assertStructuredConnectionField(t, field, connectionCredentialFileURL(valuePath))
		})
	}
}

func TestSettingsResolveDSNFileReference(t *testing.T) {
	values := newConnectionFixtureValues()
	dsn := values.dsn("database.internal", "6432", "github_scaler")
	dsnPath := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(dsnPath, []byte(dsn+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	resolved, err := primaryConnectionConfig(Settings{DSN: connectionCredentialFileURL(dsnPath), Role: "github_scaler"})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Host != "database.internal" || resolved.Port != 6432 || resolved.Database != "github_scaler" || resolved.User != values.identity || resolved.Password != values.proof {
		t.Fatalf("resolved DSN configuration = %#v", resolved)
	}
}

func writeCredentialReference(t *testing.T, name string, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return connectionCredentialFileURL(path)
}
