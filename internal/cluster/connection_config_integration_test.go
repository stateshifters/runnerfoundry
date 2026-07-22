//go:build integration

package cluster

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOperatingSystemCredentialFileReaderUsesHostPathSemantics(t *testing.T) {
	credentialPath := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(credentialPath, []byte("opaque-fixture-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	contents, err := readOperatingSystemCredentialFile(filepath.ToSlash(credentialPath))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "opaque-fixture-value\n" {
		t.Fatalf("credential file contents = %q", contents)
	}
}

func TestOperatingSystemCredentialFileReaderReportsReadFailure(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "missing-credential")
	if _, err := readOperatingSystemCredentialFile(filepath.ToSlash(missingPath)); err == nil {
		t.Fatal("expected missing credential file failure")
	}
}
