package daemon

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/meza/github-scaler/internal/config"
)

func testAppCredentials(t *testing.T) config.GitHubAppCredentials {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return config.GitHubAppCredentials{AppID: 1, PrivateKey: string(privateKeyPEM)}
}

func writeFile(t *testing.T, path string, contents string) string {
	t.Helper()
	if path == "" || !filepath.IsAbs(path) {
		if path == "" {
			path = "test-file"
		}
		path = filepath.Join(t.TempDir(), path)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
