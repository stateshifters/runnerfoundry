//go:build integration

package cluster

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/meza/RunnerFoundry/internal/postgresconfig"
)

func TestPostgreSQLReadsFileBackedCredentialsForEveryConnection(t *testing.T) {
	fixture := newCapacityTestCluster(t)
	store, _ := fileCredentialStore(t, fixture.dsn)
	if statuses := mustScaleSets(t, store, fixture.now, fixture.liveFor); len(statuses) != 3 {
		t.Fatalf("file-backed authentication did not read clustered state: %#v", statuses)
	}
}

func TestPostgreSQLReadsChangedPasswordFileOnNextConnection(t *testing.T) {
	fixture := newCapacityTestCluster(t)
	store, passwordPath := fileCredentialStore(t, fixture.dsn)
	writeCredentialFixture(t, passwordPath, "rotated-but-not-yet-valid")
	if _, err := store.ScaleSets(context.Background(), fixture.now, fixture.liveFor); err == nil {
		t.Fatal("expected the next connection to read the changed password file")
	}
}

func TestPostgreSQLReconnectsAfterPasswordFileIsRestored(t *testing.T) {
	fixture := newCapacityTestCluster(t)
	store, passwordPath := fileCredentialStore(t, fixture.dsn)
	writeCredentialFixture(t, passwordPath, "rotated-but-not-yet-valid")
	if _, err := store.ScaleSets(context.Background(), fixture.now, fixture.liveFor); err == nil {
		t.Fatal("expected invalid rotated password to fail")
	}
	writeCredentialFixture(t, passwordPath, "postgres")
	if statuses := mustScaleSets(t, store, fixture.now, fixture.liveFor); len(statuses) != 3 {
		t.Fatalf("restored file-backed authentication did not reconnect: %#v", statuses)
	}
}

func fileCredentialStore(t *testing.T, dsn string) (*Store, string) {
	t.Helper()
	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	userPath := filepath.Join(directory, "postgres-user")
	passwordPath := filepath.Join(directory, "postgres-password")
	writeCredentialFixture(t, userPath, "postgres")
	writeCredentialFixture(t, passwordPath, "postgres")
	store := NewStore(postgresconfig.Settings{
		Host: "localhost", Port: parsedDSN.Port(), Name: strings.TrimPrefix(parsedDSN.Path, "/"),
		User: credentialFileURL(userPath), Password: credentialFileURL(passwordPath), Role: "postgres",
	})
	return store, passwordPath
}

func writeCredentialFixture(t *testing.T, path string, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}
