//go:build integration

package cluster

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPostgreSQLReadsFileBackedCredentialsForEveryConnection(t *testing.T) {
	fixture := newCapacityTestCluster(t)
	parsedDSN, err := url.Parse(fixture.dsn)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	userPath := filepath.Join(directory, "postgres-user")
	passwordPath := filepath.Join(directory, "postgres-password")
	for path, contents := range map[string]string{userPath: "postgres\n", passwordPath: "postgres\n"} {
		if err = os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store := NewStore(ConnectionSettings{
		Host: "localhost", Port: parsedDSN.Port(), Name: strings.TrimPrefix(parsedDSN.Path, "/"),
		User: credentialFileURL(userPath), Password: credentialFileURL(passwordPath), Role: "postgres",
	})
	if statuses := mustScaleSets(t, store, fixture.now, fixture.liveFor); len(statuses) != 3 {
		t.Fatalf("file-backed authentication did not read clustered state: %#v", statuses)
	}
	if err = os.WriteFile(passwordPath, []byte("rotated-but-not-yet-valid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ScaleSets(fixture.ctx, fixture.now, fixture.liveFor); err == nil {
		t.Fatal("expected the next connection to read the changed password file")
	}
	if err = os.WriteFile(passwordPath, []byte("postgres\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if statuses := mustScaleSets(t, store, fixture.now, fixture.liveFor); len(statuses) != 3 {
		t.Fatalf("restored file-backed authentication did not reconnect: %#v", statuses)
	}
}
