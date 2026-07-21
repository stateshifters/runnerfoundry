//go:build integration

package cluster

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/meza/github-scaler/internal/postgresconfig"
)

func TestPostgreSQLConnectionsUseStableRoleAcrossRotatingLogins(t *testing.T) {
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	if err := store.MigrateDown(ctx, 0, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := admin.Close(ctx); closeErr != nil {
			t.Errorf("close PostgreSQL admin connection: %v", closeErr)
		}
	})
	if _, err = admin.Exec(ctx, `DROP TABLE scaler_schema_version`); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE ROLE github_scaler NOLOGIN`,
		`CREATE ROLE wrong_scaler_role NOLOGIN`,
		`CREATE ROLE scaler_one LOGIN PASSWORD 'password-one'`,
		`CREATE ROLE scaler_two LOGIN PASSWORD 'password-two'`,
		`CREATE ROLE scaler_outsider LOGIN PASSWORD 'outsider-password'`,
		`GRANT github_scaler TO scaler_one, scaler_two`,
		`GRANT wrong_scaler_role TO scaler_one, scaler_two`,
		`GRANT USAGE, CREATE ON SCHEMA public TO github_scaler`,
	} {
		if _, err = admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	credentialDirectory := t.TempDir()
	userPath := filepath.Join(credentialDirectory, "user")
	passwordPath := filepath.Join(credentialDirectory, "password")
	rolePath := filepath.Join(credentialDirectory, "role")
	writeCredential := func(path string, value string) {
		t.Helper()
		if writeErr := os.WriteFile(path, []byte(value+"\n"), 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	writeCredential(userPath, "scaler_one")
	writeCredential(passwordPath, "password-one")
	writeCredential(rolePath, "github_scaler")
	t.Setenv("PGOPTIONS", "-c role=wrong_scaler_role")
	settings := postgresconfig.Settings{
		Host: "localhost", Port: parsedDSN.Port(), Name: strings.TrimPrefix(parsedDSN.Path, "/"),
		User: credentialFileURL(userPath), Password: credentialFileURL(passwordPath), Role: credentialFileURL(rolePath),
		Direct: &postgresconfig.Endpoint{Host: "localhost"},
	}
	dynamicStore := NewStore(settings)
	if err = dynamicStore.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	assertPrimaryPostgreSQLSessionRole(t, settings, "scaler_one")
	assertDirectPostgreSQLSessionRole(t, settings, "scaler_one")
	assertStableRoleOwnership(t, admin)
	writeCredential(userPath, "scaler_outsider")
	writeCredential(passwordPath, "outsider-password")
	assertMissingRoleMembershipIsRedacted(t, dynamicStore, userPath, passwordPath, rolePath)
	writeCredential(userPath, "scaler_two")
	writeCredential(passwordPath, "password-two")
	assertPrimaryPostgreSQLSessionRole(t, settings, "scaler_two")
}

func assertStableRoleOwnership(t *testing.T, admin *pgx.Conn) {
	t.Helper()
	for _, table := range []string{"scaler_nodes", "scaler_schema_compatibility", "scaler_schema_version"} {
		var owner string
		if err := admin.QueryRow(context.Background(), `SELECT tableowner FROM pg_tables WHERE schemaname = 'public' AND tablename = $1`, table).Scan(&owner); err != nil {
			t.Fatal(err)
		}
		if owner != "github_scaler" {
			t.Fatalf("%s owner = %q", table, owner)
		}
	}
}

func assertMissingRoleMembershipIsRedacted(t *testing.T, store *Store, referencePaths ...string) {
	t.Helper()
	_, err := store.MigrationStatus(context.Background())
	if err == nil {
		t.Fatal("expected login without stable-role membership to fail")
	}
	for _, secret := range append([]string{"scaler_outsider", "outsider-password", "github_scaler"}, referencePaths...) {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("membership error exposed PostgreSQL configuration: %q", err)
		}
	}
}
