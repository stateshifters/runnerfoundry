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
	fixture := newPostgreSQLRoleFixture(t)
	assertPrimaryPostgreSQLSessionRole(t, fixture.settings, "scaler_one")
	assertDirectPostgreSQLSessionRole(t, fixture.settings, "scaler_one")
	assertStableRoleOwnership(t, fixture.admin)
	fixture.useLogin(t, "scaler_outsider", "outsider-password")
	assertMissingRoleMembershipIsRedacted(t, fixture.store, fixture.userPath, fixture.passwordPath, fixture.rolePath)
	fixture.useLogin(t, "scaler_two", "password-two")
	assertPrimaryPostgreSQLSessionRole(t, fixture.settings, "scaler_two")
}

type postgreSQLRoleFixture struct {
	admin        *pgx.Conn
	store        *Store
	settings     postgresconfig.Settings
	userPath     string
	passwordPath string
	rolePath     string
}

func newPostgreSQLRoleFixture(t *testing.T) postgreSQLRoleFixture {
	t.Helper()
	store, dsn := postgreSQLTestStore(t)
	ctx := context.Background()
	if err := store.MigrateDown(ctx, 0, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	admin := postgreSQLAdminConnection(t, dsn)
	if _, err := admin.Exec(ctx, `DROP TABLE scaler_schema_version`); err != nil {
		t.Fatal(err)
	}
	createPostgreSQLTestRoles(t, admin)
	credentials := postgreSQLRoleSettings(t, dsn)
	t.Setenv("PGOPTIONS", "-c role=wrong_scaler_role")
	dynamicStore := NewStore(credentials.settings)
	if err := dynamicStore.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return postgreSQLRoleFixture{
		admin: admin, store: dynamicStore, settings: credentials.settings,
		userPath: credentials.userPath, passwordPath: credentials.passwordPath, rolePath: credentials.rolePath,
	}
}

func createPostgreSQLTestRoles(t *testing.T, admin *pgx.Conn) {
	t.Helper()
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
		if _, err := admin.Exec(context.Background(), statement); err != nil {
			t.Fatal(err)
		}
	}
}

type roleCredentialSettings struct {
	settings     postgresconfig.Settings
	userPath     string
	passwordPath string
	rolePath     string
}

func postgreSQLRoleSettings(t *testing.T, dsn string) roleCredentialSettings {
	t.Helper()
	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	credentialDirectory := t.TempDir()
	userPath := filepath.Join(credentialDirectory, "user")
	passwordPath := filepath.Join(credentialDirectory, "password")
	rolePath := filepath.Join(credentialDirectory, "role")
	writePostgreSQLCredential(t, userPath, "scaler_one")
	writePostgreSQLCredential(t, passwordPath, "password-one")
	writePostgreSQLCredential(t, rolePath, "github_scaler")
	settings := postgresconfig.Settings{
		Host: "localhost", Port: parsedDSN.Port(), Name: strings.TrimPrefix(parsedDSN.Path, "/"),
		User: credentialFileURL(userPath), Password: credentialFileURL(passwordPath), Role: credentialFileURL(rolePath),
		Direct: &postgresconfig.Endpoint{Host: "localhost"},
	}
	return roleCredentialSettings{
		settings: settings, userPath: userPath, passwordPath: passwordPath, rolePath: rolePath,
	}
}

func (fixture postgreSQLRoleFixture) useLogin(t *testing.T, user string, password string) {
	t.Helper()
	writePostgreSQLCredential(t, fixture.userPath, user)
	writePostgreSQLCredential(t, fixture.passwordPath, password)
}

func writePostgreSQLCredential(t *testing.T, path string, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
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
