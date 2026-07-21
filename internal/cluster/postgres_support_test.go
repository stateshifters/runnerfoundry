//go:build integration

package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/meza/github-scaler/internal/postgresconfig"
)

var postgreSQLIntegrationDSN string
var postgreSQLDatabaseSequence atomic.Uint64

const (
	minimumPostgreSQLTestPort     = 1024
	postgreSQLStartAttemptLimit   = 5
	postgreSQLTestPortOptionCount = math.MaxUint16 - minimumPostgreSQLTestPort + 1
)

func TestMain(testSuite *testing.M) {
	dsn, database, runtimePath, err := startPostgreSQLIntegrationServer()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "start embedded PostgreSQL: %v\n", err)
		os.Exit(1)
	}
	postgreSQLIntegrationDSN = dsn
	exitCode := testSuite.Run()
	if err = database.Stop(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "stop embedded PostgreSQL: %v\n", err)
		exitCode = 1
	}
	if err = os.RemoveAll(runtimePath); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "remove embedded PostgreSQL runtime: %v\n", err)
		exitCode = 1
	}
	os.Exit(exitCode)
}

func assertMigrationStatus(t *testing.T, store *Store, expected MigrationStatus) {
	t.Helper()
	status, err := store.MigrationStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status != expected {
		t.Fatalf("migration status = %#v, want %#v", status, expected)
	}
}

func assertPostgreSQLTableExists(t *testing.T, dsn string, table string) {
	t.Helper()
	if !postgreSQLTableExists(t, dsn, table) {
		t.Fatalf("table %q does not exist", table)
	}
}

func assertPostgreSQLTableMissing(t *testing.T, dsn string, table string) {
	t.Helper()
	if postgreSQLTableExists(t, dsn, table) {
		t.Fatalf("table %q still exists", table)
	}
}

func postgreSQLTableExists(t *testing.T, dsn string, table string) bool {
	t.Helper()
	connection, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := connection.Close(context.Background()); closeErr != nil {
			t.Errorf("close PostgreSQL assertion connection: %v", closeErr)
		}
	}()
	var exists bool
	if err = connection.QueryRow(context.Background(), `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	return exists
}

func assertPrimaryPostgreSQLSessionRole(t *testing.T, settings postgresconfig.Settings, expectedSessionUser string) {
	t.Helper()
	connectionConfig, err := primaryConnectionConfig(settings)
	if err != nil {
		t.Fatal(err)
	}
	assertPostgreSQLSessionRole(t, connectionConfig, expectedSessionUser)
}

func assertDirectPostgreSQLSessionRole(t *testing.T, settings postgresconfig.Settings, expectedSessionUser string) {
	t.Helper()
	connectionConfig, err := directConnectionConfig(settings)
	if err != nil {
		t.Fatal(err)
	}
	assertPostgreSQLSessionRole(t, connectionConfig, expectedSessionUser)
}

func assertPostgreSQLSessionRole(t *testing.T, connectionConfig *pgx.ConnConfig, expectedSessionUser string) {
	t.Helper()
	connection, err := pgx.ConnectConfig(context.Background(), connectionConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := connection.Close(context.Background()); closeErr != nil {
			t.Errorf("close PostgreSQL role assertion connection: %v", closeErr)
		}
	}()
	var sessionUser string
	var currentUser string
	if err = connection.QueryRow(context.Background(), `SELECT session_user, current_user`).Scan(&sessionUser, &currentUser); err != nil {
		t.Fatal(err)
	}
	if sessionUser != expectedSessionUser || currentUser != "github_scaler" {
		t.Fatalf("session_user=%q current_user=%q", sessionUser, currentUser)
	}
}

func assertPostgreSQLDomainSchemaExists(t *testing.T, dsn string) {
	t.Helper()
	for _, table := range postgreSQLDomainTables() {
		assertPostgreSQLTableExists(t, dsn, table)
	}
}

func assertPostgreSQLDomainSchemaMissing(t *testing.T, dsn string) {
	t.Helper()
	for _, table := range postgreSQLDomainTables() {
		assertPostgreSQLTableMissing(t, dsn, table)
	}
}

func postgreSQLDomainTables() []string {
	return []string{"scaler_nodes", "scaler_contributions", "scaler_demand", "scaler_assignments", "scaler_schema_compatibility"}
}

func postgreSQLTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	dsn := postgreSQLTestDSN(t)
	store := NewStore(postgresconfig.Settings{DSN: dsn, Role: "postgres"})
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store, dsn
}

func postgreSQLTestDSN(t *testing.T) string {
	t.Helper()
	databaseName := fmt.Sprintf("github_scaler_test_%d", postgreSQLDatabaseSequence.Add(1))
	admin, err := pgx.Connect(context.Background(), postgreSQLIntegrationDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(context.Background(), "CREATE DATABASE "+pgx.Identifier{databaseName}.Sanitize()); err != nil {
		_ = admin.Close(context.Background())
		t.Fatal(err)
	}
	if err = admin.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		connection, connectErr := pgx.Connect(context.Background(), postgreSQLIntegrationDSN)
		if connectErr != nil {
			t.Errorf("connect to remove PostgreSQL test database: %v", connectErr)
			return
		}
		defer func() {
			if closeErr := connection.Close(context.Background()); closeErr != nil {
				t.Errorf("close PostgreSQL cleanup connection: %v", closeErr)
			}
		}()
		if _, dropErr := connection.Exec(
			context.Background(),
			"DROP DATABASE "+pgx.Identifier{databaseName}.Sanitize()+" WITH (FORCE)",
		); dropErr != nil {
			t.Errorf("remove PostgreSQL test database: %v", dropErr)
		}
	})
	parsedDSN, err := url.Parse(postgreSQLIntegrationDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsedDSN.Path = "/" + databaseName
	return parsedDSN.String()
}

func startPostgreSQLIntegrationServer() (string, *embeddedpostgres.EmbeddedPostgres, string, error) {
	return startPostgreSQLIntegrationServerWithPortCandidate(randomPostgreSQLPort)
}

func startPostgreSQLIntegrationServerWithPortCandidate(
	nextPort func() uint32,
) (string, *embeddedpostgres.EmbeddedPostgres, string, error) {
	cachePath, err := postgreSQLTestCache()
	if err != nil {
		return "", nil, "", err
	}
	var lastStartError error
	for range postgreSQLStartAttemptLimit {
		dsn, database, runtimePath, startErr := startPostgreSQLIntegrationServerOnPort(nextPort(), cachePath)
		if startErr == nil {
			return dsn, database, runtimePath, nil
		}
		lastStartError = startErr
	}
	return "", nil, "", fmt.Errorf(
		"start embedded PostgreSQL after %d attempts: %w",
		postgreSQLStartAttemptLimit,
		lastStartError,
	)
}

func startPostgreSQLIntegrationServerOnPort(
	port uint32,
	cachePath string,
) (string, *embeddedpostgres.EmbeddedPostgres, string, error) {
	runtimePath, err := os.MkdirTemp("", "github-scaler-postgres-")
	if err != nil {
		return "", nil, "", err
	}
	database := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V16).
		Port(port).
		CachePath(cachePath).
		RuntimePath(runtimePath).
		Logger(io.Discard))
	if err := database.Start(); err != nil {
		if removeErr := os.RemoveAll(runtimePath); removeErr != nil {
			return "", nil, "", errors.Join(err, fmt.Errorf("remove PostgreSQL runtime: %w", removeErr))
		}
		return "", nil, "", err
	}
	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)
	return dsn, database, runtimePath, nil
}

func randomPostgreSQLPort() uint32 {
	return minimumPostgreSQLTestPort + rand.Uint32N(postgreSQLTestPortOptionCount)
}

func postgreSQLTestCache() (string, error) {
	if configuredCache := os.Getenv("GITHUB_SCALER_POSTGRES_CACHE"); configuredCache != "" {
		return configuredCache, nil
	}
	userCache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("locate PostgreSQL test cache: %w", err)
	}
	return filepath.Join(userCache, "github-scaler", "embedded-postgres"), nil
}

func assertScaleSetStatus(t *testing.T, store *Store, now time.Time, liveFor time.Duration, expected map[string]ScaleSetStatus) {
	t.Helper()
	statuses := mustScaleSets(t, store, now, liveFor)
	if len(statuses) != len(expected) {
		t.Fatalf("statuses = %#v, want %#v", statuses, expected)
	}
	for _, status := range statuses {
		if status != expected[status.Name] {
			t.Fatalf("status %q = %#v, want %#v", status.Name, status, expected[status.Name])
		}
	}
}

func mustScaleSets(t *testing.T, store *Store, now time.Time, liveFor time.Duration) []ScaleSetStatus {
	t.Helper()
	statuses, err := store.ScaleSets(context.Background(), now, liveFor)
	if err != nil {
		t.Fatal(err)
	}
	return statuses
}

func findScaleSet(t *testing.T, statuses []ScaleSetStatus, name string) ScaleSetStatus {
	t.Helper()
	for _, status := range statuses {
		if status.Name == name {
			return status
		}
	}
	t.Fatalf("scale set %q not found in %#v", name, statuses)
	return ScaleSetStatus{}
}

func hasScaleSet(statuses []ScaleSetStatus, name string) bool {
	for _, status := range statuses {
		if status.Name == name {
			return true
		}
	}
	return false
}
