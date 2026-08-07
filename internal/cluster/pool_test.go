package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/meza/RunnerFoundry/internal/postgresconfig"
)

func TestOpenPoolBuildsPrimaryRoleScopedPoolWithoutConnectingInTest(t *testing.T) {
	wantErr := errors.New("pool boundary stopped")
	store := NewStore(postgresconfig.Settings{
		DSN:  "postgres://login@database/runnerfoundry",
		Role: "runnerfoundry",
	})
	var captured *pgxpool.Config
	store.connectPool = func(_ context.Context, configured *pgxpool.Config) (*pgxpool.Pool, error) {
		captured = configured
		return nil, wantErr
	}

	pool, err := store.OpenPool(context.Background())
	if pool != nil || !errors.Is(err, wantErr) {
		t.Fatalf("OpenPool = (%v, %v), want nil wrapped boundary error", pool, err)
	}
	if captured == nil || captured.ConnConfig == nil || captured.AfterConnect == nil {
		t.Fatalf("pool configuration = %#v", captured)
	}
}

func TestOpenPoolRejectsInvalidConfiguration(t *testing.T) {
	if _, err := newStoreWithResolver(Settings{}, testConnectionResolver(nil)).OpenPool(context.Background()); err == nil {
		t.Fatal("missing role was accepted")
	}
	if _, err := newStoreWithResolver(Settings{Role: "runnerfoundry", Port: "invalid"}, testConnectionResolver(nil)).OpenPool(context.Background()); err == nil {
		t.Fatal("invalid primary configuration was accepted")
	}
}

func TestOpenPoolReportsParserFailure(t *testing.T) {
	store := newStoreWithResolver(Settings{Role: "runnerfoundry", Host: "database"}, testConnectionResolver(nil))
	store.parsePoolConfig = func(string) (*pgxpool.Config, error) {
		return nil, errors.New("parse")
	}
	if _, err := store.OpenPool(context.Background()); err == nil {
		t.Fatal("pool parse failure was hidden")
	}
}

func TestOpenPoolReturnsConfiguredPool(t *testing.T) {
	store := newStoreWithResolver(Settings{Role: "runnerfoundry", Host: "database"}, testConnectionResolver(nil))
	var configured *pgxpool.Config
	wantPool := &pgxpool.Pool{}
	store.connectPool = func(_ context.Context, config *pgxpool.Config) (*pgxpool.Pool, error) {
		configured = config
		return wantPool, nil
	}
	var assumedRole string
	store.assumePoolRole = func(_ context.Context, _ roleConnection, role string) error {
		assumedRole = role
		return nil
	}
	pool, err := store.OpenPool(context.Background())
	if err != nil || pool != wantPool || configured == nil {
		t.Fatalf("OpenPool = (%#v, %v), config=%#v", pool, err, configured)
	}
	if err = configured.AfterConnect(context.Background(), nil); err != nil || assumedRole != "runnerfoundry" {
		t.Fatalf("AfterConnect role = %q, %v", assumedRole, err)
	}
}

func TestAssumePostgreSQLRoleCoversExecutionOutcomes(t *testing.T) {
	connection := &fakeDatabaseConnection{}
	if err := assumePostgreSQLRole(context.Background(), connection, `role"name`); err != nil {
		t.Fatal(err)
	}
	if len(connection.executions) != 1 || connection.executions[0].statement != `SET ROLE "role""name"` {
		t.Fatalf("role execution = %#v", connection.executions)
	}
	connection.execErrors = []error{errors.New("set role")}
	if err := assumePostgreSQLRole(context.Background(), connection, "runnerfoundry"); err == nil {
		t.Fatal("role assumption failure was hidden")
	}
}
