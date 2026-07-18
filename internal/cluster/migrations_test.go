package cluster

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/jackc/tern/v2/migrate"
)

func TestMigrationHelpersHandleEmptyInvalidAndCleanupCases(t *testing.T) {
	if target := migrationTarget(&migrate.Migrator{}); target != 0 {
		t.Fatalf("empty migration target = %d", target)
	}
	if err := validateRollbackTarget(-1, 1); err == nil {
		t.Fatal("expected negative rollback target rejection")
	}

	closeErr := errors.New("close failed")
	connection := &fakeDatabaseConnection{closeErr: closeErr}
	closePostgreSQLConnection(context.Background(), connection)
	if !connection.closed {
		t.Fatal("migration connection was not closed")
	}
}

func TestNewMigratorRejectsInvalidMigrationFiles(t *testing.T) {
	store, _ := postgreSQLTestStore(t)
	ctx := context.Background()
	connection, err := store.connectDirectPostgreSQL(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgreSQLConnection(ctx, connection) })

	store.migrationFiles = failingSubFS{}
	if _, err = store.newMigrator(ctx, connection); err == nil {
		t.Fatal("expected migration filesystem error")
	}
	store.migrationFiles = fstest.MapFS{
		"migrations/invalid.sql": {Data: []byte("not a tern migration")},
	}
	if _, err = store.newMigrator(ctx, connection); err == nil {
		t.Fatal("expected malformed migration error")
	}
	store.migrationFiles = fstest.MapFS{
		"migrations/001_missing_down.sql": {Data: []byte("SELECT 1;\n---- create above / drop below ----\n")},
	}
	if _, err = store.newMigrator(ctx, connection); err == nil {
		t.Fatal("expected missing Down SQL error")
	}

	if err = connection.Close(ctx); err != nil {
		t.Fatal(err)
	}
	store.migrationFiles = embeddedMigrations
	if _, err = store.newMigrator(ctx, connection); err == nil {
		t.Fatal("expected closed connection to reject migrator creation")
	}
}

type failingSubFS struct{}

func (failingSubFS) Open(string) (fs.File, error) {
	return nil, errors.New("open failed")
}

func (failingSubFS) Sub(string) (fs.FS, error) {
	return nil, errors.New("sub failed")
}
