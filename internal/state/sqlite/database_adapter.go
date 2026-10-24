package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "modernc.org/sqlite" // Register the production SQLite database/sql driver.
)

func NewDatabaseStore(database *sql.DB) *Store {
	database.SetMaxOpenConns(1)
	store := newStore(databaseExecutor{database: database})
	store.close = database.Close
	return store
}

type SchemaExtension interface {
	SchemaStatements() []string
	Upgrade(ctx context.Context, transaction *sql.Tx, version int) (int, bool, error)
}

func Open(ctx context.Context, path string, extensions ...SchemaExtension) (*Store, error) {
	return openWithDatabaseOpener(ctx, path, sql.Open, extensions...)
}

func openWithDatabaseOpener(
	ctx context.Context,
	path string,
	opener func(driverName string, dataSourceName string) (*sql.DB, error),
	extensions ...SchemaExtension,
) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("open SQLite job state: %w", err)
	}
	database, err := opener("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite job state: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	if err := acquireExclusiveSQLiteOwnership(ctx, database); err != nil {
		return nil, errors.Join(err, database.Close())
	}
	if err := initialize(ctx, database, extensions...); err != nil {
		return nil, errors.Join(err, database.Close())
	}
	return NewDatabaseStore(database), nil
}

func acquireExclusiveSQLiteOwnership(ctx context.Context, database *sql.DB) (resultErr error) {
	connection, err := database.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire exclusive SQLite ownership: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, connection.Close()) }()
	var mode string
	if err := connection.QueryRowContext(ctx, "PRAGMA main.locking_mode=EXCLUSIVE").Scan(&mode); err != nil {
		return fmt.Errorf("acquire exclusive SQLite ownership: set locking mode: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(mode), "exclusive") {
		return fmt.Errorf("acquire exclusive SQLite ownership: locking mode is %q", mode)
	}
	if _, err := connection.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
		return fmt.Errorf("acquire exclusive SQLite ownership: another process may own the database: %w", err)
	}
	if _, err := connection.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("acquire exclusive SQLite ownership: commit lock acquisition: %w", err)
	}
	return nil
}
