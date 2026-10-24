package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func initialize(ctx context.Context, database *sql.DB, extensions ...SchemaExtension) (resultErr error) {
	needsInitialization, err := validateSQLiteSchema(ctx, database, extensions...)
	if err != nil || !needsInitialization {
		return err
	}
	return createSQLiteSchema(ctx, database, extensions...)
}

func validateSQLiteSchema(ctx context.Context, database *sql.DB, extensions ...SchemaExtension) (bool, error) {
	if _, err := database.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return false, fmt.Errorf("enable SQLite foreign keys: %w", err)
	}
	var version int
	if err := database.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return false, fmt.Errorf("read SQLite job schema version: %w", err)
	}
	if version == sqliteSchemaVersion {
		return false, nil
	}
	if version >= 106 && version <= 108 {
		return false, migrateSQLiteSchema(ctx, database, version, extensions...)
	}
	if version != 0 {
		return false, fmt.Errorf("SQLite job schema version %d requires database recreation", version)
	}
	var applicationTables int
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&applicationTables); err != nil {
		return false, fmt.Errorf("inspect unversioned SQLite database: %w", err)
	}
	if applicationTables != 0 {
		return false, errors.New("unversioned SQLite database contains application tables and requires recreation")
	}
	return true, nil
}

func migrateSQLiteSchema(ctx context.Context, database *sql.DB, version int, extensions ...SchemaExtension) error {
	if version < 108 {
		nextVersion, handled, err := applySQLiteExtension(ctx, database, extensions, version)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("SQLite job schema version %d requires a schema extension upgrade", version)
		}
		version = nextVersion
	}
	if version != 108 {
		return fmt.Errorf("SQLite job schema extension produced unsupported version %d", version)
	}
	return migrateSQLite108(ctx, database)
}

func applySQLiteExtension(
	ctx context.Context,
	database *sql.DB,
	extensions []SchemaExtension,
	version int,
) (int, bool, error) {
	for _, extension := range extensions {
		nextVersion, handled, err := upgradeSQLiteExtension(ctx, database, extension, version)
		if err != nil || handled {
			return nextVersion, handled, err
		}
	}
	return version, false, nil
}

func upgradeSQLiteExtension(
	ctx context.Context,
	database *sql.DB,
	extension SchemaExtension,
	version int,
) (nextVersion int, handled bool, resultErr error) {
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return version, false, fmt.Errorf("begin SQLite schema extension upgrade: %w", err)
	}
	open := true
	defer func() {
		if open {
			resultErr = errors.Join(resultErr, transaction.Rollback())
		}
	}()
	nextVersion, handled, err = executeSQLiteExtension(ctx, transaction, extension, version)
	if err != nil || !handled {
		return nextVersion, handled, err
	}
	if err = transaction.Commit(); err != nil {
		return version, true, fmt.Errorf("commit SQLite schema extension upgrade: %w", err)
	}
	open = false
	return nextVersion, true, nil
}

func executeSQLiteExtension(
	ctx context.Context,
	transaction *sql.Tx,
	extension SchemaExtension,
	version int,
) (int, bool, error) {
	nextVersion, handled, err := extension.Upgrade(ctx, transaction, version)
	if err != nil {
		return nextVersion, handled, fmt.Errorf("upgrade SQLite schema extension: %w", err)
	}
	if !handled {
		return nextVersion, false, nil
	}
	if nextVersion != 108 {
		return version, true, fmt.Errorf("SQLite schema extension cannot upgrade version %d to %d", version, nextVersion)
	}
	if _, err := transaction.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", nextVersion)); err != nil {
		return version, true, fmt.Errorf("record SQLite schema extension upgrade: %w", err)
	}
	return nextVersion, true, nil
}

func migrateSQLite108(ctx context.Context, database *sql.DB) error {
	return runSQLiteMigration(ctx, database, "provisioning failure schema upgrade", sqliteSchemaVersionSQL,
		func(ctx context.Context, transaction *sql.Tx) error {
			if _, err := transaction.ExecContext(ctx, addSQLiteProvisioningFailureFenceSQL); err != nil {
				return fmt.Errorf("add SQLite provisioning failure fence: %w", err)
			}
			if _, err := transaction.ExecContext(ctx, sqliteProvisioningFailureSchema); err != nil {
				return fmt.Errorf("add SQLite provisioning failure state: %w", err)
			}
			return nil
		})
}

func runSQLiteMigration(
	ctx context.Context,
	database *sql.DB,
	description string,
	versionStatement string,
	upgrade func(context.Context, *sql.Tx) error,
) (resultErr error) {
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite %s: %w", description, err)
	}
	open := true
	defer func() {
		if open {
			resultErr = errors.Join(resultErr, transaction.Rollback())
		}
	}()
	if err = upgrade(ctx, transaction); err != nil {
		return err
	}
	if _, err = transaction.ExecContext(ctx, versionStatement); err != nil {
		return fmt.Errorf("record SQLite %s: %w", description, err)
	}
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("commit SQLite %s: %w", description, err)
	}
	open = false
	return nil
}

func createSQLiteSchema(ctx context.Context, database *sql.DB, extensions ...SchemaExtension) (resultErr error) {
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite job schema: %w", err)
	}
	open := true
	defer func() {
		if open {
			resultErr = errors.Join(resultErr, transaction.Rollback())
		}
	}()
	for _, statement := range sqliteSchemaStatements(extensions...) {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize SQLite job schema: %w", err)
		}
	}
	if _, err := transaction.ExecContext(ctx, sqliteSchemaVersionSQL); err != nil {
		return fmt.Errorf("record SQLite job schema version: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit SQLite job schema: %w", err)
	}
	open = false
	return nil
}
