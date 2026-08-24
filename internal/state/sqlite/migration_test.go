package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestInitializeUpgradesSchema107ByReplacingOnlyGitHubAdapterState(t *testing.T) {
	database := openSQLite107Fixture(t)
	if err := initialize(context.Background(), database); err != nil {
		t.Fatalf("upgrade schema 107: %v", err)
	}
	assertSQLiteCoreMarker(t, database)
	assertSQLiteAdapterTablesEmpty(t, database)
	if _, err := database.Exec(`INSERT INTO runnerfoundry_github_registrations
		(github_app_id, repository_url, runner_pool, installation_id, runner_group_id, scale_set_id)
		VALUES (0, 'repo', 'linux', 1, 2, 3)`); err == nil {
		t.Fatal("schema 108 accepted a non-positive GitHub App ID")
	}
}

func openSQLite107Fixture(t *testing.T) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open SQLite database: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Errorf("close SQLite database: %v", closeErr)
		}
	})
	statements := []string{
		`CREATE TABLE core_marker (value TEXT NOT NULL)`,
		`INSERT INTO core_marker(value) VALUES ('preserved')`,
		`CREATE TABLE runnerfoundry_github_registrations (
			repository_url TEXT NOT NULL, runner_pool TEXT NOT NULL, installation_id INTEGER NOT NULL,
			runner_group_id INTEGER NOT NULL, scale_set_id INTEGER NOT NULL, PRIMARY KEY(repository_url, runner_pool))`,
		`CREATE TABLE runnerfoundry_github_scale_set_ownership (
			repository_url TEXT NOT NULL, runner_pool TEXT NOT NULL, installation_id INTEGER NOT NULL,
			runner_group_id INTEGER NOT NULL, scale_set_id INTEGER NOT NULL, PRIMARY KEY(repository_url, scale_set_id))`,
		`INSERT INTO runnerfoundry_github_registrations VALUES ('repo', 'linux', 1, 2, 3)`,
		`INSERT INTO runnerfoundry_github_scale_set_ownership VALUES ('repo', 'linux', 1, 2, 3)`,
		`PRAGMA user_version = 107`,
	}
	for _, statement := range statements {
		if _, err = database.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("prepare schema 107: %v", err)
		}
	}
	return database
}

func assertSQLiteCoreMarker(t *testing.T, database *sql.DB) {
	t.Helper()
	var marker string
	if err := database.QueryRow(`SELECT value FROM core_marker`).Scan(&marker); err != nil || marker != "preserved" {
		t.Fatalf("core marker = %q, %v", marker, err)
	}
}

func assertSQLiteAdapterTablesEmpty(t *testing.T, database *sql.DB) {
	t.Helper()
	for _, table := range []string{"runnerfoundry_github_registrations", "runnerfoundry_github_scale_set_ownership"} {
		var count int
		if err := database.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s rows = %d, %v", table, count, err)
		}
	}
}

func TestInitializeUpgradesSchema105WithoutDiscardingState(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnRows(
		sqlmock.NewRows([]string{"user_version"}).AddRow(105))
	mock.ExpectBegin()
	mock.ExpectExec("ALTER TABLE runnerfoundry_processing_jobs ADD COLUMN runner_name").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE runnerfoundry_processing_jobs SET runner_name").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(sqliteGitHubMemberRunnerPoolsSchema)).WillReturnResult(sqlmock.NewResult(0, 0))
	expectSQLiteAppStateReplacement(mock, nil)
	mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectClose()
	if err = initialize(context.Background(), database); err != nil {
		t.Fatalf("upgrade schema 105: %v", err)
	}
	if err = database.Close(); err != nil {
		t.Fatalf("close database protocol fake: %v", err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("schema upgrade protocol: %v", err)
	}
}

func TestInitializeUpgradesSchema106WithGitHubDeclarations(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnRows(
		sqlmock.NewRows([]string{"user_version"}).AddRow(106))
	expectSQLite106Upgrade(mock, nil, nil, nil)
	if err = initialize(context.Background(), database); err != nil {
		t.Fatalf("upgrade schema 106: %v", err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("schema upgrade protocol: %v", err)
	}
}

func TestSQLite106UpgradeReportsBeginFailure(t *testing.T) {
	t.Parallel()
	assertSQLite106UpgradeFailure(t, func(mock sqlmock.Sqlmock, protocolErr error) {
		mock.ExpectBegin().WillReturnError(protocolErr)
	})
}

func TestSQLite106UpgradeReportsDeclarationFailure(t *testing.T) {
	t.Parallel()
	assertSQLite106UpgradeFailure(t, func(mock sqlmock.Sqlmock, protocolErr error) {
		expectSQLite106Upgrade(mock, protocolErr, nil, nil)
	})
}

func TestSQLite106UpgradeReportsVersionFailure(t *testing.T) {
	t.Parallel()
	assertSQLite106UpgradeFailure(t, func(mock sqlmock.Sqlmock, protocolErr error) {
		expectSQLite106Upgrade(mock, nil, protocolErr, nil)
	})
}

func TestSQLite106UpgradeReportsCommitFailure(t *testing.T) {
	t.Parallel()
	assertSQLite106UpgradeFailure(t, func(mock sqlmock.Sqlmock, protocolErr error) {
		expectSQLite106Upgrade(mock, nil, nil, protocolErr)
	})
}

func assertSQLite106UpgradeFailure(t *testing.T, expect func(sqlmock.Sqlmock, error)) {
	t.Helper()
	protocolErr := errors.New("protocol")
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	expect(mock, protocolErr)
	if err = migrateSQLite106(context.Background(), database); !errors.Is(err, protocolErr) {
		t.Fatalf("upgrade error = %v", err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("upgrade protocol: %v", err)
	}
}

func expectSQLite106Upgrade(mock sqlmock.Sqlmock, declarationsErr, versionErr, commitErr error) {
	mock.ExpectBegin()
	declarations := mock.ExpectExec(regexp.QuoteMeta(sqliteGitHubMemberRunnerPoolsSchema))
	if declarationsErr != nil {
		declarations.WillReturnError(declarationsErr)
		mock.ExpectRollback()
		return
	}
	declarations.WillReturnResult(sqlmock.NewResult(0, 0))
	expectSQLiteAppStateReplacement(mock, nil)
	version := mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL))
	if versionErr != nil {
		version.WillReturnError(versionErr)
		mock.ExpectRollback()
		return
	}
	version.WillReturnResult(sqlmock.NewResult(0, 0))
	commit := mock.ExpectCommit()
	if commitErr != nil {
		commit.WillReturnError(commitErr)
	}
}

func expectSQLiteAppStateReplacement(mock sqlmock.Sqlmock, failure error) {
	statements := []string{
		`DROP TABLE runnerfoundry_github_registrations`,
		`DROP TABLE runnerfoundry_github_scale_set_ownership`,
		sqliteGitHubRegistrationsSchema,
		sqliteGitHubOwnershipSchema,
	}
	for index, statement := range statements {
		expectation := mock.ExpectExec(regexp.QuoteMeta(statement))
		if failure != nil && index == 0 {
			expectation.WillReturnError(failure)
			mock.ExpectRollback()
			return
		}
		expectation.WillReturnResult(sqlmock.NewResult(0, 0))
	}
}

func TestSQLite105UpgradeReportsEveryTransactionFailure(t *testing.T) {
	assertSQLite105UpgradeFailure(t, "begin")
	assertSQLite105UpgradeFailure(t, "alter")
	assertSQLite105UpgradeFailure(t, "backfill")
	assertSQLite105UpgradeFailure(t, "declarations")
	assertSQLite105UpgradeFailure(t, "app state")
	assertSQLite105UpgradeFailure(t, "version")
	assertSQLite105UpgradeFailure(t, "commit")
}

func assertSQLite105UpgradeFailure(t *testing.T, stage string) {
	t.Helper()
	protocolErr := errors.New("protocol")
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	expectSQLite105UpgradeFailure(mock, stage, protocolErr)
	if err = migrateSQLite105(context.Background(), database); !errors.Is(err, protocolErr) {
		t.Fatalf("%s error = %v", stage, err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%s protocol: %v", stage, err)
	}
}

func expectSQLite105UpgradeFailure(mock sqlmock.Sqlmock, stage string, protocolErr error) {
	switch stage {
	case "begin":
		mock.ExpectBegin().WillReturnError(protocolErr)
	case "alter":
		mock.ExpectBegin()
		mock.ExpectExec("ALTER TABLE runnerfoundry_processing_jobs ADD COLUMN runner_name").WillReturnError(protocolErr)
		mock.ExpectRollback()
	case "backfill":
		expectSQLite105Alter(mock)
		mock.ExpectExec("UPDATE runnerfoundry_processing_jobs SET runner_name").WillReturnError(protocolErr)
		mock.ExpectRollback()
	case "declarations":
		expectSQLite105AlterAndBackfill(mock)
		mock.ExpectExec(regexp.QuoteMeta(sqliteGitHubMemberRunnerPoolsSchema)).WillReturnError(protocolErr)
		mock.ExpectRollback()
	case "version":
		expectSQLite105AlterAndBackfill(mock)
		mock.ExpectExec(regexp.QuoteMeta(sqliteGitHubMemberRunnerPoolsSchema)).WillReturnResult(sqlmock.NewResult(0, 0))
		expectSQLiteAppStateReplacement(mock, nil)
		mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL)).WillReturnError(protocolErr)
		mock.ExpectRollback()
	case "commit":
		expectSQLite105AlterAndBackfill(mock)
		mock.ExpectExec(regexp.QuoteMeta(sqliteGitHubMemberRunnerPoolsSchema)).WillReturnResult(sqlmock.NewResult(0, 0))
		expectSQLiteAppStateReplacement(mock, nil)
		mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit().WillReturnError(protocolErr)
	case "app state":
		expectSQLite105AlterAndBackfill(mock)
		mock.ExpectExec(regexp.QuoteMeta(sqliteGitHubMemberRunnerPoolsSchema)).WillReturnResult(sqlmock.NewResult(0, 0))
		expectSQLiteAppStateReplacement(mock, protocolErr)
	}
}

func expectSQLite105Alter(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec("ALTER TABLE runnerfoundry_processing_jobs ADD COLUMN runner_name").
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectSQLite105AlterAndBackfill(mock sqlmock.Sqlmock) {
	expectSQLite105Alter(mock)
	mock.ExpectExec("UPDATE runnerfoundry_processing_jobs SET runner_name").
		WillReturnResult(sqlmock.NewResult(0, 1))
}
