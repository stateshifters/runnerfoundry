package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestInitializeUpgradesSchema108WithProvisioningFailureState(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnRows(
		sqlmock.NewRows([]string{"user_version"}).AddRow(108))
	mock.ExpectBegin()
	mock.ExpectExec("ALTER TABLE runnerfoundry_processing_jobs").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS runnerfoundry_provisioning_failures").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA user_version = 109")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	if err = initialize(context.Background(), database); err != nil {
		t.Fatalf("upgrade schema 108: %v", err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("schema upgrade protocol: %v", err)
	}
}

func TestInitializeReportsExistingSchemaUpgradeFailures(t *testing.T) {
	protocolErr := errors.New("protocol")
	for _, version := range []int{106, 107} {
		database, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("create database protocol fake: %v", err)
		}
		mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnRows(
			sqlmock.NewRows([]string{"user_version"}).AddRow(version))
		mock.ExpectBegin().WillReturnError(protocolErr)
		if err = initialize(context.Background(), database); !errors.Is(err, protocolErr) {
			t.Fatalf("initialize schema %d error = %v", version, err)
		}
		if err = mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("schema %d protocol: %v", version, err)
		}
	}
}

func TestSQLite108UpgradeReportsEveryProtocolFailure(t *testing.T) {
	for failureStep := range 5 {
		t.Run(fmt.Sprintf("step %d", failureStep), func(t *testing.T) {
			assertSQLite108UpgradeFailure(t, failureStep)
		})
	}
}

func assertSQLite108UpgradeFailure(t *testing.T, failureStep int) {
	t.Helper()
	protocolErr := errors.New("protocol")
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	if failureStep == 0 {
		mock.ExpectBegin().WillReturnError(protocolErr)
	} else {
		mock.ExpectBegin()
		expectSQLite108MigrationSteps(mock, failureStep, protocolErr)
	}
	if err = migrateSQLite108(context.Background(), database); !errors.Is(err, protocolErr) {
		t.Fatalf("upgrade error = %v", err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("upgrade protocol: %v", err)
	}
}

func expectSQLite108MigrationSteps(mock sqlmock.Sqlmock, failureStep int, protocolErr error) {
	statements := []string{addSQLiteProvisioningFailureFenceSQL, sqliteProvisioningFailureSchema, sqliteSchemaVersionSQL}
	for step, statement := range statements {
		expectation := mock.ExpectExec(regexp.QuoteMeta(statement))
		if step+1 == failureStep {
			expectation.WillReturnError(protocolErr)
			mock.ExpectRollback()
			return
		}
		expectation.WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectCommit().WillReturnError(protocolErr)
}

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
		`CREATE TABLE runnerfoundry_processing_jobs (
			job_id TEXT NOT NULL PRIMARY KEY, runner_started INTEGER NOT NULL DEFAULT 0)`,
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
	expectSQLite108Upgrade(mock)
	if err = initialize(context.Background(), database); err != nil {
		t.Fatalf("upgrade schema 106: %v", err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("schema upgrade protocol: %v", err)
	}
}

func expectSQLite108Upgrade(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(addSQLiteProvisioningFailureFenceSQL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(sqliteProvisioningFailureSchema)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
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

func TestSQLite106UpgradeReportsAdapterStateReplacementFailure(t *testing.T) {
	t.Parallel()
	assertSQLite106UpgradeFailure(t, func(mock sqlmock.Sqlmock, protocolErr error) {
		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(sqliteGitHubMemberRunnerPoolsSchema)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		expectSQLiteAppStateReplacement(mock, protocolErr)
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
	version := mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersion108SQL))
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
