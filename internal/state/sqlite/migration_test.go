package sqlite

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

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

func TestSQLite105UpgradeReportsEveryTransactionFailure(t *testing.T) {
	assertSQLite105UpgradeFailure(t, "begin")
	assertSQLite105UpgradeFailure(t, "alter")
	assertSQLite105UpgradeFailure(t, "backfill")
	assertSQLite105UpgradeFailure(t, "declarations")
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
		mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL)).WillReturnError(protocolErr)
		mock.ExpectRollback()
	case "commit":
		expectSQLite105AlterAndBackfill(mock)
		mock.ExpectExec(regexp.QuoteMeta(sqliteGitHubMemberRunnerPoolsSchema)).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit().WillReturnError(protocolErr)
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
