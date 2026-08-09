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

func TestSQLite105UpgradeReportsEveryTransactionFailure(t *testing.T) {
	assertSQLite105UpgradeFailure(t, "begin")
	assertSQLite105UpgradeFailure(t, "alter")
	assertSQLite105UpgradeFailure(t, "backfill")
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
	case "version":
		expectSQLite105AlterAndBackfill(mock)
		mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL)).WillReturnError(protocolErr)
		mock.ExpectRollback()
	case "commit":
		expectSQLite105AlterAndBackfill(mock)
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
