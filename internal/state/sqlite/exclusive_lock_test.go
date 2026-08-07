package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestOpenRequiresExclusiveSQLiteOwnership(t *testing.T) {
	assertExclusiveSQLiteOpenFailure(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery(regexp.QuoteMeta("PRAGMA main.locking_mode=EXCLUSIVE")).WillReturnRows(
			sqlmock.NewRows([]string{"locking_mode"}).AddRow("normal"))
	})
}

func TestOpenReportsUnavailableExclusiveSQLiteLock(t *testing.T) {
	assertExclusiveSQLiteOpenFailure(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery(regexp.QuoteMeta("PRAGMA main.locking_mode=EXCLUSIVE")).WillReturnRows(
			sqlmock.NewRows([]string{"locking_mode"}).AddRow("exclusive"))
		mock.ExpectExec(regexp.QuoteMeta("BEGIN EXCLUSIVE")).WillReturnError(errors.New("protocol"))
	})
}

func TestExclusiveSQLiteOwnershipReportsConnectionFailure(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectClose()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := acquireExclusiveSQLiteOwnership(context.Background(), database); err == nil {
		t.Fatal("expected connection failure")
	}
}

func TestExclusiveSQLiteOwnershipReportsModeQueryFailure(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA main.locking_mode=EXCLUSIVE")).WillReturnError(errors.New("protocol"))
	if err := acquireExclusiveSQLiteOwnership(context.Background(), database); err == nil {
		t.Fatal("expected locking mode query failure")
	}
}

func TestExclusiveSQLiteOwnershipReportsCommitFailure(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA main.locking_mode=EXCLUSIVE")).WillReturnRows(
		sqlmock.NewRows([]string{"locking_mode"}).AddRow("exclusive"))
	mock.ExpectExec(regexp.QuoteMeta("BEGIN EXCLUSIVE")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("COMMIT")).WillReturnError(errors.New("protocol"))
	if err := acquireExclusiveSQLiteOwnership(context.Background(), database); err == nil {
		t.Fatal("expected lock commit failure")
	}
}

func assertExclusiveSQLiteOpenFailure(t *testing.T, configure func(sqlmock.Sqlmock)) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	configure(mock)
	mock.ExpectClose()
	_, openErr := openWithDatabaseOpener(context.Background(), "state.db", func(string, string) (*sql.DB, error) {
		return database, nil
	})
	if openErr == nil || !strings.Contains(openErr.Error(), "exclusive SQLite ownership") {
		t.Fatalf("open error = %v, want exclusive ownership error", openErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("protocol: %v", err)
	}
}
