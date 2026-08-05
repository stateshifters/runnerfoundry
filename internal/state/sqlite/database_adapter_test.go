package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/meza/RunnerFoundry/internal/core"
)

func TestOpenUsesInjectedDatabaseProtocol(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnRows(
		sqlmock.NewRows([]string{"user_version"}).AddRow(sqliteSchemaVersion))
	mock.ExpectClose()
	originalOpener := openSQLiteDatabase
	openSQLiteDatabase = func(driverName, path string) (*sql.DB, error) {
		if driverName != "sqlite" || path != "state.db" {
			t.Fatalf("open arguments = %q, %q", driverName, path)
		}
		return database, nil
	}
	t.Cleanup(func() { openSQLiteDatabase = originalOpener })
	store, err := Open(context.Background(), "state.db")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("protocol: %v", err)
	}
}

func TestOpenReportsOpenAndInitializationFailures(t *testing.T) {
	protocolErr := errors.New("protocol")
	originalOpener := openSQLiteDatabase
	t.Cleanup(func() { openSQLiteDatabase = originalOpener })
	openSQLiteDatabase = func(string, string) (*sql.DB, error) { return nil, protocolErr }
	if _, err := Open(context.Background(), "state.db"); err == nil {
		t.Fatal("expected open error")
	}
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnError(protocolErr)
	mock.ExpectClose().WillReturnError(protocolErr)
	openSQLiteDatabase = func(string, string) (*sql.DB, error) { return database, nil }
	if _, err := Open(context.Background(), "state.db"); err == nil {
		t.Fatal("expected initialization error")
	}
}

func TestInitializeAppliesFreshSchemaThroughInjectedDatabaseProtocol(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(0))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM sqlite_master").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectBegin()
	for _, statement := range sqliteSchemaStatements {
		mock.ExpectExec(regexp.QuoteMeta(statement)).WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA user_version = 101")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectClose()
	if err := initialize(context.Background(), database); err != nil {
		t.Fatalf("initialize fresh schema: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close database protocol fake: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("schema protocol: %v", err)
	}
}

func TestInitializeAcceptsCurrentFreshSchemaVersion(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnRows(
		sqlmock.NewRows([]string{"user_version"}).AddRow(sqliteSchemaVersion))
	mock.ExpectClose()
	if err := initialize(context.Background(), database); err != nil {
		t.Fatalf("reopen current schema: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close database protocol fake: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("schema protocol: %v", err)
	}
}

func TestInitializeRejectsLegacySchemaVersion(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(8))
	mock.ExpectClose()
	err = initialize(context.Background(), database)
	if err == nil || !strings.Contains(err.Error(), "requires database recreation") {
		t.Fatalf("legacy schema error = %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close database protocol fake: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("schema protocol: %v", err)
	}
}

func TestDatabaseAdapterDelegatesEnqueueTransaction(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	now := fixedTime()
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(insertJobSQL)).WillReturnResult(sqlmock.NewResult(3, 1))
	mock.ExpectQuery(regexp.QuoteMeta(lookupJobBySourceSQL)).WillReturnRows(sqliteJobRows(now))
	mock.ExpectCommit()
	mock.ExpectClose()
	store := NewDatabaseStore(database)
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-3"},
		RunnerPool: "linux", EnqueuedAt: now}
	if _, created, err := store.Enqueue(context.Background(), request); err != nil || !created {
		t.Fatalf("enqueue through adapter = %t, %v", created, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close adapter: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("adapter protocol: %v", err)
	}
}

func TestDatabaseAdapterDelegatesRowsAndScalarQueries(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	now := fixedTime()
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(pendingResultsSQL)).WillReturnRows(sqliteJobRows(now))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta(countLiveMembersSQL)).WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(2))
	store := NewDatabaseStore(database)
	if records, queryErr := store.PendingResults(context.Background(), 1); queryErr != nil || len(records) != 1 {
		t.Fatalf("query through adapter = %#v, %v", records, queryErr)
	}
	if count, countErr := store.CountLiveMembers(context.Background(), now); countErr != nil || count != 2 {
		t.Fatalf("scalar through adapter = %d, %v", count, countErr)
	}
}

func TestDatabaseAdapterReportsTransactionProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	cases := []func(sqlmock.Sqlmock){
		func(mock sqlmock.Sqlmock) { mock.ExpectBegin().WillReturnError(protocolErr) },
		func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(heartbeatMemberSQL)).WillReturnError(protocolErr)
			mock.ExpectRollback()
		},
		func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(heartbeatMemberSQL)).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(regexp.QuoteMeta(removeMemberCapacitiesSQL)).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit().WillReturnError(protocolErr)
		},
		func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(heartbeatMemberSQL)).WillReturnError(protocolErr)
			mock.ExpectRollback().WillReturnError(protocolErr)
		},
	}
	for index, configure := range cases {
		database, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("case %d: create fake: %v", index, err)
		}
		configure(mock)
		if err := NewDatabaseStore(database).PublishMemberState(context.Background(), testMemberID, fixedTime(), nil); err == nil {
			t.Fatalf("case %d: expected adapter error", index)
		}
	}
}

func TestDatabaseAdapterReportsQueryFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for index, rows := range []*sqlmock.Rows{
		nil,
		sqliteJobRows(fixedTime()).RowError(0, protocolErr),
	} {
		database, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("case %d: create fake: %v", index, err)
		}
		mock.ExpectBegin()
		query := mock.ExpectQuery(regexp.QuoteMeta(pendingResultsSQL))
		if rows == nil {
			query.WillReturnError(protocolErr)
		} else {
			query.WillReturnRows(rows)
		}
		mock.ExpectRollback()
		if _, err := NewDatabaseStore(database).PendingResults(context.Background(), 1); err == nil {
			t.Fatalf("case %d: expected query error", index)
		}
	}
}

func TestValidateSQLiteSchemaReportsProtocolAndOccupiedDatabaseErrors(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	cases := []func(sqlmock.Sqlmock){
		func(mock sqlmock.Sqlmock) {
			mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnError(protocolErr)
		},
		func(mock sqlmock.Sqlmock) {
			mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnError(protocolErr)
		},
		func(mock sqlmock.Sqlmock) {
			expectUnversionedSchema(mock)
			mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM sqlite_master").WillReturnError(protocolErr)
		},
		func(mock sqlmock.Sqlmock) {
			expectUnversionedSchema(mock)
			mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM sqlite_master").WillReturnRows(
				sqlmock.NewRows([]string{"count"}).AddRow(1))
		},
	}
	for index, configure := range cases {
		database, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("case %d: create fake: %v", index, err)
		}
		configure(mock)
		if _, err := validateSQLiteSchema(context.Background(), database); err == nil {
			t.Fatalf("case %d: expected validation error", index)
		}
	}
}

func TestCreateSQLiteSchemaReportsEveryTransactionFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for index, configure := range schemaFailureProtocols(protocolErr) {
		database, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("case %d: create fake: %v", index, err)
		}
		configure(mock)
		if err := createSQLiteSchema(context.Background(), database); err == nil {
			t.Fatalf("case %d: expected schema error", index)
		}
	}
}

func schemaFailureProtocols(protocolErr error) []func(sqlmock.Sqlmock) {
	return []func(sqlmock.Sqlmock){
		func(mock sqlmock.Sqlmock) { mock.ExpectBegin().WillReturnError(protocolErr) },
		func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaStatements[0])).WillReturnError(protocolErr)
			mock.ExpectRollback()
		},
		func(mock sqlmock.Sqlmock) {
			expectSchemaStatements(mock)
			mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL)).WillReturnError(protocolErr)
			mock.ExpectRollback()
		},
		func(mock sqlmock.Sqlmock) {
			expectSchemaStatements(mock)
			mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectCommit().WillReturnError(protocolErr)
		},
		func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaStatements[0])).WillReturnError(protocolErr)
			mock.ExpectRollback().WillReturnError(protocolErr)
		},
	}
}

func expectUnversionedSchema(mock sqlmock.Sqlmock) {
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnRows(
		sqlmock.NewRows([]string{"user_version"}).AddRow(0))
}

func expectSchemaStatements(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	for _, statement := range sqliteSchemaStatements {
		mock.ExpectExec(regexp.QuoteMeta(statement)).WillReturnResult(sqlmock.NewResult(0, 0))
	}
}

func sqliteJobRows(now time.Time) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"job_id", "source_name", "source_reference", "runner_pool", "state", "enqueued_at",
		"member_id", "claimed_at", "started_at", "acquisition_started_at", "acquired_at", "result", "result_origin", "result_reason"}).
		AddRow(testJobIDText, "github", "request-3", "linux", "attempting", now.UnixNano(), testMemberIDText, now.UnixNano(),
			now.UnixNano(), now.UnixNano(), now.UnixNano(), "failure", "service", "cleanup")
}
