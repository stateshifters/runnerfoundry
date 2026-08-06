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
	opener := func(driverName, path string) (*sql.DB, error) {
		if driverName != "sqlite" || path != "state.db" {
			t.Fatalf("open arguments = %q, %q", driverName, path)
		}
		return database, nil
	}
	store, err := openWithDatabaseOpener(context.Background(), "state.db", opener)
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
	openFailure := func(string, string) (*sql.DB, error) { return nil, protocolErr }
	if _, err := openWithDatabaseOpener(context.Background(), "state.db", openFailure); err == nil {
		t.Fatal("expected open error")
	}
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnError(protocolErr)
	mock.ExpectClose().WillReturnError(protocolErr)
	initializationFailure := func(string, string) (*sql.DB, error) { return database, nil }
	if _, err := openWithDatabaseOpener(context.Background(), "state.db", initializationFailure); err == nil {
		t.Fatal("expected initialization error")
	}
}

func TestOpenHonoursCancelledContextBeforeDatabaseAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(ctx, "must-not-be-opened.db"); !errors.Is(err, context.Canceled) {
		t.Fatal("expected canceled open error")
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
	for _, statement := range sqliteSchemaStatements() {
		mock.ExpectExec(regexp.QuoteMeta(statement)).WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
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

func TestInitialSchemaRequestsRecoveryEvidenceFields(t *testing.T) {
	var processingSchema string
	for _, statement := range sqliteSchemaStatements() {
		if strings.Contains(statement, "CREATE TABLE IF NOT EXISTS runnerfoundry_processing_jobs") {
			processingSchema = statement
			break
		}
	}
	for _, required := range []string{
		"claimed_at INTEGER NOT NULL",
		"inventory_endpoint_id TEXT NOT NULL CHECK(length(trim(inventory_endpoint_id)) > 0)",
	} {
		if !strings.Contains(processingSchema, required) {
			t.Errorf("processing schema does not contain %q", required)
		}
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
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(104))
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
	mock.ExpectExec(regexp.QuoteMeta(insertQueuedJobSQL)).WillReturnResult(sqlmock.NewResult(3, 1))
	mock.ExpectQuery(regexp.QuoteMeta(queuedJobBySourceSQL)).WillReturnRows(sqliteQueuedJobRows(now))
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

func TestDatabaseAdapterDelegatesProcessingRowsAndScalarQueries(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	now := fixedTime()
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(processingByMemberSQL)).WillReturnRows(sqliteProcessingJobRows())
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta(countLiveMembersSQL)).WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(2))
	store := NewDatabaseStore(database)
	if records, queryErr := store.ProcessingByMember(context.Background(), testMemberID, "linux"); queryErr != nil || len(records) != 1 {
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
		sqliteProcessingJobRows().RowError(0, protocolErr),
	} {
		database, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("case %d: create fake: %v", index, err)
		}
		mock.ExpectBegin()
		query := mock.ExpectQuery(regexp.QuoteMeta(processingByMemberSQL))
		if rows == nil {
			query.WillReturnError(protocolErr)
		} else {
			query.WillReturnRows(rows)
		}
		mock.ExpectRollback()
		if _, err := NewDatabaseStore(database).ProcessingByMember(context.Background(), testMemberID, "linux"); err == nil {
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
			mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaStatements()[0])).WillReturnError(protocolErr)
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
			mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaStatements()[0])).WillReturnError(protocolErr)
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
	for _, statement := range sqliteSchemaStatements() {
		mock.ExpectExec(regexp.QuoteMeta(statement)).WillReturnResult(sqlmock.NewResult(0, 0))
	}
}

func sqliteQueuedJobRows(now time.Time) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"job_id", "source_name", "source_reference", "runner_pool", "enqueued_at"}).
		AddRow(testJobIDText, "github", "request-3", "linux", now.UnixNano())
}

func sqliteProcessingJobRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"job_id", "source_name", "source_reference", "runner_pool", "member_id",
		"runner_started", "source_terminal"}).
		AddRow(testJobIDText, "github", "request-3", "linux", testMemberIDText, false, false)
}
