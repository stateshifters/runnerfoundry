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
	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestOpenUsesInjectedDatabaseProtocol(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	expectExclusiveSQLiteOwnership(mock)
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
	expectExclusiveSQLiteOwnership(mock)
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
	if result, err := store.Queue(context.Background(), request); err != nil || result.Outcome != core.QueueQueued {
		t.Fatalf("queue through adapter = %#v, %v", result, err)
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
	cases := []namedSQLMockSetup{
		{name: "begin", configure: func(mock sqlmock.Sqlmock) { mock.ExpectBegin().WillReturnError(protocolErr) }},
		{name: "execute", configure: func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(heartbeatMemberSQL)).WillReturnError(protocolErr)
			mock.ExpectRollback()
		}},
		{name: "commit", configure: func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(heartbeatMemberSQL)).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(regexp.QuoteMeta(removeMemberCapacitiesSQL)).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit().WillReturnError(protocolErr)
		}},
		{name: "rollback", configure: func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(heartbeatMemberSQL)).WillReturnError(protocolErr)
			mock.ExpectRollback().WillReturnError(protocolErr)
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			assertDatabaseAdapterFailure(t, testCase.configure)
		})
	}
}

func TestDatabaseAdapterReportsQueryFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for _, testCase := range []struct {
		name string
		rows *sqlmock.Rows
	}{
		{name: "query", rows: nil},
		{name: "row", rows: sqliteProcessingJobRows().RowError(0, protocolErr)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			assertDatabaseQueryFailure(t, testCase.rows, protocolErr)
		})
	}
}

func TestValidateSQLiteSchemaReportsProtocolAndOccupiedDatabaseErrors(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	cases := []namedSQLMockSetup{
		{name: "foreign key pragma", configure: func(mock sqlmock.Sqlmock) {
			mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnError(protocolErr)
		}},
		{name: "schema version", configure: func(mock sqlmock.Sqlmock) {
			mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnError(protocolErr)
		}},
		{name: "occupancy query", configure: func(mock sqlmock.Sqlmock) {
			expectUnversionedSchema(mock)
			mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM sqlite_master").WillReturnError(protocolErr)
		}},
		{name: "occupied unversioned database", configure: func(mock sqlmock.Sqlmock) {
			expectUnversionedSchema(mock)
			mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM sqlite_master").WillReturnRows(
				sqlmock.NewRows([]string{"count"}).AddRow(1))
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			assertSQLiteSchemaValidationFailure(t, testCase.configure)
		})
	}
}

func TestCreateSQLiteSchemaReportsEveryTransactionFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	for _, testCase := range schemaFailureProtocols(protocolErr) {
		t.Run(testCase.name, func(t *testing.T) {
			database, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("create fake: %v", err)
			}
			testCase.configure(mock)
			if err := createSQLiteSchema(context.Background(), database); err == nil {
				t.Fatal("expected schema error")
			}
		})
	}
}

type namedSQLMockSetup struct {
	name      string
	configure func(sqlmock.Sqlmock)
}

func assertDatabaseAdapterFailure(t *testing.T, configure func(sqlmock.Sqlmock)) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create fake: %v", err)
	}
	configure(mock)
	if err := NewDatabaseStore(database).PublishMemberState(context.Background(), testMemberID, fixedTime(), nil); err == nil {
		t.Fatal("expected adapter error")
	}
}

func assertDatabaseQueryFailure(t *testing.T, rows *sqlmock.Rows, protocolErr error) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create fake: %v", err)
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
		t.Fatal("expected query error")
	}
}

func assertSQLiteSchemaValidationFailure(t *testing.T, configure func(sqlmock.Sqlmock)) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create fake: %v", err)
	}
	configure(mock)
	if _, err := validateSQLiteSchema(context.Background(), database); err == nil {
		t.Fatal("expected validation error")
	}
}

func schemaFailureProtocols(protocolErr error) []namedSQLMockSetup {
	return []namedSQLMockSetup{
		{name: "begin", configure: func(mock sqlmock.Sqlmock) { mock.ExpectBegin().WillReturnError(protocolErr) }},
		{name: "schema statement", configure: func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaStatements()[0])).WillReturnError(protocolErr)
			mock.ExpectRollback()
		}},
		{name: "schema version", configure: func(mock sqlmock.Sqlmock) {
			expectSchemaStatements(mock)
			mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL)).WillReturnError(protocolErr)
			mock.ExpectRollback()
		}},
		{name: "commit", configure: func(mock sqlmock.Sqlmock) {
			expectSchemaStatements(mock)
			mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaVersionSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectCommit().WillReturnError(protocolErr)
		}},
		{name: "rollback", configure: func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(sqliteSchemaStatements()[0])).WillReturnError(protocolErr)
			mock.ExpectRollback().WillReturnError(protocolErr)
		}},
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

func expectExclusiveSQLiteOwnership(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA main.locking_mode=EXCLUSIVE")).WillReturnRows(
		sqlmock.NewRows([]string{"locking_mode"}).AddRow("exclusive"))
	mock.ExpectExec(regexp.QuoteMeta("BEGIN EXCLUSIVE")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("COMMIT")).WillReturnResult(sqlmock.NewResult(0, 0))
}

func sqliteQueuedJobRows(now time.Time) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"job_id", "source_name", "source_reference", "runner_pool", "enqueued_at"}).
		AddRow(testJobIDText, "github", "request-3", "linux", now.UnixNano())
}

func sqliteProcessingJobRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"job_id", "source_name", "source_reference", "runner_pool", "member_id",
		"runner_name", "claimed_at", "inventory_endpoint_id", "runner_started", "source_terminal", "provisioning_failure_state"}).
		AddRow(testJobIDText, "github", "request-3", "linux", testMemberIDText,
			string(core.AttemptRunnerName(testJobID, core.AttemptID{1})), fixedTime().UnixNano(), "test:endpoint", false, false, "")
}
