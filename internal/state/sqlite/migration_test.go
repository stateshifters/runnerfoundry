package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
		if err = initialize(context.Background(), database, testSchemaExtension{}); !errors.Is(err, protocolErr) {
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

func TestInitializeRequiresExtensionForSchema106(t *testing.T) {
	t.Parallel()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create database protocol fake: %v", err)
	}
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA foreign_keys = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("PRAGMA user_version")).WillReturnRows(
		sqlmock.NewRows([]string{"user_version"}).AddRow(106))
	if err = initialize(context.Background(), database); err == nil {
		t.Fatal("expected missing schema extension error")
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

type testSchemaExtension struct{}

func (testSchemaExtension) SchemaStatements() []string { return nil }

func (testSchemaExtension) Upgrade(context.Context, *sql.Tx, int) (int, bool, error) {
	return 108, true, nil
}

func TestSQLiteSchemaCompositionIncludesExtensions(t *testing.T) {
	base := len(sqliteSchemaStatements())
	statements := sqliteSchemaStatements(schemaExtensionFake{statements: []string{"extension"}})
	if len(statements) != base+1 || statements[len(statements)-1] != "extension" {
		t.Fatalf("statements = %#v", statements)
	}
}

func TestSQLiteExtensionUpgradeProtocol(t *testing.T) {
	expectedErr := errors.New("protocol")
	for _, testCase := range []extensionUpgradeCase{
		{name: "unhandled", extension: schemaExtensionFake{}, wantHandled: false},
		{name: "upgrade", extension: schemaExtensionFake{handled: true, err: expectedErr}, wantHandled: true, wantErr: expectedErr},
		{name: "version", extension: schemaExtensionFake{handled: true, nextVersion: 108}, versionErr: expectedErr,
			wantHandled: true, wantErr: expectedErr, versionUpdate: true},
		{name: "commit", extension: schemaExtensionFake{handled: true, nextVersion: 108}, commitErr: expectedErr,
			wantHandled: true, wantErr: expectedErr, versionUpdate: true},
		{name: "success", extension: schemaExtensionFake{handled: true, nextVersion: 108},
			wantHandled: true, versionUpdate: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runSQLiteExtensionUpgradeCase(t, testCase)
		})
	}
}

type extensionUpgradeCase struct {
	name          string
	extension     SchemaExtension
	versionErr    error
	commitErr     error
	wantHandled   bool
	wantErr       error
	versionUpdate bool
}

func runSQLiteExtensionUpgradeCase(t *testing.T, testCase extensionUpgradeCase) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	expectExtensionVersionUpdate(mock, testCase)
	expectExtensionCompletion(mock, testCase)
	nextVersion, handled, err := upgradeSQLiteExtension(context.Background(), database, testCase.extension, 106)
	if handled != testCase.wantHandled || !errors.Is(err, testCase.wantErr) ||
		(testCase.wantHandled && testCase.wantErr == nil && nextVersion != 108) {
		t.Fatalf("upgrade = (%d, %t, %v)", nextVersion, handled, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func expectExtensionVersionUpdate(mock sqlmock.Sqlmock, testCase extensionUpgradeCase) {
	if !testCase.versionUpdate {
		return
	}
	version := mock.ExpectExec(regexp.QuoteMeta("PRAGMA user_version = 108"))
	if testCase.versionErr != nil {
		version.WillReturnError(testCase.versionErr)
		return
	}
	version.WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectExtensionCompletion(mock sqlmock.Sqlmock, testCase extensionUpgradeCase) {
	if testCase.wantErr != nil && testCase.commitErr == nil || !testCase.wantHandled {
		mock.ExpectRollback()
		return
	}
	if testCase.commitErr != nil {
		mock.ExpectCommit().WillReturnError(testCase.commitErr)
		return
	}
	mock.ExpectCommit()
}

func TestMigrateSQLiteSchemaReportsExtensionFailure(t *testing.T) {
	expectedErr := errors.New("protocol")
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectRollback()
	if err := migrateSQLiteSchema(context.Background(), database, 106,
		schemaExtensionFake{handled: true, err: expectedErr}); !errors.Is(err, expectedErr) {
		t.Fatalf("error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteExtensionRejectsInvalidVersionTransitions(t *testing.T) {
	for _, nextVersion := range []int{106, 109} {
		database, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectBegin()
		mock.ExpectRollback()
		version, handled, upgradeErr := upgradeSQLiteExtension(context.Background(), database,
			schemaExtensionFake{handled: true, nextVersion: nextVersion}, 106)
		if upgradeErr == nil || !handled || version != 106 {
			t.Fatalf("upgrade = (%d, %t, %v)", version, handled, upgradeErr)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMigrateSQLiteSchemaRejectsUnsupportedExtensionVersion(t *testing.T) {
	database, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	if err := migrateSQLiteSchema(context.Background(), database, 109); err == nil {
		t.Fatal("expected unsupported schema version error")
	}
}

func TestMigrateSQLiteSchemaContinuesWithNeutralUpgrade(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("PRAGMA user_version = 108")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	expectSQLite108Upgrade(mock)
	if err := migrateSQLiteSchema(context.Background(), database, 106,
		schemaExtensionFake{handled: true, nextVersion: 108}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

type schemaExtensionFake struct {
	statements  []string
	nextVersion int
	handled     bool
	err         error
}

func (extension schemaExtensionFake) SchemaStatements() []string { return extension.statements }

func (extension schemaExtensionFake) Upgrade(context.Context, *sql.Tx, int) (int, bool, error) {
	return extension.nextVersion, extension.handled, extension.err
}
