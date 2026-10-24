package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strconv"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSchemaOwnsFreshGitHubTables(t *testing.T) {
	statements := Schema().SchemaStatements()
	if len(statements) != 3 {
		t.Fatalf("schema statements = %d", len(statements))
	}
}

func TestSchemaUpgradesLegacyGitHubState(t *testing.T) {
	for _, version := range []int{106, 107} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			assertLegacySchemaUpgrade(t, version)
		})
	}
}

func assertLegacySchemaUpgrade(t *testing.T, version int) {
	t.Helper()
	protocol := beginSchemaTransaction(t)
	if version == 106 {
		protocol.mock.ExpectExec(regexp.QuoteMeta(sqliteGitHubMemberRunnerPoolsSchema)).WillReturnResult(sqlmock.NewResult(0, 0))
	}
	for _, statement := range githubReplacementStatements() {
		protocol.mock.ExpectExec(regexp.QuoteMeta(statement)).WillReturnResult(sqlmock.NewResult(0, 0))
	}
	nextVersion, handled, err := Schema().Upgrade(context.Background(), protocol.transaction, version)
	if err != nil || !handled || nextVersion != 108 {
		t.Fatalf("upgrade = (%d, %t, %v)", nextVersion, handled, err)
	}
	finishSchemaTransaction(t, protocol)
}

func TestSchemaIgnoresVersionsItDoesNotOwn(t *testing.T) {
	nextVersion, handled, err := Schema().Upgrade(context.Background(), nil, 108)
	if err != nil || handled || nextVersion != 108 {
		t.Fatalf("upgrade = (%d, %t, %v)", nextVersion, handled, err)
	}
}

func TestSchemaReportsUpgradeFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	for _, version := range []int{106, 107} {
		assertSchemaUpgradeFailure(t, version, expectedErr)
	}
}

func assertSchemaUpgradeFailure(t *testing.T, version int, expectedErr error) {
	t.Helper()
	protocol := beginSchemaTransaction(t)
	statement := `DROP TABLE runnerfoundry_github_registrations`
	if version == 106 {
		statement = sqliteGitHubMemberRunnerPoolsSchema
	}
	protocol.mock.ExpectExec(regexp.QuoteMeta(statement)).WillReturnError(expectedErr)
	if _, handled, err := Schema().Upgrade(context.Background(), protocol.transaction, version); !handled || !errors.Is(err, expectedErr) {
		t.Fatalf("version %d handled = %t, error = %v", version, handled, err)
	}
	finishSchemaTransaction(t, protocol)
}

type schemaTransaction struct {
	database    *sql.DB
	mock        sqlmock.Sqlmock
	transaction *sql.Tx
}

func beginSchemaTransaction(t *testing.T) schemaTransaction {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	transaction, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return schemaTransaction{database: database, mock: mock, transaction: transaction}
}

func finishSchemaTransaction(t *testing.T, protocol schemaTransaction) {
	t.Helper()
	protocol.mock.ExpectRollback()
	protocol.mock.ExpectClose()
	if err := protocol.transaction.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := protocol.database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := protocol.mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func githubReplacementStatements() []string {
	return []string{`DROP TABLE runnerfoundry_github_registrations`, `DROP TABLE runnerfoundry_github_scale_set_ownership`,
		sqliteGitHubRegistrationsSchema, sqliteGitHubOwnershipSchema}
}

func TestSchema106ReportsAdapterReplacementFailure(t *testing.T) {
	expectedErr := errors.New("protocol")
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	transaction, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec(regexp.QuoteMeta(sqliteGitHubMemberRunnerPoolsSchema)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`DROP TABLE runnerfoundry_github_registrations`)).WillReturnError(expectedErr)
	if _, handled, err := Schema().Upgrade(context.Background(), transaction, 106); !handled || !errors.Is(err, expectedErr) {
		t.Fatalf("handled = %t, error = %v", handled, err)
	}
	mock.ExpectRollback()
	if err := transaction.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
