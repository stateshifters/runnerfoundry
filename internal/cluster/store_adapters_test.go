package cluster

import (
	"context"
	"errors"
	"io/fs"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/tern/v2/migrate"
)

func TestPostgreSQLDatabaseAdapterBeginsAnInMemoryTransaction(t *testing.T) {
	row := fakeRow{}
	connection := newPostgreSQLDatabaseConnection(
		nil,
		successfulPostgreSQLConnectionAdapter(row, &fakePGXRows{}),
		successfulPostgreSQLTransactionAdapter(row),
		"runnerfoundry",
	)
	transaction, err := connection.Begin(context.Background())
	if err != nil || transaction == nil {
		t.Fatalf("begin = %#v, %v", transaction, err)
	}
}

func TestPostgreSQLDatabaseAdapterDelegatesOperationsInMemory(t *testing.T) {
	failure := errors.New("failed")
	row := fakeRow{}
	rows := &fakePGXRows{}
	adapter := successfulPostgreSQLConnectionAdapter(row, rows)
	adapter.close = func(*pgx.Conn, context.Context) error { return failure }
	connection := newPostgreSQLDatabaseConnection(nil, adapter, successfulPostgreSQLTransactionAdapter(row), "runnerfoundry")
	if err := connection.Close(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("close error = %v", err)
	}
	if tag, execErr := connection.Exec(context.Background(), "UPDATE"); execErr != nil || tag.RowsAffected() != 1 {
		t.Fatalf("exec tag=%v error=%v", tag, execErr)
	}
	queriedRows, err := connection.Query(context.Background(), "SELECT")
	if err != nil || queriedRows == nil {
		t.Fatalf("query rows=%#v error=%v", queriedRows, err)
	}
	if err = connection.QueryRow(context.Background(), "SELECT").Scan(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgreSQLDatabaseAdapterWrapsBeginAndQueryFailures(t *testing.T) {
	failure := errors.New("failed")
	connection := newPostgreSQLDatabaseConnection(nil, postgreSQLConnectionAdapter{
		begin: func(*pgx.Conn, context.Context) (pgx.Tx, error) { return nil, failure },
		query: func(*pgx.Conn, context.Context, string, ...any) (pgx.Rows, error) {
			return nil, failure
		},
	}, postgreSQLTransactionAdapter{}, "runnerfoundry")
	if _, err := connection.Begin(context.Background()); err == nil {
		t.Fatal("expected begin failure")
	}
	if _, err := connection.Query(context.Background(), "SELECT"); err == nil {
		t.Fatal("expected query failure")
	}
}

func TestTransactionalRowsRetainCommitFailureForErr(t *testing.T) {
	commitFailure := errors.New("commit failed")
	rows := newTransactionalRows(
		&databaseRows{iterator: &fakePGXRows{}},
		&databaseTransaction{
			commit:   func(context.Context) error { return commitFailure },
			rollback: func(context.Context) error { return nil },
		},
		context.Background(),
	)

	if rows.Next() {
		t.Fatal("expected no rows")
	}
	if err := rows.Err(); !errors.Is(err, commitFailure) {
		t.Fatalf("rows err = %v, want commit failure", err)
	}
}

func TestTransactionalRowsRetainRollbackFailureForErr(t *testing.T) {
	scanFailure := errors.New("scan failed")
	rollbackFailure := errors.New("rollback failed")
	rows := newTransactionalRows(
		&databaseRows{iterator: &scriptedRowIterator{next: []bool{true, false}, scanErr: scanFailure}},
		&databaseTransaction{
			commit:   func(context.Context) error { return nil },
			rollback: func(context.Context) error { return rollbackFailure },
		},
		context.Background(),
	)

	if !rows.Next() {
		t.Fatal("expected a row")
	}
	if err := rows.Scan(); !errors.Is(err, scanFailure) {
		t.Fatalf("scan err = %v, want scan failure", err)
	}
	if err := rows.Err(); !errors.Is(err, rollbackFailure) || !errors.Is(err, scanFailure) {
		t.Fatalf("rows err = %v, want scan and rollback failures", err)
	}
}

func TestPostgreSQLTransactionAdapterDelegatesEveryOperationInMemory(t *testing.T) {
	row := fakeRow{}
	transaction := newPostgreSQLTransaction(nil, successfulPostgreSQLTransactionAdapter(row))
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec(context.Background(), "UPDATE"); err != nil {
		t.Fatal(err)
	}
	if err := transaction.QueryRow(context.Background(), "SELECT").Scan(); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPostgreSQLLeaderAdapterDelegatesEveryOperationInMemory(t *testing.T) {
	row := fakeRow{}
	adapter := postgreSQLConnectionAdapter{
		close: func(*pgx.Conn, context.Context) error { return nil },
		exec: func(*pgx.Conn, context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("SELECT 1"), nil
		},
		queryRow: func(*pgx.Conn, context.Context, string, ...any) pgx.Row { return row },
		isClosed: func(*pgx.Conn) bool { return false },
		ping:     func(*pgx.Conn, context.Context) error { return nil },
	}
	connection := newPostgreSQLLeaderConnection(nil, adapter)
	if err := connection.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Exec(context.Background(), "SELECT"); err != nil {
		t.Fatal(err)
	}
	if connection.IsClosed() {
		t.Fatal("in-memory leader connection reported closed")
	}
	if err := connection.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := connection.QueryRow(context.Background(), "SELECT").Scan(); err != nil {
		t.Fatal(err)
	}
}

func TestOperatingSystemPostgreSQLAdaptersProvideEveryDependency(t *testing.T) {
	connection := operatingSystemPostgreSQLConnectionAdapter()
	if connection.begin == nil || connection.close == nil || connection.exec == nil || connection.query == nil ||
		connection.queryRow == nil || connection.isClosed == nil || connection.ping == nil {
		t.Fatal("PostgreSQL connection adapter is incomplete")
	}
	transaction := operatingSystemPostgreSQLTransactionAdapter()
	if transaction.commit == nil || transaction.exec == nil || transaction.queryRow == nil || transaction.rollback == nil {
		t.Fatal("PostgreSQL transaction adapter is incomplete")
	}
}

func TestStoreConnectorsUseInjectedParserAndConnectorWithoutNetwork(t *testing.T) {
	originalResolver := productionConnectionSettingResolver
	t.Cleanup(func() { productionConnectionSettingResolver = originalResolver })
	productionConnectionSettingResolver = testConnectionResolver(nil)
	store := NewStore(Settings{Role: "runnerfoundry"})
	store.connectConfig = func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error) { return &pgx.Conn{}, nil }
	store.connectionAdapter = successfulPostgreSQLConnectionAdapter(fakeRow{}, &fakePGXRows{})
	store.transactionAdapter = successfulPostgreSQLTransactionAdapter(fakeRow{})
	if connection, err := store.connectPostgreSQL(context.Background()); err != nil || connection == nil {
		t.Fatalf("primary connection = %#v, %v", connection, err)
	}
	if connection, err := store.connectDirectPostgreSQL(context.Background()); err != nil || connection == nil {
		t.Fatalf("direct connection = %#v, %v", connection, err)
	}
	if connection, err := store.directConnector(context.Background()); err != nil || connection == nil {
		t.Fatalf("leader connection = %#v, %v", connection, err)
	}
}

func TestStoreConnectorsStripRoleFromStartupAndAssumeItWithSQL(t *testing.T) {
	originalResolver := productionConnectionSettingResolver
	t.Cleanup(func() { productionConnectionSettingResolver = originalResolver })
	productionConnectionSettingResolver = testConnectionResolver(nil)
	store := NewStore(Settings{DSN: "postgres://login@pool/runnerfoundry?role=wrong", Role: "runnerfoundry"})

	var primaryConfig *pgx.ConnConfig
	var directConfig *pgx.ConnConfig
	connectCalls := 0
	store.connectConfig = func(_ context.Context, config *pgx.ConnConfig) (*pgx.Conn, error) {
		connectCalls++
		if connectCalls == 1 {
			primaryConfig = config.Copy()
		} else {
			directConfig = config.Copy()
		}
		return &pgx.Conn{}, nil
	}

	executedStatements := make([]string, 0)
	directStatements := make([]string, 0)
	store.connectionAdapter = postgreSQLConnectionAdapter{
		begin: func(*pgx.Conn, context.Context) (pgx.Tx, error) { return &fakePGXTransaction{}, nil },
		close: func(*pgx.Conn, context.Context) error { return nil },
		exec: func(_ *pgx.Conn, _ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			directStatements = append(directStatements, sql)
			return pgconn.NewCommandTag("SET ROLE"), nil
		},
		query:    func(*pgx.Conn, context.Context, string, ...any) (pgx.Rows, error) { return &fakePGXRows{}, nil },
		queryRow: func(*pgx.Conn, context.Context, string, ...any) pgx.Row { return fakeRow{} },
		isClosed: func(*pgx.Conn) bool { return false },
		ping:     func(*pgx.Conn, context.Context) error { return nil },
	}
	store.transactionAdapter = postgreSQLTransactionAdapter{
		commit: func(pgx.Tx, context.Context) error { return nil },
		exec: func(_ pgx.Tx, _ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			executedStatements = append(executedStatements, sql)
			return pgconn.NewCommandTag("SET ROLE"), nil
		},
		query:    func(pgx.Tx, context.Context, string, ...any) (pgx.Rows, error) { return &fakePGXRows{}, nil },
		queryRow: func(pgx.Tx, context.Context, string, ...any) pgx.Row { return fakeRow{} },
		rollback: func(pgx.Tx, context.Context) error { return nil },
	}

	connection, err := store.connectPostgreSQL(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, present := primaryConfig.RuntimeParams["role"]; present {
		t.Fatalf("unexpected primary startup role = %q", primaryConfig.RuntimeParams["role"])
	}
	if _, err = connection.Exec(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if len(executedStatements) == 0 || executedStatements[0] != `SET LOCAL ROLE "runnerfoundry"` {
		t.Fatalf("primary role assumption = %#v", executedStatements)
	}

	if _, err = store.connectDirectPostgreSQL(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, present := directConfig.RuntimeParams["role"]; present {
		t.Fatalf("unexpected direct startup role = %q", directConfig.RuntimeParams["role"])
	}
	if len(directStatements) == 0 || directStatements[0] != `SET ROLE "runnerfoundry"` {
		t.Fatalf("direct role assumption = %#v", directStatements)
	}
}

func TestMigrationExecutorReportsTernConstructionAndCatalogFailures(t *testing.T) {
	failure := errors.New("failed")
	store := NewStore(Settings{Role: "runnerfoundry"})
	store.newTernMigrator = func(context.Context, *pgx.Conn, string) (*migrate.Migrator, error) {
		return nil, failure
	}
	connection := &postgreSQLLeaderConnection{}
	if _, err := store.newMigrationExecutor(context.Background(), connection); err == nil {
		t.Fatal("expected Tern construction failure")
	}
	store.newTernMigrator = func(context.Context, *pgx.Conn, string) (*migrate.Migrator, error) {
		return &migrate.Migrator{}, nil
	}
	store.migrationFiles = emptyFileSystem{}
	if _, err := store.newMigrationExecutor(context.Background(), connection); err == nil {
		t.Fatal("expected migration catalog failure")
	}
	store.directConnector = func(context.Context) (leaderConnection, error) {
		return &fakeDatabaseConnection{}, nil
	}
	if _, err := store.MigrationStatus(context.Background()); err == nil {
		t.Fatal("expected migration status catalog failure")
	}
}

func successfulPostgreSQLTransactionAdapter(row pgx.Row) postgreSQLTransactionAdapter {
	return postgreSQLTransactionAdapter{
		commit: func(pgx.Tx, context.Context) error { return nil },
		exec: func(pgx.Tx, context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
		query:    func(pgx.Tx, context.Context, string, ...any) (pgx.Rows, error) { return &fakePGXRows{}, nil },
		queryRow: func(pgx.Tx, context.Context, string, ...any) pgx.Row { return row },
		rollback: func(pgx.Tx, context.Context) error { return nil },
	}
}

func successfulPostgreSQLConnectionAdapter(row pgx.Row, rows pgx.Rows) postgreSQLConnectionAdapter {
	return postgreSQLConnectionAdapter{
		begin: func(*pgx.Conn, context.Context) (pgx.Tx, error) { return &fakePGXTransaction{}, nil },
		close: func(*pgx.Conn, context.Context) error { return nil },
		exec: func(*pgx.Conn, context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
		query:    func(*pgx.Conn, context.Context, string, ...any) (pgx.Rows, error) { return rows, nil },
		queryRow: func(*pgx.Conn, context.Context, string, ...any) pgx.Row { return row },
	}
}

type emptyFileSystem struct{}

func (emptyFileSystem) Open(string) (fs.File, error) { return nil, fs.ErrNotExist }

type fakePGXTransaction struct{ pgx.Tx }

type scriptedRowIterator struct {
	next    []bool
	current int
	scanErr error
}

func (*scriptedRowIterator) Close() {}

func (*scriptedRowIterator) Err() error { return nil }

func (iterator *scriptedRowIterator) Next() bool {
	if iterator.current >= len(iterator.next) {
		return false
	}
	result := iterator.next[iterator.current]
	iterator.current++
	return result
}

func (iterator *scriptedRowIterator) Scan(...any) error {
	return iterator.scanErr
}

type fakePGXRows struct{}

func (*fakePGXRows) Close()                                       {}
func (*fakePGXRows) Err() error                                   { return nil }
func (*fakePGXRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (*fakePGXRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (*fakePGXRows) Next() bool                                   { return false }
func (*fakePGXRows) Scan(...any) error                            { return nil }
func (*fakePGXRows) Values() ([]any, error)                       { return nil, nil }
func (*fakePGXRows) RawValues() [][]byte                          { return nil }
func (*fakePGXRows) Conn() *pgx.Conn                              { return nil }
