package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestTransactionalRowsRetainCommitFailureForErr(t *testing.T) {
	commitFailure := errors.New("commit failed")
	rows := newTransactionalRows(
		context.Background(),
		&databaseRows{iterator: &fakePGXRows{}},
		&databaseTransaction{
			commit:   func(context.Context) error { return commitFailure },
			rollback: func(context.Context) error { return nil },
		},
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
		context.Background(),
		&databaseRows{iterator: &scriptedRowIterator{next: []bool{true, false}, scanErr: scanFailure}},
		&databaseTransaction{
			commit:   func(context.Context) error { return nil },
			rollback: func(context.Context) error { return rollbackFailure },
		},
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

func TestStoreConnectorsStripRoleFromStartupAndAssumeItWithSQL(t *testing.T) {
	originalResolver := productionConnectionSettingResolver
	t.Cleanup(func() { productionConnectionSettingResolver = originalResolver })
	productionConnectionSettingResolver = testConnectionResolver(nil)
	store := NewStore(Settings{DSN: "postgres://login@pool/runnerfoundry?role=wrong", Role: "runnerfoundry"})
	statements := configureRoleAssumptionStore(store)

	var primaryConfig *pgx.ConnConfig
	var directConfig *pgx.ConnConfig
	store.connectConfig = captureStoreConfigs(&primaryConfig, &directConfig)

	connection, err := store.connectPostgreSQL(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertPrimaryRoleAssumption(t, primaryConfig, connection, statements)

	if _, err = store.connectDirectPostgreSQL(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertDirectRoleAssumption(t, directConfig, statements)
}

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

func captureStoreConfigs(primaryConfig **pgx.ConnConfig, directConfig **pgx.ConnConfig) func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error) {
	connectCalls := 0
	return func(_ context.Context, config *pgx.ConnConfig) (*pgx.Conn, error) {
		connectCalls++
		if connectCalls == 1 {
			*primaryConfig = config.Copy()
		} else {
			*directConfig = config.Copy()
		}
		return &pgx.Conn{}, nil
	}
}

type roleAssumptionStatements struct {
	primary []string
	direct  []string
}

func assertPrimaryRoleAssumption(
	t *testing.T,
	primaryConfig *pgx.ConnConfig,
	connection *databaseConnection,
	statements *roleAssumptionStatements,
) {
	t.Helper()
	if _, present := primaryConfig.RuntimeParams["role"]; present {
		t.Fatalf("unexpected primary startup role = %q", primaryConfig.RuntimeParams["role"])
	}
	if _, err := connection.Exec(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if len(statements.primary) == 0 || statements.primary[0] != `SET LOCAL ROLE "runnerfoundry"` {
		t.Fatalf("primary role assumption = %#v", statements.primary)
	}
}

func assertDirectRoleAssumption(t *testing.T, directConfig *pgx.ConnConfig, statements *roleAssumptionStatements) {
	t.Helper()
	if _, present := directConfig.RuntimeParams["role"]; present {
		t.Fatalf("unexpected direct startup role = %q", directConfig.RuntimeParams["role"])
	}
	if len(statements.direct) == 0 || statements.direct[0] != `SET ROLE "runnerfoundry"` {
		t.Fatalf("direct role assumption = %#v", statements.direct)
	}
}

func configureRoleAssumptionStore(store *Store) *roleAssumptionStatements {
	statements := &roleAssumptionStatements{
		primary: make([]string, 0),
		direct:  make([]string, 0),
	}
	store.connectionAdapter = postgreSQLConnectionAdapter{
		begin: func(*pgx.Conn, context.Context) (pgx.Tx, error) { return &fakePGXTransaction{}, nil },
		close: func(*pgx.Conn, context.Context) error { return nil },
		exec: func(_ *pgx.Conn, _ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			statements.direct = append(statements.direct, sql)
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
			statements.primary = append(statements.primary, sql)
			return pgconn.NewCommandTag("SET ROLE"), nil
		},
		query:    func(pgx.Tx, context.Context, string, ...any) (pgx.Rows, error) { return &fakePGXRows{}, nil },
		queryRow: func(pgx.Tx, context.Context, string, ...any) pgx.Row { return fakeRow{} },
		rollback: func(pgx.Tx, context.Context) error { return nil },
	}
	return statements
}
