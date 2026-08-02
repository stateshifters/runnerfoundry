package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestNewPostgreSQLTransactionWrapsQueryFailure(t *testing.T) {
	queryFailure := errors.New("query failed")
	transaction := newPostgreSQLTransaction(nil, postgreSQLTransactionAdapter{
		query: func(pgx.Tx, context.Context, string, ...any) (pgx.Rows, error) {
			return nil, queryFailure
		},
	})

	_, err := transaction.Query(context.Background(), "SELECT 1")
	if !errors.Is(err, queryFailure) {
		t.Fatalf("transaction query error = %v, want query failure", err)
	}
}

func TestRoleScopedTransactionStarterReportsBeginFailure(t *testing.T) {
	beginFailure := errors.New("begin failed")
	beginWithRole := newRoleScopedTransactionStarter(
		nil,
		postgreSQLConnectionAdapter{
			begin: func(*pgx.Conn, context.Context) (pgx.Tx, error) { return nil, beginFailure },
		},
		postgreSQLTransactionAdapter{},
		"runnerfoundry",
	)

	_, err := beginWithRole(context.Background())
	if !errors.Is(err, beginFailure) {
		t.Fatalf("begin with role error = %v, want begin failure", err)
	}
}

func TestRoleScopedTransactionStarterReportsRoleFailureAndRollback(t *testing.T) {
	roleFailure := errors.New("role failed")
	rollbackFailure := errors.New("rollback failed")
	beginWithRole := newRoleScopedTransactionStarter(
		nil,
		postgreSQLConnectionAdapter{
			begin: func(*pgx.Conn, context.Context) (pgx.Tx, error) { return &fakePGXTransaction{}, nil },
		},
		postgreSQLTransactionAdapter{
			exec: func(pgx.Tx, context.Context, string, ...any) (pgconn.CommandTag, error) {
				return pgconn.CommandTag{}, roleFailure
			},
			rollback: func(pgx.Tx, context.Context) error { return rollbackFailure },
		},
		"runnerfoundry",
	)

	_, err := beginWithRole(context.Background())
	if !errors.Is(err, roleFailure) || !errors.Is(err, rollbackFailure) {
		t.Fatalf("begin with role error = %v, want role and rollback failures", err)
	}
}

func TestRoleScopedStatementExecutorReportsBeginFailure(t *testing.T) {
	beginFailure := errors.New("begin failed")
	executor := roleScopedStatementExecutor(
		func(context.Context) (*databaseTransaction, error) { return nil, beginFailure },
	)

	if _, err := executor(context.Background(), "SELECT 1"); !errors.Is(err, beginFailure) {
		t.Fatalf("statement executor begin error = %v, want begin failure", err)
	}
}

func TestRoleScopedStatementExecutorReportsExecFailureAndRollback(t *testing.T) {
	execFailure := errors.New("exec failed")
	rollbackFailure := errors.New("rollback failed")
	executor := roleScopedStatementExecutor(
		func(context.Context) (*databaseTransaction, error) {
			return &databaseTransaction{
				exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
					return pgconn.CommandTag{}, execFailure
				},
				commit:   func(context.Context) error { return nil },
				rollback: func(context.Context) error { return rollbackFailure },
			}, nil
		},
	)

	if _, err := executor(context.Background(), "SELECT 1"); !errors.Is(err, execFailure) || !errors.Is(err, rollbackFailure) {
		t.Fatalf("statement executor exec error = %v, want exec and rollback failures", err)
	}
}

func TestRoleScopedStatementExecutorReportsCommitFailure(t *testing.T) {
	commitFailure := errors.New("commit failed")
	executor := roleScopedStatementExecutor(
		func(context.Context) (*databaseTransaction, error) {
			return &databaseTransaction{
				exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
					return pgconn.NewCommandTag("UPDATE 1"), nil
				},
				commit:   func(context.Context) error { return commitFailure },
				rollback: func(context.Context) error { return nil },
			}, nil
		},
	)

	if _, err := executor(context.Background(), "SELECT 1"); !errors.Is(err, commitFailure) {
		t.Fatalf("statement executor commit error = %v, want commit failure", err)
	}
}

func TestRoleScopedRowsQueryRollsBackQueryFailure(t *testing.T) {
	queryFailure := errors.New("query failed")
	rollbackFailure := errors.New("rollback failed")
	query := roleScopedRowsQuery(
		func(context.Context) (*databaseTransaction, error) {
			return &databaseTransaction{
				query: func(context.Context, string, ...any) (*databaseRows, error) {
					return nil, queryFailure
				},
				rollback: func(context.Context) error { return rollbackFailure },
			}, nil
		},
	)

	_, err := query(context.Background(), "SELECT 1")
	if !errors.Is(err, queryFailure) || !errors.Is(err, rollbackFailure) {
		t.Fatalf("rows query error = %v, want query and rollback failures", err)
	}
}

func TestRoleScopedRowQueryReturnsBeginFailureAndSingleUseScanner(t *testing.T) {
	beginFailure := errors.New("begin failed")
	queryRow := roleScopedRowQuery(
		func(context.Context) (*databaseTransaction, error) { return nil, beginFailure },
	)

	if err := queryRow(context.Background(), "SELECT 1").Scan(); !errors.Is(err, beginFailure) {
		t.Fatalf("row query error = %v, want begin failure", err)
	}

	finalizeCalls := 0
	rowScanner := &transactionalRowScanner{
		row: fakeRow{},
		finalize: func(resultErr error) error {
			finalizeCalls++
			return resultErr
		},
	}

	if err := rowScanner.Scan(); err != nil {
		t.Fatalf("first row scan error = %v", err)
	}
	if err := rowScanner.Scan(); err == nil {
		t.Fatal("expected repeated row scan failure")
	}
	if finalizeCalls != 1 {
		t.Fatalf("finalize calls = %d, want one", finalizeCalls)
	}
}

func TestTransactionalRowsCloseFinalizesCommitFailure(t *testing.T) {
	commitFailure := errors.New("commit failed")
	rows := newTransactionalRows(
		context.Background(),
		&databaseRows{iterator: &fakePGXRows{}},
		&databaseTransaction{
			commit:   func(context.Context) error { return commitFailure },
			rollback: func(context.Context) error { return nil },
		},
	)

	rows.Close()
	if err := rows.Err(); !errors.Is(err, commitFailure) {
		t.Fatalf("rows close error = %v, want commit failure", err)
	}
}

func TestConnectPostgreSQLRequiresRole(t *testing.T) {
	originalResolver := productionConnectionSettingResolver
	t.Cleanup(func() { productionConnectionSettingResolver = originalResolver })
	productionConnectionSettingResolver = testConnectionResolver(nil)

	store := NewStore(Settings{})
	if _, err := store.connectPostgreSQL(context.Background()); err == nil {
		t.Fatal("expected missing role failure for primary connector")
	}
}

func TestConnectDirectPostgreSQLAttemptsCleanupAfterRoleFailure(t *testing.T) {
	originalResolver := productionConnectionSettingResolver
	t.Cleanup(func() { productionConnectionSettingResolver = originalResolver })
	productionConnectionSettingResolver = testConnectionResolver(nil)

	closeCalls := 0
	store := redactedDirectConnectionStore(
		errors.New("driver: password=secret host=db.internal"),
		errors.New("close: endpoint postgres://secret@db.internal/runnerfoundry"),
		&closeCalls,
	)

	_, err := store.connectDirectPostgreSQL(context.Background())
	if err == nil {
		t.Fatal("expected role assumption failure")
	}
	if err.Error() != "assume PostgreSQL role" {
		t.Fatalf("direct connector error = %v, want generic role assumption failure", err)
	}
	if closeCalls != 1 {
		t.Fatalf("close calls = %d, want one cleanup attempt", closeCalls)
	}
}

func TestConnectDirectPostgreSQLRedactsRoleFailureDetails(t *testing.T) {
	store := redactedDirectConnectionStore(
		errors.New("driver: password=secret host=db.internal"),
		errors.New("close: endpoint postgres://secret@db.internal/runnerfoundry"),
		nil,
	)

	_, err := store.connectDirectPostgreSQL(context.Background())
	if err == nil {
		t.Fatal("expected role assumption failure")
	}
	assertConnectionErrorOmits(t, err.Error(), "secret", "db.internal", "postgres://", "password=")
}

func assertConnectionErrorOmits(t *testing.T, errorText string, forbiddenValues ...string) {
	t.Helper()
	for _, forbiddenValue := range forbiddenValues {
		if strings.Contains(errorText, forbiddenValue) {
			t.Fatalf("connection error exposed %q: %v", forbiddenValue, errorText)
		}
	}
}

func redactedDirectConnectionStore(roleFailure error, closeFailure error, closeCalls *int) *Store {
	store := NewStore(Settings{Role: "runnerfoundry"})
	store.connectConfig = func(context.Context, *pgx.ConnConfig) (*pgx.Conn, error) { return &pgx.Conn{}, nil }
	store.connectionAdapter = postgreSQLConnectionAdapter{
		begin: func(*pgx.Conn, context.Context) (pgx.Tx, error) { return &fakePGXTransaction{}, nil },
		close: func(*pgx.Conn, context.Context) error {
			if closeCalls != nil {
				*closeCalls = *closeCalls + 1
			}
			return closeFailure
		},
		exec: func(*pgx.Conn, context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, roleFailure
		},
		query:    func(*pgx.Conn, context.Context, string, ...any) (pgx.Rows, error) { return &fakePGXRows{}, nil },
		queryRow: func(*pgx.Conn, context.Context, string, ...any) pgx.Row { return fakeRow{} },
		isClosed: func(*pgx.Conn) bool { return false },
		ping:     func(*pgx.Conn, context.Context) error { return nil },
	}
	return store
}
