package postgres

import (
	"context"
	"errors"
	"io/fs"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestSharedBackendMechanismsDelegateToThePostgreSQLProtocol(t *testing.T) {
	result := NewResult(func() (int64, error) { return 1, nil })
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("rows = %d, error = %v", rows, err)
	}
	row := testSharedRow(t)
	rows := NewRows(func() error { return nil }, func() error { return nil }, func() bool { return false }, func(...any) error { return nil })
	if rows.Next() || rows.Err() != nil || rows.Close() != nil {
		t.Fatal("rows protocol did not delegate")
	}
	assertSharedExecutor(t, row, rows)
}

func testSharedRow(t *testing.T) Row {
	t.Helper()
	row := NewRow(func(destinations ...any) error {
		destination, ok := destinations[0].(*string)
		if !ok {
			return errors.New("destination type")
		}
		*destination = "value"
		return nil
	})
	var value string
	if err := row.Scan(&value); err != nil || value != "value" {
		t.Fatalf("value = %q, error = %v", value, err)
	}
	return row
}

func assertSharedExecutor(t *testing.T, row Row, rows Rows) {
	t.Helper()
	transaction := NewTransaction(successfulExecute, func(context.Context, string, ...any) (Rows, error) { return rows, nil },
		func(context.Context, string, ...any) Row { return row }, successfulCompletion, successfulCompletion)
	executor := NewExecutor(func(context.Context) (Transaction, error) { return transaction, nil },
		func(context.Context, string, ...any) Row { return row })
	if _, err := executor.Begin(context.Background()); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := executor.QueryRow(context.Background(), "statement").Scan(&value); err != nil {
		t.Fatal(err)
	}
}

func TestSharedBackendMechanismsPreserveNeutralQueueAndLockRules(t *testing.T) {
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux", EnqueuedAt: fixedTime()}
	transaction := &recordedTransaction{rows: []Row{rowFromValues(queuedJobValues(testJobID1, request))}}
	adapted, err := transaction.executor().Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, outcome, err := EnqueueInTransaction(context.Background(), adapted, request); err != nil || outcome != core.QueueQueued {
		t.Fatalf("outcome = %q, error = %v", outcome, err)
	}
	if err := SerializeDemandScope(context.Background(), adapted, core.DemandScope{Adapter: "github", ID: "scope"}); err != nil {
		t.Fatal(err)
	}
	if err := SerializeSource(context.Background(), adapted, request.Source); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationsExposeOwnerLocalFiles(t *testing.T) {
	entries, err := fs.ReadDir(Migrations(), ".")
	if err != nil || len(entries) != 5 {
		t.Fatalf("entries = %d, error = %v", len(entries), err)
	}
}

func TestMigrationsRejectInvalidAndMissingNames(t *testing.T) {
	for _, name := range []string{"../migration.sql", "999_missing.sql"} {
		if _, err := Migrations().Open(name); err == nil {
			t.Fatalf("open %q succeeded", name)
		}
	}
}
