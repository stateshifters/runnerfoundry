package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func TestSharedBackendMechanismsDelegateToTheSQLiteProtocol(t *testing.T) {
	result := NewResult(func() (int64, error) { return 1, nil })
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("rows = %d, error = %v", rows, err)
	}
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
	rows := NewRows(func() error { return nil }, func() error { return nil }, func() bool { return false }, func(...any) error { return nil })
	transaction := NewTransaction(successfulExecute, func(context.Context, string, ...any) (Rows, error) { return rows, nil },
		func(context.Context, string, ...any) Row { return row }, successfulCompletion, successfulCompletion)
	executor := NewExecutor(func(context.Context) (Transaction, error) { return transaction, nil },
		func(context.Context, string, ...any) Row { return row })
	store := NewStore(executor)
	if store.Backend().QueryRow(context.Background(), "statement").Scan(&value) != nil || rows.Next() || rows.Err() != nil || rows.Close() != nil {
		t.Fatal("shared backend protocol did not delegate")
	}
}

func TestSharedBackendMechanismPreservesNeutralQueueRules(t *testing.T) {
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request-3"}, RunnerPool: "linux", EnqueuedAt: fixedTime()}
	transaction := Transaction{execute: successfulExecute, queryRow: constantRow(rowFromValues(queuedJobValues())),
		commit: successfulCompletion, rollback: successfulCompletion}
	result, err := EnqueueInTransaction(context.Background(), transaction, request)
	if err != nil || result.Outcome != core.QueueQueued {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
}
