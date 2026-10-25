package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestAdapterTransactionOwnsRollbackAfterCallbackFailure(t *testing.T) {
	t.Parallel()
	database, protocol, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		if unmet := protocol.ExpectationsWereMet(); unmet != nil {
			t.Error(unmet)
		}
	})
	callbackFailure := errors.New("adapter operation failed")
	rollbackFailure := errors.New("rollback failed")
	protocol.ExpectBegin()
	protocol.ExpectExec("INSERT INTO adapter_state").WillReturnResult(sqlmock.NewResult(0, 1))
	protocol.ExpectRollback().WillReturnError(rollbackFailure)
	protocol.ExpectClose()
	err = NewDatabaseStore(database).AdapterExtension().WithinTransaction(context.Background(), func(transaction Transaction) error {
		if _, executeErr := transaction.Execute(context.Background(), "INSERT INTO adapter_state VALUES (?)", "registration"); executeErr != nil {
			return fmt.Errorf("write registration: %w", executeErr)
		}
		return callbackFailure
	})
	if !errors.Is(err, callbackFailure) || !errors.Is(err, rollbackFailure) {
		t.Fatalf("transaction failure = %v", err)
	}
}

func TestAdapterCanceledCommitPropagatesCancellation(t *testing.T) {
	database, protocol, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	protocol.ExpectBegin()
	protocol.ExpectRollback()
	protocol.ExpectClose()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = NewDatabaseStore(database).AdapterExtension().WithinTransaction(ctx, func(transaction Transaction) error {
		cancel()
		rollbackAdapterTransaction(t, transaction)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled transaction commit = %v, want cancellation", err)
	}
}

func TestAdapterCanceledTransactionReadPropagatesCancellation(t *testing.T) {
	database, protocol, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	protocol.ExpectBegin()
	protocol.ExpectRollback()
	protocol.ExpectClose()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = NewDatabaseStore(database).AdapterExtension().WithinTransaction(ctx, func(transaction Transaction) error {
		cancel()
		rollbackAdapterTransaction(t, transaction)
		return transaction.ReadRow(context.Background(), "SELECT claim", func(row Scanner) error { return row.Scan(new(int)) })
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled transaction read = %v, want cancellation", err)
	}
}

func TestAdapterCanceledTransactionRetainsIndependentCallbackFailure(t *testing.T) {
	database, protocol, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	protocol.ExpectBegin()
	protocol.ExpectRollback()
	protocol.ExpectClose()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failure := errors.New("independent storage failure")
	err = NewDatabaseStore(database).AdapterExtension().WithinTransaction(ctx, func(transaction Transaction) error {
		cancel()
		rollbackAdapterTransaction(t, transaction)
		return failure
	})
	if !errors.Is(err, failure) || !errors.Is(err, sql.ErrTxDone) || errors.Is(err, context.Canceled) {
		t.Fatalf("independent transaction failure = %v, want storage and transaction-done causes only", err)
	}
}

func TestAdapterCanceledTransactionRetainsMixedCallbackFailure(t *testing.T) {
	database, protocol, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	protocol.ExpectBegin()
	protocol.ExpectRollback()
	protocol.ExpectClose()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failure := errors.New("independent storage failure")
	err = NewDatabaseStore(database).AdapterExtension().WithinTransaction(ctx, func(Transaction) error {
		cancel()
		return errors.Join(context.Canceled, failure)
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, failure) {
		t.Fatalf("mixed canceled transaction failure = %v, want cancellation and independent failure", err)
	}
}

func rollbackAdapterTransaction(t *testing.T, transaction Transaction) {
	t.Helper()
	adapter, ok := transaction.(transactionAdapter)
	if !ok {
		t.Fatalf("transaction adapter = %T, want transactionAdapter", transaction)
	}
	if err := adapter.transaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("roll back canceled transaction: %v", err)
	}
}
