package sqlite

import (
	"context"
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
		protocol.ExpectClose()
		if closeErr := database.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	callbackFailure := errors.New("adapter operation failed")
	rollbackFailure := errors.New("rollback failed")
	protocol.ExpectBegin()
	protocol.ExpectExec("INSERT INTO adapter_state").WillReturnResult(sqlmock.NewResult(0, 1))
	protocol.ExpectRollback().WillReturnError(rollbackFailure)
	err = NewDatabaseStore(database).AdapterExtension().WithinTransaction(context.Background(), func(transaction Transaction) error {
		if _, executeErr := transaction.Execute(context.Background(), "INSERT INTO adapter_state VALUES (?)", "registration"); executeErr != nil {
			return fmt.Errorf("write registration: %w", executeErr)
		}
		return callbackFailure
	})
	if !errors.Is(err, callbackFailure) || !errors.Is(err, rollbackFailure) {
		t.Fatalf("transaction failure = %v", err)
	}
	if err := protocol.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
