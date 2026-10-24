package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMemberCountReportsDatabaseReadFailure(t *testing.T) {
	t.Parallel()
	database, protocol, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("member read failed")
	protocol.ExpectQuery("SELECT COUNT").WillReturnError(failure)
	count, err := NewDatabaseStore(database).CountLiveMembers(context.Background(), fixedTime())
	if count != 0 || !errors.Is(err, failure) {
		t.Fatalf("member count = %d, %v", count, err)
	}
	if err := protocol.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMemberPublicationRollsBackUnreliableAffectedCount(t *testing.T) {
	t.Parallel()
	database, protocol, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("affected count unavailable")
	protocol.ExpectBegin()
	protocol.ExpectExec("INSERT").WillReturnResult(sqlmock.NewErrorResult(failure))
	protocol.ExpectRollback()
	err = NewDatabaseStore(database).PublishMemberState(context.Background(), testMemberID, fixedTime(), nil)
	if !errors.Is(err, failure) {
		t.Fatalf("unreliable publication = %v", err)
	}
	if err := protocol.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAdapterTransactionCommitsACompleteSynchronousRead(t *testing.T) {
	t.Parallel()
	database, protocol, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	protocol.ExpectBegin()
	protocol.ExpectQuery("SELECT registration").WillReturnRows(sqlmock.NewRows([]string{"registration"}).AddRow("registered"))
	protocol.ExpectCommit()
	var registration string
	err = NewDatabaseStore(database).AdapterExtension().WithinTransaction(context.Background(), func(transaction Transaction) error {
		readErr := transaction.ReadRow(context.Background(), "SELECT registration FROM adapter_state", func(row Scanner) error {
			return row.Scan(&registration)
		})
		if readErr != nil {
			return fmt.Errorf("read registration: %w", readErr)
		}
		return nil
	})
	if err != nil || registration != "registered" {
		t.Fatalf("adapter registration = %q, %v", registration, err)
	}
	if err := protocol.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalRemovalReportsUnreadableOwnership(t *testing.T) {
	t.Parallel()
	database, protocol, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("ownership read failed")
	protocol.ExpectBegin()
	protocol.ExpectExec("DELETE").WillReturnResult(sqlmock.NewResult(0, 0))
	protocol.ExpectQuery("SELECT").WillReturnError(failure)
	protocol.ExpectRollback()
	result, err := NewDatabaseStore(database).RemoveTerminal(context.Background(), testJobID)
	if result.Outcome != "" || !errors.Is(err, failure) {
		t.Fatalf("terminal removal = %+v, %v", result, err)
	}
	if err := protocol.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessingSnapshotRejectsMalformedPersistedJobs(t *testing.T) {
	t.Parallel()
	database, protocol, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	protocol.ExpectBegin()
	protocol.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"invalid-column"}).AddRow("invalid"))
	protocol.ExpectRollback()
	jobs, err := NewDatabaseStore(database).ProcessingByPool(context.Background(), "linux")
	if jobs != nil || err == nil {
		t.Fatalf("malformed processing snapshot = %+v, %v", jobs, err)
	}
	if err := protocol.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
