package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
)

func TestEnqueueReportsEveryProtocolFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
	stores := []*Store{
		NewStore(Executor{begin: failingBegin(protocolErr)}),
		NewStore(Executor{begin: transactionBeginning(transactionFailingExecute(1, protocolErr))}),
		NewStore(Executor{begin: transactionBeginning(enqueueRowsAffectedError(protocolErr))}),
		NewStore(Executor{begin: transactionBeginning(enqueueTransaction(
			Row{scan: func(...any) error { return protocolErr }}, nil))}),
		NewStore(Executor{begin: transactionBeginning(enqueueTransaction(
			rowFromValues(fullSQLiteJobValues(fixedTime())), protocolErr))}),
	}
	for index, store := range stores {
		if _, _, err := store.Enqueue(context.Background(), request); err == nil {
			t.Fatalf("case %d: expected enqueue error", index)
		}
	}
	noRows := Transaction{queryRow: func(context.Context, string, ...any) Row {
		return Row{scan: func(...any) error { return sql.ErrNoRows }}
	}}
	if _, _, err := loadEnqueuedJob(context.Background(), noRows, request.Source, 1); err == nil {
		t.Fatal("expected missing inserted job error")
	}
	if _, _, err := loadEnqueuedJob(context.Background(), noRows, request.Source, 0); err == nil {
		t.Fatal("expected receipt lookup error")
	}
}

func enqueueRowsAffectedError(protocolErr error) Transaction {
	return Transaction{execute: func(context.Context, string, ...any) (Result, error) {
		return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
	}, rollback: successfulCompletion}
}

func enqueueTransaction(row Row, commitErr error) Transaction {
	return Transaction{execute: successfulExecute, queryRow: func(context.Context, string, ...any) Row { return row },
		commit: func(context.Context) error { return commitErr }, rollback: successfulCompletion}
}

func TestClaimReportsEmptyQueue(t *testing.T) {
	t.Parallel()
	store := NewStore(Executor{queryRow: constantRow(Row{scan: func(...any) error { return sql.ErrNoRows }})})
	_, claimed, err := store.ClaimOldest(context.Background(), "linux", testMemberID, fixedTime())
	if err != nil || claimed {
		t.Fatalf("empty queue = %t, %v", claimed, err)
	}
}

func TestClaimReportsProtocolError(t *testing.T) {
	t.Parallel()
	store := NewStore(Executor{queryRow: constantRow(Row{scan: func(...any) error { return errors.New("protocol") }})})
	if _, _, err := store.ClaimOldest(context.Background(), "linux", testMemberID, fixedTime()); err == nil {
		t.Fatal("expected claim error")
	}
}

func TestClaimReturnsOldestJob(t *testing.T) {
	t.Parallel()
	store := NewStore(Executor{queryRow: constantRow(rowFromValues(fullSQLiteJobValues(fixedTime())))})
	record, claimed, err := store.ClaimOldest(context.Background(), "linux", testMemberID, fixedTime())
	if err != nil || !claimed || record.Job.ID != testJobID {
		t.Fatalf("claim = %#v, %t, %v", record, claimed, err)
	}
}

func TestChangedJobAndResultPendingReportProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	broken := NewStore(Executor{queryRow: constantRow(Row{scan: func(...any) error { return protocolErr }})})
	if _, err := broken.MarkStartedBySource(context.Background(), core.SourceReference{}, fixedTime()); err == nil {
		t.Fatal("expected mark-started error")
	}
	if _, err := broken.ConfirmAcquisition(context.Background(), testJobID, fixedTime()); err == nil {
		t.Fatal("expected changed-job scan error")
	}
	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService}
	stores := []*Store{
		NewStore(Executor{begin: failingBegin(protocolErr)}),
		resultPendingErrorStore(Row{scan: func(...any) error { return protocolErr }}, Row{}, nil),
		resultPendingErrorStore(Row{scan: func(...any) error { return sql.ErrNoRows }},
			Row{scan: func(...any) error { return protocolErr }}, nil),
		resultPendingErrorStore(rowFromValues(fullSQLiteJobValues(fixedTime())), Row{}, protocolErr),
	}
	for index, store := range stores {
		if _, err := store.SetResultPending(context.Background(), testJobID, result); err == nil {
			t.Fatalf("case %d: expected result-pending error", index)
		}
	}
}

func TestRollbackHelperToleratesRollbackFailureAndClosedTransaction(t *testing.T) {
	t.Parallel()
	open := true
	rollback(context.Background(), Transaction{rollback: failingCompletion(errors.New("protocol"))}, &open)
	open = false
	rollback(context.Background(), Transaction{}, &open)
}

func resultPendingErrorStore(first Row, second Row, commitErr error) *Store {
	calls := 0
	transaction := Transaction{queryRow: func(context.Context, string, ...any) Row {
		calls++
		if calls == 1 {
			return first
		}
		return second
	}, commit: func(context.Context) error { return commitErr }, rollback: successfulCompletion}
	return NewStore(Executor{begin: transactionBeginning(transaction)})
}

func TestCleanupOperationsReportEveryProtocolFailure(t *testing.T) {
	t.Parallel()
	operations := []func(*Store) error{
		func(store *Store) error {
			_, err := store.SetMemberAttemptsResultPending(context.Background(), testMemberID, core.Result{})
			return err
		},
		func(store *Store) error {
			_, err := store.SetUnstartedAttemptsResultPending(context.Background(), "linux", fixedTime(), core.Result{})
			return err
		},
	}
	for _, operation := range operations {
		assertCleanupFailures(t, operation)
	}
}

func assertCleanupFailures(t *testing.T, operation func(*Store) error) {
	t.Helper()
	protocolErr := errors.New("protocol")
	transactions := []Transaction{
		{query: func(context.Context, string, ...any) (Rows, error) { return Rows{}, protocolErr }, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (Rows, error) {
			return rowsWithFailure(protocolErr, nil, nil), nil
		}, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (Rows, error) { return rowsFromValues(nil), nil },
			commit: failingCompletion(protocolErr), rollback: successfulCompletion},
	}
	if err := operation(NewStore(Executor{begin: failingBegin(protocolErr)})); err == nil {
		t.Fatal("expected cleanup begin error")
	}
	for index, transaction := range transactions {
		if err := operation(NewStore(Executor{begin: transactionBeginning(transaction)})); err == nil {
			t.Fatalf("case %d: expected cleanup error", index)
		}
	}
}

func TestFinalizeReportsEveryProtocolFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	pendingValues := fullSQLiteJobValues(fixedTime())
	pendingValues[4] = "result_pending"
	stores := []*Store{
		NewStore(Executor{begin: failingBegin(protocolErr)}),
		finalizeErrorStore(Row{scan: func(...any) error { return protocolErr }}, 0, nil),
		finalizeErrorStore(rowFromValues(queuedSQLiteValues()), 0, nil),
		finalizeErrorStore(rowFromValues(pendingValues), 1, nil),
		finalizeErrorStore(rowFromValues(pendingValues), 2, nil),
		finalizeErrorStore(rowFromValues(pendingValues), 0, protocolErr),
	}
	for index, store := range stores {
		if _, err := store.Finalize(context.Background(), testJobID, core.ResultDeliveryReported); err == nil {
			t.Fatalf("case %d: expected finalize error", index)
		}
	}
}

func finalizeErrorStore(row Row, executeFailure int, commitErr error) *Store {
	transaction := transactionFailingExecute(executeFailure, errors.New("protocol"))
	if executeFailure == 0 {
		transaction.execute = successfulExecute
	}
	transaction.queryRow = func(context.Context, string, ...any) Row { return row }
	transaction.commit = func(context.Context) error { return commitErr }
	return NewStore(Executor{begin: transactionBeginning(transaction)})
}

func queuedSQLiteValues() []any {
	values := fullSQLiteJobValues(fixedTime())
	values[4] = "queued"
	values[11] = sql.NullString{}
	return values
}

func constantRow(row Row) func(context.Context, string, ...any) Row {
	return func(context.Context, string, ...any) Row { return row }
}
