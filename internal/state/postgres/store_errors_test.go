package postgres

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
	for index, buildStore := range enqueueFailureStores(protocolErr) {
		if _, _, err := buildStore().Enqueue(context.Background(), request); err == nil {
			t.Fatalf("case %d: expected enqueue error", index)
		}
	}
}

func enqueueFailureStores(protocolErr error) []func() *Store {
	return []func() *Store{
		func() *Store { return NewStore(Executor{begin: failingBegin(protocolErr)}) },
		func() *Store {
			return NewStore(Executor{begin: transactionBeginning(transactionFailingExecute(1, protocolErr))})
		},
		func() *Store {
			return NewStore(Executor{begin: transactionBeginning(transactionFailingExecute(2, protocolErr))})
		},
		func() *Store {
			transaction := enqueueTransaction(rowFromValues(fullPostgreSQLJobValues(fixedTime())), nil)
			transaction.execute = func(context.Context, string, ...any) (Result, error) {
				return Result{rowsAffected: func() (int64, error) { return 0, protocolErr }}, nil
			}
			return NewStore(Executor{begin: transactionBeginning(transaction)})
		},
		func() *Store {
			return NewStore(Executor{begin: transactionBeginning(enqueueTransaction(
				Row{scan: func(...any) error { return protocolErr }}, nil))})
		},
		func() *Store {
			return NewStore(Executor{begin: transactionBeginning(enqueueTransaction(
				rowFromValues(fullPostgreSQLJobValues(fixedTime())), protocolErr))})
		},
	}
}

func TestLoadEnqueuedJobReportsMissingJobAndReceipt(t *testing.T) {
	t.Parallel()
	request := core.JobRequest{Source: core.SourceReference{Adapter: "github", ID: "request"}, RunnerPool: "linux"}
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

func enqueueTransaction(row Row, commitErr error) Transaction {
	return Transaction{execute: successfulExecute, queryRow: func(context.Context, string, ...any) Row { return row },
		commit: func(context.Context) error { return commitErr }, rollback: successfulCompletion}
}

func TestClaimReportsEveryProtocolOutcome(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	stores := []*Store{
		NewStore(Executor{begin: failingBegin(protocolErr)}),
		NewStore(Executor{begin: transactionBeginning(transactionFailingExecute(1, protocolErr))}),
		claimErrorStore(Row{scan: func(...any) error { return sql.ErrNoRows }}, nil),
		claimErrorStore(Row{scan: func(...any) error { return protocolErr }}, nil),
		claimErrorStore(rowFromValues(fullPostgreSQLJobValues(fixedTime())), protocolErr),
	}
	for index, store := range stores {
		_, claimed, err := store.ClaimOldest(context.Background(), "linux", testMemberIDA, fixedTime())
		if index == 2 {
			if err != nil || claimed {
				t.Fatalf("empty queue = claimed %t, %v", claimed, err)
			}
			continue
		}
		if err == nil {
			t.Fatalf("case %d: expected claim error", index)
		}
	}
}

func claimErrorStore(row Row, commitErr error) *Store {
	transaction := Transaction{execute: successfulExecute, queryRow: func(context.Context, string, ...any) Row { return row },
		commit: func(context.Context) error { return commitErr }, rollback: successfulCompletion}
	return NewStore(Executor{begin: transactionBeginning(transaction)})
}

func TestChangedJobAndResultPendingReportProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	brokenRowStore := NewStore(Executor{queryRow: func(context.Context, string, ...any) Row {
		return Row{scan: func(...any) error { return protocolErr }}
	}})
	if _, err := brokenRowStore.MarkStartedBySource(context.Background(), core.SourceReference{}, fixedTime()); err == nil {
		t.Fatal("expected changed-job scan error")
	}

	result := core.Result{Outcome: core.OutcomeFailure, Origin: core.ResultOriginService}
	stores := []*Store{
		NewStore(Executor{begin: failingBegin(protocolErr)}),
		resultPendingErrorStore(Row{scan: func(...any) error { return protocolErr }}, Row{}, nil),
		resultPendingErrorStore(Row{scan: func(...any) error { return sql.ErrNoRows }},
			Row{scan: func(...any) error { return protocolErr }}, nil),
		resultPendingErrorStore(rowFromValues(fullPostgreSQLJobValues(fixedTime())), Row{}, protocolErr),
	}
	for index, store := range stores {
		if _, err := store.SetResultPending(context.Background(), testJobID3, result); err == nil {
			t.Fatalf("case %d: expected result-pending error", index)
		}
	}
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
			_, err := store.SetMemberAttemptsResultPending(context.Background(), testMemberIDA, core.Result{})
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
	pendingValues := fullPostgreSQLJobValues(fixedTime())
	pendingValues[4] = "result_pending"
	stores := []*Store{
		NewStore(Executor{begin: failingBegin(protocolErr)}),
		finalizeErrorStore(Row{scan: func(...any) error { return protocolErr }}, 0, nil),
		finalizeErrorStore(rowFromValues(queuedPostgreSQLValues()), 0, nil),
		finalizeErrorStore(rowFromValues(pendingValues), 1, nil),
		finalizeErrorStore(rowFromValues(pendingValues), 2, nil),
		finalizeErrorStore(rowFromValues(pendingValues), 3, nil),
		finalizeErrorStore(rowFromValues(pendingValues), 0, protocolErr),
	}
	for index, store := range stores {
		if _, err := store.Finalize(context.Background(), testJobID3, core.ResultDeliveryReported); err == nil {
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

func queuedPostgreSQLValues() []any {
	values := fullPostgreSQLJobValues(fixedTime())
	values[4] = "queued"
	values[11] = sql.NullString{}
	return values
}

func failingBegin(protocolErr error) func(context.Context) (Transaction, error) {
	return func(context.Context) (Transaction, error) { return Transaction{}, protocolErr }
}
