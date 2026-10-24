package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestPGXExecutorCommitsSuccessfulOperation(t *testing.T) {
	t.Parallel()
	commits, rollbacks := 0, 0
	transaction := &pgxTransactionFake{commit: func(context.Context) error { commits++; return nil },
		rollback: func(context.Context) error { rollbacks++; return nil }}
	connection := connectionFromPGXFunctions(func(context.Context) (pgx.Tx, error) { return transaction, nil }, nil)
	err := connection.executor.WithinTransaction(context.Background(), func(Transaction) error { return nil })
	if err != nil || commits != 1 || rollbacks != 0 {
		t.Fatalf("transaction error=%v commits=%d rollbacks=%d", err, commits, rollbacks)
	}
}

func TestPGXExecutorReportsBeginFailure(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("begin")
	connection := connectionFromPGXFunctions(func(context.Context) (pgx.Tx, error) {
		return nil, protocolErr
	}, nil)
	err := connection.executor.WithinTransaction(context.Background(), func(Transaction) error { return nil })
	if !errors.Is(err, protocolErr) {
		t.Fatalf("transaction error=%v", err)
	}
}

func TestPGXExecutorPreservesOperationAndRollbackFailures(t *testing.T) {
	t.Parallel()
	operationErr := errors.New("operation")
	rollbackErr := errors.New("rollback")
	transaction := &pgxTransactionFake{rollback: func(context.Context) error { return rollbackErr }}
	connection := connectionFromPGXFunctions(func(context.Context) (pgx.Tx, error) { return transaction, nil }, nil)
	err := connection.executor.WithinTransaction(context.Background(), func(Transaction) error { return operationErr })
	if !errors.Is(err, operationErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("transaction error=%v", err)
	}
}

func TestPGXExecutorPreservesCommitUncertaintyAndRollbackFailure(t *testing.T) {
	t.Parallel()
	commitErr := errors.New("commit")
	rollbackErr := errors.New("rollback")
	transaction := &pgxTransactionFake{commit: func(context.Context) error { return commitErr },
		rollback: func(context.Context) error { return rollbackErr }}
	connection := connectionFromPGXFunctions(func(context.Context) (pgx.Tx, error) { return transaction, nil }, nil)
	err := connection.executor.WithinTransaction(context.Background(), func(Transaction) error { return nil })
	if !errors.Is(err, commitErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("transaction error=%v", err)
	}
}

type pgxTransactionFake struct {
	pgx.Tx
	commit   func(context.Context) error
	rollback func(context.Context) error
}

func (transaction *pgxTransactionFake) Commit(ctx context.Context) error {
	return transaction.commit(ctx)
}

func (transaction *pgxTransactionFake) Rollback(ctx context.Context) error {
	return transaction.rollback(ctx)
}
