package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	statecontract "github.com/meza/RunnerFoundry/internal/state"
)

func TestRegistrationLookupReportsMissingAndProtocolFailure(t *testing.T) {
	t.Parallel()
	missing := NewStore(Executor{queryRow: constantRow(Row{scan: func(...any) error { return sql.ErrNoRows }})})
	if _, found, err := missing.Registration(context.Background(), "repo", "linux"); err != nil || found {
		t.Fatalf("missing registration found=%t err=%v", found, err)
	}
	broken := NewStore(Executor{queryRow: constantRow(Row{scan: func(...any) error { return errors.New("protocol") }})})
	if _, _, err := broken.Registration(context.Background(), "repo", "linux"); err == nil {
		t.Fatal("expected lookup failure")
	}
}

func TestRegistrationMutationsReportProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	registration := statecontract.Registration{RepositoryURL: "repo", RunnerPool: "linux"}
	for index, executor := range []Executor{
		{begin: failingBegin(protocolErr)},
		{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr), rollback: successfulCompletion})},
		{begin: transactionBeginning(Transaction{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion})},
		{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr), rollback: failingCompletion(protocolErr)})},
	} {
		if err := NewStore(executor).UpsertRegistration(context.Background(), registration); err == nil {
			t.Fatalf("mutation case %d", index)
		}
	}
}

func TestRegistrationListReportsProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if _, err := NewStore(Executor{begin: failingBegin(protocolErr)}).Registrations(context.Background()); err == nil {
		t.Fatal("expected begin failure")
	}
	for index, transaction := range failingListTransactions(protocolErr) {
		if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).Registrations(context.Background()); err == nil {
			t.Fatalf("list case %d", index)
		}
	}
}
