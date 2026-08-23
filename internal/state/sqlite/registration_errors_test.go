package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
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
	registration := githubsource.Registration{RepositoryURL: "repo", RunnerPool: "linux"}
	for _, testCase := range []struct {
		name     string
		executor Executor
	}{
		{name: "begin", executor: Executor{begin: failingBegin(protocolErr)}},
		{name: "mutation", executor: Executor{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr), rollback: successfulCompletion})}},
		{name: "commit", executor: Executor{begin: transactionBeginning(Transaction{execute: successfulExecute, commit: failingCompletion(protocolErr), rollback: successfulCompletion})}},
		{name: "rollback", executor: Executor{begin: transactionBeginning(Transaction{execute: failingExecute(protocolErr), rollback: failingCompletion(protocolErr)})}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := NewStore(testCase.executor).UpsertRegistration(context.Background(), registration); err == nil {
				t.Fatal("mutation succeeded")
			}
		})
	}
}

func TestRegistrationListReportsProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	if _, err := NewStore(Executor{begin: failingBegin(protocolErr)}).Registrations(context.Background()); err == nil {
		t.Fatal("expected begin failure")
	}
	for _, testCase := range namedFailingListTransactions(protocolErr) {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(Executor{begin: transactionBeginning(testCase.transaction)}).Registrations(context.Background()); err == nil {
				t.Fatal("list succeeded")
			}
		})
	}
}
