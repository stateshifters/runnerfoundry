package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	statecontract "github.com/meza/RunnerFoundry/internal/state"
)

func TestReplaceRegistrationsRequestsOneCommittedSnapshot(t *testing.T) {
	protocol := &recordedTransaction{}
	records := []statecontract.Registration{{RepositoryURL: "https://example.test/repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}}
	if err := NewStore(protocol.executor()).ReplaceRegistrations(context.Background(), records); err != nil {
		t.Fatalf("replace registrations: %v", err)
	}
	if protocol.commitCalls != 1 || len(protocol.statements) != 2 ||
		!strings.HasPrefix(protocol.statements[0], "DELETE FROM") || len(protocol.arguments[1]) != 5 {
		t.Fatalf("replacement protocol = %#v %#v", protocol.statements, protocol.arguments)
	}
}

func TestReplaceRegistrationsReportsTransactionFailures(t *testing.T) {
	wantErr := errors.New("protocol")
	records := []statecontract.Registration{{RepositoryURL: "repo", RunnerPool: "linux"}}
	executors := []Executor{
		{begin: func(context.Context) (Transaction, error) { return Transaction{}, wantErr }},
		{begin: transactionBeginning(transactionFailingExecute(1, wantErr))},
		{begin: transactionBeginning(transactionFailingExecute(2, wantErr))},
		{begin: transactionBeginning(Transaction{execute: successfulExecute, commit: failingCompletion(wantErr), rollback: successfulCompletion})},
	}
	for index, executor := range executors {
		if err := NewStore(executor).ReplaceRegistrations(context.Background(), records); err == nil {
			t.Fatalf("failure %d was ignored", index)
		}
	}
}
