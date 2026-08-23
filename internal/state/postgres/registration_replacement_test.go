package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestUpsertRegistrationCommitsActiveAndOwnershipTogether(t *testing.T) {
	assertRegistrationPairProtocol(t, (*Store).UpsertRegistration)
}

func TestDeleteRegistrationStateGuardsAndCommitsBothRecords(t *testing.T) {
	assertRegistrationPairProtocol(t, (*Store).DeleteRegistrationState)
}

func assertRegistrationPairProtocol(
	t *testing.T,
	operation func(*Store, context.Context, githubsource.Registration) error,
) {
	t.Helper()
	protocol := &recordedTransaction{}
	record := githubsource.Registration{RepositoryURL: "https://example.test/repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	if err := operation(NewStore(protocol.executor()), context.Background(), record); err != nil {
		t.Fatalf("registration pair operation: %v", err)
	}
	if protocol.commitCalls != 1 || len(protocol.statements) != 2 ||
		!strings.Contains(protocol.statements[0], "runnerfoundry_github_registrations") ||
		!strings.Contains(protocol.statements[1], "runnerfoundry_github_scale_set_ownership") ||
		len(protocol.arguments[0]) != 5 || len(protocol.arguments[1]) != 5 {
		t.Fatalf("registration pair protocol = %#v %#v", protocol.statements, protocol.arguments)
	}
}

func TestOwnedRegistrationsReturnsDurableOwnership(t *testing.T) {
	transaction := Transaction{query: func(context.Context, string, ...any) (Rows, error) {
		return rowsFromValues([][]any{{"repo", "linux", int64(1), int64(2), int64(3)}}), nil
	}, commit: successfulCompletion, rollback: successfulCompletion}
	registrations, err := NewStore(Executor{begin: transactionBeginning(transaction)}).
		OwnedRegistrations(context.Background())
	if err != nil || len(registrations) != 1 || registrations[0].ScaleSetID != 3 {
		t.Fatalf("owned registrations = (%#v, %v)", registrations, err)
	}
}

func TestRegistrationDeletionOperationsUseGuardedKeys(t *testing.T) {
	record := githubsource.Registration{RepositoryURL: "repo", RunnerPool: "linux", InstallationID: 1,
		RunnerGroupID: 2, ScaleSetID: 3}
	assertGuardedRegistrationDelete(t, record, (*Store).DeleteActiveRegistration)
	assertGuardedRegistrationDelete(t, record, (*Store).DeleteOwnedRegistration)
}

func assertGuardedRegistrationDelete(t *testing.T, record githubsource.Registration,
	operation func(*Store, context.Context, githubsource.Registration) error,
) {
	t.Helper()
	protocol := &recordedTransaction{}
	if err := operation(NewStore(protocol.executor()), context.Background(), record); err != nil {
		t.Fatalf("delete registration: %v", err)
	}
	if protocol.commitCalls != 1 || len(protocol.statements) != 1 || len(protocol.arguments[0]) != 5 {
		t.Fatalf("statements = %#v, arguments = %#v, commits = %d",
			protocol.statements, protocol.arguments, protocol.commitCalls)
	}
}

func TestUpsertRegistrationRollsBackEveryFailure(t *testing.T) {
	assertRegistrationPairFailures(t, (*Store).UpsertRegistration)
}

func TestDeleteRegistrationStateRollsBackEveryFailure(t *testing.T) {
	assertRegistrationPairFailures(t, (*Store).DeleteRegistrationState)
}

func assertRegistrationPairFailures(
	t *testing.T,
	operation func(*Store, context.Context, githubsource.Registration) error,
) {
	t.Helper()
	wantErr := errors.New("protocol")
	record := githubsource.Registration{RepositoryURL: "repo", RunnerPool: "linux", InstallationID: 1,
		RunnerGroupID: 2, ScaleSetID: 3}
	tests := []struct {
		name     string
		executor Executor
	}{
		{name: "begin", executor: Executor{begin: func(context.Context) (Transaction, error) { return Transaction{}, wantErr }}},
		{name: "active", executor: Executor{begin: transactionBeginning(transactionFailingExecute(1, wantErr))}},
		{name: "ownership", executor: Executor{begin: transactionBeginning(transactionFailingExecute(2, wantErr))}},
		{name: "commit", executor: Executor{begin: transactionBeginning(Transaction{
			execute: successfulExecute, commit: failingCompletion(wantErr), rollback: successfulCompletion,
		})}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if err := operation(NewStore(testCase.executor), context.Background(), record); err == nil {
				t.Fatal("failure was ignored")
			}
		})
	}
}

func TestOwnedRegistrationsReportsTransactionFailures(t *testing.T) {
	wantErr := errors.New("protocol")
	tests := []namedTransaction{
		{name: "query", transaction: Transaction{query: func(context.Context, string, ...any) (Rows, error) {
			return Rows{}, wantErr
		}, rollback: successfulCompletion}},
		{name: "commit", transaction: Transaction{query: func(context.Context, string, ...any) (Rows, error) {
			return rowsFromValues(nil), nil
		},
			commit: failingCompletion(wantErr), rollback: successfulCompletion}},
	}
	if _, err := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return Transaction{}, wantErr }}).
		OwnedRegistrations(context.Background()); err == nil {
		t.Fatal("begin failure was ignored")
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(Executor{begin: transactionBeginning(testCase.transaction)}).
				OwnedRegistrations(context.Background()); err == nil {
				t.Fatal("failure was ignored")
			}
		})
	}
}
