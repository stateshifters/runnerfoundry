package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
)

func TestRegistrationMutationsReportTransactionFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	record := githubsource.Registration{GitHubAppID: 7, RepositoryURL: "repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	for _, testCase := range []struct {
		name         string
		transactions *postgresTransactionRunner
	}{
		{name: "transaction", transactions: &postgresTransactionRunner{err: expectedErr}},
		{name: "active", transactions: postgresExecuteFailure(1, expectedErr)},
		{name: "ownership", transactions: postgresExecuteFailure(2, expectedErr)},
		{name: "completion", transactions: &postgresTransactionRunner{
			transaction: successfulPostgresRunner(func(string) [][]any { return nil }).transaction,
			afterErr:    expectedErr,
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := NewStore(testCase.transactions, postgresMemberLiveness{})
			if err := store.UpsertRegistration(context.Background(), record); !errors.Is(err, expectedErr) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestSingleRegistrationMutationsReportTransactionFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	for _, transactions := range []*postgresTransactionRunner{
		{err: expectedErr},
		postgresExecuteFailure(1, expectedErr),
		{
			transaction: successfulPostgresRunner(func(string) [][]any { return nil }).transaction,
			afterErr:    expectedErr,
		},
	} {
		store := NewStore(transactions, postgresMemberLiveness{})
		if err := store.DeleteActiveRegistration(context.Background(), githubsource.Registration{}); !errors.Is(err, expectedErr) {
			t.Fatalf("error = %v", err)
		}
	}
}

func TestRegistrationListsReturnNoSnapshotAfterFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	registrationRow := []any{int64(7), "repo", "linux", int64(1), int64(2), int64(3)}
	for _, testCase := range []struct {
		name         string
		transactions *postgresTransactionRunner
	}{
		{name: "transaction", transactions: &postgresTransactionRunner{err: expectedErr}},
		{name: "visit", transactions: postgresVisitFailure(expectedErr)},
		{name: "scan", transactions: postgresScanFailure(expectedErr)},
		{name: "completion", transactions: &postgresTransactionRunner{
			transaction: successfulPostgresRunner(func(string) [][]any { return [][]any{registrationRow} }).transaction,
			afterErr:    expectedErr,
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := NewStore(testCase.transactions, postgresMemberLiveness{})
			registrations, err := store.Registrations(context.Background())
			if !errors.Is(err, expectedErr) {
				t.Fatalf("error = %v", err)
			}
			if registrations != nil {
				t.Fatalf("registrations = %#v, want nil", registrations)
			}
		})
	}
}

func TestDeclarationMutationsReportTransactionFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	declarations := []githubsource.RunnerPoolDeclaration{{RunnerPool: "linux", RunnerGroup: "default"}}
	for _, transactions := range []*postgresTransactionRunner{
		{err: expectedErr}, postgresExecuteFailure(1, expectedErr), postgresExecuteFailure(2, expectedErr),
		{
			transaction: successfulPostgresRunner(func(string) [][]any { return nil }).transaction,
			afterErr:    expectedErr,
		},
	} {
		store := NewStore(transactions, postgresMemberLiveness{})
		if err := store.ReplaceGitHubRunnerPoolDeclarations(context.Background(), "member", declarations); !errors.Is(err, expectedErr) {
			t.Fatalf("mutation error = %v", err)
		}
	}
}

func TestDeclarationListsReturnNoSnapshotAfterTransactionFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	for _, transactions := range []*postgresTransactionRunner{
		{err: expectedErr}, postgresVisitFailure(expectedErr), postgresScanFailure(expectedErr),
		{
			transaction: successfulPostgresRunner(func(string) [][]any {
				return [][]any{{"member", "linux", "default"}}
			}).transaction,
			afterErr: expectedErr,
		},
	} {
		store := NewStore(transactions, postgresMemberLiveness{members: []core.MemberID{"member"}})
		result, err := store.LiveGitHubRunnerPoolDeclarations(context.Background(), time.Time{})
		if !errors.Is(err, expectedErr) {
			t.Fatalf("list error = %v", err)
		}
		if result != nil {
			t.Fatalf("declarations = %#v, want nil", result)
		}
	}
}

func TestDeclarationListsReportLivenessFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	store := NewStore(successfulPostgresRunner(func(string) [][]any { return nil }), postgresMemberLiveness{err: expectedErr})
	if _, err := store.LiveGitHubRunnerPoolDeclarations(context.Background(), time.Time{}); !errors.Is(err, expectedErr) {
		t.Fatalf("liveness error = %v", err)
	}
}

func postgresExecuteFailure(failureCall int, expectedErr error) *postgresTransactionRunner {
	calls := 0
	transaction := &postgresTransaction{
		execute: func(context.Context, string, ...any) (int64, error) {
			calls++
			if calls == failureCall {
				return 0, expectedErr
			}
			return 1, nil
		},
	}
	return &postgresTransactionRunner{transaction: transaction}
}

func postgresVisitFailure(expectedErr error) *postgresTransactionRunner {
	transaction := &postgresTransaction{
		visitRows: func(context.Context, string, func(postgresstate.Scanner) error, ...any) error {
			return expectedErr
		},
	}
	return &postgresTransactionRunner{transaction: transaction}
}

func postgresScanFailure(expectedErr error) *postgresTransactionRunner {
	transaction := &postgresTransaction{
		visitRows: func(_ context.Context, _ string, visit func(postgresstate.Scanner) error, _ ...any) error {
			return visit(postgresScanner{err: expectedErr})
		},
	}
	return &postgresTransactionRunner{transaction: transaction}
}
