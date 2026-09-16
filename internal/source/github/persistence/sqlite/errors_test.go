package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	sqlitestate "github.com/stateshifters/runnerfoundry/internal/state/sqlite"
)

func TestRegistrationMutationsReportTransactionFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	record := githubsource.Registration{GitHubAppID: 7, RepositoryURL: "repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	for _, testCase := range []struct {
		name         string
		transactions *sqliteTransactionRunner
	}{
		{name: "transaction", transactions: &sqliteTransactionRunner{err: expectedErr}},
		{name: "active", transactions: sqliteExecuteFailure(1, expectedErr)},
		{name: "ownership", transactions: sqliteExecuteFailure(2, expectedErr)},
		{name: "completion", transactions: &sqliteTransactionRunner{
			transaction: successfulSQLiteRunner(func(string) [][]any { return nil }).transaction,
			afterErr:    expectedErr,
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := NewStore(testCase.transactions, sqliteMemberLiveness{})
			if err := store.UpsertRegistration(context.Background(), record); !errors.Is(err, expectedErr) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestSingleRegistrationMutationsReportTransactionFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	for _, transactions := range []*sqliteTransactionRunner{
		{err: expectedErr},
		sqliteExecuteFailure(1, expectedErr),
		{
			transaction: successfulSQLiteRunner(func(string) [][]any { return nil }).transaction,
			afterErr:    expectedErr,
		},
	} {
		store := NewStore(transactions, sqliteMemberLiveness{})
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
		transactions *sqliteTransactionRunner
	}{
		{name: "transaction", transactions: &sqliteTransactionRunner{err: expectedErr}},
		{name: "visit", transactions: sqliteVisitFailure(expectedErr)},
		{name: "scan", transactions: sqliteScanFailure(expectedErr)},
		{name: "completion", transactions: &sqliteTransactionRunner{
			transaction: successfulSQLiteRunner(func(string) [][]any { return [][]any{registrationRow} }).transaction,
			afterErr:    expectedErr,
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := NewStore(testCase.transactions, sqliteMemberLiveness{})
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
	for _, transactions := range []*sqliteTransactionRunner{
		{err: expectedErr}, sqliteExecuteFailure(1, expectedErr), sqliteExecuteFailure(2, expectedErr),
		{
			transaction: successfulSQLiteRunner(func(string) [][]any { return nil }).transaction,
			afterErr:    expectedErr,
		},
	} {
		store := NewStore(transactions, sqliteMemberLiveness{})
		if err := store.ReplaceGitHubRunnerPoolDeclarations(context.Background(), "member", declarations); !errors.Is(err, expectedErr) {
			t.Fatalf("mutation error = %v", err)
		}
	}
}

func TestDeclarationListsReturnNoSnapshotAfterTransactionFailures(t *testing.T) {
	expectedErr := errors.New("protocol")
	for _, transactions := range []*sqliteTransactionRunner{
		{err: expectedErr}, sqliteVisitFailure(expectedErr), sqliteScanFailure(expectedErr),
		{
			transaction: successfulSQLiteRunner(func(string) [][]any {
				return [][]any{{"member", "linux", "default"}}
			}).transaction,
			afterErr: expectedErr,
		},
	} {
		store := NewStore(transactions, sqliteMemberLiveness{members: []core.MemberID{"member"}})
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
	store := NewStore(successfulSQLiteRunner(func(string) [][]any { return nil }), sqliteMemberLiveness{err: expectedErr})
	if _, err := store.LiveGitHubRunnerPoolDeclarations(context.Background(), time.Time{}); !errors.Is(err, expectedErr) {
		t.Fatalf("liveness error = %v", err)
	}
}

func sqliteExecuteFailure(failureCall int, expectedErr error) *sqliteTransactionRunner {
	calls := 0
	transaction := &sqliteTransaction{
		execute: func(context.Context, string, ...any) (int64, error) {
			calls++
			if calls == failureCall {
				return 0, expectedErr
			}
			return 1, nil
		},
	}
	return &sqliteTransactionRunner{transaction: transaction}
}

func sqliteVisitFailure(expectedErr error) *sqliteTransactionRunner {
	transaction := &sqliteTransaction{
		visitRows: func(context.Context, string, func(sqlitestate.Scanner) error, ...any) error {
			return expectedErr
		},
	}
	return &sqliteTransactionRunner{transaction: transaction}
}

func sqliteScanFailure(expectedErr error) *sqliteTransactionRunner {
	transaction := &sqliteTransaction{
		visitRows: func(_ context.Context, _ string, visit func(sqlitestate.Scanner) error, _ ...any) error {
			return visit(sqliteScanner{err: expectedErr})
		},
	}
	return &sqliteTransactionRunner{transaction: transaction}
}
