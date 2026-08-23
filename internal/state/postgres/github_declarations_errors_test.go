package postgres

import (
	"context"
	"errors"
	"testing"

	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestReplaceGitHubRunnerPoolDeclarationsReportsProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	declarations := []githubsource.RunnerPoolDeclaration{{RunnerPool: "linux", RunnerGroup: "default"}}
	transactions := []struct {
		name     string
		executor Executor
	}{
		{name: "begin", executor: Executor{begin: func(context.Context) (Transaction, error) { return Transaction{}, protocolErr }}},
		{name: "delete", executor: Executor{begin: transactionBeginning(transactionFailingExecute(1, protocolErr))}},
		{name: "insert", executor: Executor{begin: transactionBeginning(transactionFailingExecute(2, protocolErr))}},
		{name: "commit", executor: Executor{begin: transactionBeginning(Transaction{execute: successfulExecute,
			commit: failingCompletion(protocolErr), rollback: successfulCompletion})}},
	}
	for _, testCase := range transactions {
		t.Run(testCase.name, func(t *testing.T) {
			if err := NewStore(testCase.executor).ReplaceGitHubRunnerPoolDeclarations(
				context.Background(), "member", declarations); !errors.Is(err, protocolErr) {
				t.Fatalf("error = %v, want protocol failure", err)
			}
		})
	}
}

func TestLiveGitHubRunnerPoolDeclarationsReportsProtocolFailures(t *testing.T) {
	t.Parallel()
	protocolErr := errors.New("protocol")
	brokenRows := func(scanErr, iterationErr error) Rows {
		first := true
		return Rows{close: func() error { return nil }, err: func() error { return iterationErr },
			next: func() bool { result := first && scanErr != nil; first = false; return result },
			scan: func(...any) error { return scanErr }}
	}
	tests := []struct {
		name     string
		executor Executor
	}{
		{name: "begin", executor: Executor{begin: func(context.Context) (Transaction, error) { return Transaction{}, protocolErr }}},
		{name: "query", executor: Executor{begin: transactionBeginning(Transaction{query: func(context.Context, string, ...any) (Rows, error) {
			return Rows{}, protocolErr
		}, rollback: successfulCompletion})}},
		{name: "scan", executor: declarationListExecutor(brokenRows(protocolErr, nil), nil)},
		{name: "iteration", executor: declarationListExecutor(brokenRows(nil, protocolErr), nil)},
		{name: "commit", executor: declarationListExecutor(rowsFromValues(nil), protocolErr)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := NewStore(testCase.executor).LiveGitHubRunnerPoolDeclarations(
				context.Background(), fixedTime()); !errors.Is(err, protocolErr) {
				t.Fatalf("error = %v, want protocol failure", err)
			}
		})
	}
}

func declarationListExecutor(rows Rows, commitErr error) Executor {
	commit := successfulCompletion
	if commitErr != nil {
		commit = failingCompletion(commitErr)
	}
	return Executor{begin: transactionBeginning(Transaction{query: func(context.Context, string, ...any) (Rows, error) {
		return rows, nil
	}, commit: commit, rollback: successfulCompletion})}
}
