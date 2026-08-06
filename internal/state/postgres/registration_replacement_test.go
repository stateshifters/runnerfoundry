package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestReplaceRegistrationsRequestsOneCommittedSnapshot(t *testing.T) {
	protocol := &recordedTransaction{}
	records := []githubsource.Registration{{RepositoryURL: "https://example.test/repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}}
	snapshot := githubsource.RegistrationSnapshot{Active: records, Owned: records}
	if err := NewStore(protocol.executor()).ReplaceRegistrationSnapshot(context.Background(), snapshot); err != nil {
		t.Fatalf("replace registrations: %v", err)
	}
	if protocol.commitCalls != 1 || len(protocol.statements) != 4 ||
		!strings.HasPrefix(protocol.statements[0], "DELETE FROM") ||
		!strings.HasPrefix(protocol.statements[1], "DELETE FROM") ||
		len(protocol.arguments[2]) != 5 || len(protocol.arguments[3]) != 5 {
		t.Fatalf("replacement protocol = %#v %#v", protocol.statements, protocol.arguments)
	}
}

func TestReplaceRegistrationSnapshotRollsBackEveryFailure(t *testing.T) {
	wantErr := errors.New("protocol")
	record := githubsource.Registration{RepositoryURL: "repo", RunnerPool: "linux", InstallationID: 1,
		RunnerGroupID: 2, ScaleSetID: 3}
	snapshot := githubsource.RegistrationSnapshot{Active: []githubsource.Registration{record}, Owned: []githubsource.Registration{record}}
	executors := []Executor{{begin: func(context.Context) (Transaction, error) { return Transaction{}, wantErr }}}
	for failureCall := 1; failureCall <= 4; failureCall++ {
		executors = append(executors, Executor{begin: transactionBeginning(transactionFailingExecute(failureCall, wantErr))})
	}
	executors = append(executors, Executor{begin: transactionBeginning(Transaction{
		execute: successfulExecute, commit: failingCompletion(wantErr), rollback: successfulCompletion,
	})})
	for index, executor := range executors {
		if err := NewStore(executor).ReplaceRegistrationSnapshot(context.Background(), snapshot); err == nil {
			t.Fatalf("failure %d was ignored", index)
		}
	}
}

func TestRegistrationSnapshotReadsActiveAndOwnedAtomically(t *testing.T) {
	active := []any{"repo", "linux", int64(1), int64(2), int64(3)}
	owned := []any{"repo", "old", int64(1), int64(4), int64(5)}
	transaction := Transaction{
		query: func(_ context.Context, statement string, _ ...any) (Rows, error) {
			if statement == registrationsSQL {
				return rowsFromValues([][]any{active}), nil
			}
			return rowsFromValues([][]any{owned}), nil
		},
		commit: successfulCompletion, rollback: successfulCompletion,
	}
	snapshot, err := NewStore(Executor{begin: transactionBeginning(transaction)}).RegistrationSnapshot(context.Background())
	if err != nil || len(snapshot.Active) != 1 || len(snapshot.Owned) != 1 || snapshot.Owned[0].ScaleSetID != 5 {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
}

func TestRegistrationSnapshotReportsTransactionFailures(t *testing.T) {
	wantErr := errors.New("protocol")
	successfulRows := func() Rows { return rowsFromValues(nil) }
	transactions := []Transaction{
		{query: func(context.Context, string, ...any) (Rows, error) { return Rows{}, wantErr }, rollback: successfulCompletion},
		{query: func() func(context.Context, string, ...any) (Rows, error) {
			calls := 0
			return func(context.Context, string, ...any) (Rows, error) {
				calls++
				if calls == 2 {
					return Rows{}, wantErr
				}
				return successfulRows(), nil
			}
		}(), rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (Rows, error) { return successfulRows(), nil },
			commit: failingCompletion(wantErr), rollback: successfulCompletion},
	}
	if _, err := NewStore(Executor{begin: func(context.Context) (Transaction, error) { return Transaction{}, wantErr }}).
		RegistrationSnapshot(context.Background()); err == nil {
		t.Fatal("begin failure was ignored")
	}
	for index, transaction := range transactions {
		if _, err := NewStore(Executor{begin: transactionBeginning(transaction)}).RegistrationSnapshot(context.Background()); err == nil {
			t.Fatalf("failure %d was ignored", index)
		}
	}
}
