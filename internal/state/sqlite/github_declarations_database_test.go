package sqlite

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestDatabaseStoreReplacesAndListsLiveGitHubRunnerPoolDeclarations(t *testing.T) {
	t.Parallel()
	store := openGitHubDeclarationTestStore(t)
	now := fixedTime()
	member := core.MemberID("serenity")
	if err := store.PublishMemberState(context.Background(), member, now, nil); err != nil {
		t.Fatalf("publish member: %v", err)
	}
	initial := []githubsource.RunnerPoolDeclaration{
		{RunnerPool: "windows-latest", RunnerGroup: "default"},
		{RunnerPool: "windows-slim", RunnerGroup: "default"},
	}
	replaceGitHubDeclarationTestState(t, store, member, initial)
	declarations, err := store.LiveGitHubRunnerPoolDeclarations(context.Background(), now.Add(-time.Second))
	if err != nil {
		t.Fatalf("list declarations: %v", err)
	}
	want := []githubsource.MemberRunnerPoolDeclaration{{MemberID: member, RunnerPool: "windows-latest", RunnerGroup: "default"}}
	if !reflect.DeepEqual(declarations, want) {
		t.Fatalf("declarations = %#v, want %#v", declarations, want)
	}
}

func openGitHubDeclarationTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("open SQLite store: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close SQLite store: %v", closeErr)
		}
	})
	return store
}

func replaceGitHubDeclarationTestState(
	t *testing.T,
	store *Store,
	member core.MemberID,
	initial []githubsource.RunnerPoolDeclaration,
) {
	t.Helper()
	if err := store.ReplaceGitHubRunnerPoolDeclarations(context.Background(), member, initial); err != nil {
		t.Fatalf("replace declarations: %v", err)
	}
	if err := store.ReplaceGitHubRunnerPoolDeclarations(context.Background(), member, initial[:1]); err != nil {
		t.Fatalf("replace declarations again: %v", err)
	}
}
