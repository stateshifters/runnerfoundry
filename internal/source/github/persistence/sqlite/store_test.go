package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/state"
)

func TestStorePersistsGitHubRegistrationAndDeclarationState(t *testing.T) {
	registration := githubsource.Registration{GitHubAppID: 7, RepositoryURL: "https://example.test/repo", RunnerPool: "linux",
		InstallationID: 1, RunnerGroupID: 2, ScaleSetID: 3}
	transactions := successfulSQLiteRunner(func(statement string) [][]any {
		switch {
		case strings.Contains(statement, "github_member_runner_pools"):
			return [][]any{{"member-1", "linux", "default"}, {"member-2", "windows", "default"}}
		case strings.Contains(statement, "github_"):
			return [][]any{{int64(7), registration.RepositoryURL, "linux", int64(1), int64(2), int64(3)}}
		default:
			return nil
		}
	})
	store := NewStore(transactions, sqliteMemberLiveness{members: []core.MemberID{"member-1"}})
	ctx := context.Background()
	assertRegistrationMutations(ctx, t, store, registration)
	assertRegistrationLists(ctx, t, store, registration)
	assertDeclarations(ctx, t, store)
}

func assertRegistrationMutations(ctx context.Context, t *testing.T, store *Store, registration githubsource.Registration) {
	t.Helper()
	if err := store.UpsertRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func(context.Context, githubsource.Registration) error{
		store.DeleteRegistrationState, store.DeleteActiveRegistration, store.DeleteOwnedRegistration,
	} {
		if err := operation(ctx, registration); err != nil {
			t.Fatal(err)
		}
	}
}

func assertRegistrationLists(ctx context.Context, t *testing.T, store *Store, registration githubsource.Registration) {
	t.Helper()
	active, err := store.Registrations(ctx)
	if err != nil || len(active) != 1 || active[0] != registration {
		t.Fatalf("active registrations = %#v, error = %v", active, err)
	}
	owned, err := store.OwnedRegistrations(ctx)
	if err != nil || len(owned) != 1 || owned[0] != registration {
		t.Fatalf("owned registrations = %#v, error = %v", owned, err)
	}
}

func assertDeclarations(ctx context.Context, t *testing.T, store *Store) {
	t.Helper()
	declarations := []githubsource.RunnerPoolDeclaration{{RunnerPool: "linux", RunnerGroup: "default"}}
	err := store.ReplaceGitHubRunnerPoolDeclarations(ctx, "member-1", declarations)
	if err != nil {
		t.Fatal(err)
	}
	live, err := store.LiveGitHubRunnerPoolDeclarations(ctx, time.Unix(1_700_000_000, 0).UTC())
	if err != nil || len(live) != 1 || live[0].MemberID != "member-1" || live[0].RunnerPool != "linux" {
		t.Fatalf("live declarations = %#v, error = %v", live, err)
	}
}

type sqliteMemberLiveness struct {
	members []core.MemberID
	err     error
}

func (liveness sqliteMemberLiveness) LiveMemberIDs(context.Context, time.Time) ([]core.MemberID, error) {
	return liveness.members, liveness.err
}

var _ state.MemberLivenessReader = sqliteMemberLiveness{}
