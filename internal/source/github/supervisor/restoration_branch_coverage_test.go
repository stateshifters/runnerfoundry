package supervisor

import (
	"context"
	"errors"
	"testing"
	"time"

	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestGitHubSourceRuntimeCoversStableAndRejectedLeadershipOpen(t *testing.T) {
	wantErr := errors.New("snapshot failed")
	manager := &githubManagerFake{snapshotErr: wantErr}
	source := newGitHubSourceRuntime(manager, &sourceCapacityProviderFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, func() time.Time { return time.Time{} })
	if err := source.OpenLeader(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("OpenLeader() error = %v, want %v", err, wantErr)
	}
	manager.setSnapshotError(nil)
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("OpenLeader() error = %v", err)
	}
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("stable OpenLeader() error = %v", err)
	}
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimeIgnoresDuplicateDiscoveryAndSessionScheduling(t *testing.T) {
	record := githubsource.Registration{RepositoryURL: "repo", RunnerPool: "linux"}
	manager := &githubManagerFake{adapters: []*githubsource.Adapter{{RunnerPool: "linux", Repository: "repo"}}}
	source := newGitHubSourceRuntime(manager, &sourceCapacityProviderFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, func() time.Time { return time.Time{} })
	term := &githubLeadershipTerm{discoveryActive: true}
	source.startDiscovery(context.Background(), nil)
	source.startDiscovery(context.Background(), term)

	source.desired[registrationKey(record)] = record
	source.opening[registrationKey(record)] = record
	source.scheduleRequiredRegistrationSessions(context.Background(), term)
	if len(term.operations) != 0 {
		t.Fatalf("duplicate schedule started %d operations", len(term.operations))
	}
}
