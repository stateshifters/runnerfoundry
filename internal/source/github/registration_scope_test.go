package github_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestAppScopedRegistrationsIsolateReadsAndStampWrites(t *testing.T) {
	base, scoped := appScopedRegistrationFixture(t)
	assertAppScopedReads(t, scoped)
	assertAppScopedWrites(t, base, scoped)
}

func appScopedRegistrationFixture(t *testing.T) (*appScopeRepositoryFake, *githubsource.AppScopedRegistrationRepository) {
	t.Helper()
	base := &appScopeRepositoryFake{active: []githubsource.Registration{
		{GitHubAppID: 11, RepositoryURL: "first"},
		{GitHubAppID: 22, RepositoryURL: "second"},
	}, owned: []githubsource.Registration{{GitHubAppID: 22, RepositoryURL: "owned"}}}
	scoped, err := githubsource.ScopeRegistrationRepository(base, 22)
	if err != nil {
		t.Fatalf("scope repository: %v", err)
	}
	return base, scoped
}

func assertAppScopedReads(t *testing.T, scoped *githubsource.AppScopedRegistrationRepository) {
	t.Helper()
	active, err := scoped.Registrations(context.Background())
	if err != nil {
		t.Fatalf("read scoped active registrations: %v", err)
	}
	owned, err := scoped.OwnedRegistrations(context.Background())
	if err != nil {
		t.Fatalf("read scoped ownership: %v", err)
	}
	if len(active) != 1 || active[0].RepositoryURL != "second" || len(owned) != 1 {
		t.Fatalf("scoped snapshot = %#v %#v", active, owned)
	}
}

func assertAppScopedWrites(
	t *testing.T,
	base *appScopeRepositoryFake,
	scoped *githubsource.AppScopedRegistrationRepository,
) {
	t.Helper()
	err := scoped.UpsertRegistration(context.Background(), githubsource.Registration{RepositoryURL: "new"})
	if err != nil {
		t.Fatalf("upsert scoped registration: %v", err)
	}
	if base.upserted.GitHubAppID != 22 {
		t.Fatalf("upserted App ID = %d", base.upserted.GitHubAppID)
	}
	for _, operation := range []func() error{
		func() error {
			return scoped.DeleteActiveRegistration(context.Background(), githubsource.Registration{})
		},
		func() error { return scoped.DeleteOwnedRegistration(context.Background(), githubsource.Registration{}) },
		func() error { return scoped.DeleteRegistrationState(context.Background(), githubsource.Registration{}) },
	} {
		if err = operation(); err != nil {
			t.Fatalf("delete scoped registration: %v", err)
		}
	}
}

func TestAppScopedRegistrationsRejectInvalidScopeAndCrossAppMutation(t *testing.T) {
	if _, err := githubsource.ScopeRegistrationRepository(nil, 1); err == nil {
		t.Fatal("nil registration repository was accepted")
	}
	if _, err := githubsource.ScopeRegistrationRepository(&appScopeRepositoryFake{}, 0); err == nil {
		t.Fatal("non-positive App ID was accepted")
	}
	scoped, err := githubsource.ScopeRegistrationRepository(&appScopeRepositoryFake{}, 22)
	if err != nil {
		t.Fatalf("scope repository: %v", err)
	}
	crossApp := githubsource.Registration{GitHubAppID: 11}
	operations := []func() error{
		func() error { return scoped.UpsertRegistration(context.Background(), crossApp) },
		func() error { return scoped.DeleteActiveRegistration(context.Background(), crossApp) },
		func() error { return scoped.DeleteOwnedRegistration(context.Background(), crossApp) },
		func() error { return scoped.DeleteRegistrationState(context.Background(), crossApp) },
	}
	for _, operation := range operations {
		if err = operation(); err == nil {
			t.Fatal("cross-App mutation was accepted")
		}
	}
}

func TestAppScopedRegistrationsWrapRepositoryFailures(t *testing.T) {
	wantErr := errors.New("unavailable")
	base := &appScopeRepositoryFake{err: wantErr}
	scoped, err := githubsource.ScopeRegistrationRepository(base, 22)
	if err != nil {
		t.Fatalf("scope repository: %v", err)
	}
	operations := []func() error{
		func() error {
			_, operationErr := scoped.Registrations(context.Background())
			return fmt.Errorf("read active registrations: %w", operationErr)
		},
		func() error {
			_, operationErr := scoped.OwnedRegistrations(context.Background())
			return fmt.Errorf("read ownership: %w", operationErr)
		},
		func() error { return scoped.UpsertRegistration(context.Background(), githubsource.Registration{}) },
		func() error {
			return scoped.DeleteActiveRegistration(context.Background(), githubsource.Registration{})
		},
		func() error { return scoped.DeleteOwnedRegistration(context.Background(), githubsource.Registration{}) },
		func() error { return scoped.DeleteRegistrationState(context.Background(), githubsource.Registration{}) },
	}
	for index, operation := range operations {
		if operationErr := operation(); !errors.Is(operationErr, wantErr) {
			t.Fatalf("operation %d error = %v", index, operationErr)
		}
	}
}

func TestAppScopedRegistrationsForwardSharedRunnerPoolDeclarations(t *testing.T) {
	base := &declarationScopeRepositoryFake{appScopeRepositoryFake: appScopeRepositoryFake{}}
	scoped, err := githubsource.ScopeRegistrationRepository(base, 22)
	if err != nil {
		t.Fatalf("scope repository: %v", err)
	}
	declarations := []githubsource.RunnerPoolDeclaration{{RunnerPool: "linux", RunnerGroup: "default"}}
	if err = scoped.ReplaceGitHubRunnerPoolDeclarations(context.Background(), "member", declarations); err != nil {
		t.Fatalf("replace declarations: %v", err)
	}
	loaded, err := scoped.LiveGitHubRunnerPoolDeclarations(context.Background(), time.Time{})
	if err != nil || len(loaded) != 1 || loaded[0].RunnerPool != "linux" {
		t.Fatalf("declarations = %#v, %v", loaded, err)
	}
	manager, err := githubsource.NewManager(scopeDiscoverer{}, func(context.Context, transport.Repository) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{}, nil
	}, scoped, githubsource.ManagerObserver{})
	if err != nil || !manager.UsesSharedRunnerPoolDeclarations() {
		t.Fatalf("shared declarations available = %t, %v", manager != nil && manager.UsesSharedRunnerPoolDeclarations(), err)
	}
}

func TestAppScopedDeclarationsReportUnavailable(t *testing.T) {
	scoped, err := githubsource.ScopeRegistrationRepository(&appScopeRepositoryFake{}, 22)
	if err != nil {
		t.Fatalf("scope repository: %v", err)
	}
	if err = scoped.ReplaceGitHubRunnerPoolDeclarations(context.Background(), "member", nil); !errors.Is(err, githubsource.ErrRunnerPoolDeclarationPersistenceUnavailable) {
		t.Fatalf("replace unavailable error = %v", err)
	}
	if _, err = scoped.LiveGitHubRunnerPoolDeclarations(context.Background(), time.Time{}); !errors.Is(err, githubsource.ErrRunnerPoolDeclarationPersistenceUnavailable) {
		t.Fatalf("read unavailable error = %v", err)
	}
	manager, err := githubsource.NewManager(scopeDiscoverer{}, func(context.Context, transport.Repository) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{}, nil
	}, scoped, githubsource.ManagerObserver{})
	if err != nil || manager.UsesSharedRunnerPoolDeclarations() {
		t.Fatalf("unavailable declarations exposed = %t, %v", manager != nil && manager.UsesSharedRunnerPoolDeclarations(), err)
	}
}

func TestAppScopedDeclarationsWrapRepositoryFailures(t *testing.T) {
	wantErr := errors.New("unavailable")
	failing, err := githubsource.ScopeRegistrationRepository(&declarationScopeRepositoryFake{
		appScopeRepositoryFake: appScopeRepositoryFake{}, declarationErr: wantErr,
	}, 22)
	if err != nil {
		t.Fatalf("scope failing repository: %v", err)
	}
	if err = failing.ReplaceGitHubRunnerPoolDeclarations(context.Background(), "member", nil); !errors.Is(err, wantErr) {
		t.Fatalf("replace wrapped error = %v", err)
	}
	if _, err = failing.LiveGitHubRunnerPoolDeclarations(context.Background(), time.Time{}); !errors.Is(err, wantErr) {
		t.Fatalf("read wrapped error = %v", err)
	}
}

type appScopeRepositoryFake struct {
	active, owned []githubsource.Registration
	upserted      githubsource.Registration
	err           error
}

func (fake *appScopeRepositoryFake) Registrations(context.Context) ([]githubsource.Registration, error) {
	return fake.active, fake.err
}
func (fake *appScopeRepositoryFake) OwnedRegistrations(context.Context) ([]githubsource.Registration, error) {
	return fake.owned, fake.err
}
func (fake *appScopeRepositoryFake) UpsertRegistration(_ context.Context, record githubsource.Registration) error {
	fake.upserted = record
	return fake.err
}
func (fake *appScopeRepositoryFake) DeleteActiveRegistration(context.Context, githubsource.Registration) error {
	return fake.err
}
func (fake *appScopeRepositoryFake) DeleteOwnedRegistration(context.Context, githubsource.Registration) error {
	return fake.err
}
func (fake *appScopeRepositoryFake) DeleteRegistrationState(context.Context, githubsource.Registration) error {
	return fake.err
}

type declarationScopeRepositoryFake struct {
	appScopeRepositoryFake
	declarations   []githubsource.MemberRunnerPoolDeclaration
	declarationErr error
}

func (fake *declarationScopeRepositoryFake) ReplaceGitHubRunnerPoolDeclarations(
	_ context.Context,
	memberID core.MemberID,
	declarations []githubsource.RunnerPoolDeclaration,
) error {
	fake.declarations = nil
	for _, declaration := range declarations {
		fake.declarations = append(fake.declarations, githubsource.MemberRunnerPoolDeclaration{
			MemberID: memberID, RunnerPool: declaration.RunnerPool, RunnerGroup: declaration.RunnerGroup,
		})
	}
	return fake.declarationErr
}

func (fake *declarationScopeRepositoryFake) LiveGitHubRunnerPoolDeclarations(
	context.Context,
	time.Time,
) ([]githubsource.MemberRunnerPoolDeclaration, error) {
	return fake.declarations, fake.declarationErr
}

type scopeDiscoverer struct{}

func (scopeDiscoverer) Discover(context.Context) (transport.DiscoveryResult, error) {
	return transport.DiscoveryResult{}, nil
}
