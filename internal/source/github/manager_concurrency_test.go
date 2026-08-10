package github_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestManagerReconcileBoundsRepositoryConcurrencyAndPreservesDiscoveryOrder(t *testing.T) {
	repositories := managerRepositories(5)
	manager, started, release := newBlockingReconcileManager(t, repositories)
	result := startManagerReconciliation(manager)
	assertRepositoryConcurrencyBound(t, started, release)
	reconciled := <-result
	if reconciled.err != nil {
		t.Fatalf("Reconcile() error = %v", reconciled.err)
	}
	assertRegistrationDiscoveryOrder(t, reconciled.snapshot, repositories)
}

func newBlockingReconcileManager(
	t *testing.T,
	repositories []transport.Repository,
) (*githubsource.Manager, <-chan string, chan<- struct{}) {
	t.Helper()
	started := make(chan string, len(repositories))
	release := make(chan struct{}, len(repositories))
	factory := func(_ context.Context, repository transport.Repository) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{EnsureRunnerPool: func(
			context.Context,
			string,
			string,
		) (transport.Registration, error) {
			started <- repository.URL
			<-release
			return transport.Registration{ID: transport.ScaleSetID(repository.InstallationID), RunnerGroupID: 1}, nil
		}}, nil
	}
	manager := newManagerFixture(t, &managerDiscovererFake{repositories: repositories}, factory, &registrationRepositoryFake{})
	return manager, started, release
}

func startManagerReconciliation(manager *githubsource.Manager) <-chan reconcileResult {
	result := make(chan reconcileResult, 1)
	go func() {
		snapshot, err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
			RunnerPool: core.RunnerPool("linux"), RunnerGroup: "default",
		}}})
		result <- reconcileResult{snapshot: snapshot, err: err}
	}()
	return result
}

func assertRepositoryConcurrencyBound(t *testing.T, started <-chan string, release chan<- struct{}) {
	t.Helper()
	for range 4 {
		<-started
	}
	if queued := len(started); queued != 0 {
		t.Fatalf("repositories started beyond concurrency bound: %d", queued)
	}
	release <- struct{}{}
	<-started
	for range 4 {
		release <- struct{}{}
	}
}

func assertRegistrationDiscoveryOrder(
	t *testing.T,
	registrations []githubsource.Registration,
	repositories []transport.Repository,
) {
	t.Helper()
	want := make([]string, len(repositories))
	got := make([]string, len(registrations))
	for index := range repositories {
		want[index] = repositories[index].URL
		got[index] = registrations[index].RepositoryURL
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("registration order = %#v, want %#v", got, want)
	}
}

func TestManagerReconcileReturnsErrorsInDiscoveryOrder(t *testing.T) {
	repositories := managerRepositories(2)
	firstErr := errors.New("first repository failed")
	secondErr := errors.New("second repository failed")
	errorsByRepository := map[string]error{repositories[0].URL: firstErr, repositories[1].URL: secondErr}
	manager := newManagerFixture(t, &managerDiscovererFake{repositories: repositories}, func(
		_ context.Context,
		repository transport.Repository,
	) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{}, errorsByRepository[repository.URL]
	}, &registrationRepositoryFake{})

	_, err := manager.Reconcile(context.Background(), githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
		RunnerPool: core.RunnerPool("linux"), RunnerGroup: "default",
	}}})
	if !errors.Is(err, firstErr) {
		t.Fatalf("Reconcile() error = %v, want first discovery-ordered error %v", err, firstErr)
	}
	if !errors.Is(err, secondErr) {
		t.Fatalf("Reconcile() error = %v, want second discovery-ordered error %v", err, secondErr)
	}
	if got := err.Error(); got != "create GitHub client for repository \"https://github.com/octo/repository-001\": first repository failed\n"+
		"create GitHub client for repository \"https://github.com/octo/repository-002\": second repository failed" {
		t.Fatalf("Reconcile() error order = %q", got)
	}
}

type reconcileResult struct {
	snapshot []githubsource.Registration
	err      error
}
