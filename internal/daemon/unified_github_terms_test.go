package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestGitHubLeadershipReconnectsCommittedRepositoryWhileDiscoveryIsBlocked(t *testing.T) {
	t.Parallel()

	record := githubsource.Registration{RepositoryURL: "https://github.example/octo/repo", InstallationID: 1,
		RunnerPool: "linux", RunnerGroupID: 2, ScaleSetID: 3}
	manager := newTermManagerFake(t, githubsource.RegistrationSnapshot{Active: []githubsource.Registration{record}})
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })

	opened := make(chan error, 1)
	go func() { opened <- source.OpenLeader(context.Background()) }()
	<-manager.reconcileStarted
	select {
	case err := <-opened:
		if err != nil {
			t.Fatalf("OpenLeader() error = %v", err)
		}
	default:
		close(manager.releaseReconcile)
		t.Fatal("OpenLeader() waited for discovery")
	}
	<-manager.sessionOpened
	close(manager.releaseReconcile)
	if err := source.CloseLeader(context.Background()); err != nil {
		t.Fatalf("CloseLeader() error = %v", err)
	}
}

type termManagerFake struct {
	manager          *githubsource.Manager
	snapshot         githubsource.RegistrationSnapshot
	reconcileStarted chan struct{}
	releaseReconcile chan struct{}
	sessionOpened    chan struct{}
}

func newTermManagerFake(t *testing.T, snapshot githubsource.RegistrationSnapshot) *termManagerFake {
	t.Helper()
	fake := &termManagerFake{snapshot: snapshot, reconcileStarted: make(chan struct{}),
		releaseReconcile: make(chan struct{}), sessionOpened: make(chan struct{}, 1)}
	repository := &termRegistrationRepository{snapshot: snapshot}
	manager, err := githubsource.NewManager(termDiscoverer{}, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{OpenSession: func(
			context.Context,
			transport.Registration,
			string,
		) (githubsource.ManagedSession, error) {
			fake.sessionOpened <- struct{}{}
			return termSessionFake{}, nil
		}}, nil
	}, repository, githubsource.ManagerObserver{})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	fake.manager = manager
	return fake
}

func (fake *termManagerFake) RegistrationSnapshot(context.Context) (githubsource.RegistrationSnapshot, error) {
	return fake.snapshot, nil
}

func (fake *termManagerFake) Reconcile(ctx context.Context, _ githubsource.ManagerConfig) (githubsource.RegistrationSnapshot, error) {
	close(fake.reconcileStarted)
	select {
	case <-fake.releaseReconcile:
		return fake.snapshot, nil
	case <-ctx.Done():
		return githubsource.RegistrationSnapshot{}, fmt.Errorf("wait for reconciliation release: %w", ctx.Err())
	}
}

func (*termManagerFake) RefreshMembers(context.Context) error { return nil }

func (fake *termManagerFake) OpenRepositorySessions(ctx context.Context, records []githubsource.Registration,
	owner string, handler githubsource.EventHandler,
) (*githubsource.RepositorySessionGeneration, error) {
	generation, err := fake.manager.OpenRepositorySessions(ctx, records, owner, handler)
	if err != nil {
		return generation, fmt.Errorf("open repository sessions: %w", err)
	}
	return generation, nil
}

func (*termManagerFake) OpenLeader(context.Context, string, githubsource.EventHandler) ([]*githubsource.Adapter, error) {
	return nil, nil
}
func (*termManagerFake) CloseLeader(context.Context) error { return nil }

type termDiscoverer struct{}

func (termDiscoverer) Discover(context.Context) (transport.DiscoveryResult, error) {
	return transport.DiscoveryResult{}, nil
}

type termRegistrationRepository struct {
	snapshot githubsource.RegistrationSnapshot
}

func (repository *termRegistrationRepository) RegistrationSnapshot(context.Context) (githubsource.RegistrationSnapshot, error) {
	return repository.snapshot, nil
}
func (repository *termRegistrationRepository) ReplaceRegistrationSnapshot(
	_ context.Context,
	snapshot githubsource.RegistrationSnapshot,
) error {
	repository.snapshot = snapshot
	return nil
}

type termSessionFake struct{}

func (termSessionFake) GetMessage(context.Context, int, int) (*upstream.RunnerScaleSetMessage, error) {
	return nil, githubsource.ErrNoMessage
}
func (termSessionFake) AcquireRequests(context.Context, []int64) ([]int64, error) { return nil, nil }
func (termSessionFake) DeleteMessage(context.Context, int) error                  { return nil }
func (termSessionFake) Close(context.Context) error                               { return nil }
