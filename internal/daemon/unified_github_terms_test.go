package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
)

func TestGitHubLeadershipReconnectsCommittedRepositoryWhileDiscoveryIsBlocked(t *testing.T) {
	t.Parallel()

	record := githubsource.Registration{RepositoryURL: "https://github.example/octo/repo", InstallationID: 1,
		RunnerPool: "linux", RunnerGroupID: 2, ScaleSetID: 3}
	manager := newTermManagerFake(t, githubsource.RegistrationSnapshot{Active: []githubsource.Registration{record}})
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
			RunnerPool: "linux", RunnerGroup: "default",
		}}}, time.Minute, func() time.Time { return time.Time{} })

	openContext, cancelOpen := context.WithTimeout(context.Background(), time.Second)
	defer cancelOpen()
	opened := make(chan error, 1)
	go func() { opened <- source.OpenLeader(openContext) }()
	<-manager.reconcileStarted
	select {
	case err := <-opened:
		if err != nil {
			t.Fatalf("OpenLeader() error = %v", err)
		}
	case <-openContext.Done():
		close(manager.releaseReconcile)
		t.Fatalf("OpenLeader() waited for discovery: %v", openContext.Err())
	}
	<-manager.sessionOpened
	close(manager.releaseReconcile)
	if err := source.CloseLeader(context.Background()); err != nil {
		t.Fatalf("CloseLeader() error = %v", err)
	}
}

func TestGitHubLeadershipReconnectsCommittedRegistrationAbsentFromLocalConfiguration(t *testing.T) {
	t.Parallel()

	record := githubsource.Registration{RepositoryURL: "https://github.example/octo/repo", InstallationID: 1,
		RunnerPool: "retired", RunnerGroupID: 2, ScaleSetID: 3}
	manager := newTermManagerFake(t, githubsource.RegistrationSnapshot{Active: []githubsource.Registration{record}})
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })

	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("OpenLeader() error = %v", err)
	}
	if len(source.desired) != 1 || len(source.opening) != 1 {
		close(manager.releaseReconcile)
		t.Fatalf("committed registration was not restored: desired=%#v opening=%#v",
			source.desired, source.opening)
	}
	<-manager.sessionOpened
	close(manager.releaseReconcile)
	if err := source.CloseLeader(context.Background()); err != nil {
		t.Fatalf("CloseLeader() error = %v", err)
	}
}

func TestGitHubLeadershipRestoresSiblingWhenScaleSetIsMissing(t *testing.T) {
	t.Parallel()

	records := []githubsource.Registration{
		{RepositoryURL: "https://github.example/octo/repo", InstallationID: 1,
			RunnerPool: "linux", RunnerGroupID: 2, ScaleSetID: 3},
		{RepositoryURL: "https://github.example/octo/repo", InstallationID: 1,
			RunnerPool: "windows", RunnerGroupID: 2, ScaleSetID: 4},
	}
	manager := newTermManagerFake(t, githubsource.RegistrationSnapshot{Active: records, Owned: records},
		map[transport.ScaleSetID]error{4: upstream.NotFoundError})
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{
			{RunnerPool: "linux", RunnerGroup: "default"},
			{RunnerPool: "windows", RunnerGroup: "default"},
		}}, time.Minute, func() time.Time { return time.Time{} })

	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("OpenLeader() error = %v", err)
	}
	awaitInstalledGitHubAdapters(t, source, 1)
	if len(source.desired) != 1 || source.desired[registrationKey(records[0])] != records[0] {
		t.Fatalf("restored registrations = %#v", source.desired)
	}
	close(manager.releaseReconcile)
	if err := source.CloseLeader(context.Background()); err != nil {
		t.Fatalf("CloseLeader() error = %v", err)
	}
}

func TestGitHubLeadershipActivatesSiblingWhileTransientFailureRetries(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("session temporarily unavailable")
	records := []githubsource.Registration{
		{RepositoryURL: "https://github.example/octo/repo", InstallationID: 1,
			RunnerPool: "linux", RunnerGroupID: 2, ScaleSetID: 3},
		{RepositoryURL: "https://github.example/octo/repo", InstallationID: 1,
			RunnerPool: "windows", RunnerGroupID: 2, ScaleSetID: 4},
	}
	manager := newTermManagerFake(t, githubsource.RegistrationSnapshot{Active: records, Owned: records},
		map[transport.ScaleSetID]error{4: wantErr})
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{
			{RunnerPool: "linux", RunnerGroup: "default"},
			{RunnerPool: "windows", RunnerGroup: "default"},
		}}, time.Minute, func() time.Time { return time.Time{} })

	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("OpenLeader() error = %v", err)
	}
	awaitGitHubOpeningError(t, source, wantErr)
	awaitInstalledSiblingSessions(t, source, wantErr, 1)
	if len(source.adapters) != 1 || len(source.desired) != 2 {
		t.Fatalf("active adapters = %d, retryable registrations = %#v", len(source.adapters), source.desired)
	}
	close(manager.releaseReconcile)
	if err := source.CloseLeader(context.Background()); err != nil {
		t.Fatalf("CloseLeader() error = %v", err)
	}
}

func TestGitHubLeadershipAccumulatesSuccessfulSiblingFromLaterRetry(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("session temporarily unavailable")
	source, manager := newAccumulatingSiblingFixture(t, wantErr)
	awaitGitHubOpeningError(t, source, wantErr)
	manager.setOpenErrors(map[transport.ScaleSetID]error{5: wantErr})
	awaitInstalledSiblingSessions(t, source, wantErr, 2)
	if len(source.adapters) != 2 {
		close(manager.releaseReconcile)
		t.Fatalf("active sibling sessions = %d, want 2 after later retry", len(source.adapters))
	}
	close(manager.releaseReconcile)
	if err := source.CloseLeader(context.Background()); err != nil {
		t.Fatalf("CloseLeader() error = %v", err)
	}
}

func newAccumulatingSiblingFixture(
	t *testing.T,
	wantErr error,
) (*githubSourceRuntime, *termManagerFake) {
	t.Helper()
	records := []githubsource.Registration{
		{RepositoryURL: "https://github.example/octo/repo", InstallationID: 1,
			RunnerPool: "linux", RunnerGroupID: 2, ScaleSetID: 3},
		{RepositoryURL: "https://github.example/octo/repo", InstallationID: 2,
			RunnerPool: "windows", RunnerGroupID: 2, ScaleSetID: 4},
		{RepositoryURL: "https://github.example/octo/repo", InstallationID: 3,
			RunnerPool: "macos", RunnerGroupID: 2, ScaleSetID: 5},
	}
	manager := newTermManagerFake(t, githubsource.RegistrationSnapshot{Active: records, Owned: records},
		map[transport.ScaleSetID]error{4: wantErr, 5: wantErr})
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{
			{RunnerPool: "linux", RunnerGroup: "default"},
			{RunnerPool: "windows", RunnerGroup: "default"},
			{RunnerPool: "macos", RunnerGroup: "default"},
		}}, time.Minute, func() time.Time { return time.Time{} })

	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("OpenLeader() error = %v", err)
	}
	return source, manager
}

func awaitInstalledSiblingSessions(
	t *testing.T,
	source *githubSourceRuntime,
	wantErr error,
	want int,
) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for len(source.adapters) < want {
		select {
		case result := <-source.term.openResults:
			source.term.openResults <- result
		case <-timer.C:
			t.Fatalf("timed out waiting for %d active sibling sessions; active=%d opening=%d desired=%d",
				want, len(source.adapters), len(source.opening), len(source.desired))
		}
		err := source.Poll(context.Background(), time.Time{})
		if err != nil && !errors.Is(err, wantErr) {
			t.Fatalf("Poll() error = %v, want retry failure %v", err, wantErr)
		}
	}
}

type termManagerFake struct {
	manager          *githubsource.Manager
	snapshot         githubsource.RegistrationSnapshot
	openErrorsMutex  sync.RWMutex
	openErrors       map[transport.ScaleSetID]error
	reconcileStarted chan struct{}
	releaseReconcile chan struct{}
	sessionOpened    chan struct{}
}

func newTermManagerFake(
	t *testing.T,
	snapshot githubsource.RegistrationSnapshot,
	openErrors ...map[transport.ScaleSetID]error,
) *termManagerFake {
	t.Helper()
	fake := &termManagerFake{snapshot: snapshot, reconcileStarted: make(chan struct{}),
		releaseReconcile: make(chan struct{}), sessionOpened: make(chan struct{}, 1)}
	if len(openErrors) != 0 {
		fake.openErrors = openErrors[0]
	}
	manager, err := githubsource.NewManager(termDiscoverer{}, fake.newClient,
		&termRegistrationRepository{snapshot: snapshot}, githubsource.ManagerObserver{})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	fake.manager = manager
	return fake
}

func (fake *termManagerFake) newClient(
	context.Context,
	transport.Repository,
) (githubsource.ManagerClient, error) {
	return githubsource.ManagerClient{OpenSession: func(
		ctx context.Context,
		registration transport.Registration,
		owner string,
	) (githubsource.ManagedSession, error) {
		return fake.openSession(ctx, registration, owner)
	},
		ResolveRunnerGroup: func(context.Context, string) (transport.RunnerGroupID, error) { return 2, nil }}, nil
}

func (fake *termManagerFake) openSession(
	_ context.Context,
	registration transport.Registration,
	_ string,
) (*termSessionFake, error) {
	fake.openErrorsMutex.RLock()
	openErr := fake.openErrors[registration.ID]
	fake.openErrorsMutex.RUnlock()
	if openErr != nil {
		return nil, openErr
	}
	select {
	case fake.sessionOpened <- struct{}{}:
	default:
	}
	return &termSessionFake{}, nil
}

func (fake *termManagerFake) setOpenErrors(openErrors map[transport.ScaleSetID]error) {
	fake.openErrorsMutex.Lock()
	defer fake.openErrorsMutex.Unlock()
	fake.openErrors = openErrors
}

func (fake *termManagerFake) ActiveRegistrations(context.Context) ([]githubsource.Registration, error) {
	return fake.snapshot.Active, nil
}

func (fake *termManagerFake) Reconcile(ctx context.Context, _ githubsource.ManagerConfig) ([]githubsource.Registration, error) {
	close(fake.reconcileStarted)
	select {
	case <-fake.releaseReconcile:
		return fake.snapshot.Active, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for reconciliation release: %w", ctx.Err())
	}
}

func (*termManagerFake) RefreshMembers(context.Context, githubsource.ManagerConfig) error { return nil }

func (fake *termManagerFake) OpenRegistrationSession(
	ctx context.Context,
	record githubsource.Registration,
	configured githubsource.RunnerPoolConfig,
	owner string,
	handler githubsource.EventHandler,
) (*githubsource.RegistrationSession, error) {
	session, err := fake.manager.OpenRegistrationSession(ctx, record, configured, owner, handler)
	if err != nil {
		return session, fmt.Errorf("open registration session: %w", err)
	}
	return session, nil
}

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
func (repository *termRegistrationRepository) Registrations(context.Context) ([]githubsource.Registration, error) {
	return append([]githubsource.Registration(nil), repository.snapshot.Active...), nil
}
func (repository *termRegistrationRepository) OwnedRegistrations(context.Context) ([]githubsource.Registration, error) {
	return append([]githubsource.Registration(nil), repository.snapshot.Owned...), nil
}
func (repository *termRegistrationRepository) UpsertRegistration(_ context.Context, record githubsource.Registration) error {
	repository.snapshot.Active = append(repository.snapshot.Active, record)
	repository.snapshot.Owned = append(repository.snapshot.Owned, record)
	return nil
}
func (*termRegistrationRepository) DeleteActiveRegistration(context.Context, githubsource.Registration) error {
	return nil
}
func (*termRegistrationRepository) DeleteOwnedRegistration(context.Context, githubsource.Registration) error {
	return nil
}
func (*termRegistrationRepository) DeleteRegistrationState(context.Context, githubsource.Registration) error {
	return nil
}
func (*termRegistrationRepository) DeleteRepositoryRegistrationState(
	context.Context,
	string,
	transport.InstallationID,
) error {
	return nil
}

type termSessionFake struct{}

func (termSessionFake) GetMessage(context.Context, int, int) (*upstream.RunnerScaleSetMessage, error) {
	return nil, githubsource.ErrNoMessage
}
func (termSessionFake) AcquireRequests(context.Context, []int64) ([]int64, error) { return nil, nil }
func (termSessionFake) DeleteMessage(context.Context, int) error                  { return nil }
func (termSessionFake) Close(context.Context) error                               { return nil }
