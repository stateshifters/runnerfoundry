//go:build e2e

package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/runtime"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	"github.com/stateshifters/runnerfoundry/internal/source/github/transport"
	"github.com/stateshifters/runnerfoundry/internal/state/sqlite"
)

func TestProductContractGitHubRepositoriesReconcileAndPollIndependently(t *testing.T) {
	if !e2eBackendEnabled("sqlite") {
		return
	}
	ctx := context.Background()
	fixture := newMultiRepositoryFixture(t)
	fixture.reconcile(ctx, t)
	fixture.assertRouteLocalConflict(ctx, t)
	first := fixture.openSession(ctx, t, fixture.records[0])
	second := fixture.openSession(ctx, t, fixture.records[1])
	exerciseSharedRepositoryCapacityAndFIFO(ctx, t, fixture.store, fixture.host, first, second)
}

type multiRepositoryFixture struct {
	store     *sqlite.Store
	host      *multiRepositoryGitHost
	discovery *paginatedRepositoryDiscovery
	manager   *githubsource.Manager
	config    githubsource.RunnerPoolConfig
	handler   githubsource.EventHandler
	records   []githubsource.Registration
}

func newMultiRepositoryFixture(t *testing.T) *multiRepositoryFixture {
	t.Helper()
	ctx := context.Background()
	store, err := sqlite.Open(ctx, ":memory:")
	if err != nil {
		t.Fatalf("open multi-repository SQLite state: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close multi-repository SQLite state: %v", closeErr)
		}
	})
	if err = store.PublishMemberState(ctx, "multi-repository-member", time.Now(), nil); err != nil {
		t.Fatalf("publish multi-repository member: %v", err)
	}

	fixture := &multiRepositoryFixture{store: store, host: newMultiRepositoryGitHost(6)}
	fixture.discovery = newPaginatedRepositoryDiscovery(fixture.host.repositories)
	manager, err := githubsource.NewManager(
		transport.Discoverer{API: fixture.discovery}, fixture.host.Client, store,
		githubsource.ManagerObserver{CurrentTime: time.Now},
	)
	if err != nil {
		t.Fatalf("build multi-repository manager: %v", err)
	}
	fixture.manager = manager
	fixture.config = githubsource.RunnerPoolConfig{RunnerPool: "linux", RunnerGroup: "default"}
	fixture.handler = newGitHubDemandHandler(runtime.NewQueueHandler(store), store, nil, "sqlite")
	return fixture
}

func (fixture *multiRepositoryFixture) reconcile(ctx context.Context, t *testing.T) {
	t.Helper()
	config := githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{fixture.config}}
	reconcileResult := make(chan error, 1)
	go func() {
		_, reconcileErr := fixture.manager.Reconcile(ctx, config)
		reconcileResult <- reconcileErr
	}()
	fixture.host.assertRepositoryConcurrency(t, 4)
	fixture.host.releaseEnsures()
	if reconcileErr := <-reconcileResult; reconcileErr == nil {
		t.Fatal("multi-repository reconciliation unexpectedly ignored one failed route")
	}
	fixture.discovery.assertPages(t)
	assertMultiRepositoryRegistrations(ctx, t, fixture.manager, 5)
	fixture.records = multiRepositoryRecords(ctx, t, fixture.manager)
}

func (fixture *multiRepositoryFixture) assertRouteLocalConflict(ctx context.Context, t *testing.T) {
	t.Helper()
	fixture.host.conflictNextOpen(fixture.records[0].RepositoryURL)
	session, err := fixture.manager.OpenRegistrationSession(
		ctx, fixture.records[0], fixture.config, "owner", fixture.handler,
	)
	if session != nil || !errors.Is(err, upstream.ConflictError) {
		t.Fatalf("route-local 409 restoration = (%v, %v), want retained conflict", session, err)
	}
	assertMultiRepositoryRegistrations(ctx, t, fixture.manager, 5)
}

func (fixture *multiRepositoryFixture) openSession(
	ctx context.Context,
	t *testing.T,
	record githubsource.Registration,
) *githubsource.RegistrationSession {
	t.Helper()
	session := openMultiRepositorySession(
		ctx, t, fixture.manager, record, fixture.config, fixture.handler,
	)
	t.Cleanup(func() {
		if closeErr := session.Close(ctx); closeErr != nil {
			t.Errorf("close multi-repository session: %v", closeErr)
		}
	})
	return session
}

func assertMultiRepositoryRegistrations(
	ctx context.Context,
	t *testing.T,
	manager *githubsource.Manager,
	want int,
) {
	t.Helper()
	snapshot, err := manager.RegistrationSnapshot(ctx)
	if err != nil || len(snapshot.Active) != want || len(snapshot.Owned) != want {
		t.Fatalf("multi-repository registration snapshot = %+v, error = %v, want %d routes", snapshot, err, want)
	}
}

func multiRepositoryRecords(
	ctx context.Context,
	t *testing.T,
	manager *githubsource.Manager,
) []githubsource.Registration {
	t.Helper()
	snapshot, err := manager.RegistrationSnapshot(ctx)
	if err != nil || len(snapshot.Active) < 2 {
		t.Fatalf("load multi-repository registrations = %+v, error = %v", snapshot, err)
	}
	return snapshot.Active
}

func openMultiRepositorySession(
	ctx context.Context,
	t *testing.T,
	manager *githubsource.Manager,
	record githubsource.Registration,
	configured githubsource.RunnerPoolConfig,
	handler githubsource.EventHandler,
) *githubsource.RegistrationSession {
	t.Helper()
	session, err := manager.OpenRegistrationSession(ctx, record, configured, "owner", handler)
	if err != nil {
		t.Fatalf("open multi-repository session for %q: %v", record.RepositoryURL, err)
	}
	return session
}

func exerciseSharedRepositoryCapacityAndFIFO(
	ctx context.Context,
	t *testing.T,
	store *sqlite.Store,
	host *multiRepositoryGitHost,
	first *githubsource.RegistrationSession,
	second *githubsource.RegistrationSession,
) {
	t.Helper()
	firstRecord, secondRecord := first.Registration(), second.Registration()
	host.queue(firstRecord.RepositoryURL, 2101, time.Now().Add(-time.Minute))
	host.queue(secondRecord.RepositoryURL, 2102, time.Now())
	for _, session := range []*githubsource.RegistrationSession{first, second} {
		result, err := session.Adapter().Poll(ctx, 1)
		if err != nil || !result.Acknowledged {
			t.Fatalf("multi-repository poll = (%+v, %v)", result, err)
		}
	}
	for _, repository := range []string{firstRecord.RepositoryURL, secondRecord.RepositoryURL} {
		assertPublishedCapacity(t, host.sessions[repository], 1, "shared multi-repository capacity")
	}
	firstClaim := claimMultiRepositoryJob(ctx, t, store, time.Now())
	secondClaim := claimMultiRepositoryJob(ctx, t, store, time.Now().Add(time.Second))
	assertClaimRepository(t, firstClaim, firstRecord.RepositoryURL)
	assertClaimRepository(t, secondClaim, secondRecord.RepositoryURL)
}

func claimMultiRepositoryJob(
	ctx context.Context,
	t *testing.T,
	store *sqlite.Store,
	claimedAt time.Time,
) core.ProcessingJob {
	t.Helper()
	result, err := store.ClaimOldest(ctx, "linux", core.ProcessingClaim{
		MemberID: "multi-repository-member", ClaimedAt: claimedAt, InventoryEndpointID: "multi-repository-endpoint",
	})
	if err != nil || result.Outcome != core.ClaimClaimed {
		t.Fatalf("claim multi-repository job = %+v, error = %v", result, err)
	}
	return result.Processing
}

func assertClaimRepository(t *testing.T, processing core.ProcessingJob, want string) {
	t.Helper()
	identity, err := githubsource.DecodeSourceID(processing.Job.Source.ID)
	if err != nil || identity.Repository != want {
		t.Fatalf("claimed multi-repository identity = %+v, error = %v, want repository %q", identity, err, want)
	}
}

type multiRepositoryGitHost struct {
	mutex             sync.Mutex
	repositories      []transport.Repository
	sessions          map[string]*e2eGitHubSession
	ensureStarted     chan string
	ensureRelease     chan struct{}
	activeEnsures     int
	maximumEnsures    int
	failingRepository string
	conflictOpen      map[string]bool
}

func newMultiRepositoryGitHost(count int) *multiRepositoryGitHost {
	host := &multiRepositoryGitHost{
		repositories: make([]transport.Repository, 0, count), sessions: make(map[string]*e2eGitHubSession, count),
		ensureStarted: make(chan string, count), ensureRelease: make(chan struct{}), conflictOpen: make(map[string]bool),
	}
	for index := 0; index < count; index++ {
		url := fmt.Sprintf("https://github.example/acme/repository-%d", index)
		installationID := transport.InstallationID(71)
		if index == count-1 {
			installationID = 72
		}
		host.repositories = append(host.repositories, transport.Repository{InstallationID: installationID, URL: url})
		host.sessions[url] = newE2EGitHubSession()
	}
	host.failingRepository = host.repositories[count-1].URL
	return host
}

func (host *multiRepositoryGitHost) Discover(context.Context) (transport.DiscoveryResult, error) {
	return transport.DiscoveryResult{Repositories: append([]transport.Repository(nil), host.repositories...)}, nil
}

func (host *multiRepositoryGitHost) Client(
	_ context.Context,
	repository transport.Repository,
) (githubsource.ManagerClient, error) {
	return githubsource.ManagerClient{
		EnsureRunnerPool: func(ctx context.Context, pool, group string) (transport.Registration, error) {
			return host.ensureRunnerPool(ctx, repository, pool, group)
		},
		ResolveRunnerGroup: func(context.Context, string) (transport.RunnerGroupID, error) { return 11, nil },
		OpenSession: func(
			_ context.Context, registration transport.Registration, _ string,
		) (githubsource.ManagedSession, error) {
			return host.openSession(repository, registration)
		},
		GenerateBootstrap: func(context.Context, transport.Registration, string) ([]byte, error) {
			return []byte("multi-repository-bootstrap"), nil
		},
		CleanupRunner: func(context.Context, string) error { return nil },
	}, nil
}

func (host *multiRepositoryGitHost) ensureRunnerPool(
	ctx context.Context,
	repository transport.Repository,
	pool string,
	group string,
) (transport.Registration, error) {
	host.mutex.Lock()
	host.activeEnsures++
	if host.activeEnsures > host.maximumEnsures {
		host.maximumEnsures = host.activeEnsures
	}
	host.mutex.Unlock()
	host.ensureStarted <- repository.URL
	select {
	case <-host.ensureRelease:
	case <-ctx.Done():
		return transport.Registration{}, fmt.Errorf("wait to ensure Runner Pool: %w", ctx.Err())
	}
	host.mutex.Lock()
	host.activeEnsures--
	host.mutex.Unlock()
	if repository.URL == host.failingRepository {
		return transport.Registration{}, errors.New("scripted repository reconciliation failure")
	}
	if pool != "linux" || group != "default" {
		return transport.Registration{}, errors.New("unexpected multi-repository Runner Pool")
	}
	identifier := transport.ScaleSetID(100 + repositoryIndex(host.repositories, repository.URL))
	return transport.Registration{ID: identifier, Name: pool, RunnerGroupID: 11}, nil
}

func repositoryIndex(repositories []transport.Repository, url string) int {
	for index, repository := range repositories {
		if repository.URL == url {
			return index
		}
	}
	return -100
}

func (host *multiRepositoryGitHost) assertRepositoryConcurrency(t *testing.T, want int) {
	t.Helper()
	for index := 0; index < want; index++ {
		select {
		case <-host.ensureStarted:
		case <-time.After(e2eWaitLimit):
			t.Fatalf("timed out waiting for repository ensure %d of %d", index+1, want)
		}
	}
	select {
	case repository := <-host.ensureStarted:
		t.Fatalf("repository concurrency exceeded %d with %q", want, repository)
	default:
	}
	host.mutex.Lock()
	maximum := host.maximumEnsures
	host.mutex.Unlock()
	if maximum != want {
		t.Fatalf("repository concurrency = %d, want %d", maximum, want)
	}
}

func (host *multiRepositoryGitHost) releaseEnsures() { close(host.ensureRelease) }

func (host *multiRepositoryGitHost) conflictNextOpen(repository string) {
	host.mutex.Lock()
	host.conflictOpen[repository] = true
	host.mutex.Unlock()
}

func (host *multiRepositoryGitHost) openSession(
	repository transport.Repository,
	registration transport.Registration,
) (*e2eGitHubSession, error) {
	host.mutex.Lock()
	conflict := host.conflictOpen[repository.URL]
	delete(host.conflictOpen, repository.URL)
	session := host.sessions[repository.URL]
	host.mutex.Unlock()
	if conflict {
		return nil, upstream.ConflictError
	}
	if session == nil || registration.Name != "linux" {
		return nil, errors.New("unexpected multi-repository session")
	}
	return session, nil
}

func (host *multiRepositoryGitHost) queue(repository string, requestID int64, queuedAt time.Time) {
	session := host.sessions[repository]
	message := &upstream.RunnerScaleSetMessage{
		Statistics: e2eMessageStatistics(1),
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{
			RunnerRequestID: requestID, QueueTime: queuedAt,
		}}},
	}
	session.assignMessageID(message)
	session.messages <- message
}

type paginatedRepositoryDiscovery struct {
	mutex             sync.Mutex
	repositories      []transport.Repository
	installationPages []int
	repositoryPages   map[transport.InstallationID][]int
}

func newPaginatedRepositoryDiscovery(repositories []transport.Repository) *paginatedRepositoryDiscovery {
	return &paginatedRepositoryDiscovery{
		repositories:    append([]transport.Repository(nil), repositories...),
		repositoryPages: make(map[transport.InstallationID][]int),
	}
}

func (api *paginatedRepositoryDiscovery) ListInstallations(
	_ context.Context,
	page int,
) ([]transport.InstallationID, int, error) {
	api.mutex.Lock()
	api.installationPages = append(api.installationPages, page)
	api.mutex.Unlock()
	if page == 1 {
		return []transport.InstallationID{71}, 2, nil
	}
	if page == 2 {
		return []transport.InstallationID{72}, 0, nil
	}
	return nil, 0, fmt.Errorf("unexpected installation page %d", page)
}

func (api *paginatedRepositoryDiscovery) ListRepositories(
	_ context.Context,
	installationID transport.InstallationID,
	page int,
) ([]string, int, error) {
	api.mutex.Lock()
	api.repositoryPages[installationID] = append(api.repositoryPages[installationID], page)
	api.mutex.Unlock()
	matching := make([]string, 0)
	for _, repository := range api.repositories {
		if repository.InstallationID == installationID {
			matching = append(matching, repository.URL)
		}
	}
	if installationID == 71 && page == 1 {
		return append([]string(nil), matching[:3]...), 2, nil
	}
	if installationID == 71 && page == 2 {
		return append([]string(nil), matching[3:]...), 0, nil
	}
	if installationID == 72 && page == 1 {
		return matching, 0, nil
	}
	return nil, 0, fmt.Errorf("unexpected repository page %d for installation %d", page, installationID)
}

func (api *paginatedRepositoryDiscovery) assertPages(t *testing.T) {
	t.Helper()
	api.mutex.Lock()
	defer api.mutex.Unlock()
	if fmt.Sprint(api.installationPages) != "[1 2]" ||
		fmt.Sprint(api.repositoryPages[71]) != "[1 2]" || fmt.Sprint(api.repositoryPages[72]) != "[1]" {
		t.Fatalf("paginated discovery calls = installations %v repositories %v",
			api.installationPages, api.repositoryPages)
	}
}

var _ transport.DiscoveryAPI = (*paginatedRepositoryDiscovery)(nil)
