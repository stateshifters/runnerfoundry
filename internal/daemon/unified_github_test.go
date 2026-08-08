package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func TestGitHubSourceRuntimePublishesAvailableCapacityWithEachPoll(t *testing.T) {
	t.Parallel()

	messageClient := &capacityMessageClientFake{finished: make(chan struct{}, 1)}
	adapter := &githubsource.Adapter{Client: messageClient, Handler: eventHandlerFake{}, RunnerPool: "linux"}
	manager := &githubManagerFake{adapters: []*githubsource.Adapter{
		adapter,
		{Client: &capacityMessageClientFake{}, Handler: eventHandlerFake{}, RunnerPool: "linux"},
	}}
	capacities := &capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 7}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	source := newGitHubSourceRuntime(manager, capacities, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{RunnerPools: []githubsource.RunnerPoolConfig{{
			RunnerPool: "linux", RunnerGroup: "default",
		}}}, time.Minute, func() time.Time { return now })

	mustOpenGitHubSource(t, source)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	mustCloseGitHubSource(t, source)
	if messageClient.maxCapacity != 7 {
		t.Fatalf("poll capacity = %d, want available 7", messageClient.maxCapacity)
	}
	if len(capacities.queries) != 2 {
		t.Fatalf("available-capacity queries = %d, want one per repository", len(capacities.queries))
	}
}

func TestGitHubSourceRuntimeReconcilesDiscoveryOnlyWhenDue(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	manager := &githubManagerFake{}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })

	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open leader: %v", err)
	}
	awaitGitHubDiscovery(t, source)
	if err := source.Poll(context.Background(), now); err != nil {
		t.Fatalf("publish before discovery is due: %v", err)
	}
	now = now.Add(time.Minute)
	if err := source.Poll(context.Background(), now); err != nil {
		t.Fatalf("publish when discovery is due: %v", err)
	}
	awaitGitHubDiscovery(t, source)
	if manager.reconciles != 2 || manager.opens != 0 {
		t.Fatalf("reconciles=%d opens=%d, want two discoveries and no empty repository opens", manager.reconciles, manager.opens)
	}
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimePreservesPollsForReorderedEquivalentRegistrations(t *testing.T) {
	t.Parallel()
	fixture := newReorderedRegistrationFixture()

	mustOpenGitHubSource(t, fixture.source)
	awaitInstalledGitHubAdapters(t, fixture.source, len(fixture.clients))
	awaitGitHubDiscovery(t, fixture.source)
	mustPollGitHubSource(t, fixture.source, time.Time{})
	for index, client := range fixture.clients {
		assertPolledCapacity(t, client.started, 1, fmt.Sprintf("repository session %d", index+1))
	}
	fixture.manager.setReconcileSnapshot(fixture.reordered)
	fixture.now = fixture.now.Add(time.Minute)
	mustPollGitHubSource(t, fixture.source, time.Time{})
	awaitGitHubDiscovery(t, fixture.source)

	if len(fixture.source.opening) != 0 || fixture.manager.opens != 2 {
		t.Fatalf("equivalent discovery scheduled replacements: opening=%d opens=%d",
			len(fixture.source.opening), fixture.manager.opens)
	}
	if len(fixture.source.polls) != len(fixture.clients) {
		t.Fatalf("active polls = %d, want %d preserved", len(fixture.source.polls), len(fixture.clients))
	}
	fixture.clients[0].messages <- &upstream.RunnerScaleSetMessage{MessageID: 9,
		JobAvailableMessages: []*upstream.JobAvailable{{JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42}}}}
	request := <-fixture.queued
	if request.RunnerPool != "linux" {
		t.Fatalf("preserved poll queued work for Runner Pool %q", request.RunnerPool)
	}
	mustCloseGitHubSource(t, fixture.source)
}

type reorderedRegistrationFixture struct {
	source    *githubSourceRuntime
	manager   *githubManagerFake
	clients   []*blockingCapacityMessageClient
	reordered githubsource.RegistrationSnapshot
	queued    chan core.JobRequest
	now       time.Time
}

func newReorderedRegistrationFixture() *reorderedRegistrationFixture {
	fixture := &reorderedRegistrationFixture{
		now: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC), queued: make(chan core.JobRequest, 1),
	}
	fixture.clients = []*blockingCapacityMessageClient{
		newBlockingCapacityMessageClient(), newBlockingCapacityMessageClient(),
		newBlockingCapacityMessageClient(), newBlockingCapacityMessageClient(),
	}
	fixture.manager = &githubManagerFake{adapters: []*githubsource.Adapter{
		{Client: fixture.clients[0], Repository: "https://github.example/one", RunnerPool: "linux"},
		{Client: fixture.clients[1], Repository: "https://github.example/one", RunnerPool: "windows"},
		{Client: fixture.clients[2], Repository: "https://github.example/two", RunnerPool: "linux"},
		{Client: fixture.clients[3], Repository: "https://github.example/two", RunnerPool: "windows"},
	}}
	facts := fixture.manager.snapshotFacts()
	fixture.reordered = githubsource.RegistrationSnapshot{Active: []githubsource.Registration{
		facts.Active[3], facts.Active[2], facts.Active[1], facts.Active[0],
	}}
	fixture.source = newGitHubSourceRuntime(fixture.manager,
		&capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 1, "windows": 1}},
		"member-1", recordingQueueEventHandler{queued: fixture.queued}, githubsource.ManagerConfig{}, time.Minute,
		func() time.Time { return fixture.now })
	return fixture
}

func TestGitHubSourceRuntimeRefreshesMemberRoutesAtDiscoveryCadence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	manager := &githubManagerFake{}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })

	if err := source.RefreshMembers(context.Background()); err != nil {
		t.Fatalf("refresh member routes: %v", err)
	}
	if err := source.RefreshMembers(context.Background()); err != nil {
		t.Fatalf("refresh member routes before due: %v", err)
	}
	now = now.Add(time.Minute)
	if err := source.RefreshMembers(context.Background()); err != nil {
		t.Fatalf("refresh member routes when due: %v", err)
	}
	if got := occurrences(manager.events, "refresh"); got != 2 {
		t.Fatalf("route refreshes = %d, want 2", got)
	}
}

func TestGitHubSourceRuntimeRestoresExistingAdaptersAfterReplacementOpenFails(t *testing.T) {
	t.Parallel()

	client := newBlockingCapacityMessageClient()
	adapter := &githubsource.Adapter{Client: client, Handler: eventHandlerFake{}, Repository: "one", RunnerPool: "linux"}
	manager := &githubManagerFake{adapters: []*githubsource.Adapter{adapter}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	source := newGitHubSourceRuntime(manager,
		&capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 1}},
		"member-1", eventHandlerFake{}, githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })
	mustOpenGitHubSource(t, source)

	now = now.Add(time.Minute)
	replacementErr := errors.New("replacement failed")
	manager.setRepositoryOpen(
		[]*githubsource.Adapter{{Client: &capacityMessageClientFake{}, Handler: eventHandlerFake{},
			Repository: "one", RunnerPool: "windows"}}, replacementErr)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	awaitGitHubDiscovery(t, source)
	awaitGitHubOpeningError(t, source, replacementErr)
	if len(source.adapters) != 1 {
		t.Fatalf("adapters after replacement failure = %#v, want one existing adapter", source.adapters)
	}
	managed, managedSession := source.adapters[0].Client.(githubManagedSessionFake)
	if !managedSession || managed.MessageClient != client {
		t.Fatalf("adapters after replacement failure = %#v, want existing adapter", source.adapters)
	}
	assertPolledCapacity(t, client.started, 1, "restored repository")
	manager.setOpenError(nil)
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimeRetainsPollsWhenCancellationDeadlineExpires(t *testing.T) {
	t.Parallel()

	client := newStubbornCapacityMessageClient()
	manager := &githubManagerFake{adapters: []*githubsource.Adapter{{
		Client: client, Handler: eventHandlerFake{}, Repository: "one", RunnerPool: "linux",
	}}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	source := newGitHubSourceRuntime(manager,
		&capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 1}},
		"member-1", eventHandlerFake{}, githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })
	mustOpenGitHubSource(t, source)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertPolledCapacity(t, client.started, 1, "stubborn repository")

	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if err := source.CloseLeader(cancelledContext); err == nil {
		t.Fatal("leader close ignored poll cancellation deadline")
	}
	close(client.release)
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimeReportsPeerInvalidationDeadline(t *testing.T) {
	t.Parallel()

	messageClient := newMessageThenBlockingClient()
	peerClient := newStubbornCapacityMessageClient()
	unrelatedClient := newStubbornCapacityMessageClient()
	adapters := []*githubsource.Adapter{
		{Client: messageClient, Handler: eventHandlerFake{}, Repository: "message", RunnerPool: "linux"},
		{Client: peerClient, Handler: eventHandlerFake{}, Repository: "peer", RunnerPool: "linux"},
		{Client: unrelatedClient, Handler: eventHandlerFake{}, Repository: "other", RunnerPool: "windows"},
	}
	capacities := &capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 2, "windows": 1}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	source := newGitHubPollingTestSource(adapters, capacities, func() time.Time { return now })
	mustOpenGitHubSource(t, source)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertPolledCapacity(t, messageClient.started, 2, "message repository")
	assertPolledCapacity(t, peerClient.started, 2, "peer repository")
	assertPolledCapacity(t, unrelatedClient.started, 1, "unrelated repository")
	<-messageClient.firstReturned
	preserveCompletedPollResult(source, source.adapters[0])

	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if err := source.Poll(cancelledContext, now.Add(-time.Minute)); err == nil {
		t.Fatal("peer invalidation ignored cancellation deadline")
	}
	close(peerClient.release)
	close(unrelatedClient.release)
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimeKeepsInstalledAdaptersWhenOldSessionCloseFails(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("old session close failed")
	messageClient := &capacityMessageClientFake{}
	manager := &githubManagerFake{
		adapters: []*githubsource.Adapter{{Client: messageClient, Handler: eventHandlerFake{}, RunnerPool: "linux"}},
		closeErr: closeErr,
	}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })

	mustOpenGitHubSource(t, source)
	if err := source.CloseLeader(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("close error = %v, want %v", err, closeErr)
	}
	if source.leaderInstalled || source.term != nil || len(source.adapters) != 0 {
		t.Fatal("failed session cleanup restored fenced leadership")
	}
}

func TestGitHubSourceRuntimeReportsLifecycleAndCapacityFailures(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("GitHub failed")
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	manager := &githubManagerFake{refreshErr: wantErr}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })
	if err := source.RefreshMembers(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("refresh error = %v", err)
	}
	manager.refreshErr = nil
	manager.reconcileErr = wantErr
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open leader: %v", err)
	}
	awaitGitHubDiscoveryError(t, source, wantErr)
	mustCloseGitHubSource(t, source)
	manager.reconcileErr = nil
	manager.refreshErr = wantErr
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open leader: %v", err)
	}
	awaitGitHubDiscoveryError(t, source, wantErr)
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimeReportsCapacityAndCloseFailures(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("GitHub failed")
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	manager := &githubManagerFake{}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })
	manager.refreshErr = nil
	messageClient := &capacityMessageClientFake{finished: make(chan struct{}, 1)}
	manager.adapters = []*githubsource.Adapter{{Client: messageClient, Handler: eventHandlerFake{}, RunnerPool: "linux"}}
	mustOpenGitHubSource(t, source)
	source.capacities = &capacityAggregatorFake{err: wantErr}
	if err := source.Poll(context.Background(), now); !errors.Is(err, wantErr) {
		t.Fatalf("capacity error = %v", err)
	}
	<-messageClient.finished
	if messageClient.maxCapacity != 0 {
		t.Fatalf("capacity after read failure = %d, want zero", messageClient.maxCapacity)
	}
	manager.closeErr = wantErr
	if err := source.CloseLeader(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("close error = %v", err)
	}
}

type githubManagerFake struct {
	mu                sync.Mutex
	adapters          []*githubsource.Adapter
	reconciles        int
	opens             int
	openErr           error
	reconcileErr      error
	reconcileSnapshot githubsource.RegistrationSnapshot
	refreshErr        error
	snapshotErr       error
	closeErr          error
	events            []string
}

func (fake *githubManagerFake) setRepositoryOpen(adapters []*githubsource.Adapter, openErr error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.adapters = adapters
	fake.openErr = openErr
}

func (fake *githubManagerFake) setOpenError(openErr error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.openErr = openErr
}

func (fake *githubManagerFake) setCloseError(closeErr error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.closeErr = closeErr
}

func (fake *githubManagerFake) setSnapshotError(snapshotErr error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.snapshotErr = snapshotErr
}

func (fake *githubManagerFake) setReconcileSnapshot(snapshot githubsource.RegistrationSnapshot) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.reconcileSnapshot = snapshot
}

func (fake *githubManagerFake) RegistrationSnapshot(context.Context) (githubsource.RegistrationSnapshot, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.snapshotFacts(), fake.snapshotErr
}

func (fake *githubManagerFake) Reconcile(context.Context, githubsource.ManagerConfig) (githubsource.RegistrationSnapshot, error) {
	fake.mu.Lock()
	fake.reconciles++
	fake.events = append(fake.events, "reconcile")
	snapshot := fake.snapshotFacts()
	if fake.reconcileSnapshot.Active != nil || fake.reconcileSnapshot.Owned != nil {
		snapshot = fake.reconcileSnapshot
	}
	reconcileErr := fake.reconcileErr
	fake.mu.Unlock()
	return snapshot, reconcileErr
}
func (fake *githubManagerFake) RefreshMembers(context.Context) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.events = append(fake.events, "refresh")
	return fake.refreshErr
}
func (fake *githubManagerFake) OpenLeader(context.Context, string, githubsource.EventHandler) ([]*githubsource.Adapter, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.opens++
	fake.events = append(fake.events, "open")
	return fake.adapters, fake.openErr
}
func (fake *githubManagerFake) CloseLeader(context.Context) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.events = append(fake.events, "close")
	return fake.closeErr
}

func (fake *githubManagerFake) OpenRepositorySessions(
	ctx context.Context,
	records []githubsource.Registration,
	owner string,
	handler githubsource.EventHandler,
) (*githubsource.RepositorySessionGeneration, error) {
	adapters, openErr := fake.beginRepositorySessionOpen()
	if openErr != nil {
		return nil, openErr
	}
	manager, err := newFakeRepositorySessionManager(fake, adapters)
	if err != nil {
		return nil, err
	}
	generation, err := manager.OpenRepositorySessions(ctx, records, owner, handler)
	if err != nil {
		return generation, fmt.Errorf("open test repository sessions: %w", err)
	}
	return generation, nil
}

func (fake *githubManagerFake) beginRepositorySessionOpen() ([]*githubsource.Adapter, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.opens++
	fake.events = append(fake.events, "open")
	if fake.openErr != nil {
		return nil, fake.openErr
	}
	return append([]*githubsource.Adapter(nil), fake.adapters...), nil
}

func newFakeRepositorySessionManager(
	fake *githubManagerFake,
	adapters []*githubsource.Adapter,
) (*githubsource.Manager, error) {
	manager, err := githubsource.NewManager(termDiscoverer{}, func(
		context.Context,
		transport.Repository,
	) (githubsource.ManagerClient, error) {
		return githubsource.ManagerClient{OpenSession: func(
			_ context.Context,
			registration transport.Registration,
			_ string,
		) (githubsource.ManagedSession, error) {
			index := int(registration.ID) - 1
			return githubManagedSessionFake{MessageClient: adapters[index].Client, manager: fake}, nil
		}}, nil
	}, &termRegistrationRepository{}, githubsource.ManagerObserver{})
	if err != nil {
		return nil, fmt.Errorf("create test GitHub manager: %w", err)
	}
	return manager, nil
}

func (fake *githubManagerFake) snapshotFacts() githubsource.RegistrationSnapshot {
	records := make([]githubsource.Registration, 0, len(fake.adapters))
	for index, adapter := range fake.adapters {
		repository := adapter.Repository
		if repository == "" {
			repository = fmt.Sprintf("https://github.example/repository-%d", index+1)
		}
		records = append(records, githubsource.Registration{RepositoryURL: repository, InstallationID: 1,
			RunnerPool: adapter.RunnerPool, RunnerGroupID: 1, ScaleSetID: transport.ScaleSetID(index + 1)})
	}
	return githubsource.RegistrationSnapshot{Active: records, Owned: append([]githubsource.Registration(nil), records...)}
}

type githubManagedSessionFake struct {
	githubsource.MessageClient
	manager *githubManagerFake
}

func (session githubManagedSessionFake) Close(context.Context) error {
	session.manager.mu.Lock()
	defer session.manager.mu.Unlock()
	return session.manager.closeErr
}

type capacityAggregatorFake struct {
	capacities map[core.RunnerPool]int
	queries    []core.RunnerPool
	err        error
}

func (fake *capacityAggregatorFake) AvailableRunnerPoolCapacity(
	_ context.Context,
	runnerPool core.RunnerPool,
	_ time.Time,
) (int, error) {
	fake.queries = append(fake.queries, runnerPool)
	return fake.capacities[runnerPool], fake.err
}

type capacityMessageClientFake struct {
	maxCapacity int
	calls       int
	err         error
	finished    chan struct{}
}

func (fake *capacityMessageClientFake) GetMessage(ctx context.Context, _, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	fake.maxCapacity = maxCapacity
	fake.calls++
	if fake.finished != nil {
		select {
		case fake.finished <- struct{}{}:
		case <-ctx.Done():
			return nil, fmt.Errorf("finish test message request: %w", ctx.Err())
		}
	}
	if fake.err != nil {
		return nil, fake.err
	}
	return nil, githubsource.ErrNoMessage
}

func (*capacityMessageClientFake) DeleteMessage(context.Context, int) error { return nil }
func (*capacityMessageClientFake) AcquireRequests(_ context.Context, requestIDs []int64) ([]int64, error) {
	return requestIDs, nil
}

type blockingCapacityMessageClient struct {
	started  chan int
	messages chan *upstream.RunnerScaleSetMessage
}

func newBlockingCapacityMessageClient() *blockingCapacityMessageClient {
	return &blockingCapacityMessageClient{started: make(chan int, 1), messages: make(chan *upstream.RunnerScaleSetMessage, 1)}
}

func (client *blockingCapacityMessageClient) GetMessage(ctx context.Context, _, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	client.started <- maxCapacity
	select {
	case message := <-client.messages:
		return message, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("blocked GitHub poll: %w", ctx.Err())
	}
}

func (*blockingCapacityMessageClient) DeleteMessage(context.Context, int) error { return nil }
func (*blockingCapacityMessageClient) AcquireRequests(_ context.Context, requestIDs []int64) ([]int64, error) {
	return requestIDs, nil
}

type messageThenBlockingClient struct {
	started       chan int
	firstReturned chan struct{}
	calls         int
}

func newMessageThenBlockingClient() *messageThenBlockingClient {
	return &messageThenBlockingClient{started: make(chan int, 1), firstReturned: make(chan struct{})}
}

func (client *messageThenBlockingClient) GetMessage(
	ctx context.Context,
	_ int,
	maxCapacity int,
) (*upstream.RunnerScaleSetMessage, error) {
	client.started <- maxCapacity
	client.calls++
	if client.calls == 1 {
		close(client.firstReturned)
		return &upstream.RunnerScaleSetMessage{MessageID: 1}, nil
	}
	<-ctx.Done()
	return nil, fmt.Errorf("blocked GitHub poll: %w", ctx.Err())
}

func (*messageThenBlockingClient) DeleteMessage(context.Context, int) error { return nil }
func (*messageThenBlockingClient) AcquireRequests(_ context.Context, requestIDs []int64) ([]int64, error) {
	return requestIDs, nil
}

type invalidMessageThenBlockingClient struct {
	started       chan int
	firstReturned chan struct{}
	calls         int
}

func newInvalidMessageThenBlockingClient() *invalidMessageThenBlockingClient {
	return &invalidMessageThenBlockingClient{started: make(chan int, 1), firstReturned: make(chan struct{})}
}

func (client *invalidMessageThenBlockingClient) GetMessage(
	ctx context.Context,
	_ int,
	maxCapacity int,
) (*upstream.RunnerScaleSetMessage, error) {
	client.started <- maxCapacity
	client.calls++
	if client.calls == 1 {
		close(client.firstReturned)
		return &upstream.RunnerScaleSetMessage{MessageID: 1, JobAvailableMessages: []*upstream.JobAvailable{{}}}, nil
	}
	<-ctx.Done()
	return nil, fmt.Errorf("blocked GitHub poll: %w", ctx.Err())
}

func (*invalidMessageThenBlockingClient) DeleteMessage(context.Context, int) error { return nil }
func (*invalidMessageThenBlockingClient) AcquireRequests(
	_ context.Context,
	requestIDs []int64,
) ([]int64, error) {
	return requestIDs, nil
}

type stubbornCapacityMessageClient struct {
	started chan int
	release chan struct{}
}

func newStubbornCapacityMessageClient() *stubbornCapacityMessageClient {
	return &stubbornCapacityMessageClient{started: make(chan int, 1), release: make(chan struct{})}
}

func (client *stubbornCapacityMessageClient) GetMessage(
	_ context.Context,
	_ int,
	maxCapacity int,
) (*upstream.RunnerScaleSetMessage, error) {
	client.started <- maxCapacity
	<-client.release
	return nil, githubsource.ErrNoMessage
}

func (*stubbornCapacityMessageClient) DeleteMessage(context.Context, int) error { return nil }
func (*stubbornCapacityMessageClient) AcquireRequests(_ context.Context, requestIDs []int64) ([]int64, error) {
	return requestIDs, nil
}

type eventHandlerFake struct{}

type recordingQueueEventHandler struct{ queued chan core.JobRequest }

func (handler recordingQueueEventHandler) Request(_ context.Context, request core.JobRequest) (core.EnqueueResult, error) {
	handler.queued <- request
	return core.EnqueueResult{Outcome: core.QueueQueued, Job: core.Job{ID: core.JobID{1}}}, nil
}
func (recordingQueueEventHandler) RunnerStarted(
	context.Context,
	core.RunnerObservation,
) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}
func (recordingQueueEventHandler) Terminalize(
	context.Context,
	core.RunnerObservation,
) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}

func (eventHandlerFake) Request(context.Context, core.JobRequest) (core.EnqueueResult, error) {
	return core.EnqueueResult{Outcome: core.QueueQueued}, nil
}
func (eventHandlerFake) RunnerStarted(context.Context, core.RunnerObservation) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}
func (eventHandlerFake) Terminalize(context.Context, core.RunnerObservation) (core.ObservationResult, error) {
	return core.ObservationResult{}, nil
}
