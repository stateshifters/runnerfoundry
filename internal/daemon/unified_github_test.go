package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
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
	if err := source.Poll(context.Background(), now); err != nil {
		t.Fatalf("publish before discovery is due: %v", err)
	}
	now = now.Add(time.Minute)
	if err := source.Poll(context.Background(), now); err != nil {
		t.Fatalf("publish when discovery is due: %v", err)
	}
	if manager.reconciles != 2 || manager.opens != 2 {
		t.Fatalf("reconciles=%d opens=%d, want 2 each", manager.reconciles, manager.opens)
	}
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

func TestGitHubSourceRuntimePollsSessionsIndependently(t *testing.T) {
	t.Parallel()

	firstErr := errors.New("first session failed")
	firstClient := &capacityMessageClientFake{err: firstErr, finished: make(chan struct{}, 1)}
	secondClient := &capacityMessageClientFake{finished: make(chan struct{}, 1)}
	manager := &githubManagerFake{adapters: []*githubsource.Adapter{
		{Client: firstClient, Handler: eventHandlerFake{}, RunnerPool: "first"},
		{Client: secondClient, Handler: eventHandlerFake{}, RunnerPool: "second"},
	}}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open leader: %v", err)
	}
	if err := source.Poll(context.Background(), time.Time{}); err != nil {
		t.Fatalf("launch polls: %v", err)
	}
	<-firstClient.finished
	<-secondClient.finished
	err := source.Poll(context.Background(), time.Time{})
	if !errors.Is(err, firstErr) {
		t.Fatalf("poll error = %v, want first session failure", err)
	}
	if closeErr := source.CloseLeader(context.Background()); closeErr != nil {
		t.Fatalf("close leader: %v", closeErr)
	}
}

func TestGitHubSourceRuntimeLaunchesRepositoryPollsWithoutBlockingMaintenance(t *testing.T) {
	t.Parallel()

	firstClient := newBlockingCapacityMessageClient()
	secondClient := newBlockingCapacityMessageClient()
	manager := &githubManagerFake{adapters: []*githubsource.Adapter{
		{Client: firstClient, Handler: eventHandlerFake{}, Repository: "one", RunnerPool: "linux"},
		{Client: secondClient, Handler: eventHandlerFake{}, Repository: "two", RunnerPool: "linux"},
	}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	source := newGitHubSourceRuntime(manager,
		&capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 7}},
		"member-1", eventHandlerFake{}, githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })
	mustOpenGitHubSource(t, source)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertPolledCapacity(t, firstClient.started, 7, "first repository")
	assertPolledCapacity(t, secondClient.started, 7, "second repository")
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	select {
	case capacity := <-firstClient.started:
		t.Fatalf("first repository received overlapping poll with capacity %d", capacity)
	default:
	}
	select {
	case capacity := <-secondClient.started:
		t.Fatalf("second repository received overlapping poll with capacity %d", capacity)
	default:
	}
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimeRefreshesPeerCapacityAfterMessage(t *testing.T) {
	t.Parallel()

	messageClient := newMessageThenBlockingClient()
	peerClient := newBlockingCapacityMessageClient()
	unrelatedClient := newBlockingCapacityMessageClient()
	adapters := []*githubsource.Adapter{
		{Client: messageClient, Handler: eventHandlerFake{}, Repository: "one", RunnerPool: "linux"},
		{Client: peerClient, Handler: eventHandlerFake{}, Repository: "two", RunnerPool: "linux"},
		{Client: unrelatedClient, Handler: eventHandlerFake{}, Repository: "three", RunnerPool: "windows"},
	}
	capacities := &capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 7, "windows": 3}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	source, manager := newGitHubPollingTestSource(adapters, capacities, func() time.Time { return now })
	mustOpenGitHubSource(t, source)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertPolledCapacity(t, messageClient.started, 7, "message repository")
	assertPolledCapacity(t, peerClient.started, 7, "peer repository")
	assertPolledCapacity(t, unrelatedClient.started, 3, "unrelated repository")
	<-messageClient.firstReturned
	preserveCompletedPollResult(source, manager.adapters[0])

	capacities.capacities["linux"] = 6
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertPolledCapacity(t, messageClient.started, 6, "refreshed message repository")
	assertPolledCapacity(t, peerClient.started, 6, "refreshed peer repository")
	assertNoPolledCapacity(t, unrelatedClient.started, "unrelated repository")
	mustCloseGitHubSource(t, source)
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
	manager.adapters = nil
	manager.openErr = errors.New("replacement failed")
	if err := source.Poll(context.Background(), now.Add(-time.Minute)); err == nil {
		t.Fatal("replacement open failure was not reported")
	}
	if len(source.adapters) != 1 || source.adapters[0] != adapter {
		t.Fatalf("adapters after replacement failure = %#v, want existing adapter", source.adapters)
	}
	assertPolledCapacity(t, client.started, 1, "restored repository")
	manager.openErr = nil
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

	now = now.Add(time.Minute)
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if err := source.Poll(cancelledContext, now.Add(-time.Minute)); err == nil {
		t.Fatal("discovery replacement ignored poll cancellation deadline")
	}
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
	source, manager := newGitHubPollingTestSource(adapters, capacities, func() time.Time { return now })
	mustOpenGitHubSource(t, source)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertPolledCapacity(t, messageClient.started, 2, "message repository")
	assertPolledCapacity(t, peerClient.started, 2, "peer repository")
	assertPolledCapacity(t, unrelatedClient.started, 1, "unrelated repository")
	<-messageClient.firstReturned
	preserveCompletedPollResult(source, manager.adapters[0])

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
		openErr:  closeErr,
	}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })

	if err := source.OpenLeader(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("open error = %v, want old close error", err)
	}
	if err := source.OpenLeader(context.Background()); err != nil || manager.opens != 1 {
		t.Fatalf("acknowledge installed replacement = %v, opens %d, want one open", err, manager.opens)
	}
	if err := source.Poll(context.Background(), time.Time{}); err != nil {
		t.Fatalf("poll installed replacement: %v", err)
	}
	if err := source.CloseLeader(context.Background()); err != nil {
		t.Fatalf("close replacement: %v", err)
	}
	if messageClient.calls != 1 {
		t.Fatal("replacement adapter was not polled")
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
	if err := source.OpenLeader(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("reconcile error = %v", err)
	}
	if err := source.Poll(context.Background(), now); !errors.Is(err, wantErr) {
		t.Fatalf("due discovery error = %v", err)
	}
	manager.reconcileErr = nil
	manager.refreshErr = wantErr
	if err := source.OpenLeader(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("open refresh error = %v", err)
	}
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
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatal(err)
	}
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
	adapters     []*githubsource.Adapter
	reconciles   int
	opens        int
	openErr      error
	reconcileErr error
	refreshErr   error
	closeErr     error
	events       []string
}

func (fake *githubManagerFake) Reconcile(context.Context, githubsource.ManagerConfig) error {
	fake.reconciles++
	fake.events = append(fake.events, "reconcile")
	return fake.reconcileErr
}
func (fake *githubManagerFake) RefreshMembers(context.Context) error {
	fake.events = append(fake.events, "refresh")
	return fake.refreshErr
}
func (fake *githubManagerFake) OpenLeader(context.Context, string, githubsource.EventHandler) ([]*githubsource.Adapter, error) {
	fake.opens++
	fake.events = append(fake.events, "open")
	return fake.adapters, fake.openErr
}
func (fake *githubManagerFake) CloseLeader(context.Context) error {
	fake.events = append(fake.events, "close")
	return fake.closeErr
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

func (fake *capacityMessageClientFake) GetMessage(_ context.Context, _, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	fake.maxCapacity = maxCapacity
	fake.calls++
	if fake.finished != nil {
		fake.finished <- struct{}{}
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
	started chan int
}

func newBlockingCapacityMessageClient() *blockingCapacityMessageClient {
	return &blockingCapacityMessageClient{started: make(chan int, 1)}
}

func (client *blockingCapacityMessageClient) GetMessage(ctx context.Context, _, maxCapacity int) (*upstream.RunnerScaleSetMessage, error) {
	client.started <- maxCapacity
	<-ctx.Done()
	return nil, fmt.Errorf("blocked GitHub poll: %w", ctx.Err())
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

func (eventHandlerFake) Request(context.Context, core.JobRequest) error { return nil }
func (eventHandlerFake) RunnerStarted(context.Context, core.SourceReference) error {
	return nil
}
func (eventHandlerFake) Terminalize(context.Context, core.SourceReference) error { return nil }

func mustOpenGitHubSource(t *testing.T, source *githubSourceRuntime) {
	t.Helper()
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open GitHub source: %v", err)
	}
}

func mustPollGitHubSource(t *testing.T, source *githubSourceRuntime, cutoff time.Time) {
	t.Helper()
	if err := source.Poll(context.Background(), cutoff); err != nil {
		t.Fatalf("poll GitHub source: %v", err)
	}
}

func mustCloseGitHubSource(t *testing.T, source *githubSourceRuntime) {
	t.Helper()
	if err := source.CloseLeader(context.Background()); err != nil {
		t.Fatalf("close GitHub source: %v", err)
	}
}

func assertPolledCapacity(t *testing.T, capacities <-chan int, want int, repository string) {
	t.Helper()
	if capacity := <-capacities; capacity != want {
		t.Fatalf("%s capacity = %d, want %d", repository, capacity, want)
	}
}

func assertNoPolledCapacity(t *testing.T, capacities <-chan int, repository string) {
	t.Helper()
	select {
	case capacity := <-capacities:
		t.Fatalf("%s was restarted with capacity %d", repository, capacity)
	default:
	}
}

func preserveCompletedPollResult(source *githubSourceRuntime, adapter *githubsource.Adapter) {
	attempt := source.polls[adapter]
	result := <-attempt.result
	attempt.result <- result
}

func newGitHubPollingTestSource(
	adapters []*githubsource.Adapter,
	capacities *capacityAggregatorFake,
	currentTime func() time.Time,
) (*githubSourceRuntime, *githubManagerFake) {
	manager := &githubManagerFake{adapters: adapters}
	source := newGitHubSourceRuntime(manager, capacities, "member-1", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, currentTime)
	return source, manager
}
