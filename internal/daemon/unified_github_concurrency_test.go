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
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

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
		"member-1", eventHandlerFake{}, managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return now })
	mustOpenGitHubSource(t, source)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertPolledCapacity(t, firstClient.started, 7, "first repository")
	assertPolledCapacity(t, secondClient.started, 7, "second repository")
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertNoPolledCapacity(t, firstClient.started, "first repository")
	assertNoPolledCapacity(t, secondClient.started, "second repository")
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimeBoundsConcurrentSessionRestorationAtSixteen(t *testing.T) {
	t.Parallel()

	adapters := make([]*githubsource.Adapter, 17)
	for index := range adapters {
		adapters[index] = &githubsource.Adapter{Client: &capacityMessageClientFake{}, Handler: eventHandlerFake{},
			Repository: fmt.Sprintf("https://github.example/repository-%02d", index), RunnerPool: "linux"}
	}
	started := make(chan struct{}, len(adapters))
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	manager := &githubManagerFake{adapters: adapters, openStarted: started, openRelease: release}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member-1", eventHandlerFake{},
		managerConfigForAdapters(adapters), time.Minute, func() time.Time { return time.Time{} })
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("OpenLeader() error = %v", err)
	}
	awaitSessionRestorationStarts(t, started, 16)
	select {
	case <-started:
		t.Fatal("seventeenth session restoration exceeded the concurrency bound")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("seventeenth session restoration did not start after capacity became available")
	}
	mustCloseGitHubSource(t, source)
}

func awaitSessionRestorationStarts(t *testing.T, started <-chan struct{}, count int) {
	t.Helper()
	for current := 1; current <= count; current++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("concurrent session restorations = %d, want %d", current-1, count)
		}
	}
}

func TestGitHubSourceRuntimeLeavesPeerPollRunningAfterMessage(t *testing.T) {
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
	source := newGitHubPollingTestSource(adapters, capacities, func() time.Time { return now })
	mustOpenGitHubSource(t, source)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertPolledCapacity(t, messageClient.started, 7, "message repository")
	assertPolledCapacity(t, peerClient.started, 7, "peer repository")
	assertPolledCapacity(t, unrelatedClient.started, 3, "unrelated repository")
	<-messageClient.firstReturned
	preserveCompletedPollResult(source, source.adapters[0])

	capacities.capacities["linux"] = 6
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertPolledCapacity(t, messageClient.started, 6, "refreshed message repository")
	assertNoPolledCapacity(t, peerClient.started, "peer repository")
	assertNoPolledCapacity(t, unrelatedClient.started, "unrelated repository")
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimeRefreshesPeerCapacityAfterPollReturns(t *testing.T) {
	t.Parallel()

	fixture := newPeerCapacityRefreshFixture()
	fixture.open(t)
	fixture.completeMessagePoll()
	fixture.refreshCompletedRepositoryWithoutWaitingForPeer(t)
	fixture.completePeerPoll(t)
	mustCloseGitHubSource(t, fixture.source)
}

type peerCapacityRefreshFixture struct {
	source        *githubSourceRuntime
	messageClient *messageThenBlockingClient
	peerClient    *stubbornCapacityMessageClient
	capacities    *capacityAggregatorFake
	now           time.Time
}

func newPeerCapacityRefreshFixture() *peerCapacityRefreshFixture {
	messageClient := newMessageThenBlockingClient()
	peerClient := newStubbornCapacityMessageClient()
	adapters := []*githubsource.Adapter{
		{Client: messageClient, Handler: eventHandlerFake{}, Repository: "one", RunnerPool: "linux"},
		{Client: peerClient, Handler: eventHandlerFake{}, Repository: "two", RunnerPool: "linux"},
	}
	capacities := &capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 7}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	source := newGitHubPollingTestSource(adapters, capacities, func() time.Time { return now })
	return &peerCapacityRefreshFixture{source: source, messageClient: messageClient, peerClient: peerClient,
		capacities: capacities, now: now}
}

func (fixture *peerCapacityRefreshFixture) open(t *testing.T) {
	t.Helper()
	mustOpenGitHubSource(t, fixture.source)
	mustPollGitHubSource(t, fixture.source, fixture.now.Add(-time.Minute))
	assertPolledCapacity(t, fixture.messageClient.started, 7, "message repository")
	assertPolledCapacity(t, fixture.peerClient.started, 7, "peer repository")
}

func (fixture *peerCapacityRefreshFixture) completeMessagePoll() {
	<-fixture.messageClient.firstReturned
	preserveCompletedPollResult(fixture.source, fixture.source.adapters[0])
}

func (fixture *peerCapacityRefreshFixture) refreshCompletedRepositoryWithoutWaitingForPeer(t *testing.T) {
	t.Helper()
	fixture.capacities.capacities["linux"] = 6
	pollFinished := make(chan error, 1)
	go func() { pollFinished <- fixture.source.Poll(context.Background(), fixture.now.Add(-time.Minute)) }()
	select {
	case err := <-pollFinished:
		if err != nil {
			t.Fatalf("poll GitHub source: %v", err)
		}
	case <-time.After(time.Second):
		close(fixture.peerClient.release)
		<-pollFinished
		t.Fatal("peer repository blocked GitHub source maintenance")
	}
	assertPolledCapacity(t, fixture.messageClient.started, 6, "refreshed message repository")
	assertNoPolledCapacity(t, fixture.peerClient.started, "peer repository")
}

func (fixture *peerCapacityRefreshFixture) completePeerPoll(t *testing.T) {
	t.Helper()
	close(fixture.peerClient.release)
	preserveCompletedPollResult(fixture.source, fixture.source.adapters[1])
	mustPollGitHubSource(t, fixture.source, fixture.now.Add(-time.Minute))
	assertPolledCapacity(t, fixture.peerClient.started, 6, "refreshed peer repository")
}

func TestGitHubSourceRuntimeDoesNotRefreshPeerCapacityAfterUnhandledMessage(t *testing.T) {
	t.Parallel()

	failedClient := newUnhandledMessageThenBlockingClient()
	peerClient := newBlockingCapacityMessageClient()
	adapters := []*githubsource.Adapter{
		{Client: failedClient, Handler: eventHandlerFake{}, Repository: "one", RunnerPool: "linux"},
		{Client: peerClient, Handler: eventHandlerFake{}, Repository: "two", RunnerPool: "linux"},
	}
	capacities := &capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 7}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	manager := &githubManagerFake{adapters: adapters}
	source := newGitHubSourceRuntime(manager, capacities, "member-1",
		failingQueueEventHandler{err: errors.New("durable enqueue failed")},
		managerConfigForAdapters(adapters), time.Minute, func() time.Time { return now })
	mustOpenGitHubSource(t, source)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertPolledCapacity(t, failedClient.started, 7, "failed repository")
	assertPolledCapacity(t, peerClient.started, 7, "peer repository")
	<-failedClient.firstReturned
	preserveCompletedPollResult(source, source.adapters[0])

	capacities.capacities["linux"] = 6
	if err := source.Poll(context.Background(), now.Add(-time.Minute)); err == nil {
		t.Fatal("unhandled GitHub message error was not reported")
	}
	assertPolledCapacity(t, failedClient.started, 6, "retried failed repository")
	assertNoPolledCapacity(t, peerClient.started, "peer repository")
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimeProcessesDemandAfterIgnoredLifecycleObservation(t *testing.T) {
	t.Parallel()

	fixture := newIgnoredLifecyclePollingFixture()
	fixture.open(t)
	fixture.processIgnoredLifecycle(t)
	fixture.processValidDemand(t)
	mustCloseGitHubSource(t, fixture.source)
}

type ignoredLifecyclePollingFixture struct {
	source          *githubSourceRuntime
	lifecycleClient *blockingCapacityMessageClient
	demandClient    *blockingCapacityMessageClient
	handler         *lifecycleThenQueueEventHandler
	now             time.Time
}

func newIgnoredLifecyclePollingFixture() *ignoredLifecyclePollingFixture {
	lifecycleClient := newBlockingCapacityMessageClient()
	demandClient := newBlockingCapacityMessageClient()
	handler := &lifecycleThenQueueEventHandler{
		queued: make(chan core.JobRequest, 1), woken: make(chan core.RunnerPool, 1),
	}
	manager := &githubManagerFake{adapters: []*githubsource.Adapter{
		{Client: lifecycleClient, Repository: "one", RunnerPool: "linux"},
		{Client: demandClient, Repository: "two", RunnerPool: "linux"},
	}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	source := newGitHubSourceRuntime(manager,
		&capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 1}},
		"member-1", handler, managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return now })
	return &ignoredLifecyclePollingFixture{source: source, lifecycleClient: lifecycleClient,
		demandClient: demandClient, handler: handler, now: now}
}

func (fixture *ignoredLifecyclePollingFixture) open(t *testing.T) {
	t.Helper()
	mustOpenGitHubSource(t, fixture.source)
	mustPollGitHubSource(t, fixture.source, fixture.now.Add(-time.Minute))
	assertPolledCapacity(t, fixture.lifecycleClient.started, 1, "lifecycle repository")
	assertPolledCapacity(t, fixture.demandClient.started, 1, "demand repository")
}

func (fixture *ignoredLifecyclePollingFixture) processIgnoredLifecycle(t *testing.T) {
	t.Helper()
	fixture.lifecycleClient.messages <- &upstream.RunnerScaleSetMessage{MessageID: 1,
		Statistics: &upstream.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*upstream.JobCompleted{{RunnerID: 502,
			JobMessageBase: upstream.JobMessageBase{JobID: "poison-job", WorkflowRunID: 902}}}}
	if messageID := <-fixture.lifecycleClient.acknowledged; messageID != 1 {
		t.Fatalf("lifecycle acknowledgement = %d, want 1", messageID)
	}
	if fixture.handler.terminalCalls != 0 {
		t.Fatalf("terminal handler calls = %d, want zero", fixture.handler.terminalCalls)
	}
	preserveCompletedPollResult(fixture.source, fixture.source.adapters[0])
	mustPollGitHubSource(t, fixture.source, fixture.now.Add(-time.Minute))
	assertPolledCapacity(t, fixture.lifecycleClient.started, 1, "restarted lifecycle repository")
	assertNoPolledCapacity(t, fixture.demandClient.started, "demand repository")
}

func (fixture *ignoredLifecyclePollingFixture) processValidDemand(t *testing.T) {
	t.Helper()
	fixture.demandClient.messages <- &upstream.RunnerScaleSetMessage{MessageID: 2,
		Statistics: &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		JobAvailableMessages: []*upstream.JobAvailable{{
			JobMessageBase: upstream.JobMessageBase{RunnerRequestID: 42},
		}}}
	request := <-fixture.handler.queued
	assertQueuedGitHubDemand(t, request, "two", "linux", 42)
	if runnerPool := <-fixture.handler.woken; runnerPool != "linux" {
		t.Fatalf("job-processing wake = %q, want linux", runnerPool)
	}
	if messageID := <-fixture.demandClient.acknowledged; messageID != 2 {
		t.Fatalf("demand acknowledgement = %d, want 2", messageID)
	}
}

func assertQueuedGitHubDemand(
	t *testing.T,
	request core.JobRequest,
	repository string,
	runnerPool core.RunnerPool,
	requestID int64,
) {
	t.Helper()
	identity, err := githubsource.DecodeSourceID(request.Source.ID)
	if err != nil || request.Source.Adapter != githubsource.AdapterName || request.RunnerPool != runnerPool ||
		identity.Repository != repository || identity.RunnerPool != runnerPool || identity.RequestID != requestID {
		t.Fatalf("queued demand = %#v", request)
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
		managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return time.Time{} })
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("open leader: %v", err)
	}
	awaitInstalledGitHubAdapters(t, source, 2)
	if err := source.Poll(context.Background(), time.Time{}); err != nil {
		t.Fatalf("launch polls: %v", err)
	}
	<-firstClient.finished
	<-secondClient.finished
	err := awaitGitHubPollError(source)
	if !errors.Is(err, firstErr) {
		t.Fatalf("poll error = %v, want first session failure", err)
	}
	if closeErr := source.CloseLeader(context.Background()); closeErr != nil {
		t.Fatalf("close leader: %v", closeErr)
	}
}

func awaitInstalledGitHubAdapters(t *testing.T, source *githubSourceRuntime, want int) {
	t.Helper()
	for len(source.opening) != 0 {
		if failures := awaitAllGitHubRegistrationOpenResults(source); len(failures) != 0 {
			t.Fatalf("install GitHub repository sessions: %v", errors.Join(failures...))
		}
	}
	if len(source.adapters) != want {
		t.Fatalf("installed adapters = %d, want %d", len(source.adapters), want)
	}
}

func awaitGitHubPollError(source *githubSourceRuntime) error {
	for _, attempt := range source.polls {
		result := <-attempt.result
		attempt.result <- result
	}
	return source.Poll(context.Background(), time.Time{})
}
