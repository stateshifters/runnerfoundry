package daemon

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
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
		"member-1", eventHandlerFake{}, githubsource.ManagerConfig{}, time.Minute, func() time.Time { return now })
	mustOpenGitHubSource(t, source)
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertPolledCapacity(t, firstClient.started, 7, "first repository")
	assertPolledCapacity(t, secondClient.started, 7, "second repository")
	mustPollGitHubSource(t, source, now.Add(-time.Minute))
	assertNoPolledCapacity(t, firstClient.started, "first repository")
	assertNoPolledCapacity(t, secondClient.started, "second repository")
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
	assertPolledCapacity(t, peerClient.started, 6, "refreshed peer repository")
	assertNoPolledCapacity(t, unrelatedClient.started, "unrelated repository")
	mustCloseGitHubSource(t, source)
}

func TestGitHubSourceRuntimeDoesNotRefreshPeerCapacityAfterUnhandledMessage(t *testing.T) {
	t.Parallel()

	failedClient := newInvalidMessageThenBlockingClient()
	peerClient := newBlockingCapacityMessageClient()
	adapters := []*githubsource.Adapter{
		{Client: failedClient, Handler: eventHandlerFake{}, Repository: "one", RunnerPool: "linux"},
		{Client: peerClient, Handler: eventHandlerFake{}, Repository: "two", RunnerPool: "linux"},
	}
	capacities := &capacityAggregatorFake{capacities: map[core.RunnerPool]int{"linux": 7}}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	source := newGitHubPollingTestSource(adapters, capacities, func() time.Time { return now })
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
	for attempts := 0; len(source.opening) != 0 && attempts < 10_000; attempts++ {
		runtime.Gosched()
		_ = source.harvestRepositoryOpenResults(context.Background(), source.term)
	}
	if len(source.adapters) != want {
		t.Fatalf("installed adapters = %d, want %d", len(source.adapters), want)
	}
}

func awaitGitHubPollError(source *githubSourceRuntime) error {
	for attempts := 0; attempts < 10_000; attempts++ {
		runtime.Gosched()
		if err := source.Poll(context.Background(), time.Time{}); err != nil {
			return err
		}
	}
	return nil
}
