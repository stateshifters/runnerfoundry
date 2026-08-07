package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

func TestGitHubSourceRuntimeCoversInactiveCleanupPaths(t *testing.T) {
	manager := branchGitHubManager()
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	if err := source.Poll(context.Background(), time.Time{}); err != nil {
		t.Fatalf("inactive Poll() error = %v", err)
	}
	source.reportSessionActivationProgress()
	if err := source.stopRepositoryGenerationPolls(context.Background(), nil); err != nil {
		t.Fatalf("stop nil generation: %v", err)
	}
	if err := source.closeRepositoryGeneration(context.Background(), nil, "close nil"); err != nil {
		t.Fatalf("close nil generation: %v", err)
	}
}

func TestGitHubSourceRuntimeRetriesGenerationRetirement(t *testing.T) {
	manager := branchGitHubManager()
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	generation := openBranchGeneration(t, manager)
	manager.setCloseError(errors.New("close failed"))
	if err := source.closeRepositoryGeneration(context.Background(), generation, "close generation"); err == nil {
		t.Fatal("close failure was not reported")
	}
	if err := source.CloseLeader(context.Background()); err == nil {
		t.Fatal("retirement retry failure was not reported")
	}
	manager.setCloseError(nil)
	if err := source.CloseLeader(context.Background()); err != nil {
		t.Fatalf("retirement retry: %v", err)
	}

	retrying := openBranchGeneration(t, manager)
	manager.setCloseError(errors.New("poll retirement failed"))
	source.leaderInstalled = true
	source.term = branchLeadershipTerm(1)
	source.term.discoveryActive = true
	source.retiring = append(source.retiring, retrying)
	if err := source.Poll(context.Background(), time.Time{}); err == nil {
		t.Fatal("active retirement retry failure was not reported")
	}
	manager.setCloseError(nil)
	if err := source.retryRetiringGenerations(context.Background()); err != nil {
		t.Fatalf("finish active retirement retry: %v", err)
	}
}

func TestGitHubSourceRuntimeRejectsStaleAndInvalidOpenResults(t *testing.T) {
	manager := branchGitHubManager()
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	records := manager.snapshotFacts().Active
	term := branchLeadershipTerm(2)
	source.term = term
	source.leaderInstalled = true
	source.desired[records[0].RepositoryURL] = records

	stale := githubRepositoryOpenResult{epoch: 1, repository: records[0].RepositoryURL,
		records: records, generation: openBranchGeneration(t, manager)}
	if err := source.installRepositoryGeneration(context.Background(), term, stale); err != nil {
		t.Fatalf("discard stale generation: %v", err)
	}
	invalid := githubRepositoryOpenResult{epoch: 2, repository: records[0].RepositoryURL, records: records}
	if err := source.installRepositoryGeneration(context.Background(), term, invalid); err == nil {
		t.Fatal("nil generation was accepted")
	}
}

func TestGitHubSourceRuntimeRejectsReplacementWhenOldPollCannotStop(t *testing.T) {
	manager := branchGitHubManager()
	records := manager.snapshotFacts().Active
	previous := openBranchGeneration(t, manager)
	replacement := openBranchGeneration(t, manager)
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	term := branchLeadershipTerm(1)
	source.term = term
	source.leaderInstalled = true
	source.desired[records[0].RepositoryURL] = records
	source.generations[records[0].RepositoryURL] = previous
	adapter := previous.Adapters()[0]
	source.polls[adapter] = &githubPollAttempt{cancel: func() {}, result: make(chan githubPollResult)}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	result := githubRepositoryOpenResult{epoch: 1, repository: records[0].RepositoryURL,
		records: records, generation: replacement}
	if err := source.installRepositoryGeneration(canceled, term, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("install replacement error = %v", err)
	}
	term.cancelOperations()
	if err := source.waitForTerm(context.Background(), term); err != nil {
		t.Fatalf("wait for retry operation: %v", err)
	}
	if err := previous.Close(context.Background()); err != nil {
		t.Fatalf("close previous generation: %v", err)
	}
}

func TestGitHubSourceRuntimeRetiresOnlyRemovedRepositories(t *testing.T) {
	manager := branchGitHubManager()
	records := manager.snapshotFacts().Active
	retained := openBranchGeneration(t, manager)
	removed := openBranchGeneration(t, manager)
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	source.desired["retained"] = records
	source.generations["retained"] = retained
	source.generations["removed"] = removed
	removedAdapter := removed.Adapters()[0]
	completed := make(chan githubPollResult, 1)
	completed <- githubPollResult{}
	source.polls[removedAdapter] = &githubPollAttempt{cancel: func() {}, result: completed}
	manager.setCloseError(errors.New("close removed generation"))
	if resultErrors := source.retireRemovedRepositories(context.Background()); len(resultErrors) != 1 {
		t.Fatalf("retirement errors = %v", resultErrors)
	}
	manager.setCloseError(nil)
	if err := source.retryRetiringGenerations(context.Background()); err != nil {
		t.Fatalf("retry retirement: %v", err)
	}
	if err := retained.Close(context.Background()); err != nil {
		t.Fatalf("close retained generation: %v", err)
	}
}

func TestGitHubSourceRuntimeKeepsRemovedRepositoryWhenPollCannotStop(t *testing.T) {
	manager := branchGitHubManager()
	generation := openBranchGeneration(t, manager)
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	source.generations["removed"] = generation
	adapter := generation.Adapters()[0]
	source.polls[adapter] = &githubPollAttempt{cancel: func() {}, result: make(chan githubPollResult)}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if resultErrors := source.retireRemovedRepositories(canceled); len(resultErrors) != 1 {
		t.Fatalf("retirement errors = %v", resultErrors)
	}
	if source.generations["removed"] == nil {
		t.Fatal("removed generation was discarded while its poll remained active")
	}
	delete(source.polls, adapter)
	if err := generation.Close(context.Background()); err != nil {
		t.Fatalf("close removed generation: %v", err)
	}
}

func TestGitHubSourceRuntimeCoversStaleDiscoveryAndCanceledDrain(t *testing.T) {
	manager := branchGitHubManager()
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	term := branchLeadershipTerm(2)
	source.term = term
	source.leaderInstalled = true
	term.discoveryResult <- githubDiscoveryResult{epoch: 1}
	if resultErrors := source.harvestDiscovery(context.Background(), term); len(resultErrors) != 0 {
		t.Fatalf("stale discovery errors = %v", resultErrors)
	}

	generation := openBranchGeneration(t, manager)
	manager.setCloseError(errors.New("drain close failed"))
	term.openResults <- githubRepositoryOpenResult{generation: generation}
	if err := source.drainCancelledTerm(context.Background(), term); err == nil {
		t.Fatal("canceled term drain failure was not reported")
	}
	manager.setCloseError(nil)
	if err := source.retryRetiringGenerations(context.Background()); err != nil {
		t.Fatalf("retry drained generation: %v", err)
	}
}

func TestGitHubSourceRuntimeAbandonsGenerationCanceledDuringResultDelivery(t *testing.T) {
	manager := branchGitHubManager()
	generation := openBranchGeneration(t, manager)
	staticManager := &staticGenerationManager{githubManagerRuntime: manager, generation: generation}
	source := newGitHubSourceRuntime(staticManager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	term := branchLeadershipTerm(1)
	for range cap(term.openResults) {
		term.openResults <- githubRepositoryOpenResult{}
	}
	ctx := newCancelAfterFirstErrContext(false)
	term.background.Add(1)
	source.openRepositoryGeneration(ctx, term, "repository", manager.snapshotFacts().Active)
	source.collectAbandonedGenerations()
	if len(source.retiring) != 1 {
		t.Fatalf("abandoned generations = %d, want 1", len(source.retiring))
	}
	if err := source.retryRetiringGenerations(context.Background()); err != nil {
		t.Fatalf("retire abandoned generation: %v", err)
	}
}

func TestGitHubSourceRuntimeAbandonsGenerationCanceledAfterOpening(t *testing.T) {
	manager := branchGitHubManager()
	generation := openBranchGeneration(t, manager)
	staticManager := &staticGenerationManager{githubManagerRuntime: manager, generation: generation}
	source := newGitHubSourceRuntime(staticManager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	term := branchLeadershipTerm(1)
	term.background.Add(1)
	source.openRepositoryGeneration(newCancelAfterFirstErrContext(true), term, "repository",
		manager.snapshotFacts().Active)
	source.collectAbandonedGenerations()
	if len(source.retiring) != 1 {
		t.Fatalf("abandoned generations = %d, want 1", len(source.retiring))
	}
	if err := source.retryRetiringGenerations(context.Background()); err != nil {
		t.Fatalf("retire abandoned generation: %v", err)
	}
}

func TestGitHubSourceRuntimeStopsOpeningWhileWaitingForRepositorySlot(t *testing.T) {
	manager := branchGitHubManager()
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	term := branchLeadershipTerm(1)
	for range cap(term.openSlots) {
		term.openSlots <- struct{}{}
	}
	term.background.Add(1)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	source.openRepositoryGeneration(canceled, term, "repository", manager.snapshotFacts().Active)
	manager.mu.Lock()
	opens := manager.opens
	manager.mu.Unlock()
	if opens != 0 {
		t.Fatalf("session opens = %d, want zero", opens)
	}
}

func branchGitHubManager() *githubManagerFake {
	return &githubManagerFake{adapters: []*githubsource.Adapter{{
		Client: &capacityMessageClientFake{}, Handler: eventHandlerFake{},
		Repository: "repository", RunnerPool: core.RunnerPool("linux"),
	}}}
}

func branchLeadershipTerm(epoch uint64) *githubLeadershipTerm {
	return &githubLeadershipTerm{epoch: epoch, openResults: make(chan githubRepositoryOpenResult, 4),
		discoveryResult: make(chan githubDiscoveryResult, 1), openSlots: make(chan struct{}, 4),
		operations: make(map[uint64]context.CancelFunc)}
}

func openBranchGeneration(t *testing.T, manager *githubManagerFake) *githubsource.RepositorySessionGeneration {
	t.Helper()
	generation, err := manager.OpenRepositorySessions(
		context.Background(), manager.snapshotFacts().Active, "member", eventHandlerFake{},
	)
	if err != nil {
		t.Fatalf("open test repository generation: %v", err)
	}
	return generation
}

type staticGenerationManager struct {
	githubManagerRuntime
	generation *githubsource.RepositorySessionGeneration
}

func (manager *staticGenerationManager) OpenRepositorySessions(
	context.Context,
	[]githubsource.Registration,
	string,
	githubsource.EventHandler,
) (*githubsource.RepositorySessionGeneration, error) {
	return manager.generation, nil
}

type cancelAfterFirstErrContext struct {
	done              chan struct{}
	once              sync.Once
	canceledInitially bool
}

func newCancelAfterFirstErrContext(canceledInitially bool) *cancelAfterFirstErrContext {
	return &cancelAfterFirstErrContext{done: make(chan struct{}), canceledInitially: canceledInitially}
}

func (*cancelAfterFirstErrContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ctx *cancelAfterFirstErrContext) Done() <-chan struct{}   { return ctx.done }

func (ctx *cancelAfterFirstErrContext) Err() error {
	first := false
	ctx.once.Do(func() {
		first = true
		close(ctx.done)
	})
	if first && !ctx.canceledInitially {
		return nil
	}
	return context.Canceled
}

func (*cancelAfterFirstErrContext) Value(any) any { return nil }
