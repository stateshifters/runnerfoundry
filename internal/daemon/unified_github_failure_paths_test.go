package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
)

func TestGitHubSourceRuntimeCoversInactiveCleanupPaths(t *testing.T) {
	source := newGitHubSourceRuntime(branchGitHubManager(), &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	if err := source.Poll(context.Background(), time.Time{}); err != nil {
		t.Fatalf("inactive Poll() error = %v", err)
	}
	source.reportSessionActivationProgress()
	if err := source.stopRegistrationSessionPoll(context.Background(), nil); err != nil {
		t.Fatalf("stop nil session: %v", err)
	}
	if err := source.closeRegistrationSession(context.Background(), nil, "close nil"); err != nil {
		t.Fatalf("close nil session: %v", err)
	}
}

func TestGitHubSourceRuntimeStopsWaitingForPollsWhenContextIsCanceled(t *testing.T) {
	t.Parallel()

	cancelCalled := false
	adapter := &githubsource.Adapter{}
	source := &githubSourceRuntime{polls: map[*githubsource.Adapter]*githubPollAttempt{
		adapter: {cancel: func() { cancelCalled = true }, result: make(chan githubPollResult)},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := source.stopAllPolls(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("stopAllPolls() error = %v, want %v", err, context.Canceled)
	}
	if !cancelCalled {
		t.Fatal("stopAllPolls() did not cancel the in-flight poll")
	}
}

func TestGitHubSourceRuntimeStopsOnlySelectedPolls(t *testing.T) {
	kept := &githubsource.Adapter{}
	stopped := &githubsource.Adapter{}
	stoppedResult := make(chan githubPollResult, 1)
	stoppedResult <- githubPollResult{}
	source := &githubSourceRuntime{polls: map[*githubsource.Adapter]*githubPollAttempt{
		kept:    {cancel: func() {}, result: make(chan githubPollResult)},
		stopped: {cancel: func() {}, result: stoppedResult},
	}}
	if err := source.stopPolls(context.Background(), func(adapter *githubsource.Adapter) bool {
		return adapter == stopped
	}); err != nil {
		t.Fatalf("stop selected poll: %v", err)
	}
	if _, exists := source.polls[stopped]; exists {
		t.Fatal("selected poll was retained")
	}
	if _, exists := source.polls[kept]; !exists {
		t.Fatal("unselected poll was removed")
	}
}

func TestGitHubSourceRuntimeRetriesSessionRetirement(t *testing.T) {
	manager := branchGitHubManager()
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return time.Time{} })
	session := openBranchSession(t, manager)
	manager.setCloseError(errors.New("close failed"))
	if err := source.closeRegistrationSession(context.Background(), session, "close session"); err == nil {
		t.Fatal("close failure was not reported")
	}
	if err := source.CloseLeader(context.Background()); err == nil {
		t.Fatal("retirement retry failure was not reported")
	}
	manager.setCloseError(nil)
	if err := source.CloseLeader(context.Background()); err != nil {
		t.Fatalf("retirement retry: %v", err)
	}

	retrying := openBranchSession(t, manager)
	manager.setCloseError(errors.New("poll retirement failed"))
	source.leaderInstalled = true
	source.term = branchLeadershipTerm(1)
	source.term.discoveryActive = true
	source.retiring = append(source.retiring, retrying)
	if err := source.Poll(context.Background(), time.Time{}); err == nil {
		t.Fatal("active retirement retry failure was not reported")
	}
	manager.setCloseError(nil)
	if err := source.retryRetiringSessions(context.Background()); err != nil {
		t.Fatalf("finish active retirement retry: %v", err)
	}
}

func TestGitHubSourceRuntimeRejectsStaleOpenResult(t *testing.T) {
	manager := branchGitHubManager()
	record := manager.snapshotFacts().Active[0]
	session := openBranchSession(t, manager)
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return time.Time{} })
	term := branchLeadershipTerm(2)
	source.term = term
	source.leaderInstalled = true
	source.desired[registrationKey(record)] = record
	stale := githubRegistrationOpenResult{epoch: 1, key: registrationKey(record), requested: record, session: session}
	if err := source.installRegistrationSession(context.Background(), term, stale); err != nil {
		t.Fatalf("discard stale session: %v", err)
	}
}

func TestGitHubSourceRuntimeRejectsChangedRouteIdentityAndRetainsFailedCleanup(t *testing.T) {
	manager := branchGitHubManager()
	session := openBranchSession(t, manager)
	requested := session.Registration()
	requested.RunnerPool = "windows"
	key := registrationKey(requested)
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return time.Time{} })
	term := branchLeadershipTerm(1)
	source.term = term
	source.leaderInstalled = true
	source.desired[key] = requested
	manager.setCloseError(errors.New("close invalid session"))

	err := source.installRegistrationSession(context.Background(), term, githubRegistrationOpenResult{
		epoch: 1, key: key, requested: requested, session: session,
	})
	if err == nil || len(source.retiring) != 1 || len(source.sessions) != 0 {
		t.Fatalf("changed route result = error %v, retiring %d, installed %d",
			err, len(source.retiring), len(source.sessions))
	}
	manager.setCloseError(nil)
	if err := source.retryRetiringSessions(context.Background()); err != nil {
		t.Fatalf("retry invalid session cleanup: %v", err)
	}
}

func TestGitHubSourceRuntimeRejectsReplacementWhenOldPollCannotStop(t *testing.T) {
	manager := branchGitHubManager()
	record := manager.snapshotFacts().Active[0]
	previous := openBranchSession(t, manager)
	replacement := openBranchSession(t, manager)
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return time.Time{} })
	term := branchLeadershipTerm(1)
	source.term = term
	source.leaderInstalled = true
	key := registrationKey(record)
	source.desired[key] = record
	source.sessions[key] = previous
	source.polls[previous.Adapter()] = &githubPollAttempt{cancel: func() {}, result: make(chan githubPollResult)}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	result := githubRegistrationOpenResult{epoch: 1, key: key, requested: record, session: replacement}
	if err := source.installRegistrationSession(canceled, term, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("install replacement error = %v", err)
	}
	delete(source.polls, previous.Adapter())
	if err := previous.Close(context.Background()); err != nil {
		t.Fatalf("close previous session: %v", err)
	}
}

func TestGitHubSourceRuntimeRetiresOnlyRemovedRegistration(t *testing.T) {
	manager := branchGitHubManager()
	record := manager.snapshotFacts().Active[0]
	retained := openBranchSession(t, manager)
	removed := openBranchSession(t, manager)
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return time.Time{} })
	retainedKey := githubRegistrationKey{repository: "retained", runnerPool: record.RunnerPool}
	removedKey := githubRegistrationKey{repository: "removed", runnerPool: record.RunnerPool}
	source.desired[retainedKey] = record
	source.sessions[retainedKey] = retained
	source.sessions[removedKey] = removed
	completed := make(chan githubPollResult, 1)
	completed <- githubPollResult{}
	source.polls[removed.Adapter()] = &githubPollAttempt{cancel: func() {}, result: completed}
	manager.setCloseError(errors.New("close removed session"))
	if resultErrors := source.retireRemovedRegistrationSessions(context.Background()); len(resultErrors) != 0 {
		t.Fatalf("retirement errors = %v", resultErrors)
	}
	if err := source.retryRetiringSessions(context.Background()); err == nil {
		t.Fatal("retirement retry succeeded despite close failure")
	}
	manager.setCloseError(nil)
	if err := source.retryRetiringSessions(context.Background()); err != nil {
		t.Fatalf("retry retirement: %v", err)
	}
	if err := retained.Close(context.Background()); err != nil {
		t.Fatalf("close retained session: %v", err)
	}
}

func TestGitHubSourceRuntimeBoundsSessionRetirementWork(t *testing.T) {
	manager := branchGitHubManager()
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return time.Time{} })
	for range githubSessionRetirementLimit + 2 {
		source.retiring = append(source.retiring, openBranchSession(t, manager))
	}
	manager.setCloseError(errors.New("close failed"))
	if err := source.retryRetiringSessions(context.Background()); err == nil {
		t.Fatal("retirement retry succeeded despite close failures")
	}
	manager.mu.Lock()
	closeCalls := manager.closeCalls
	manager.mu.Unlock()
	if closeCalls != githubSessionRetirementLimit {
		t.Fatalf("close calls = %d, want bounded %d", closeCalls, githubSessionRetirementLimit)
	}
	if len(source.retiring) != githubSessionRetirementLimit+2 {
		t.Fatalf("retained retirement work = %d, want 6", len(source.retiring))
	}
}

func TestGitHubSourceRuntimeReportsDeclarationPublicationFailure(t *testing.T) {
	wantErr := errors.New("declaration failure")
	manager, source := newDeclarationTestSource()
	manager.publishErr = wantErr
	if err := source.RefreshMembers(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("RefreshMembers() error = %v, want publication failure", err)
	}
}

func TestGitHubSourceRuntimePreservesLeadershipWhenDeclarationReadFails(t *testing.T) {
	wantErr := errors.New("declaration failure")
	manager, source := newDeclarationTestSource()
	manager.readErr = wantErr
	openDeclarationTestLeader(t, source)
	if err := source.Poll(context.Background(), time.Time{}); !errors.Is(err, wantErr) {
		t.Fatalf("Poll() error = %v, want declaration read failure", err)
	}
	if manager.base.reconciles != 0 {
		t.Fatalf("reconciliations = %d, want zero", manager.base.reconciles)
	}
	closeDeclarationTestLeader(t, source)
}

func TestGitHubSourceRuntimeFallsBackWhenDeclarationPersistenceIsUnavailable(t *testing.T) {
	manager, source := newDeclarationTestSource()
	manager.readErr = githubsource.ErrRunnerPoolDeclarationPersistenceUnavailable
	openDeclarationTestLeader(t, source)
	if err := source.Poll(context.Background(), time.Time{}); err != nil {
		t.Fatalf("Poll() fallback error = %v", err)
	}
	awaitGitHubDiscovery(t, source)
	closeDeclarationTestLeader(t, source)
}

func TestGitHubSourceRuntimeReportsConflictsWithoutBlockingOtherDeclarations(t *testing.T) {
	manager, source := newDeclarationTestSource()
	manager.snapshot = githubsource.RunnerPoolDeclarationSnapshot{
		RunnerPools:           []githubsource.RunnerPoolConfig{{RunnerPool: "linux", RunnerGroup: "default"}},
		ConflictedRunnerPools: []core.RunnerPool{"windows"},
	}
	openDeclarationTestLeader(t, source)
	if err := source.Poll(context.Background(), time.Time{}); err == nil {
		t.Fatal("Poll() did not report conflicting live declarations")
	}
	awaitGitHubDiscovery(t, source)
	closeDeclarationTestLeader(t, source)
}

func newDeclarationTestSource() (*declarationRuntimeFake, *githubSourceRuntime) {
	base := &githubManagerFake{}
	manager := &declarationRuntimeFake{githubManagerRuntime: base, base: base}
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		githubsource.ManagerConfig{}, time.Minute, func() time.Time { return time.Time{} })
	return manager, source
}

func openDeclarationTestLeader(t *testing.T, source *githubSourceRuntime) {
	t.Helper()
	if err := source.OpenLeader(context.Background()); err != nil {
		t.Fatalf("OpenLeader() error = %v", err)
	}
}

func closeDeclarationTestLeader(t *testing.T, source *githubSourceRuntime) {
	t.Helper()
	if err := source.CloseLeader(context.Background()); err != nil {
		t.Fatalf("CloseLeader() error = %v", err)
	}
}

type declarationRuntimeFake struct {
	githubManagerRuntime
	base       *githubManagerFake
	snapshot   githubsource.RunnerPoolDeclarationSnapshot
	publishErr error
	readErr    error
}

func (*declarationRuntimeFake) UsesSharedRunnerPoolDeclarations() bool { return true }

func (fake *declarationRuntimeFake) PublishRunnerPoolDeclarations(
	_ context.Context,
	_ core.MemberID,
	_ githubsource.ManagerConfig,
) error {
	return fake.publishErr
}

func (fake *declarationRuntimeFake) ClusterRunnerPools(
	_ context.Context,
	_ time.Time,
) (githubsource.RunnerPoolDeclarationSnapshot, error) {
	return fake.snapshot, fake.readErr
}

func TestGitHubSourceRuntimeRetainsRemovedSessionWhenPollCannotStop(t *testing.T) {
	manager := branchGitHubManager()
	session := openBranchSession(t, manager)
	key := registrationKey(session.Registration())
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return time.Time{} })
	source.sessions[key] = session
	source.polls[session.Adapter()] = &githubPollAttempt{cancel: func() {}, result: make(chan githubPollResult)}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	resultErrors := source.retireRemovedRegistrationSessions(canceled)
	if len(resultErrors) != 1 || source.sessions[key] != session {
		t.Fatalf("retirement errors = %v, retained session = %t", resultErrors, source.sessions[key] == session)
	}
	delete(source.polls, session.Adapter())
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("close retained removed session: %v", err)
	}
}

func TestGitHubSourceRuntimeCoversStaleDiscoveryAndCanceledDrain(t *testing.T) {
	manager := branchGitHubManager()
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return time.Time{} })
	term := branchLeadershipTerm(2)
	source.term = term
	source.leaderInstalled = true
	term.discoveryResult <- githubDiscoveryResult{epoch: 1}
	if resultErrors := source.harvestDiscovery(context.Background(), term); len(resultErrors) != 0 {
		t.Fatalf("stale discovery errors = %v", resultErrors)
	}
	session := openBranchSession(t, manager)
	manager.setCloseError(errors.New("drain close failed"))
	term.openResults <- githubRegistrationOpenResult{session: session}
	if err := source.drainCancelledTerm(context.Background(), term); err == nil {
		t.Fatal("canceled term drain failure was not reported")
	}
	manager.setCloseError(nil)
	if err := source.retryRetiringSessions(context.Background()); err != nil {
		t.Fatalf("retry drained session: %v", err)
	}
}

func TestGitHubSourceRuntimeAbandonsSessionCanceledAfterOpening(t *testing.T) {
	manager := branchGitHubManager()
	record := manager.snapshotFacts().Active[0]
	session := openBranchSession(t, manager)
	staticManager := &staticRegistrationSessionManager{githubManagerRuntime: manager, session: session}
	source := newGitHubSourceRuntime(staticManager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return time.Time{} })
	term := branchLeadershipTerm(1)
	term.background.Add(1)
	configured := source.config.RunnerPools[0]
	source.openRegistrationSession(newCancelAfterFirstErrContext(true), term, registrationKey(record), record, configured)
	source.collectAbandonedSessions()
	if len(source.retiring) != 1 {
		t.Fatalf("abandoned sessions = %d, want 1", len(source.retiring))
	}
	if err := source.retryRetiringSessions(context.Background()); err != nil {
		t.Fatalf("retire abandoned session: %v", err)
	}
}

func TestGitHubSourceRuntimeAbandonsSessionCanceledDuringResultDelivery(t *testing.T) {
	manager := branchGitHubManager()
	record := manager.snapshotFacts().Active[0]
	session := openBranchSession(t, manager)
	staticManager := &staticRegistrationSessionManager{githubManagerRuntime: manager, session: session}
	source := newGitHubSourceRuntime(staticManager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return time.Time{} })
	term := branchLeadershipTerm(1)
	for range cap(term.openResults) {
		term.openResults <- githubRegistrationOpenResult{}
	}
	term.background.Add(1)
	configured := source.config.RunnerPools[0]

	source.openRegistrationSession(newCancelAfterFirstErrContext(false), term,
		registrationKey(record), record, configured)
	source.collectAbandonedSessions()
	if len(source.retiring) != 1 {
		t.Fatalf("abandoned sessions = %d, want 1", len(source.retiring))
	}
	if err := source.retryRetiringSessions(context.Background()); err != nil {
		t.Fatalf("retire abandoned session: %v", err)
	}
}

func TestGitHubSourceRuntimeStopsOpeningWhileWaitingForSlot(t *testing.T) {
	manager := branchGitHubManager()
	record := manager.snapshotFacts().Active[0]
	source := newGitHubSourceRuntime(manager, &capacityAggregatorFake{}, "member", eventHandlerFake{},
		managerConfigForAdapters(manager.adapters), time.Minute, func() time.Time { return time.Time{} })
	term := branchLeadershipTerm(1)
	for range cap(term.openSlots) {
		term.openSlots <- struct{}{}
	}
	term.background.Add(1)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	configured := source.config.RunnerPools[0]
	source.openRegistrationSession(canceled, term, registrationKey(record), record, configured)
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
	return &githubLeadershipTerm{epoch: epoch, openResults: make(chan githubRegistrationOpenResult, 4),
		discoveryResult: make(chan githubDiscoveryResult, 1), openSlots: make(chan struct{}, 4),
		operations: make(map[uint64]context.CancelFunc)}
}

func openBranchSession(t *testing.T, manager *githubManagerFake) *githubsource.RegistrationSession {
	t.Helper()
	record := manager.snapshotFacts().Active[0]
	configured := managerConfigForAdapters(manager.adapters).RunnerPools[0]
	session, err := manager.OpenRegistrationSession(context.Background(), record, configured, "member", eventHandlerFake{})
	if err != nil {
		t.Fatalf("open test registration session: %v", err)
	}
	return session
}

type staticRegistrationSessionManager struct {
	githubManagerRuntime
	session *githubsource.RegistrationSession
}

func (manager *staticRegistrationSessionManager) OpenRegistrationSession(
	context.Context,
	githubsource.Registration,
	githubsource.RunnerPoolConfig,
	string,
	githubsource.EventHandler,
) (*githubsource.RegistrationSession, error) {
	return manager.session, nil
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
