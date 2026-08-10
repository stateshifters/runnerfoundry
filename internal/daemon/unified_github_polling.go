package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

type githubPollResult struct {
	poll githubsource.PollResult
	err  error
}

type githubPollAttempt struct {
	cancel context.CancelFunc
	result chan githubPollResult
}

type runnerPoolPublication struct {
	capacity int
	count    int
}

const githubSessionRetirementLimit = 4

// Poll advances leader-owned GitHub message polling without waiting for an
// upstream long poll. Calls must remain serialized with OpenLeader and
// CloseLeader; each adapter has at most one in-flight request.
func (source *githubSourceRuntime) Poll(ctx context.Context, liveMemberCutoff time.Time) error {
	if !source.leaderInstalled || source.term == nil {
		return nil
	}
	term := source.term
	pollErrors := source.harvestRegistrationOpenResults(ctx, term)
	pollErrors = append(pollErrors, source.harvestDiscovery(ctx, term)...)
	pollErrors = append(pollErrors, source.retireRemovedRegistrationSessions(ctx)...)
	source.collectAbandonedSessions()
	pollErrors = append(pollErrors, source.harvestPolls()...)
	pollErrors = append(pollErrors, source.startDueDiscovery(ctx, term, liveMemberCutoff)...)
	pollErrors = append(pollErrors, source.launchIdlePolls(ctx, liveMemberCutoff)...)
	if cleanupErr := source.retryRetiringSessions(ctx); cleanupErr != nil {
		pollErrors = append(pollErrors, cleanupErr)
	}
	return errors.Join(pollErrors...)
}

func (source *githubSourceRuntime) startDueDiscovery(
	ctx context.Context,
	term *githubLeadershipTerm,
	liveMemberCutoff time.Time,
) []error {
	if source.currentTime().Before(source.nextDiscovery) || term.discoveryActive {
		return nil
	}
	config, resultErrors, ready := source.clusterReconciliationConfig(ctx, liveMemberCutoff)
	if ready {
		source.startDiscovery(ctx, term, config)
	}
	return resultErrors
}

func (source *githubSourceRuntime) clusterReconciliationConfig(
	ctx context.Context,
	liveMemberCutoff time.Time,
) (githubsource.ManagerConfig, []error, bool) {
	declarationReader, available := source.manager.(interface {
		ClusterRunnerPools(
			ctx context.Context,
			liveMemberCutoff time.Time,
		) (githubsource.RunnerPoolDeclarationSnapshot, error)
	})
	if !available {
		return source.config, nil, true
	}
	declarations, err := declarationReader.ClusterRunnerPools(ctx, liveMemberCutoff)
	if errors.Is(err, githubsource.ErrRunnerPoolDeclarationPersistenceUnavailable) {
		return source.config, nil, true
	}
	if err != nil {
		return githubsource.ManagerConfig{}, []error{markOperationallyReported(err)}, false
	}
	config := source.config
	config.RunnerPools = declarations.RunnerPools
	config.PreserveRunnerPools = declarations.ConflictedRunnerPools
	return config, declarationConflictErrors(declarations.ConflictedRunnerPools), true
}

func declarationConflictErrors(runnerPools []core.RunnerPool) []error {
	resultErrors := make([]error, 0, len(runnerPools))
	for _, runnerPool := range runnerPools {
		resultErrors = append(resultErrors, markOperationallyReported(fmt.Errorf(
			"live members declare conflicting GitHub runner groups for Runner Pool %q", runnerPool)))
	}
	return resultErrors
}

func (source *githubSourceRuntime) harvestRegistrationOpenResults(
	ctx context.Context,
	term *githubLeadershipTerm,
) []error {
	results := source.drainRegistrationOpenResults(term)
	sort.SliceStable(results, func(first, second int) bool {
		if results[first].key.repository != results[second].key.repository {
			return results[first].key.repository < results[second].key.repository
		}
		return results[first].key.runnerPool < results[second].key.runnerPool
	})
	resultErrors := make([]error, 0, len(results))
	for _, result := range results {
		if err := source.installRegistrationSession(ctx, term, result); err != nil {
			resultErrors = append(resultErrors, err)
		}
	}
	return resultErrors
}

func (*githubSourceRuntime) drainRegistrationOpenResults(
	term *githubLeadershipTerm,
) []githubRegistrationOpenResult {
	results := make([]githubRegistrationOpenResult, 0)
	for {
		select {
		case result := <-term.openResults:
			results = append(results, result)
		default:
			return results
		}
	}
}

func (source *githubSourceRuntime) installRegistrationSession(
	ctx context.Context,
	term *githubLeadershipTerm,
	result githubRegistrationOpenResult,
) error {
	if pending, expected := source.opening[result.key]; expected && pending == result.requested {
		delete(source.opening, result.key)
	}
	if !source.acceptsRegistrationResult(term, result) {
		return source.closeRegistrationSession(ctx, result.session, "discard stale GitHub registration session")
	}
	if errors.Is(result.err, githubsource.ErrRegistrationNotAdmitted) {
		source.removeDesiredRegistration(result.key)
		return source.closeRegistrationSession(ctx, result.session, "retire unadmitted GitHub registration session")
	}
	if result.err != nil {
		cleanupErr := source.closeRegistrationSession(ctx, result.session, "retire failed GitHub registration session")
		source.scheduleRequiredRegistrationSessions(ctx, term)
		return markOperationallyReported(errors.Join(
			fmt.Errorf("open GitHub registration session for Runner Pool %q in %q: %w",
				result.key.runnerPool, result.key.repository, result.err), cleanupErr,
		))
	}
	if result.session == nil {
		source.scheduleRequiredRegistrationSessions(ctx, term)
		return markOperationallyReported(fmt.Errorf(
			"open GitHub registration session for Runner Pool %q in %q: manager returned nil session",
			result.key.runnerPool, result.key.repository,
		))
	}
	return source.installOpenedRegistrationSession(ctx, term, result)
}

func (source *githubSourceRuntime) removeDesiredRegistration(key githubRegistrationKey) {
	delete(source.desired, key)
	if !source.sessionActivationActive {
		return
	}
	source.sessionActivationTotal = len(source.desired)
	source.reportSessionActivationProgress()
}

func (source *githubSourceRuntime) installOpenedRegistrationSession(
	ctx context.Context,
	term *githubLeadershipTerm,
	result githubRegistrationOpenResult,
) error {
	registration := result.session.Registration()
	if registrationKey(registration) != result.key {
		return errors.Join(
			fmt.Errorf("open GitHub registration session for Runner Pool %q in %q: manager changed route identity",
				result.key.runnerPool, result.key.repository),
			source.closeRegistrationSession(ctx, result.session, "retire invalid GitHub registration session"),
		)
	}
	previous := source.sessions[result.key]
	if err := source.stopRegistrationSessionPoll(ctx, previous); err != nil {
		cleanupErr := source.closeRegistrationSession(ctx, result.session, "retire rejected GitHub registration session")
		source.scheduleRequiredRegistrationSessions(ctx, term)
		return errors.Join(err, cleanupErr)
	}
	source.desired[result.key] = registration
	source.sessions[result.key] = result.session
	source.rebuildAdapters()
	source.reportSessionActivationProgress()
	return source.closeRegistrationSession(ctx, previous,
		fmt.Sprintf("retire replaced GitHub registration session for Runner Pool %q in %q",
			result.key.runnerPool, result.key.repository))
}

func (source *githubSourceRuntime) acceptsRegistrationResult(
	term *githubLeadershipTerm,
	result githubRegistrationOpenResult,
) bool {
	return result.epoch == term.epoch && source.term == term && source.leaderInstalled &&
		source.desired[result.key] == result.requested
}

func (source *githubSourceRuntime) stopRegistrationSessionPoll(
	ctx context.Context,
	session *githubsource.RegistrationSession,
) error {
	if session == nil {
		return nil
	}
	return source.stopPolls(ctx, func(adapter *githubsource.Adapter) bool {
		return adapter == session.Adapter()
	})
}

func (source *githubSourceRuntime) closeRegistrationSession(
	ctx context.Context,
	session *githubsource.RegistrationSession,
	description string,
) error {
	if session == nil {
		return nil
	}
	if err := session.Close(ctx); err != nil {
		source.retiring = append(source.retiring, session)
		return fmt.Errorf("%s: %w", description, err)
	}
	return nil
}

func (source *githubSourceRuntime) harvestDiscovery(
	ctx context.Context,
	term *githubLeadershipTerm,
) []error {
	select {
	case result := <-term.discoveryResult:
		term.discoveryActive = false
		if result.epoch != term.epoch || source.term != term || !source.leaderInstalled {
			return nil
		}
		source.nextDiscovery = result.completedAt.Add(source.discoveryInterval)
		if result.err != nil {
			if !githubsource.IsPruneFailure(result.err) {
				return []error{markOperationallyReported(fmt.Errorf("reconcile GitHub registrations: %w", result.err))}
			}
			resultErrors := source.applyDiscoveredRegistrations(ctx, term, result.registrations)
			return append(resultErrors, markOperationallyReported(
				fmt.Errorf("prune GitHub registrations: %w", result.err),
			))
		}
		return source.applyDiscoveredRegistrations(ctx, term, result.registrations)
	default:
		return nil
	}
}

func (source *githubSourceRuntime) applyDiscoveredRegistrations(
	ctx context.Context,
	term *githubLeadershipTerm,
	registrations []githubsource.Registration,
) []error {
	source.desired = configuredGitHubRegistrationRoutes(registrations)
	source.beginSessionActivation()
	source.scheduleRequiredRegistrationSessions(ctx, term)
	source.nextMemberRefresh = time.Time{}
	return nil
}

func (source *githubSourceRuntime) retireRemovedRegistrationSessions(ctx context.Context) []error {
	resultErrors := make([]error, 0)
	for _, key := range sortedGitHubRegistrationKeys(source.sessions) {
		if _, retained := source.desired[key]; retained {
			continue
		}
		session := source.sessions[key]
		if err := source.stopPolls(ctx, func(adapter *githubsource.Adapter) bool {
			return adapter == session.Adapter()
		}); err != nil {
			resultErrors = append(resultErrors, err)
			continue
		}
		delete(source.sessions, key)
		source.retiring = append(source.retiring, session)
	}
	source.rebuildAdapters()
	return resultErrors
}

func (source *githubSourceRuntime) retryRetiringSessions(ctx context.Context) error {
	retiring := source.retiring
	source.retiring = nil
	limit := min(githubSessionRetirementLimit, len(retiring))
	var joined error
	for _, session := range retiring[:limit] {
		if err := session.Close(ctx); err != nil {
			source.retiring = append(source.retiring, session)
			joined = errors.Join(joined, err)
		}
	}
	source.retiring = append(source.retiring, retiring[limit:]...)
	return joined
}

func (source *githubSourceRuntime) rebuildAdapters() {
	adapters := make([]*githubsource.Adapter, 0, len(source.sessions))
	for _, key := range sortedGitHubRegistrationKeys(source.sessions) {
		adapters = append(adapters, source.sessions[key].Adapter())
	}
	source.adapters = adapters
}

func (source *githubSourceRuntime) harvestPolls() []error {
	pollErrors := make([]error, 0)
	for _, adapter := range source.adapters {
		attempt, exists := source.polls[adapter]
		if !exists {
			continue
		}
		result, complete := completedPoll(attempt)
		if !complete {
			continue
		}
		delete(source.polls, adapter)
		if result.err != nil {
			pollErrors = append(pollErrors, markOperationallyReported(describeGitHubPollError(adapter, result.err)))
		}
	}
	return pollErrors
}

func completedPoll(attempt *githubPollAttempt) (githubPollResult, bool) {
	select {
	case result := <-attempt.result:
		return result, true
	default:
		return githubPollResult{}, false
	}
}

func (source *githubSourceRuntime) launchIdlePolls(
	ctx context.Context,
	liveMemberCutoff time.Time,
) []error {
	capacityErrors := make([]error, 0)
	capacities := make(map[core.RunnerPool]runnerPoolPublication)
	for _, adapter := range source.adapters {
		if _, active := source.polls[adapter]; active {
			continue
		}
		capacity, err := source.launchAdapterPoll(ctx, adapter, liveMemberCutoff, capacities)
		if err != nil {
			capacityErrors = append(capacityErrors, err)
		}
		source.launchPoll(ctx, adapter, capacity)
	}
	source.publishCapacityChanges(capacities)
	return capacityErrors
}

func (source *githubSourceRuntime) launchAdapterPoll(ctx context.Context, adapter *githubsource.Adapter,
	liveMemberCutoff time.Time, capacities map[core.RunnerPool]runnerPoolPublication,
) (int, error) {
	aggregate, observed := capacities[adapter.RunnerPool]
	if observed {
		aggregate.count++
		capacities[adapter.RunnerPool] = aggregate
		reportProgress(source.report, slog.LevelDebug, operationalEvent{operation: "capacity_publication", stage: "session",
			outcome: "published", runnerPool: adapter.RunnerPool, repository: adapter.Repository,
			capacity: aggregate.capacity})
		return aggregate.capacity, nil
	}
	capacity, err := source.capacities.AvailableRunnerPoolCapacity(ctx, adapter.RunnerPool, liveMemberCutoff)
	if err != nil {
		reportOperational(source.report, operationalEvent{operation: "capacity_publication", outcome: "failed",
			runnerPool: adapter.RunnerPool, err: err})
		aggregate.count = 1
		capacities[adapter.RunnerPool] = aggregate
		return aggregate.capacity, markOperationallyReported(fmt.Errorf(
			"read Runner Pool %q available capacity: %w", adapter.RunnerPool, err,
		))
	}
	reportProgress(source.report, slog.LevelDebug, operationalEvent{operation: "capacity_publication", stage: "session",
		outcome: "published", runnerPool: adapter.RunnerPool, repository: adapter.Repository, capacity: capacity})
	aggregate.capacity = capacity
	aggregate.count = 1
	capacities[adapter.RunnerPool] = aggregate
	return capacity, nil
}

func (source *githubSourceRuntime) publishCapacityChanges(capacities map[core.RunnerPool]runnerPoolPublication) {
	for runnerPool, aggregate := range capacities {
		previous, exists := source.publishedCapacity[runnerPool]
		if exists && previous == aggregate.capacity {
			continue
		}
		source.publishedCapacity[runnerPool] = aggregate.capacity
		reportProgress(source.report, slog.LevelInfo, operationalEvent{operation: "capacity_publication", stage: "runner_pool",
			outcome: "published", runnerPool: runnerPool, capacity: aggregate.capacity, count: aggregate.count})
	}
}

func (source *githubSourceRuntime) launchPoll(
	ctx context.Context,
	adapter *githubsource.Adapter,
	capacity int,
) {
	pollContext, cancel := context.WithCancel(ctx)
	results := make(chan githubPollResult, 1)
	source.polls[adapter] = &githubPollAttempt{cancel: cancel, result: results}
	go func() {
		pollResult, err := adapter.Poll(pollContext, capacity)
		results <- githubPollResult{poll: pollResult, err: err}
	}()
}

func (source *githubSourceRuntime) stopAllPolls(ctx context.Context) error {
	return source.stopPolls(ctx, func(*githubsource.Adapter) bool { return true })
}

func (source *githubSourceRuntime) stopPolls(
	ctx context.Context,
	shouldStop func(*githubsource.Adapter) bool,
) error {
	for adapter, attempt := range source.polls {
		if shouldStop(adapter) {
			attempt.cancel()
		}
	}
	for adapter, attempt := range source.polls {
		if !shouldStop(adapter) {
			continue
		}
		select {
		case <-attempt.result:
			delete(source.polls, adapter)
		case <-ctx.Done():
			return fmt.Errorf("wait for GitHub polls to stop: %w", ctx.Err())
		}
	}
	return nil
}

func describeGitHubPollError(adapter *githubsource.Adapter, err error) error {
	return fmt.Errorf("poll GitHub Runner Pool %q for %q: %w", adapter.RunnerPool, adapter.Repository, err)
}
