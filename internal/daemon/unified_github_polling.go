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

// Poll advances leader-owned GitHub message polling without waiting for an
// upstream long poll. Calls must remain serialized with OpenLeader and
// CloseLeader; each adapter has at most one in-flight request.
func (source *githubSourceRuntime) Poll(ctx context.Context, liveMemberCutoff time.Time) error {
	if !source.leaderInstalled || source.term == nil {
		return nil
	}
	term := source.term
	pollErrors := source.harvestRepositoryOpenResults(ctx, term)
	pollErrors = append(pollErrors, source.harvestDiscovery(ctx, term)...)
	pollErrors = append(pollErrors, source.retireRemovedRepositories(ctx)...)
	source.collectAbandonedGenerations()
	if cleanupErr := source.retryRetiringGenerations(ctx); cleanupErr != nil {
		pollErrors = append(pollErrors, cleanupErr)
	}
	harvestedPollErrors, changedPools := source.harvestPolls()
	pollErrors = append(pollErrors, harvestedPollErrors...)
	if err := source.stopChangedPools(ctx, changedPools); err != nil {
		pollErrors = append(pollErrors, err)
	}
	if !source.currentTime().Before(source.nextDiscovery) && !term.discoveryActive {
		source.startDiscovery(ctx, term)
		source.nextDiscovery = source.currentTime().Add(source.discoveryInterval)
	}
	pollErrors = append(pollErrors, source.launchIdlePolls(ctx, liveMemberCutoff)...)
	return errors.Join(pollErrors...)
}

func (source *githubSourceRuntime) harvestRepositoryOpenResults(
	ctx context.Context,
	term *githubLeadershipTerm,
) []error {
	results := source.drainRepositoryOpenResults(term)
	sort.SliceStable(results, func(first, second int) bool {
		return results[first].repository < results[second].repository
	})
	resultErrors := make([]error, 0, len(results))
	for _, result := range results {
		if err := source.installRepositoryGeneration(ctx, term, result); err != nil {
			resultErrors = append(resultErrors, err)
		}
	}
	return resultErrors
}

func (*githubSourceRuntime) drainRepositoryOpenResults(
	term *githubLeadershipTerm,
) []githubRepositoryOpenResult {
	results := make([]githubRepositoryOpenResult, 0)
	for {
		select {
		case result := <-term.openResults:
			results = append(results, result)
		default:
			return results
		}
	}
}

func (source *githubSourceRuntime) installRepositoryGeneration(
	ctx context.Context,
	term *githubLeadershipTerm,
	result githubRepositoryOpenResult,
) error {
	if pending, expected := source.opening[result.repository]; expected && sameRegistrations(pending, result.records) {
		delete(source.opening, result.repository)
	}
	if !source.acceptsRepositoryResult(term, result) {
		return source.closeRepositoryGeneration(ctx, result.generation, "discard stale GitHub repository sessions")
	}
	if result.err != nil {
		return markOperationallyReported(source.handleRepositoryOpenFailure(ctx, term, result))
	}
	if result.generation == nil {
		return fmt.Errorf("open GitHub repository sessions for %q: manager returned nil generation", result.repository)
	}
	previous := source.generations[result.repository]
	if err := source.stopRepositoryGenerationPolls(ctx, previous); err != nil {
		cleanupErr := source.closeRepositoryGeneration(ctx, result.generation, "retire rejected GitHub repository sessions")
		source.scheduleRequiredRepositoryGenerations(ctx, term)
		return errors.Join(err, cleanupErr)
	}
	source.generations[result.repository] = result.generation
	source.rebuildAdapters()
	source.reportSessionActivationProgress()
	return source.closeRepositoryGeneration(ctx, previous,
		fmt.Sprintf("retire replaced GitHub repository sessions for %q", result.repository))
}

func (source *githubSourceRuntime) acceptsRepositoryResult(
	term *githubLeadershipTerm,
	result githubRepositoryOpenResult,
) bool {
	return result.epoch == term.epoch && source.term == term && source.leaderInstalled &&
		sameRegistrations(source.desired[result.repository], result.records)
}

func (source *githubSourceRuntime) handleRepositoryOpenFailure(
	ctx context.Context,
	term *githubLeadershipTerm,
	result githubRepositoryOpenResult,
) error {
	cleanupErr := source.closeRepositoryGeneration(ctx, result.generation, "retire partial GitHub repository sessions")
	source.scheduleRequiredRepositoryGenerations(ctx, term)
	return errors.Join(
		fmt.Errorf("open GitHub repository sessions for %q: %w", result.repository, result.err),
		cleanupErr,
	)
}

func (source *githubSourceRuntime) stopRepositoryGenerationPolls(
	ctx context.Context,
	generation *githubsource.RepositorySessionGeneration,
) error {
	if generation == nil {
		return nil
	}
	adapters := make(map[*githubsource.Adapter]struct{})
	for _, adapter := range generation.Adapters() {
		adapters[adapter] = struct{}{}
	}
	return source.stopPolls(ctx, func(adapter *githubsource.Adapter) bool {
		_, found := adapters[adapter]
		return found
	})
}

func (source *githubSourceRuntime) closeRepositoryGeneration(
	ctx context.Context,
	generation *githubsource.RepositorySessionGeneration,
	description string,
) error {
	if generation == nil {
		return nil
	}
	if err := generation.Close(ctx); err != nil {
		source.retiring = append(source.retiring, generation)
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
		if result.err != nil {
			return []error{markOperationallyReported(fmt.Errorf("reconcile GitHub registrations: %w", result.err))}
		}
		source.desired = groupActiveRegistrations(result.snapshot.Active)
		resultErrors := make([]error, 0)
		source.beginSessionActivation()
		source.scheduleRequiredRepositoryGenerations(ctx, term)
		if err := source.manager.RefreshMembers(ctx); err != nil {
			resultErrors = append(resultErrors, markOperationallyReported(
				fmt.Errorf("refresh GitHub member routes: %w", err),
			))
		} else {
			source.nextMemberRefresh = source.currentTime().Add(source.discoveryInterval)
		}
		return resultErrors
	default:
		return nil
	}
}

func (source *githubSourceRuntime) retireRemovedRepositories(ctx context.Context) []error {
	resultErrors := make([]error, 0)
	for _, repository := range sortedGenerationRepositories(source.generations) {
		if _, retained := source.desired[repository]; retained {
			continue
		}
		generation := source.generations[repository]
		generationAdapters := make(map[*githubsource.Adapter]struct{})
		for _, adapter := range generation.Adapters() {
			generationAdapters[adapter] = struct{}{}
		}
		if err := source.stopPolls(ctx, func(adapter *githubsource.Adapter) bool {
			_, found := generationAdapters[adapter]
			return found
		}); err != nil {
			resultErrors = append(resultErrors, err)
			continue
		}
		delete(source.generations, repository)
		if err := generation.Close(ctx); err != nil {
			source.retiring = append(source.retiring, generation)
			resultErrors = append(resultErrors,
				fmt.Errorf("retire removed GitHub repository sessions for %q: %w", repository, err))
		}
	}
	source.rebuildAdapters()
	return resultErrors
}

func (source *githubSourceRuntime) retryRetiringGenerations(ctx context.Context) error {
	retiring := source.retiring
	source.retiring = nil
	var joined error
	for _, generation := range retiring {
		if err := generation.Close(ctx); err != nil {
			source.retiring = append(source.retiring, generation)
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

func (source *githubSourceRuntime) rebuildAdapters() {
	adapters := make([]*githubsource.Adapter, 0)
	for _, repository := range sortedGenerationRepositories(source.generations) {
		adapters = append(adapters, source.generations[repository].Adapters()...)
	}
	source.adapters = adapters
}

func (source *githubSourceRuntime) harvestPolls() ([]error, map[core.RunnerPool]struct{}) {
	pollErrors := make([]error, 0)
	changedPools := make(map[core.RunnerPool]struct{})
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
		if result.poll.Handled {
			changedPools[adapter.RunnerPool] = struct{}{}
		}
		if result.err != nil {
			pollErrors = append(pollErrors, markOperationallyReported(describeGitHubPollError(adapter, result.err)))
		}
	}
	return pollErrors, changedPools
}

func completedPoll(attempt *githubPollAttempt) (githubPollResult, bool) {
	select {
	case result := <-attempt.result:
		return result, true
	default:
		return githubPollResult{}, false
	}
}

func (source *githubSourceRuntime) stopChangedPools(
	ctx context.Context,
	changedPools map[core.RunnerPool]struct{},
) error {
	if len(changedPools) == 0 {
		return nil
	}
	return source.stopPolls(ctx, func(adapter *githubsource.Adapter) bool {
		_, changed := changedPools[adapter.RunnerPool]
		return changed
	})
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
	capacity, err := source.capacities.AvailableRunnerPoolCapacity(ctx, adapter.RunnerPool, liveMemberCutoff)
	if err != nil {
		reportOperational(source.report, operationalEvent{operation: "capacity_publication", outcome: "failed",
			runnerPool: adapter.RunnerPool, err: err})
		return 0, markOperationallyReported(fmt.Errorf(
			"read Runner Pool %q available capacity: %w", adapter.RunnerPool, err,
		))
	}
	reportProgress(source.report, slog.LevelDebug, operationalEvent{operation: "capacity_publication", stage: "session",
		outcome: "published", runnerPool: adapter.RunnerPool, repository: adapter.Repository, capacity: capacity})
	aggregate := capacities[adapter.RunnerPool]
	aggregate.capacity = capacity
	aggregate.count++
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
