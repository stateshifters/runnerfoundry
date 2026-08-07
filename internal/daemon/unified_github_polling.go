package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

type githubPollResult struct {
	messageReceived bool
	err             error
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
	pollErrors, changedPools := source.harvestPolls()
	if err := source.stopChangedPools(ctx, changedPools); err != nil {
		pollErrors = append(pollErrors, err)
	}
	if !source.currentTime().Before(source.nextDiscovery) {
		if err := source.reconcileAndOpen(ctx); err != nil {
			pollErrors = append(pollErrors, err)
		}
	}
	pollErrors = append(pollErrors, source.launchIdlePolls(ctx, liveMemberCutoff)...)
	return errors.Join(pollErrors...)
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
		if result.messageReceived {
			changedPools[adapter.RunnerPool] = struct{}{}
		}
		if result.err != nil {
			pollErrors = append(pollErrors, describeGitHubPollError(adapter, result.err))
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
		return 0, fmt.Errorf("read Runner Pool %q available capacity: %w", adapter.RunnerPool, err)
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
		received, err := adapter.PollOnce(pollContext, capacity)
		results <- githubPollResult{messageReceived: received, err: err}
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
