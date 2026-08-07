package daemon

import (
	"context"
	"fmt"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	githubsource "github.com/meza/RunnerFoundry/internal/source/github"
)

type githubManagerRuntime interface {
	Reconcile(ctx context.Context, config githubsource.ManagerConfig) error
	RefreshMembers(ctx context.Context) error
	OpenLeader(ctx context.Context, owner string, handler githubsource.EventHandler) ([]*githubsource.Adapter, error)
	CloseLeader(ctx context.Context) error
}

type capacityAggregator interface {
	AvailableRunnerPoolCapacity(ctx context.Context, runnerPool core.RunnerPool, cutoff time.Time) (int, error)
}

type githubSourceRuntime struct {
	manager           githubManagerRuntime
	capacities        capacityAggregator
	owner             string
	handler           githubsource.EventHandler
	config            githubsource.ManagerConfig
	discoveryInterval time.Duration
	currentTime       func() time.Time
	nextDiscovery     time.Time
	nextMemberRefresh time.Time
	adapters          []*githubsource.Adapter
	polls             map[*githubsource.Adapter]*githubPollAttempt
	leaderInstalled   bool
	report            func(error)
}

func newGitHubSourceRuntime(
	manager githubManagerRuntime,
	capacities capacityAggregator,
	owner string,
	handler githubsource.EventHandler,
	config githubsource.ManagerConfig,
	discoveryInterval time.Duration,
	currentTime func() time.Time,
) *githubSourceRuntime {
	return &githubSourceRuntime{
		manager: manager, capacities: capacities, owner: owner, handler: handler,
		config: config, discoveryInterval: discoveryInterval, currentTime: currentTime,
		polls: make(map[*githubsource.Adapter]*githubPollAttempt),
	}
}

func (source *githubSourceRuntime) RefreshMembers(ctx context.Context) error {
	if source.currentTime().Before(source.nextMemberRefresh) {
		return nil
	}
	if err := source.manager.RefreshMembers(ctx); err != nil {
		return fmt.Errorf("refresh GitHub member routes: %w", err)
	}
	source.nextMemberRefresh = source.currentTime().Add(source.discoveryInterval)
	return nil
}

func (source *githubSourceRuntime) OpenLeader(ctx context.Context) error {
	if source.leaderInstalled {
		return nil
	}
	return source.reconcileAndOpen(ctx)
}

func (source *githubSourceRuntime) reconcileAndOpen(ctx context.Context) error {
	if err := source.manager.Reconcile(ctx, source.config); err != nil {
		return fmt.Errorf("reconcile GitHub registrations: %w", err)
	}
	if err := source.manager.RefreshMembers(ctx); err != nil {
		return fmt.Errorf("refresh GitHub member routes: %w", err)
	}
	previousAdapters := source.adapters
	if err := source.stopAllPolls(ctx); err != nil {
		return err
	}
	adapters, err := source.manager.OpenLeader(ctx, source.owner, source.handler)
	if err == nil || adapters != nil {
		source.adapters = adapters
		source.leaderInstalled = true
		source.nextDiscovery = source.currentTime().Add(source.discoveryInterval)
		source.nextMemberRefresh = source.nextDiscovery
	}
	if err != nil {
		if adapters == nil {
			source.adapters = previousAdapters
		}
		return fmt.Errorf("open GitHub leader sessions: %w", err)
	}
	return nil
}

func (source *githubSourceRuntime) CloseLeader(ctx context.Context) error {
	if err := source.stopAllPolls(ctx); err != nil {
		return err
	}
	if err := source.manager.CloseLeader(ctx); err != nil {
		return fmt.Errorf("close GitHub leader sessions: %w", err)
	}
	source.adapters = nil
	source.leaderInstalled = false
	return nil
}
