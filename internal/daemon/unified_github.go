package daemon

import (
	"context"
	"errors"
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
	AggregateRunnerPoolCapacity(ctx context.Context, runnerPool core.RunnerPool, cutoff time.Time) (int, error)
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
	return source.reconcileAndOpen(ctx)
}

func (source *githubSourceRuntime) reconcileAndOpen(ctx context.Context) error {
	if err := source.manager.Reconcile(ctx, source.config); err != nil {
		return fmt.Errorf("reconcile GitHub registrations: %w", err)
	}
	if err := source.manager.RefreshMembers(ctx); err != nil {
		return fmt.Errorf("refresh GitHub member routes: %w", err)
	}
	adapters, err := source.manager.OpenLeader(ctx, source.owner, source.handler)
	if err == nil || adapters != nil {
		source.adapters = adapters
		source.nextDiscovery = source.currentTime().Add(source.discoveryInterval)
		source.nextMemberRefresh = source.nextDiscovery
	}
	if err != nil {
		return fmt.Errorf("open GitHub leader sessions: %w", err)
	}
	return nil
}

func (source *githubSourceRuntime) PublishCapacity(ctx context.Context, liveMemberCutoff time.Time) error {
	if !source.currentTime().Before(source.nextDiscovery) {
		if err := source.reconcileAndOpen(ctx); err != nil {
			return err
		}
	}

	capacityByPool := make(map[core.RunnerPool]int, len(source.config.RunnerPools))
	capacityErrors := make([]error, 0)
	for _, adapter := range source.adapters {
		capacity, found := capacityByPool[adapter.RunnerPool]
		if !found {
			var err error
			capacity, err = source.capacities.AggregateRunnerPoolCapacity(ctx, adapter.RunnerPool, liveMemberCutoff)
			if err != nil {
				capacityErrors = append(capacityErrors,
					fmt.Errorf("aggregate Runner Pool %q capacity: %w", adapter.RunnerPool, err))
				continue
			}
			capacityByPool[adapter.RunnerPool] = capacity
		}
		adapter.MaxCapacity = capacity
	}
	return errors.Join(capacityErrors...)
}

func (source *githubSourceRuntime) Poll(ctx context.Context) error {
	pollErrors := make([]error, 0)
	for _, adapter := range source.adapters {
		if err := adapter.PollOnce(ctx); err != nil {
			pollErrors = append(pollErrors,
				fmt.Errorf("poll GitHub Runner Pool %q for %q: %w", adapter.RunnerPool, adapter.Repository, err))
		}
	}
	return errors.Join(pollErrors...)
}

func (source *githubSourceRuntime) CloseLeader(ctx context.Context) error {
	source.adapters = nil
	if err := source.manager.CloseLeader(ctx); err != nil {
		return fmt.Errorf("close GitHub leader sessions: %w", err)
	}
	return nil
}
