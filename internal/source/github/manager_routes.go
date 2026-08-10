package github

import (
	"context"
	"errors"
	"fmt"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/core"
)

// RefreshMembers validates current configuration and independently verifies
// each active registration before replacing member bootstrap routes.
// Unconfigured or stale registrations are not admitted. No leader message
// session is needed on ordinary members.
func (manager *Manager) RefreshMembers(ctx context.Context, config ManagerConfig) (resultErr error) {
	if err := validateManagerConfig(config); err != nil {
		return err
	}
	started := manager.observer.currentTime()
	routeCount := 0
	manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_routes",
		Stage: "replace_routes", Outcome: "started"})
	defer func() {
		outcome := "completed"
		if resultErr != nil {
			outcome = "failed"
		}
		manager.report(ManagerActivity{Visibility: ActivityInfo, Operation: "source_routes",
			Stage: "replace_routes", Outcome: outcome, Count: routeCount,
			Duration: manager.observer.currentTime().Sub(started), Err: resultErr})
	}()
	registrations, err := manager.ActiveRegistrations(ctx)
	if err != nil {
		return fmt.Errorf("list GitHub registrations: %w", err)
	}
	routes, err := manager.buildMemberRoutes(ctx, registrations, config)
	replaceErr := manager.source.ReplaceRoutes(routes)
	if replaceErr != nil {
		replaceErr = fmt.Errorf("replace GitHub member routes: %w", replaceErr)
	}
	routeCount = len(routes)
	return errors.Join(err, replaceErr)
}

func (manager *Manager) buildMemberRoutes(
	ctx context.Context,
	registrations []Registration,
	config ManagerConfig,
) ([]MemberRoute, error) {
	configured := make(map[core.RunnerPool]RunnerPoolConfig, len(config.RunnerPools))
	for _, runnerPool := range config.RunnerPools {
		configured[runnerPool.RunnerPool] = runnerPool
	}
	routes := make([]MemberRoute, 0, len(registrations))
	var joined error
	for index, record := range registrations {
		runnerPool, admitted := configured[record.RunnerPool]
		if !admitted {
			continue
		}
		started := manager.observer.currentTime()
		manager.reportRouteActivity(record, index+1, len(registrations), "started", started, nil)
		route, admitted, routeErr := manager.buildMemberRoute(ctx, record, runnerPool)
		if routeErr != nil || !admitted {
			manager.reportRouteActivity(record, index+1, len(registrations), "failed", started, routeErr)
			joined = errors.Join(joined, routeErr)
			continue
		}
		routes = append(routes, route)
		manager.reportRouteActivity(record, index+1, len(registrations), "completed", started, nil)
	}
	return routes, joined
}

func (manager *Manager) buildMemberRoute(
	ctx context.Context,
	record Registration,
	runnerPool RunnerPoolConfig,
) (MemberRoute, bool, error) {
	client, err := manager.factory(ctx, repositoryFromRegistration(record))
	if err != nil {
		return manager.handleMissingMemberRouteClient(ctx, record, err)
	}
	if client.ResolveRunnerGroup == nil {
		return MemberRoute{}, false, errors.New("GitHub runner-group resolution is required")
	}
	runnerGroupID, err := client.ResolveRunnerGroup(ctx, runnerPool.RunnerGroup)
	if err != nil {
		return MemberRoute{}, false, fmt.Errorf("resolve GitHub runner group %q for repository %q: %w",
			runnerPool.RunnerGroup, record.RepositoryURL, err)
	}
	if runnerGroupID != record.RunnerGroupID {
		return MemberRoute{}, false, fmt.Errorf(
			"configured runner group for Runner Pool %q does not match committed GitHub registration",
			record.RunnerPool,
		)
	}
	return MemberRoute{Repository: record.RepositoryURL, RunnerPool: record.RunnerPool,
		Client: managedBootstrapClient{client: client}, Registration: transportRegistration(record)}, true, nil
}

func (manager *Manager) handleMissingMemberRouteClient(
	ctx context.Context,
	record Registration,
	err error,
) (MemberRoute, bool, error) {
	if errors.Is(err, upstream.NotFoundError) {
		return MemberRoute{}, false, manager.deleteMissingRegistration(ctx, record)
	}
	return MemberRoute{}, false, fmt.Errorf("create GitHub member client for repository %q: %w", record.RepositoryURL, err)
}

func (manager *Manager) reportRouteActivity(record Registration, current, total int, outcome string,
	started time.Time, err error,
) {
	duration := time.Duration(0)
	if outcome != "started" {
		duration = manager.observer.currentTime().Sub(started)
	}
	manager.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_routes",
		Stage: "configure_route", Outcome: outcome, Repository: record.RepositoryURL, RunnerPool: record.RunnerPool,
		Current: current, Total: total, Duration: duration, Err: err})
}
