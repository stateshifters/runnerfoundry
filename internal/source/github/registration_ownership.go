package github

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/meza/RunnerFoundry/internal/core"
	"github.com/meza/RunnerFoundry/internal/source/github/transport"
)

func pruneRepository(ctx context.Context, client ManagerClient, active, owned []Registration) ([]Registration, error) {
	currentByGroup := groupOwnedRunnerPools(active)
	ownedByGroup := groupOwnedRunnerPools(owned)
	removed, err := pruneRunnerPoolGroups(ctx, client, currentByGroup, ownedByGroup)
	if err != nil {
		return nil, err
	}
	return retainOwnedRegistrations(owned, removed), nil
}

func (manager *Manager) pruneReconciledRegistrations(
	ctx context.Context,
	repositories []transport.Repository,
	active []Registration,
) error {
	owned, err := manager.registrations.OwnedRegistrations(ctx)
	if err != nil {
		return PruneFailure{Err: fmt.Errorf("load owned GitHub registrations: %w", err)}
	}
	canonical, err := canonicalRegistrationSnapshot(RegistrationSnapshot{Owned: owned})
	if err != nil {
		return PruneFailure{Err: fmt.Errorf("validate owned GitHub registrations: %w", err)}
	}
	if _, err = manager.pruneOwnedRegistrations(ctx, repositories, active, canonical.Owned); err != nil {
		return PruneFailure{Err: err}
	}
	return nil
}

func groupOwnedRunnerPools(registrations []Registration) map[transport.RunnerGroupID][]transport.OwnedRunnerPool {
	grouped := make(map[transport.RunnerGroupID][]transport.OwnedRunnerPool)
	for _, registration := range registrations {
		grouped[registration.RunnerGroupID] = append(grouped[registration.RunnerGroupID], ownedRunnerPool(registration))
	}
	return grouped
}

func pruneRunnerPoolGroups(
	ctx context.Context,
	client ManagerClient,
	currentByGroup map[transport.RunnerGroupID][]transport.OwnedRunnerPool,
	ownedByGroup map[transport.RunnerGroupID][]transport.OwnedRunnerPool,
) (map[transport.OwnedRunnerPool]struct{}, error) {
	removed := make(map[transport.OwnedRunnerPool]struct{})
	for _, runnerGroupID := range registrationGroupIDs(currentByGroup, ownedByGroup) {
		result, err := client.PruneRunnerPools(ctx, runnerGroupID, currentByGroup[runnerGroupID], ownedByGroup[runnerGroupID])
		if err != nil {
			return nil, err
		}
		for _, registration := range result.Removed {
			removed[registration] = struct{}{}
		}
	}
	return removed, nil
}

func retainOwnedRegistrations(owned []Registration, removed map[transport.OwnedRunnerPool]struct{}) []Registration {
	retained := make([]Registration, 0, len(owned))
	for _, registration := range owned {
		if _, wasRemoved := removed[ownedRunnerPool(registration)]; !wasRemoved {
			retained = append(retained, registration)
		}
	}
	return retained
}

func ownedRunnerPool(registration Registration) transport.OwnedRunnerPool {
	return transport.OwnedRunnerPool{ID: registration.ScaleSetID, Name: string(registration.RunnerPool), RunnerGroupID: registration.RunnerGroupID}
}

func registrationGroupIDs(current, owned map[transport.RunnerGroupID][]transport.OwnedRunnerPool) []transport.RunnerGroupID {
	groups := make(map[transport.RunnerGroupID]struct{}, len(current)+len(owned))
	for groupID := range current {
		groups[groupID] = struct{}{}
	}
	for groupID := range owned {
		groups[groupID] = struct{}{}
	}
	groupIDs := make([]transport.RunnerGroupID, 0, len(groups))
	for groupID := range groups {
		groupIDs = append(groupIDs, groupID)
	}
	sort.Slice(groupIDs, func(first, second int) bool { return groupIDs[first] < groupIDs[second] })
	return groupIDs
}

type registrationIdentity struct {
	repositoryURL string
	scaleSetID    transport.ScaleSetID
}

type activeRegistrationIdentity struct {
	repositoryURL string
	runnerPool    core.RunnerPool
}

func (manager *Manager) deleteInactiveRegistrations(
	ctx context.Context,
	previous []Registration,
	active []Registration,
) error {
	absent := absentRegistrations(previous, active, func(registration Registration) activeRegistrationIdentity {
		return activeRegistrationIdentity{repositoryURL: registration.RepositoryURL, runnerPool: registration.RunnerPool}
	})
	for _, registration := range absent {
		if err := manager.registrations.DeleteActiveRegistration(ctx, registration); err != nil {
			return fmt.Errorf("delete inactive GitHub registration for repository %q and Runner Pool %q: %w",
				registration.RepositoryURL, registration.RunnerPool, err)
		}
	}
	return nil
}

func (manager *Manager) deletePrunedOwnership(
	ctx context.Context,
	previous []Registration,
	retained []Registration,
) error {
	absent := absentRegistrations(previous, retained, func(registration Registration) registrationIdentity {
		return registrationIdentity{repositoryURL: registration.RepositoryURL, scaleSetID: registration.ScaleSetID}
	})
	for _, registration := range absent {
		if err := manager.registrations.DeleteOwnedRegistration(ctx, registration); err != nil {
			return fmt.Errorf("delete pruned GitHub ownership for repository %q and scale set %d: %w",
				registration.RepositoryURL, registration.ScaleSetID, err)
		}
	}
	return nil
}

func absentRegistrations[Identity comparable](
	previous, current []Registration,
	identity func(Registration) Identity,
) []Registration {
	currentIdentities := make(map[Identity]struct{}, len(current))
	for _, registration := range current {
		currentIdentities[identity(registration)] = struct{}{}
	}
	absent := make([]Registration, 0)
	for _, registration := range previous {
		if _, exists := currentIdentities[identity(registration)]; !exists {
			absent = append(absent, registration)
		}
	}
	return absent
}

func (manager *Manager) pruneRegistrations(
	ctx context.Context,
	repositories []transport.Repository,
	active []Registration,
	owned []Registration,
) ([]Registration, error) {
	for index, repository := range repositories {
		started := manager.observer.currentTime()
		manager.reportPruneRepository(repository.URL, index+1, len(repositories), "started", started, nil)
		client, err := manager.factory(ctx, repository)
		if err != nil {
			manager.reportPruneRepository(repository.URL, index+1, len(repositories), "failed", started, err)
			return nil, fmt.Errorf("create GitHub prune client for repository %q: %w", repository.URL, err)
		}
		retained, err := pruneRepository(ctx, client,
			registrationsForRepository(active, repository.URL), registrationsForRepository(owned, repository.URL))
		if err != nil {
			manager.reportPruneRepository(repository.URL, index+1, len(repositories), "failed", started, err)
			return nil, fmt.Errorf("prune repository %q: %w", repository.URL, err)
		}
		manager.reportPruneRepository(repository.URL, index+1, len(repositories), "completed", started, nil)
		owned = replaceRepositoryRegistrations(owned, repository.URL, retained)
	}
	return owned, nil
}

func (manager *Manager) reportPruneRepository(repository string, current, total int, outcome string,
	started time.Time, err error,
) {
	duration := time.Duration(0)
	if outcome != "started" {
		duration = manager.observer.currentTime().Sub(started)
	}
	manager.report(ManagerActivity{Visibility: ActivityDebug, Operation: "source_reconciliation",
		Stage: "prune_repository", Outcome: outcome, Repository: repository,
		Current: current, Total: total, Duration: duration, Err: err})
}

func registrationsForRepository(registrations []Registration, repositoryURL string) []Registration {
	result := make([]Registration, 0)
	for _, registration := range registrations {
		if registration.RepositoryURL == repositoryURL {
			result = append(result, registration)
		}
	}
	return result
}

func replaceRepositoryRegistrations(registrations []Registration, repositoryURL string, replacement []Registration) []Registration {
	result := make([]Registration, 0, len(registrations)-len(registrationsForRepository(registrations, repositoryURL))+len(replacement))
	for _, registration := range registrations {
		if registration.RepositoryURL != repositoryURL {
			result = append(result, registration)
		}
	}
	return append(result, replacement...)
}
