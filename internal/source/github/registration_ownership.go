package github

import (
	"context"
	"fmt"
	"sort"

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

func groupOwnedRunnerPools(registrations []Registration) map[int64][]transport.OwnedRunnerPool {
	grouped := make(map[int64][]transport.OwnedRunnerPool)
	for _, registration := range registrations {
		grouped[registration.RunnerGroupID] = append(grouped[registration.RunnerGroupID], ownedRunnerPool(registration))
	}
	return grouped
}

func pruneRunnerPoolGroups(
	ctx context.Context,
	client ManagerClient,
	currentByGroup map[int64][]transport.OwnedRunnerPool,
	ownedByGroup map[int64][]transport.OwnedRunnerPool,
) (map[transport.OwnedRunnerPool]struct{}, error) {
	removed := make(map[transport.OwnedRunnerPool]struct{})
	for _, runnerGroupID := range registrationGroupIDs(currentByGroup, ownedByGroup) {
		result, err := client.PruneRunnerPools(ctx, int(runnerGroupID), currentByGroup[runnerGroupID], ownedByGroup[runnerGroupID])
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
	return transport.OwnedRunnerPool{ID: int(registration.ScaleSetID), Name: string(registration.RunnerPool), RunnerGroupID: int(registration.RunnerGroupID)}
}

func registrationGroupIDs(current, owned map[int64][]transport.OwnedRunnerPool) []int64 {
	groups := make(map[int64]struct{}, len(current)+len(owned))
	for groupID := range current {
		groups[groupID] = struct{}{}
	}
	for groupID := range owned {
		groups[groupID] = struct{}{}
	}
	groupIDs := make([]int64, 0, len(groups))
	for groupID := range groups {
		groupIDs = append(groupIDs, groupID)
	}
	sort.Slice(groupIDs, func(first, second int) bool { return groupIDs[first] < groupIDs[second] })
	return groupIDs
}

func mergeOwnedRegistrations(previous, active []Registration) []Registration {
	owned := append([]Registration(nil), previous...)
	indices := make(map[registrationIdentity]int, len(owned))
	for index, registration := range owned {
		indices[registrationIdentity{repositoryURL: registration.RepositoryURL, scaleSetID: registration.ScaleSetID}] = index
	}
	for _, registration := range active {
		identity := registrationIdentity{repositoryURL: registration.RepositoryURL, scaleSetID: registration.ScaleSetID}
		if index, exists := indices[identity]; exists {
			owned[index] = registration
			continue
		}
		indices[identity] = len(owned)
		owned = append(owned, registration)
	}
	return owned
}

type registrationIdentity struct {
	repositoryURL string
	scaleSetID    int64
}

func (manager *Manager) pruneRegistrations(
	ctx context.Context,
	repositories []transport.Repository,
	active []Registration,
	owned []Registration,
) ([]Registration, error) {
	for _, repository := range repositories {
		client, err := manager.factory(ctx, repository)
		if err != nil {
			return nil, fmt.Errorf("create GitHub prune client for repository %q: %w", repository.URL, err)
		}
		retained, err := pruneRepository(ctx, client,
			registrationsForRepository(active, repository.URL), registrationsForRepository(owned, repository.URL))
		if err != nil {
			return nil, fmt.Errorf("prune repository %q: %w", repository.URL, err)
		}
		owned = replaceRepositoryRegistrations(owned, repository.URL, retained)
	}
	return owned, nil
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
