package transport

import (
	"context"
	"errors"
	"fmt"

	upstream "github.com/actions/scaleset"
)

type OwnedRunnerPool struct {
	ID            int
	Name          string
	RunnerGroupID int
}

type PruneResult struct {
	Removed []OwnedRunnerPool
}

type pruneAction int

const (
	retainOwnership pruneAction = iota
	removeOwnership
	deleteAndRemoveOwnership
)

// PruneRunnerPools removes absent registrations only when their settings
// match the fingerprint used by EnsureRunnerPool. It never changes job state.
func (client *Client) PruneRunnerPools(
	ctx context.Context,
	runnerGroupID int,
	currentRunnerPools []OwnedRunnerPool,
	ownedRunnerPools []OwnedRunnerPool,
) (PruneResult, error) {
	if client == nil || client.registry == nil {
		return PruneResult{}, errors.New("GitHub prune registry API is required")
	}
	runnerScaleSets, err := client.registry.ListRunnerScaleSets(ctx, runnerGroupID)
	if err != nil {
		return PruneResult{}, fmt.Errorf("list runner scale sets for runner group %d: %w", runnerGroupID, err)
	}
	return client.deleteAbsentOwnedRunnerPools(ctx, runnerGroupID, currentRunnerPools, ownedRunnerPools, runnerScaleSets)
}

func (client *Client) deleteAbsentOwnedRunnerPools(
	ctx context.Context,
	runnerGroupID int,
	currentRunnerPools []OwnedRunnerPool,
	ownedRunnerPools []OwnedRunnerPool,
	runnerScaleSets []upstream.RunnerScaleSet,
) (PruneResult, error) {
	current := make(map[OwnedRunnerPool]struct{}, len(currentRunnerPools))
	for _, registration := range currentRunnerPools {
		current[registration] = struct{}{}
	}
	upstreamByID := make(map[int]upstream.RunnerScaleSet, len(runnerScaleSets))
	for _, runnerScaleSet := range runnerScaleSets {
		upstreamByID[runnerScaleSet.ID] = runnerScaleSet
	}
	result := PruneResult{Removed: make([]OwnedRunnerPool, 0, len(ownedRunnerPools))}
	for _, owned := range ownedRunnerPools {
		switch classifyOwnedRunnerPool(runnerGroupID, owned, current, upstreamByID) {
		case retainOwnership:
		case removeOwnership:
			result.Removed = append(result.Removed, owned)
		case deleteAndRemoveOwnership:
			if err := client.deleteOwnedRunnerPool(ctx, owned); err != nil {
				return result, err
			}
			result.Removed = append(result.Removed, owned)
		}
	}
	return result, nil
}

func classifyOwnedRunnerPool(
	runnerGroupID int,
	owned OwnedRunnerPool,
	current map[OwnedRunnerPool]struct{},
	upstreamByID map[int]upstream.RunnerScaleSet,
) pruneAction {
	if owned.RunnerGroupID != runnerGroupID {
		return retainOwnership
	}
	if _, configured := current[owned]; configured {
		return retainOwnership
	}
	runnerScaleSet, exists := upstreamByID[owned.ID]
	if !exists {
		return removeOwnership
	}
	if runnerScaleSet.Name != owned.Name || runnerScaleSet.RunnerGroupID != owned.RunnerGroupID {
		return retainOwnership
	}
	return deleteAndRemoveOwnership
}

func (client *Client) deleteOwnedRunnerPool(ctx context.Context, owned OwnedRunnerPool) error {
	if err := client.registry.DeleteRunnerScaleSet(ctx, owned.ID); err != nil && !errors.Is(err, upstream.NotFoundError) {
		return fmt.Errorf("delete runner scale set %q: %w", owned.Name, err)
	}
	return nil
}
