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

// PruneRunnerPools removes absent registrations only when their settings
// match the fingerprint used by EnsureRunnerPool. It never changes job state.
func (client *Client) PruneRunnerPools(
	ctx context.Context,
	runnerGroupName string,
	desiredRunnerPools []string,
	ownedRunnerPools []OwnedRunnerPool,
) error {
	if client == nil || client.registrations == nil || client.registry == nil {
		return errors.New("GitHub prune APIs are required")
	}
	runnerGroupID, err := client.runnerGroupID(ctx, runnerGroupName)
	if err != nil {
		return err
	}
	runnerScaleSets, err := client.registry.ListRunnerScaleSets(ctx, runnerGroupID)
	if err != nil {
		return fmt.Errorf("list runner scale sets for prune: %w", err)
	}
	desired := desiredRunnerPoolNames(desiredRunnerPools)
	return client.deleteAbsentOwnedRunnerPools(ctx, runnerGroupID, desired, ownedRunnerPools, runnerScaleSets)
}

func desiredRunnerPoolNames(names []string) map[string]struct{} {
	desired := make(map[string]struct{}, len(names))
	for _, name := range names {
		desired[name] = struct{}{}
	}
	return desired
}

func (client *Client) deleteAbsentOwnedRunnerPools(
	ctx context.Context,
	runnerGroupID int,
	desired map[string]struct{},
	ownedRunnerPools []OwnedRunnerPool,
	runnerScaleSets []upstream.RunnerScaleSet,
) error {
	ownedByID := make(map[int]OwnedRunnerPool, len(ownedRunnerPools))
	for _, owned := range ownedRunnerPools {
		if owned.RunnerGroupID == runnerGroupID {
			ownedByID[owned.ID] = owned
		}
	}
	for _, runnerScaleSet := range runnerScaleSets {
		owned, recorded := ownedByID[runnerScaleSet.ID]
		_, configured := desired[runnerScaleSet.Name]
		if configured || !recorded || owned.Name != runnerScaleSet.Name {
			continue
		}
		if err := client.registry.DeleteRunnerScaleSet(ctx, runnerScaleSet.ID); err != nil {
			return fmt.Errorf("delete runner scale set %q: %w", runnerScaleSet.Name, err)
		}
	}
	return nil
}
