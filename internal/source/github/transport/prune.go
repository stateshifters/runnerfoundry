package transport

import (
	"context"
	"errors"
	"fmt"

	upstream "github.com/actions/scaleset"
)

// PruneRunnerPools removes absent registrations only when their settings
// match the fingerprint used by EnsureRunnerPool. It never changes job state.
func (client *Client) PruneRunnerPools(
	ctx context.Context,
	runnerGroupName string,
	desiredRunnerPools []string,
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
	return client.deleteAbsentOwnedRunnerPools(ctx, desired, runnerScaleSets)
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
	desired map[string]struct{},
	runnerScaleSets []upstream.RunnerScaleSet,
) error {
	for _, runnerScaleSet := range runnerScaleSets {
		if _, configured := desired[runnerScaleSet.Name]; configured || !ownedRunnerScaleSet(runnerScaleSet) {
			continue
		}
		if err := client.registry.DeleteRunnerScaleSet(ctx, runnerScaleSet.ID); err != nil {
			return fmt.Errorf("delete runner scale set %q: %w", runnerScaleSet.Name, err)
		}
	}
	return nil
}

func ownedRunnerScaleSet(runnerScaleSet upstream.RunnerScaleSet) bool {
	if !runnerScaleSet.RunnerSetting.DisableUpdate || len(runnerScaleSet.Labels) != 1 {
		return false
	}
	label := runnerScaleSet.Labels[0]
	return label.Name == runnerScaleSet.Name && label.Type == "System"
}
