package execution

import (
	"context"
	"errors"
	"fmt"
)

type runnerInventoryIdentifier interface {
	runnerInventoryIdentity() string
}

// AggregateRunnerInventory provides one fail-closed view of all active runner
// inventories. Instances returned by Inspect retain only transient routing data,
// allowing Destroy to use the backend that observed the runner.
type AggregateRunnerInventory struct {
	inventories []RunnerInventory
}

// NewAggregateRunnerInventory combines active inventories and removes duplicate
// observations of the same underlying mechanism or endpoint. Inventories that
// do not expose an internal identity remain distinct.
func NewAggregateRunnerInventory(inventories ...RunnerInventory) AggregateRunnerInventory {
	uniqueInventories := make([]RunnerInventory, 0, len(inventories))
	seenIdentities := make(map[string]struct{}, len(inventories))
	for _, inventory := range inventories {
		identifier, identified := inventory.(runnerInventoryIdentifier)
		if identified {
			identity := identifier.runnerInventoryIdentity()
			if _, duplicate := seenIdentities[identity]; duplicate {
				continue
			}
			seenIdentities[identity] = struct{}{}
		}
		uniqueInventories = append(uniqueInventories, inventory)
	}
	return AggregateRunnerInventory{inventories: uniqueInventories}
}

// Inspect queries every active inventory. Any backend error or duplicate runner
// name makes the complete observation unknown, so no partial result is returned.
func (inventory AggregateRunnerInventory) Inspect(
	ctx context.Context,
	runnerPool string,
) ([]RunnerInstance, error) {
	instances := make([]RunnerInstance, 0)
	seenRunnerNames := make(map[string]struct{})
	var inspectionErrors []error
	for _, backend := range inventory.inventories {
		backendInstances, err := backend.Inspect(ctx, runnerPool)
		if err != nil {
			inspectionErrors = append(inspectionErrors, fmt.Errorf("inspect runner inventory: %w", err))
			continue
		}
		for _, instance := range backendInstances {
			if _, duplicate := seenRunnerNames[instance.RunnerName]; duplicate {
				inspectionErrors = append(inspectionErrors, fmt.Errorf(
					"runner name %q was reported by more than one inventory",
					instance.RunnerName,
				))
				continue
			}
			seenRunnerNames[instance.RunnerName] = struct{}{}
			instance.origin = &runnerInventoryOrigin{inventory: backend}
			instances = append(instances, instance)
		}
	}
	if err := errors.Join(inspectionErrors...); err != nil {
		return nil, err
	}
	return instances, nil
}

// Destroy routes an inspected instance back to the inventory that observed it.
// Instances not returned by this aggregate cannot be destroyed through it.
func (inventory AggregateRunnerInventory) Destroy(ctx context.Context, instance RunnerInstance) error {
	if instance.origin == nil {
		return errors.New("destroy runner: instance has no inventory origin")
	}
	origin := instance.origin
	instance.origin = nil
	if err := origin.inventory.Destroy(ctx, instance); err != nil {
		return fmt.Errorf("destroy runner through originating inventory: %w", err)
	}
	return nil
}
