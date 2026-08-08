package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/meza/RunnerFoundry/internal/core"
)

// AggregateRunnerInventory provides one fail-closed view of all active runner
// inventories. Instances returned by Inspect retain only transient routing data,
// allowing Destroy to use the backend that observed the runner.
type AggregateRunnerInventory struct {
	inventories []RunnerInventory
	cache       *inventorySnapshotCache
}

type inventorySnapshotCache struct {
	mutex     sync.RWMutex
	instances []RunnerInstance
	ready     bool
}

// NewAggregateRunnerInventory combines active inventories and removes duplicate
// endpoint identities while preserving first occurrence order. The aggregate
// is safe for concurrent Inspect and Refresh calls.
func NewAggregateRunnerInventory(inventories ...RunnerInventory) AggregateRunnerInventory {
	uniqueInventories := make([]RunnerInventory, 0, len(inventories))
	seenIdentities := make(map[string]struct{}, len(inventories))
	for _, inventory := range inventories {
		identity := string(inventory.EndpointID())
		if _, duplicate := seenIdentities[identity]; duplicate {
			continue
		}
		seenIdentities[identity] = struct{}{}
		uniqueInventories = append(uniqueInventories, inventory)
	}
	return AggregateRunnerInventory{inventories: uniqueInventories, cache: &inventorySnapshotCache{}}
}

// Inspect returns a defensive copy of the latest successful Refresh snapshot.
// Before the first successful Refresh it queries every active inventory to
// obtain an initial view. Any backend error or duplicate runner name makes the
// complete observation unknown, so no partial result is returned.
func (inventory AggregateRunnerInventory) Inspect(ctx context.Context) ([]RunnerInstance, error) {
	if inventory.cache != nil {
		inventory.cache.mutex.RLock()
		if inventory.cache.ready {
			instances := append([]RunnerInstance(nil), inventory.cache.instances...)
			inventory.cache.mutex.RUnlock()
			return instances, nil
		}
		inventory.cache.mutex.RUnlock()
	}
	return inventory.inspectBackends(ctx)
}

// Refresh replaces the cycle snapshot atomically after every backend succeeds.
// A failed refresh clears readiness so later Inspect calls retry observation.
func (inventory AggregateRunnerInventory) Refresh(ctx context.Context) error {
	instances, err := inventory.inspectBackends(ctx)
	if inventory.cache == nil {
		return err
	}
	inventory.cache.mutex.Lock()
	defer inventory.cache.mutex.Unlock()
	inventory.cache.ready = err == nil
	inventory.cache.instances = append(inventory.cache.instances[:0], instances...)
	return err
}

func (inventory AggregateRunnerInventory) inspectBackends(ctx context.Context) ([]RunnerInstance, error) {
	instances := make([]RunnerInstance, 0)
	seenRunnerNames := make(map[core.RunnerName]struct{})
	var inspectionErrors []error
	for _, backend := range inventory.inventories {
		backendInstances, err := backend.Inspect(ctx)
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
			instance.EndpointID = backend.EndpointID()
			instance.origin = &runnerInventoryOrigin{inventory: backend}
			instances = append(instances, instance)
		}
	}
	if err := errors.Join(inspectionErrors...); err != nil {
		return nil, err
	}
	return instances, nil
}

// EndpointID returns the synthetic identity of the aggregate boundary.
func (inventory AggregateRunnerInventory) EndpointID() InventoryEndpointID { return "aggregate" }

// KnowsEndpoint reports whether an active child inventory owns endpointID.
func (inventory AggregateRunnerInventory) KnowsEndpoint(endpointID InventoryEndpointID) bool {
	for _, backend := range inventory.inventories {
		if backend.KnowsEndpoint(endpointID) {
			return true
		}
	}
	return false
}

func (inventory AggregateRunnerInventory) CanInspectEndpointForMember(
	endpointID InventoryEndpointID,
	memberID core.MemberID,
) bool {
	for _, backend := range inventory.inventories {
		if backend.CanInspectEndpointForMember(endpointID, memberID) {
			return true
		}
	}
	return false
}

// InspectEndpoint obtains fresh evidence from the matching child inventory and
// returns an error when the endpoint is no longer configured.
func (inventory AggregateRunnerInventory) InspectEndpoint(
	ctx context.Context,
	endpointID InventoryEndpointID,
) ([]RunnerInstance, error) {
	for _, backend := range inventory.inventories {
		if !backend.KnowsEndpoint(endpointID) {
			continue
		}
		instances, err := backend.Inspect(ctx)
		if err != nil {
			return nil, fmt.Errorf("inspect endpoint %q: %w", endpointID, err)
		}
		for index := range instances {
			instances[index].EndpointID = endpointID
			instances[index].origin = &runnerInventoryOrigin{inventory: backend}
		}
		return instances, nil
	}
	return nil, fmt.Errorf("inventory endpoint %q is not configured", endpointID)
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
