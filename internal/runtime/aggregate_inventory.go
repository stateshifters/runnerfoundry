package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

// AggregateRunnerInventory provides one fail-closed view of all active runner
// inventories. Instances returned by Inspect retain only transient routing data,
// allowing Destroy to use the backend that observed the runner.
type AggregateRunnerInventory struct {
	routes []inventoryRoute
	cache  *inventorySnapshotCache
}

type inventoryRoute struct {
	inventory   execution.RunnerInventory
	observation *execution.InventoryObservation
}

type inventorySnapshotCache struct {
	mutex     sync.RWMutex
	instances []execution.RunnerInstance
	ready     bool
}

// NewAggregateRunnerInventory combines active inventories and removes duplicate
// endpoint identities while preserving first occurrence order. The aggregate
// is safe for concurrent Inspect and Refresh calls.
func NewAggregateRunnerInventory(inventories ...execution.RunnerInventory) AggregateRunnerInventory {
	routes := make([]inventoryRoute, 0, len(inventories))
	seenIdentities := make(map[string]struct{}, len(inventories))
	for _, inventory := range inventories {
		identity := string(inventory.EndpointID())
		if _, duplicate := seenIdentities[identity]; duplicate {
			continue
		}
		seenIdentities[identity] = struct{}{}
		routes = append(routes, inventoryRoute{
			inventory: inventory, observation: &execution.InventoryObservation{},
		})
	}
	return AggregateRunnerInventory{routes: routes, cache: &inventorySnapshotCache{}}
}

// Inspect returns a defensive copy of the latest successful Refresh snapshot.
// Before the first successful Refresh it queries every active inventory to
// obtain an initial view. Any backend error or duplicate runner name makes the
// complete observation unknown, so no partial result is returned.
func (inventory AggregateRunnerInventory) Inspect(ctx context.Context) ([]execution.RunnerInstance, error) {
	if inventory.cache != nil {
		inventory.cache.mutex.RLock()
		if inventory.cache.ready {
			instances := append([]execution.RunnerInstance(nil), inventory.cache.instances...)
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

func (inventory AggregateRunnerInventory) inspectBackends(ctx context.Context) ([]execution.RunnerInstance, error) {
	instances := make([]execution.RunnerInstance, 0)
	seenRunnerNames := make(map[core.RunnerName]struct{})
	var inspectionErrors []error
	for _, route := range inventory.routes {
		backend := route.inventory
		backendInstances, err := route.refresh(ctx)
		if err != nil {
			inspectionErrors = append(inspectionErrors, err)
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
			instance.InventoryObservation = route.observation
			instances = append(instances, instance)
		}
	}
	if err := errors.Join(inspectionErrors...); err != nil {
		return nil, err
	}
	return instances, nil
}

func (route inventoryRoute) refresh(ctx context.Context) ([]execution.RunnerInstance, error) {
	if err := route.inventory.Refresh(ctx); err != nil {
		return nil, fmt.Errorf("refresh runner inventory: %w", err)
	}
	instances, err := route.inventory.Inspect(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect runner inventory: %w", err)
	}
	return instances, nil
}

// EndpointID returns the synthetic identity of the aggregate boundary.
func (inventory AggregateRunnerInventory) EndpointID() execution.InventoryEndpointID {
	return "aggregate"
}

// KnowsEndpoint reports whether an active child inventory owns endpointID.
func (inventory AggregateRunnerInventory) KnowsEndpoint(endpointID execution.InventoryEndpointID) bool {
	for _, route := range inventory.routes {
		backend := route.inventory
		if backend.KnowsEndpoint(endpointID) {
			return true
		}
	}
	return false
}

func (inventory AggregateRunnerInventory) CanInspectEndpointForMember(
	endpointID execution.InventoryEndpointID,
	memberID core.MemberID,
) bool {
	for _, route := range inventory.routes {
		backend := route.inventory
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
	endpointID execution.InventoryEndpointID,
) ([]execution.RunnerInstance, error) {
	for _, route := range inventory.routes {
		backend := route.inventory
		if !backend.KnowsEndpoint(endpointID) {
			continue
		}
		instances, err := backend.InspectEndpoint(ctx, endpointID)
		if err != nil {
			return nil, fmt.Errorf("inspect endpoint %q: %w", endpointID, err)
		}
		for index := range instances {
			instances[index].EndpointID = endpointID
			instances[index].InventoryObservation = route.observation
		}
		return instances, nil
	}
	return nil, fmt.Errorf("inventory endpoint %q is not configured", endpointID)
}

// Destroy routes an inspected instance back to the inventory that observed it.
func (inventory AggregateRunnerInventory) Destroy(ctx context.Context, instance execution.RunnerInstance) error {
	if instance.InventoryObservation == nil {
		return errors.New("destroy runner: instance has no inventory origin")
	}
	for _, route := range inventory.routes {
		if route.observation != instance.InventoryObservation {
			continue
		}
		instance.InventoryObservation = nil
		if err := route.inventory.Destroy(ctx, instance); err != nil {
			return fmt.Errorf("destroy runner through originating inventory: %w", err)
		}
		return nil
	}
	return errors.New("destroy runner: instance inventory origin is not configured")
}
