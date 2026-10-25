package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestAggregateRunnerInventoryRefreshesCachedChildrenForEveryCycle(t *testing.T) {
	backend := &snapshotInventoryFake{runnerInventoryFake: runnerInventoryFake{identity: "orchard:controller"}, current: []execution.RunnerInstance{{ID: "first", RunnerName: "runner-1"}}}
	inventory := NewAggregateRunnerInventory(backend)
	initial, err := inventory.Inspect(context.Background())
	if err != nil || len(initial) != 1 || initial[0].ID != "first" {
		t.Fatalf("initial inventory = %+v, %v", initial, err)
	}
	backend.current = []execution.RunnerInstance{{ID: "second", RunnerName: "runner-2"}}
	if err = inventory.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	refreshed, err := inventory.Inspect(context.Background())
	if err != nil || len(refreshed) != 1 || refreshed[0].ID != "second" {
		t.Fatalf("refreshed inventory = %+v, %v", refreshed, err)
	}
	backend.current = []execution.RunnerInstance{{ID: "third", RunnerName: "runner-3"}}
	if err = inventory.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	refreshed, err = inventory.Inspect(context.Background())
	if err != nil || len(refreshed) != 1 || refreshed[0].ID != "third" {
		t.Fatalf("next cycle inventory = %+v, %v", refreshed, err)
	}
}

type snapshotInventoryFake struct {
	runnerInventoryFake
	current    []execution.RunnerInstance
	refreshErr error
}

func (inventory *snapshotInventoryFake) Refresh(context.Context) error {
	if inventory.refreshErr != nil {
		return inventory.refreshErr
	}
	inventory.instances = append([]execution.RunnerInstance(nil), inventory.current...)
	return nil
}

func (inventory *snapshotInventoryFake) InspectEndpoint(_ context.Context, endpoint execution.InventoryEndpointID) ([]execution.RunnerInstance, error) {
	if !inventory.KnowsEndpoint(endpoint) {
		return nil, errors.New("unknown endpoint")
	}
	return append([]execution.RunnerInstance(nil), inventory.current...), nil
}

func TestAggregateRunnerInventoryRecoveryObservesFreshEndpointWithoutChangingCycleSnapshot(t *testing.T) {
	backend := &snapshotInventoryFake{runnerInventoryFake: runnerInventoryFake{identity: "orchard:controller"}, current: []execution.RunnerInstance{{ID: "cycle", RunnerName: "runner-1"}}}
	inventory := NewAggregateRunnerInventory(backend)
	if err := inventory.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.current = []execution.RunnerInstance{{ID: "fresh", RunnerName: "runner-2"}}
	fresh, err := inventory.InspectEndpoint(context.Background(), "orchard:controller")
	if err != nil || len(fresh) != 1 || fresh[0].ID != "fresh" || fresh[0].EndpointID != "orchard:controller" {
		t.Fatalf("recovery inventory = %+v, %v", fresh, err)
	}
	if err = inventory.Destroy(context.Background(), fresh[0]); err != nil {
		t.Fatal(err)
	}
	if len(backend.destroyed) != 1 || backend.destroyed[0].ID != "fresh" || backend.destroyed[0].InventoryObservation != nil {
		t.Fatalf("routed recovery destruction = %+v", backend.destroyed)
	}
	snapshot, err := inventory.Inspect(context.Background())
	if err != nil || len(snapshot) != 1 || snapshot[0].ID != "cycle" {
		t.Fatalf("cycle snapshot = %+v, %v", snapshot, err)
	}
}

func TestAggregateRunnerInventoryRefreshFailureInvalidatesCompleteSnapshot(t *testing.T) {
	first := &snapshotInventoryFake{runnerInventoryFake: runnerInventoryFake{identity: "first"}, current: []execution.RunnerInstance{{RunnerName: "runner-1"}}}
	second := &snapshotInventoryFake{runnerInventoryFake: runnerInventoryFake{identity: "second"}, current: []execution.RunnerInstance{{RunnerName: "runner-2"}}}
	inventory := NewAggregateRunnerInventory(first, second)
	if err := inventory.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	first.refreshErr = errors.New("first endpoint unavailable")
	second.refreshErr = errors.New("second endpoint unavailable")
	err := inventory.Refresh(context.Background())
	if !errors.Is(err, first.refreshErr) || !errors.Is(err, second.refreshErr) {
		t.Fatalf("refresh error = %v", err)
	}
	instances, err := inventory.Inspect(context.Background())
	if instances != nil || !errors.Is(err, first.refreshErr) || !errors.Is(err, second.refreshErr) {
		t.Fatalf("failed cycle snapshot = %+v, %v", instances, err)
	}
	first.refreshErr = nil
	second.refreshErr = nil
	first.current = []execution.RunnerInstance{{RunnerName: "runner-3"}}
	second.current = []execution.RunnerInstance{{RunnerName: "runner-4"}}
	instances, err = inventory.Inspect(context.Background())
	if err != nil || len(instances) != 2 || instances[0].RunnerName != "runner-3" || instances[1].RunnerName != "runner-4" {
		t.Fatalf("recovered cycle snapshot = %+v, %v", instances, err)
	}
}

func TestAggregateRunnerInventoryPublishesCompleteSnapshotsDuringConcurrentRefresh(t *testing.T) {
	first := &snapshotInventoryFake{runnerInventoryFake: runnerInventoryFake{identity: "first"}, current: []execution.RunnerInstance{{ID: "old-first", RunnerName: "runner-1"}}}
	second := &blockingRefreshInventory{snapshotInventoryFake: snapshotInventoryFake{runnerInventoryFake: runnerInventoryFake{identity: "second"}, current: []execution.RunnerInstance{{ID: "old-second", RunnerName: "runner-2"}}}}
	inventory := NewAggregateRunnerInventory(first, second)
	if err := inventory.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	first.current = []execution.RunnerInstance{{ID: "new-first", RunnerName: "runner-3"}}
	second.current = []execution.RunnerInstance{{ID: "new-second", RunnerName: "runner-4"}}
	second.refreshStarted = make(chan struct{})
	second.refreshContinue = make(chan struct{})
	finished := make(chan error, 1)
	go func() { finished <- inventory.Refresh(context.Background()) }()
	<-second.refreshStarted
	before, err := inventory.Inspect(context.Background())
	close(second.refreshContinue)
	refreshErr := <-finished
	assertAggregateInventoryIDs(t, before, err, "old-first", "old-second")
	if refreshErr != nil {
		t.Fatal(refreshErr)
	}
	after, err := inventory.Inspect(context.Background())
	assertAggregateInventoryIDs(t, after, err, "new-first", "new-second")
	after[0].ID = "caller mutation"
	retained, err := inventory.Inspect(context.Background())
	assertAggregateInventoryIDs(t, retained, err, "new-first", "new-second")
}

type blockingRefreshInventory struct {
	snapshotInventoryFake
	refreshStarted  chan struct{}
	refreshContinue chan struct{}
}

func (inventory *blockingRefreshInventory) Refresh(ctx context.Context) error {
	if inventory.refreshStarted != nil {
		close(inventory.refreshStarted)
		<-inventory.refreshContinue
	}
	return inventory.snapshotInventoryFake.Refresh(ctx)
}

func assertAggregateInventoryIDs(t *testing.T, instances []execution.RunnerInstance, err error, want ...string) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	identities := make([]string, 0, len(instances))
	for _, instance := range instances {
		identities = append(identities, instance.ID)
	}
	if !reflect.DeepEqual(identities, want) {
		t.Fatalf("inventory identities = %v, want %v", identities, want)
	}
}
