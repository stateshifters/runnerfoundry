package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
)

func TestAggregateRunnerInventoryMergesActiveBackendsAndRoutesDestruction(t *testing.T) {
	dockerInventory := &runnerInventoryFake{
		identity:  "docker-compose",
		instances: []execution.RunnerInstance{{ID: "container-1", RunnerName: "runner-1", State: execution.RunnerStateRunning}},
	}
	libvirtInventory := &runnerInventoryFake{
		identity:  "libvirt:qemu:///system",
		instances: []execution.RunnerInstance{{ID: "domain-2", RunnerName: "runner-2", State: execution.RunnerStateCompleted}},
	}
	inventory := NewAggregateRunnerInventory(dockerInventory, libvirtInventory)

	instances, err := inventory.Inspect(context.Background())
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if len(instances) != 2 {
		t.Fatalf("Inspect() instances = %#v, want two", instances)
	}
	if err = inventory.Destroy(context.Background(), instances[1]); err != nil {
		t.Fatalf("Destroy() error = %v", err)
	}
	if len(dockerInventory.destroyed) != 0 {
		t.Fatalf("Docker destroyed = %#v, want none", dockerInventory.destroyed)
	}
	if len(libvirtInventory.destroyed) != 1 || libvirtInventory.destroyed[0].ID != "domain-2" {
		t.Fatalf("libvirt destroyed = %#v, want domain-2", libvirtInventory.destroyed)
	}
}

func TestAggregateRunnerInventoryFailsClosedAfterInspectingEveryBackend(t *testing.T) {
	expectedErr := errors.New("inventory unavailable")
	failingInventory := &runnerInventoryFake{identity: "docker-compose", inspectErr: expectedErr}
	availableInventory := &runnerInventoryFake{
		identity:  "libvirt:qemu:///system",
		instances: []execution.RunnerInstance{{RunnerName: "runner-1"}},
	}
	inventory := NewAggregateRunnerInventory(failingInventory, availableInventory)

	instances, err := inventory.Inspect(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("Inspect() error = %v, want wrapped %v", err, expectedErr)
	}
	if instances != nil {
		t.Fatalf("Inspect() instances = %#v, want nil", instances)
	}
	if failingInventory.inspections != 1 || availableInventory.inspections != 1 {
		t.Fatalf("inspection counts = %d, %d, want 1, 1", failingInventory.inspections, availableInventory.inspections)
	}
}

func TestAggregateRunnerInventoryRejectsDuplicateRunnerNames(t *testing.T) {
	inventory := NewAggregateRunnerInventory(
		&runnerInventoryFake{identity: "docker-compose", instances: []execution.RunnerInstance{{RunnerName: "runner-1"}}},
		&runnerInventoryFake{identity: "libvirt:qemu:///system", instances: []execution.RunnerInstance{{RunnerName: "runner-1"}}},
	)

	instances, err := inventory.Inspect(context.Background())
	if err == nil || !strings.Contains(err.Error(), `runner name "runner-1"`) {
		t.Fatalf("Inspect() error = %v, want duplicate runner name", err)
	}
	if instances != nil {
		t.Fatalf("Inspect() instances = %#v, want nil", instances)
	}
}

func TestAggregateRunnerInventoryDeduplicatesEquivalentBackends(t *testing.T) {
	firstDockerInventory := &runnerInventoryFake{identity: "docker-compose"}
	secondDockerInventory := &runnerInventoryFake{identity: "docker-compose"}
	firstLibvirtInventory := &runnerInventoryFake{identity: "libvirt:qemu:///system"}
	secondLibvirtInventory := &runnerInventoryFake{identity: "libvirt:qemu:///system"}
	inventory := NewAggregateRunnerInventory(
		firstDockerInventory,
		secondDockerInventory,
		firstLibvirtInventory,
		secondLibvirtInventory,
	)

	if _, err := inventory.Inspect(context.Background()); err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if firstDockerInventory.inspections != 1 || secondDockerInventory.inspections != 0 {
		t.Fatalf("Docker inspection counts = %d, %d, want 1, 0", firstDockerInventory.inspections, secondDockerInventory.inspections)
	}
	if firstLibvirtInventory.inspections != 1 || secondLibvirtInventory.inspections != 0 {
		t.Fatalf("libvirt inspection counts = %d, %d, want 1, 0", firstLibvirtInventory.inspections, secondLibvirtInventory.inspections)
	}
}

func TestAggregateRunnerInventoryRejectsDestructionWithoutObservedOrigin(t *testing.T) {
	inventory := NewAggregateRunnerInventory()

	err := inventory.Destroy(context.Background(), execution.RunnerInstance{RunnerName: "runner-1"})
	if err == nil || !strings.Contains(err.Error(), "no inventory origin") {
		t.Fatalf("Destroy() error = %v, want missing origin", err)
	}
	err = inventory.Destroy(context.Background(), execution.RunnerInstance{
		RunnerName: "runner-1", InventoryObservation: &execution.InventoryObservation{},
	})
	if err == nil || !strings.Contains(err.Error(), "is not configured") {
		t.Fatalf("Destroy() error = %v, want unavailable origin", err)
	}
}

func TestAggregateRunnerInventoryPreservesOriginDestroyFailure(t *testing.T) {
	expectedErr := errors.New("destroy failed")
	backend := &runnerInventoryFake{
		identity:   "docker-compose",
		instances:  []execution.RunnerInstance{{RunnerName: "runner-1"}},
		destroyErr: expectedErr,
	}
	inventory := NewAggregateRunnerInventory(backend)
	instances, err := inventory.Inspect(context.Background())
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}

	err = inventory.Destroy(context.Background(), instances[0])
	if !errors.Is(err, expectedErr) {
		t.Fatalf("Destroy() error = %v, want wrapped %v", err, expectedErr)
	}
	if backend.destroyed[0].InventoryObservation != nil {
		t.Fatal("origin routing data leaked into backend destruction")
	}
}

func TestAggregateRunnerInventoryRefreshesAndReusesCycleSnapshot(t *testing.T) {
	backend := &runnerInventoryFake{identity: "docker:member", instances: []execution.RunnerInstance{{
		RunnerName: "runner-1", RunnerPool: "linux",
	}}}
	inventory := NewAggregateRunnerInventory(backend)
	if err := inventory.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	first, err := inventory.Inspect(context.Background())
	if err != nil || len(first) != 1 || first[0].EndpointID != "docker:member" || backend.inspections != 1 {
		t.Fatalf("snapshot = (%#v, %v), inspections=%d", first, err, backend.inspections)
	}
	if _, err = inventory.Inspect(context.Background()); err != nil || backend.inspections != 1 {
		t.Fatalf("cached inspection error=%v calls=%d", err, backend.inspections)
	}
	if inventory.EndpointID() != "aggregate" || !inventory.KnowsEndpoint("docker:member") ||
		inventory.KnowsEndpoint("unknown") {
		t.Fatal("aggregate endpoint identity is inconsistent")
	}
	instances, err := inventory.InspectEndpoint(context.Background(), "docker:member")
	if err != nil || len(instances) != 1 || backend.inspections != 2 {
		t.Fatalf("targeted inspection = (%#v, %v), calls=%d", instances, err, backend.inspections)
	}
	if _, err := inventory.InspectEndpoint(context.Background(), "unknown"); err == nil {
		t.Fatal("unknown targeted endpoint was accepted")
	}
}

func TestAggregateRunnerInventoryDelegatesEndpointInspectability(t *testing.T) {
	inventory := NewAggregateRunnerInventory(&runnerInventoryFake{identity: "docker:member"})
	if !inventory.CanInspectEndpointForMember("docker:member", "member") {
		t.Fatal("configured endpoint was not inspectable")
	}
	if inventory.CanInspectEndpointForMember("unknown", "member") {
		t.Fatal("unknown endpoint was inspectable")
	}
}

func TestAggregateRunnerInventoryRefreshFailsClosed(t *testing.T) {
	wantErr := errors.New("offline")
	inventory := NewAggregateRunnerInventory(&runnerInventoryFake{identity: "docker:member", inspectErr: wantErr})
	if err := inventory.Refresh(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Refresh() error = %v", err)
	}
}

func TestAggregateRunnerInventoryRefreshWithoutCacheReturnsInspectionFailure(t *testing.T) {
	wantErr := errors.New("offline")
	inventory := NewAggregateRunnerInventory(
		&runnerInventoryFake{identity: "docker:member", inspectErr: wantErr},
	)
	inventory.cache = nil

	if err := inventory.Refresh(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Refresh() error = %v, want wrapped %v", err, wantErr)
	}
}

func TestAggregateRunnerInventoryInspectEndpointReturnsBackendFailure(t *testing.T) {
	wantErr := errors.New("offline")
	inventory := NewAggregateRunnerInventory(
		&runnerInventoryFake{identity: "docker:member", inspectErr: wantErr},
	)

	instances, err := inventory.InspectEndpoint(context.Background(), "docker:member")
	if !errors.Is(err, wantErr) {
		t.Fatalf("InspectEndpoint() error = %v, want wrapped %v", err, wantErr)
	}
	if instances != nil {
		t.Fatalf("InspectEndpoint() instances = %#v, want nil", instances)
	}
}

type runnerInventoryFake struct {
	identity    string
	instances   []execution.RunnerInstance
	inspectErr  error
	inspections int
	destroyed   []execution.RunnerInstance
	destroyErr  error
}

func (inventory *runnerInventoryFake) EndpointID() execution.InventoryEndpointID {
	return execution.InventoryEndpointID(inventory.identity)
}
func (inventory *runnerInventoryFake) KnowsEndpoint(endpointID execution.InventoryEndpointID) bool {
	return inventory.EndpointID() == endpointID
}

func (inventory *runnerInventoryFake) CanInspectEndpointForMember(
	endpointID execution.InventoryEndpointID,
	_ core.MemberID,
) bool {
	return inventory.KnowsEndpoint(endpointID)
}
func (inventory *runnerInventoryFake) Refresh(context.Context) error { return nil }
func (inventory *runnerInventoryFake) InspectEndpoint(ctx context.Context, endpointID execution.InventoryEndpointID) ([]execution.RunnerInstance, error) {
	if !inventory.KnowsEndpoint(endpointID) {
		return nil, errors.New("unknown endpoint")
	}
	return inventory.Inspect(ctx)
}

func (inventory *runnerInventoryFake) Inspect(context.Context) ([]execution.RunnerInstance, error) {
	inventory.inspections++
	return append([]execution.RunnerInstance(nil), inventory.instances...), inventory.inspectErr
}

func (inventory *runnerInventoryFake) Destroy(_ context.Context, instance execution.RunnerInstance) error {
	inventory.destroyed = append(inventory.destroyed, instance)
	return inventory.destroyErr
}
