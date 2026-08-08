package execution

import (
	"context"
	"testing"

	providerdocker "github.com/meza/RunnerFoundry/internal/provider/docker"
)

func TestDockerComposeServicesOnOneHostShareInventory(t *testing.T) {
	t.Parallel()
	const inventoryOutput = `{"ID":"container-1","State":"running","Labels":"runnerfoundry.runner-pool=linux,runnerfoundry.runner=runnerfoundry-1"}`
	first := NewDockerComposeProvider(
		&recordingDockerTransport{listOutput: inventoryOutput},
		providerdocker.ComposeConfig{ComposeFile: "compose.yaml", Service: "runner"},
		"member-a",
	)
	second := NewDockerComposeProvider(
		&recordingDockerTransport{listOutput: inventoryOutput},
		providerdocker.ComposeConfig{ComposeFile: "compose.yaml", Service: "gpu-runner"},
		"member-a",
	)
	inventory := NewAggregateRunnerInventory(first, second)

	if err := inventory.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh shared Docker inventory: %v", err)
	}
	instances, err := inventory.Inspect(context.Background())
	if err != nil || len(instances) != 1 {
		t.Fatalf("shared Docker inventory = %#v, error = %v", instances, err)
	}
}

func TestDockerInventoryRoutesLegacyProcessingEndpoint(t *testing.T) {
	t.Parallel()
	const inventoryOutput = `{"ID":"container-1","State":"running","Labels":"runnerfoundry.runner-pool=linux,runnerfoundry.runner=runnerfoundry-1"}`
	provider := NewDockerComposeProvider(
		&recordingDockerTransport{listOutput: inventoryOutput},
		providerdocker.ComposeConfig{ComposeFile: "compose.yaml", Service: "runner"},
		"member-a",
	)
	inventory := NewAggregateRunnerInventory(provider)
	legacyEndpoint := InventoryEndpointID("docker-compose:member-a?compose=compose.yaml&service=runner")

	instances, err := inventory.InspectEndpoint(context.Background(), legacyEndpoint)
	if err != nil || len(instances) != 1 || instances[0].EndpointID != legacyEndpoint {
		t.Fatalf("legacy Docker inventory = %#v, error = %v", instances, err)
	}
}
