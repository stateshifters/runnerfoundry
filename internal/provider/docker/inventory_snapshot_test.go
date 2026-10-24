package docker

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/execution"
)

const snapshotFirstContainer = `{"ID":"first","State":"running","RunnerPool":"pool","RunnerName":"first"}`
const snapshotSecondContainer = `{"ID":"second","State":"exited","RunnerPool":"pool","RunnerName":"second"}`

func TestInventorySnapshotChangesOnlyAfterRefresh(t *testing.T) {
	commands := &recordingCommandRunner{output: snapshotFirstContainer}
	inventory := NewInventory(commands, "member")
	first := inspectDockerSnapshot(t, inventory)
	first[0].ID = "mutated"
	commands.output = snapshotSecondContainer
	cached := inspectDockerSnapshot(t, inventory)
	if len(cached) != 1 || cached[0].ID != "first" || len(commands.commands) != 1 {
		t.Fatalf("snapshot=%+v calls=%+v", cached, commands.commands)
	}
	copyOfInventory := inventory
	if err := copyOfInventory.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	refreshed := inspectDockerSnapshot(t, inventory)
	if len(refreshed) != 1 || refreshed[0].ID != "second" || len(commands.commands) != 2 {
		t.Fatalf("refreshed=%+v calls=%+v", refreshed, commands.commands)
	}
}

func TestEndpointInspectionRemainsFreshWithoutReplacingCycleSnapshot(t *testing.T) {
	commands := &recordingCommandRunner{output: snapshotFirstContainer}
	inventory := NewInventory(commands, "member")
	initial := inspectDockerSnapshot(t, inventory)
	commands.output = snapshotSecondContainer
	legacy := execution.InventoryEndpointID("docker-compose:member?compose=old.yaml&service=runner")
	fresh, err := inventory.InspectEndpoint(context.Background(), legacy)
	if err != nil || len(fresh) != 1 || fresh[0].ID != "second" || fresh[0].EndpointID != legacy {
		t.Fatalf("endpoint=%+v error=%v", fresh, err)
	}
	if cached := inspectDockerSnapshot(t, inventory); !reflect.DeepEqual(cached, initial) {
		t.Fatalf("endpoint inspection replaced snapshot=%+v", cached)
	}
}

func TestFailedRefreshInvalidatesCompleteDockerSnapshot(t *testing.T) {
	commands := &recordingCommandRunner{output: snapshotFirstContainer}
	inventory := NewInventory(commands, "member")
	inspectDockerSnapshot(t, inventory)
	commands.output = snapshotSecondContainer + "\n{"
	if err := inventory.Refresh(context.Background()); err == nil {
		t.Fatal("partial observation accepted")
	}
	commands.output = snapshotSecondContainer
	if instances, err := inventory.Inspect(context.Background()); err == nil || len(instances) != 0 {
		t.Fatalf("uncertain snapshot=%+v error=%v", instances, err)
	}
	if err := inventory.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if instances := inspectDockerSnapshot(t, inventory); len(instances) != 1 || instances[0].ID != "second" {
		t.Fatalf("recovered=%+v", instances)
	}
}

func TestFailedDockerRemovalUsesFreshAbsenceEvidence(t *testing.T) {
	for _, test := range []struct {
		name        string
		initial     string
		current     string
		wantFailure bool
	}{
		{name: "cached absence cannot confirm removal", current: snapshotFirstContainer, wantFailure: true},
		{name: "cached presence cannot reject removal", initial: snapshotFirstContainer},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := errors.New("removal uncertain")
			commands := &recordingCommandRunner{output: test.initial, removeError: failure}
			inventory := NewInventory(commands, "member")
			inspectDockerSnapshot(t, inventory)
			commands.output = test.current
			err := inventory.Destroy(context.Background(), execution.RunnerInstance{ID: "first", RunnerName: "first"})
			if errors.Is(err, failure) != test.wantFailure {
				t.Fatalf("removal=%v", err)
			}
		})
	}
}

func TestRefreshPublishesEmptyDockerSnapshot(t *testing.T) {
	commands := &recordingCommandRunner{output: snapshotFirstContainer}
	inventory := NewInventory(commands, "member")
	inspectDockerSnapshot(t, inventory)
	commands.output = ""
	if err := inventory.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if instances := inspectDockerSnapshot(t, inventory); len(instances) != 0 {
		t.Fatalf("removed containers remain in snapshot=%+v", instances)
	}
}

func inspectDockerSnapshot(t *testing.T, inventory Inventory) []execution.RunnerInstance {
	t.Helper()
	instances, err := inventory.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return instances
}
